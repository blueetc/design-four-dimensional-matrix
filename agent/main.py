"""Conversation loop: model → tool call → tool server → result → model.

Supports both single-shot execution and interactive REPL mode with
conversation history preserved across follow-up questions.

Multi-model support:
- ``/models``            — list locally available Ollama models
- ``/model <name>``      — switch the active model mid-conversation
- ``/ask <model> <msg>`` — one-shot question to a different model
- ``/panel <msg>``       — ask all (or selected) models the same question
- ``/orch <task>``       — orchestrate: director model delegates to workers
"""

from __future__ import annotations

import json
import sys

import requests

from .ollama_client import list_models, ollama_chat
from .prompts import DEV_PROMPT, KNOWLEDGE_PROMPT, ORCHESTRATOR_PROMPT, SYSTEM_PROMPT

TOOLSERVER = "http://127.0.0.1:7331"


def _try_parse_tool_call(text: str) -> dict | None:
    """Return a parsed tool-call dict if *text* is a valid JSON tool invocation."""
    text = text.strip()
    if not (text.startswith("{") and text.endswith("}")):
        return None
    try:
        obj = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    if isinstance(obj, dict) and "tool" in obj:
        return obj
    return None


TOOLS = {
    "get_system_info": "/tool/get_system_info",
    "run_command": "/tool/run_command",
    "read_file": "/tool/read_file",
    "write_file": "/tool/write_file",
    "list_dir": "/tool/list_dir",
    "stat": "/tool/stat",
    "db_schema": "/tool/db_schema",
    "db_query": "/tool/db_query",
    "db_exec": "/tool/db_exec",
    # Wide-table pipeline
    "analyze_fields": "/tool/analyze_fields",
    "design_wide_table": "/tool/design_wide_table",
    "create_wide_table": "/tool/create_wide_table",
    "etl_to_wide_table": "/tool/etl_to_wide_table",
    "visualize_3d": "/tool/visualize_3d",
    # Model discovery
    "list_models": "/tool/list_models",
}

MAX_ITERATIONS = 50

# Exit commands recognised by the interactive REPL.
_EXIT_COMMANDS = frozenset({"exit", "quit", "bye", "q", "退出", "结束"})

# Default set of panel models.  Users override via ``/panel+ model``
# and ``/panel- model`` during a session.
DEFAULT_PANEL_MODELS: list[str] = []


def call_tool(tool: str, args: dict) -> dict:
    """Forward a tool invocation to the local tool server."""
    url = TOOLSERVER + TOOLS[tool]
    resp = requests.post(url, json=args, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _init_messages() -> list[dict]:
    """Create the initial system-prompt messages list."""
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "system", "content": KNOWLEDGE_PROMPT},
        {"role": "system", "content": DEV_PROMPT},
    ]


# ---------------------------------------------------------------------------
# Model helpers
# ---------------------------------------------------------------------------

def _format_model_list(models: list[dict]) -> str:
    """Pretty-print a list of Ollama models for terminal output."""
    if not models:
        return "(无法连接 Ollama 或没有可用模型)"
    lines: list[str] = []
    for m in models:
        name = m.get("name", "?")
        size_bytes = m.get("size", 0)
        if size_bytes > 0:
            size_gb = size_bytes / (1024 ** 3)
            size_str = f"{size_gb:.1f} GB" if size_gb >= 1 else f"{size_bytes / (1024**2):.0f} MB"
        else:
            size_str = "cloud"
        lines.append(f"  {name:<35s} {size_str}")
    return "\n".join(lines)


def _resolve_panel_models(
    panel_models: list[str],
    active_model: str,
) -> list[str]:
    """Return the list of models to use for a panel discussion.

    If *panel_models* is empty, tries to auto-discover from Ollama and
    falls back to just the *active_model*.
    """
    if panel_models:
        return list(panel_models)
    available = list_models()
    if available:
        return [m["name"] for m in available]
    return [active_model]


# ---------------------------------------------------------------------------
# Core agent loop
# ---------------------------------------------------------------------------

def run(
    task: str,
    model: str = "qwen2.5:7b",
    messages: list[dict] | None = None,
) -> list[dict]:
    """Run the agent loop for a single *task*.

    Parameters
    ----------
    task:
        The user instruction / follow-up question.
    model:
        Ollama model tag.
    messages:
        Optional conversation history.  When provided the new *task* is
        appended and the model sees the full prior context, enabling
        follow-up questions that reference earlier results.  When *None*
        a fresh conversation is started.

    Returns
    -------
    list[dict]
        The updated messages list so callers can feed it back for the
        next turn.
    """
    if messages is None:
        messages = _init_messages()

    messages.append({"role": "user", "content": task})

    for _ in range(MAX_ITERATIONS):
        resp = ollama_chat(model=model, messages=messages)
        content: str = resp["message"]["content"].strip()

        # Convention: a bare JSON object with a "tool" key means "call this tool"
        call = _try_parse_tool_call(content)
        if call is not None:
            tool_name = call["tool"]
            args = call.get("args", {})

            if tool_name not in TOOLS:
                err = f"Unknown tool requested: {tool_name}"
                print(err)
                messages.append({"role": "assistant", "content": err})
                break

            tool_result = call_tool(tool_name, args)
            messages.append({"role": "assistant", "content": content})
            messages.append({
                "role": "assistant",
                "content": f"[tool result] {json.dumps(tool_result, ensure_ascii=False)}",
            })
            continue

        # Natural-language response – print and finish this turn.
        print(content)
        messages.append({"role": "assistant", "content": content})
        break

    return messages


# ---------------------------------------------------------------------------
# Multi-model orchestration
# ---------------------------------------------------------------------------

MAX_ORCH_ROUNDS = 15


def _try_parse_orchestrator_action(text: str) -> dict | None:
    """Parse an orchestrator JSON action from *text*.

    Valid actions have an ``"action"`` key with value
    ``"delegate"``, ``"broadcast"``, or ``"finish"``.
    """
    text = text.strip()
    if not (text.startswith("{") and text.endswith("}")):
        return None
    try:
        obj = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    if isinstance(obj, dict) and "action" in obj:
        return obj
    return None


def run_orchestrate(
    task: str,
    director_model: str = "qwen2.5:7b",
    worker_models: list[str] | None = None,
    max_rounds: int = MAX_ORCH_ROUNDS,
) -> dict:
    """Run multi-model orchestration: a director delegates to workers.

    Parameters
    ----------
    task:
        The high-level task from the user.
    director_model:
        Ollama model used as the orchestrator / director.
    worker_models:
        List of worker model names.  Auto-discovered if *None*.
    max_rounds:
        Maximum delegate/broadcast rounds before forcing finish.

    Returns
    -------
    dict
        ``{"summary": "...", "rounds": [...], "round_count": N}``
        where each round records the director action and worker responses.
    """
    if worker_models is None:
        available = list_models()
        worker_models = [m["name"] for m in available] if available else [director_model]

    model_list_str = "\n".join(f"- {m}" for m in worker_models)
    orch_prompt = ORCHESTRATOR_PROMPT.format(
        available_models=model_list_str,
        max_rounds=max_rounds,
    )

    messages: list[dict] = [{"role": "system", "content": orch_prompt}]
    messages.append({"role": "user", "content": f"用户任务：{task}"})

    rounds: list[dict] = []

    for round_idx in range(max_rounds):
        resp = ollama_chat(model=director_model, messages=messages)
        director_text: str = resp["message"]["content"].strip()
        messages.append({"role": "assistant", "content": director_text})

        action = _try_parse_orchestrator_action(director_text)

        if action is None:
            # Director gave plain text — treat as implicit finish
            print(f"[指挥官] {director_text}")
            rounds.append({"action": "text", "content": director_text})
            return {
                "summary": director_text,
                "rounds": rounds,
                "round_count": round_idx + 1,
            }

        act_type = action.get("action", "")

        # ---- finish ----
        if act_type == "finish":
            summary = action.get("summary", "（无汇总）")
            print(f"\n=== 编排完成（{round_idx + 1} 轮） ===")
            print(summary)
            rounds.append({"action": "finish", "summary": summary})
            return {
                "summary": summary,
                "rounds": rounds,
                "round_count": round_idx + 1,
            }

        # ---- delegate ----
        if act_type == "delegate":
            target = action.get("model", worker_models[0])
            subtask = action.get("subtask", "")
            print(f"[指挥官 → {target}] {subtask}")

            try:
                worker_resp = ollama_chat(
                    model=target,
                    messages=[{"role": "user", "content": subtask}],
                )
                worker_text = worker_resp["message"]["content"].strip()
            except Exception as exc:
                worker_text = f"(工作模型 {target} 调用失败: {exc})"

            print(f"[{target}] {worker_text[:200]}{'...' if len(worker_text) > 200 else ''}\n")
            feedback = f"[worker:{target}] {worker_text}"
            messages.append({"role": "user", "content": feedback})
            rounds.append({
                "action": "delegate",
                "model": target,
                "subtask": subtask,
                "response": worker_text,
            })
            continue

        # ---- broadcast ----
        if act_type == "broadcast":
            question = action.get("question", "")
            print(f"[指挥官 → 广播] {question}")
            broadcast_results: list[dict] = []
            all_feedback: list[str] = []

            for wm in worker_models:
                try:
                    worker_resp = ollama_chat(
                        model=wm,
                        messages=[{"role": "user", "content": question}],
                    )
                    worker_text = worker_resp["message"]["content"].strip()
                except Exception as exc:
                    worker_text = f"(工作模型 {wm} 调用失败: {exc})"
                print(f"  [{wm}] {worker_text[:200]}{'...' if len(worker_text) > 200 else ''}")
                broadcast_results.append({"model": wm, "response": worker_text})
                all_feedback.append(f"[worker:{wm}] {worker_text}")

            print()
            feedback = "\n".join(all_feedback)
            messages.append({"role": "user", "content": feedback})
            rounds.append({
                "action": "broadcast",
                "question": question,
                "responses": broadcast_results,
            })
            continue

        # Unknown action — treat as plain text
        print(f"[指挥官] {director_text}")
        rounds.append({"action": "unknown", "content": director_text})

    # Exhausted rounds
    print(f"\n=== 编排结束（达到最大 {max_rounds} 轮） ===")
    return {
        "summary": "(达到最大轮数，未能完成汇总)",
        "rounds": rounds,
        "round_count": max_rounds,
    }


# ---------------------------------------------------------------------------
# Slash-command dispatcher (used by the interactive REPL)
# ---------------------------------------------------------------------------

def _handle_slash_command(
    cmd: str,
    *,
    active_model: str,
    messages: list[dict],
    panel_models: list[str],
) -> tuple[str, list[dict], list[str], bool]:
    """Process a ``/``-prefixed command typed in the REPL.

    Returns ``(active_model, messages, panel_models, handled)`` where
    *handled* is ``True`` when the input was consumed by this function
    (so the caller should NOT forward it to the LLM).
    """
    parts = cmd.split(None, 1)
    verb = parts[0].lower()
    rest = parts[1].strip() if len(parts) > 1 else ""

    # /models – list available models
    if verb == "/models":
        models = list_models()
        print(_format_model_list(models))
        return active_model, messages, panel_models, True

    # /model <name> – switch the active model
    if verb == "/model":
        if not rest:
            print(f"当前模型: {active_model}")
        else:
            active_model = rest
            print(f"已切换模型 → {active_model}")
        return active_model, messages, panel_models, True

    # /ask <model> <question> – one-shot to a different model
    if verb == "/ask":
        ask_parts = rest.split(None, 1)
        if len(ask_parts) < 2:
            print("用法: /ask <模型名> <问题>")
            return active_model, messages, panel_models, True
        tmp_model, question = ask_parts
        print(f"[{tmp_model}]")
        messages = run(question, model=tmp_model, messages=messages)
        return active_model, messages, panel_models, True

    # /panel <question> – ask every panel model
    if verb == "/panel":
        if not rest:
            if panel_models:
                print("Panel 模型: " + ", ".join(panel_models))
            else:
                print("Panel 为空，将自动使用所有本地模型。")
            return active_model, messages, panel_models, True
        models_to_ask = _resolve_panel_models(panel_models, active_model)
        print(f"=== Panel 讨论 ({len(models_to_ask)} 个模型) ===\n")
        for m in models_to_ask:
            print(f"--- [{m}] ---")
            # Panel Q&A is appended to the shared conversation so the
            # user can reference all model answers in subsequent turns.
            messages.append({"role": "user", "content": f"[panel:{m}] {rest}"})
            try:
                resp = ollama_chat(model=m, messages=messages)
                content = resp["message"]["content"].strip()
            except Exception as exc:
                content = f"(模型 {m} 调用失败: {exc})"
            print(content)
            messages.append({
                "role": "assistant",
                "content": f"[{m}] {content}",
            })
            print()
        print("=== Panel 结束 ===")
        return active_model, messages, panel_models, True

    # /panel+ <model> – add model to panel list
    if verb == "/panel+":
        if rest and rest not in panel_models:
            panel_models.append(rest)
            print(f"已添加 {rest} 到 Panel → {panel_models}")
        return active_model, messages, panel_models, True

    # /panel- <model> – remove model from panel list
    if verb == "/panel-":
        if rest in panel_models:
            panel_models.remove(rest)
            print(f"已移除 {rest} → {panel_models}")
        return active_model, messages, panel_models, True

    # /orch <task> – multi-model orchestration
    if verb == "/orch":
        if not rest:
            print("用法: /orch <任务描述>")
            return active_model, messages, panel_models, True
        workers = _resolve_panel_models(panel_models, active_model)
        result = run_orchestrate(
            task=rest,
            director_model=active_model,
            worker_models=workers,
        )
        # Record the orchestration result in the shared conversation
        messages.append({"role": "user", "content": f"[orch] {rest}"})
        messages.append({
            "role": "assistant",
            "content": f"[编排结果 ({result['round_count']}轮)] {result['summary']}",
        })
        return active_model, messages, panel_models, True

    # /knowledge – display the built-in knowledge summary
    if verb == "/knowledge":
        print(KNOWLEDGE_PROMPT)
        return active_model, messages, panel_models, True

    # /help
    if verb == "/help":
        print(
            "可用命令:\n"
            "  /models              列出本地可用模型\n"
            "  /model [名称]        查看或切换当前模型\n"
            "  /ask <模型> <问题>   向指定模型提问（单次）\n"
            "  /panel <问题>        向所有 Panel 模型提问\n"
            "  /panel+ <模型>       添加模型到 Panel\n"
            "  /panel- <模型>       从 Panel 移除模型\n"
            "  /orch <任务>         多模型编排（指挥官模式）\n"
            "  /knowledge           查看内置知识库\n"
            "  /help                显示此帮助\n"
            "  exit/quit/q/退出     退出"
        )
        return active_model, messages, panel_models, True

    return active_model, messages, panel_models, False


# ---------------------------------------------------------------------------
# Interactive REPL
# ---------------------------------------------------------------------------

def run_interactive(model: str = "qwen2.5:7b") -> None:
    """Start an interactive REPL that keeps conversation history.

    The user can type follow-up questions or new tasks.  The full
    conversation context is preserved so the model can reference prior
    tool results and answers.

    Slash commands for multi-model support:

    - ``/models``              — list locally available Ollama models
    - ``/model <name>``        — switch the active model
    - ``/ask <model> <msg>``   — one-shot question to another model
    - ``/panel <msg>``         — ask all panel models the same question
    - ``/panel+ <model>``      — add a model to the panel
    - ``/panel- <model>``      — remove a model from the panel
    - ``/orch <task>``         — orchestrate: director delegates to workers
    - ``/help``                — show available commands

    Type ``exit``, ``quit``, ``q``, ``bye``, ``退出``, or ``结束`` to
    leave the session.  Press :kbd:`Ctrl-C` or :kbd:`Ctrl-D` at any
    time to exit immediately.
    """
    messages: list[dict] = _init_messages()
    panel_models: list[str] = list(DEFAULT_PANEL_MODELS)

    print("=== 本地自动化代理（交互模式） ===")
    print(f"模型: {model}")
    print("输入任务或追问，输入 /help 查看命令，exit/quit/q/退出/结束 退出。\n")

    while True:
        try:
            user_input = input("You> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n再见！")
            break

        if not user_input:
            continue
        if user_input.lower() in _EXIT_COMMANDS:
            print("再见！")
            break

        # Handle slash commands
        if user_input.startswith("/"):
            model, messages, panel_models, handled = _handle_slash_command(
                user_input,
                active_model=model,
                messages=messages,
                panel_models=panel_models,
            )
            if handled:
                print()
                continue

        messages = run(user_input, model=model, messages=messages)
        print()  # blank line between turns


def main() -> None:
    """CLI entry-point.

    Usage::

        # Single-shot (original behaviour)
        python -m agent.main "创建 hello.txt 并读取"

        # Interactive REPL with follow-up support
        python -m agent.main -i
        python -m agent.main --interactive
        python -m agent.main --interactive --model qwen2.5:14b
    """
    args = sys.argv[1:]

    # Parse flags
    interactive = False
    model = "qwen2.5:7b"
    positional: list[str] = []

    i = 0
    while i < len(args):
        if args[i] in ("-i", "--interactive"):
            interactive = True
        elif args[i] in ("-m", "--model") and i + 1 < len(args):
            i += 1
            model = args[i]
        else:
            positional.append(args[i])
        i += 1

    if interactive:
        run_interactive(model=model)
    else:
        task = " ".join(positional) if positional else (
            "在workspace里创建一个hello.txt，内容为Hello from agent，然后读取并显示它。"
        )
        run(task, model=model)


if __name__ == "__main__":
    main()
