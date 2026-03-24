"""Conversation loop: model → tool call → tool server → result → model.

Supports both single-shot execution and interactive REPL mode with
conversation history preserved across follow-up questions.
"""

from __future__ import annotations

import json
import sys

import requests

from .ollama_client import ollama_chat
from .prompts import DEV_PROMPT, SYSTEM_PROMPT

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
}

MAX_ITERATIONS = 50

# Exit commands recognised by the interactive REPL.
_EXIT_COMMANDS = frozenset({"exit", "quit", "bye", "q", "退出", "结束"})


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
        {"role": "system", "content": DEV_PROMPT},
    ]


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


def run_interactive(model: str = "qwen2.5:7b") -> None:
    """Start an interactive REPL that keeps conversation history.

    The user can type follow-up questions or new tasks.  The full
    conversation context is preserved so the model can reference prior
    tool results and answers.

    Type ``exit``, ``quit``, ``q``, ``bye``, ``退出``, or ``结束`` to
    leave the session.  Press :kbd:`Ctrl-C` or :kbd:`Ctrl-D` at any
    time to exit immediately.
    """
    messages: list[dict] = _init_messages()

    print("=== 本地自动化代理（交互模式） ===")
    print(f"模型: {model}")
    print("输入任务或追问，输入 exit/quit/q/退出/结束 退出。\n")

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
