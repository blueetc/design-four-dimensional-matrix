"""Conversation loop: model → tool call → tool server → result → model."""

from __future__ import annotations

import json
import sys

import requests

from .ollama_client import ollama_chat
from .prompts import DEV_PROMPT, SYSTEM_PROMPT

TOOLSERVER = "http://127.0.0.1:7331"

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


def call_tool(tool: str, args: dict) -> dict:
    """Forward a tool invocation to the local tool server."""
    url = TOOLSERVER + TOOLS[tool]
    resp = requests.post(url, json=args, timeout=60)
    resp.raise_for_status()
    return resp.json()


def run(task: str, model: str = "qwen2.5:7b") -> None:
    """Run the agent loop for a given *task* string."""
    messages: list[dict] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "system", "content": DEV_PROMPT},
        {"role": "user", "content": task},
    ]

    for _ in range(MAX_ITERATIONS):
        resp = ollama_chat(model=model, messages=messages)
        content: str = resp["message"]["content"].strip()

        # Convention: a bare JSON object means "call this tool"
        if content.startswith("{") and content.endswith("}"):
            try:
                call = json.loads(content)
            except json.JSONDecodeError:
                print(content)
                break

            tool = call.get("tool")
            args = call.get("args", {})

            if tool not in TOOLS:
                print(f"Unknown tool requested: {tool}")
                break

            tool_result = call_tool(tool, args)
            messages.append({"role": "assistant", "content": content})
            messages.append({"role": "tool", "content": json.dumps(tool_result, ensure_ascii=False)})
            continue

        # Otherwise the model produced a natural-language response – print and stop.
        print(content)
        break


def main() -> None:
    """CLI entry-point: pass the task as the first argument or use the default demo task."""
    task = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else (
        "在workspace里创建一个hello.txt，内容为Hello from agent，然后读取并显示它。"
    )
    run(task)


if __name__ == "__main__":
    main()
