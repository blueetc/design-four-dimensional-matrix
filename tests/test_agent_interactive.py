"""Tests for the interactive agent loop (continuous tasks + follow-ups)."""

from __future__ import annotations

import json
from unittest import mock

import pytest

from agent.main import (
    _EXIT_COMMANDS,
    _init_messages,
    _try_parse_tool_call,
    run,
    run_interactive,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_ollama_response(content: str) -> dict:
    """Construct a minimal Ollama-style chat response."""
    return {"message": {"content": content}}


def _fake_tool_response(**kwargs: object) -> dict:
    return {"ok": True, "tool": "mock", "result": kwargs, "error": None}


# ---------------------------------------------------------------------------
# _try_parse_tool_call
# ---------------------------------------------------------------------------

class TestTryParseToolCall:
    def test_valid_tool_call(self) -> None:
        text = json.dumps({"tool": "read_file", "args": {"path": "a.txt"}})
        assert _try_parse_tool_call(text) == {
            "tool": "read_file",
            "args": {"path": "a.txt"},
        }

    def test_returns_none_for_plain_text(self) -> None:
        assert _try_parse_tool_call("hello world") is None

    def test_returns_none_for_json_without_tool_key(self) -> None:
        assert _try_parse_tool_call('{"key": "value"}') is None

    def test_returns_none_for_invalid_json(self) -> None:
        assert _try_parse_tool_call("{not json}") is None


# ---------------------------------------------------------------------------
# _init_messages
# ---------------------------------------------------------------------------

class TestInitMessages:
    def test_creates_two_system_messages(self) -> None:
        msgs = _init_messages()
        assert len(msgs) == 3
        assert all(m["role"] == "system" for m in msgs)


# ---------------------------------------------------------------------------
# run – single turn
# ---------------------------------------------------------------------------

class TestRunSingleTurn:
    @mock.patch("agent.main.ollama_chat")
    def test_natural_language_response(self, mock_chat: mock.Mock) -> None:
        """Model answers with plain text → printed and returned."""
        mock_chat.return_value = _fake_ollama_response("任务完成。")
        msgs = run("做个测试")
        # messages should contain system(3) + user(1) + assistant(1)
        assert len(msgs) == 5
        assert msgs[-1]["role"] == "assistant"
        assert msgs[-1]["content"] == "任务完成。"

    @mock.patch("agent.main.call_tool")
    @mock.patch("agent.main.ollama_chat")
    def test_tool_call_then_response(
        self, mock_chat: mock.Mock, mock_tool: mock.Mock,
    ) -> None:
        """Model calls a tool, then answers with plain text."""
        tool_call = json.dumps({"tool": "read_file", "args": {"path": "a.txt"}})
        mock_chat.side_effect = [
            _fake_ollama_response(tool_call),
            _fake_ollama_response("文件内容是 hello。"),
        ]
        mock_tool.return_value = _fake_tool_response(content="hello")

        msgs = run("读取 a.txt")
        # system(3) + user(1) + assistant(tool call) + assistant(tool result) + assistant(NL)
        assert len(msgs) == 7
        assert msgs[-1]["content"] == "文件内容是 hello。"

    @mock.patch("agent.main.ollama_chat")
    def test_unknown_tool_breaks(self, mock_chat: mock.Mock) -> None:
        """Requesting an unknown tool should stop the loop."""
        bad_call = json.dumps({"tool": "delete_everything", "args": {}})
        mock_chat.return_value = _fake_ollama_response(bad_call)
        msgs = run("坏任务")
        # system(3) + user(1) + assistant error msg(1)
        assert len(msgs) == 5
        assert "Unknown tool" in msgs[-1]["content"]


# ---------------------------------------------------------------------------
# run – multi-turn (conversation history preserved)
# ---------------------------------------------------------------------------

class TestRunMultiTurn:
    @mock.patch("agent.main.ollama_chat")
    def test_follow_up_preserves_history(self, mock_chat: mock.Mock) -> None:
        """Two successive run() calls share the same messages list."""
        mock_chat.return_value = _fake_ollama_response("第一轮完成。")
        msgs = run("第一个任务")
        assert len(msgs) == 5  # 3 sys + 1 user + 1 assistant

        mock_chat.return_value = _fake_ollama_response("第二轮完成。")
        msgs = run("追问", messages=msgs)
        # previous 5 + new user(1) + new assistant(1)
        assert len(msgs) == 7
        assert msgs[-2]["role"] == "user"
        assert msgs[-2]["content"] == "追问"
        assert msgs[-1]["content"] == "第二轮完成。"

    @mock.patch("agent.main.ollama_chat")
    def test_three_turns(self, mock_chat: mock.Mock) -> None:
        """Three consecutive turns keep accumulating context."""
        mock_chat.return_value = _fake_ollama_response("r1")
        msgs = run("t1")
        mock_chat.return_value = _fake_ollama_response("r2")
        msgs = run("t2", messages=msgs)
        mock_chat.return_value = _fake_ollama_response("r3")
        msgs = run("t3", messages=msgs)
        assert len(msgs) == 3 + 3 * 2  # 3 system + 3*(user+assistant)


# ---------------------------------------------------------------------------
# run_interactive
# ---------------------------------------------------------------------------

class TestRunInteractive:
    @mock.patch("agent.main.run")
    @mock.patch("builtins.input", side_effect=["你好", "exit"])
    def test_basic_repl(
        self, mock_input: mock.Mock, mock_run: mock.Mock,
    ) -> None:
        """REPL processes one task then exits on 'exit'."""
        mock_run.return_value = _init_messages() + [
            {"role": "user", "content": "你好"},
            {"role": "assistant", "content": "你好！"},
        ]
        run_interactive()
        assert mock_run.call_count == 1

    @mock.patch("agent.main.run")
    @mock.patch("builtins.input", side_effect=["任务1", "追问", "quit"])
    def test_multi_turn_repl(
        self, mock_input: mock.Mock, mock_run: mock.Mock,
    ) -> None:
        """REPL handles two tasks before quitting."""
        base = _init_messages()
        mock_run.side_effect = [
            base + [
                {"role": "user", "content": "任务1"},
                {"role": "assistant", "content": "done1"},
            ],
            base + [
                {"role": "user", "content": "任务1"},
                {"role": "assistant", "content": "done1"},
                {"role": "user", "content": "追问"},
                {"role": "assistant", "content": "done2"},
            ],
        ]
        run_interactive()
        assert mock_run.call_count == 2

    @mock.patch("agent.main.run")
    @mock.patch("builtins.input", side_effect=EOFError)
    def test_eof_exits_gracefully(
        self, mock_input: mock.Mock, mock_run: mock.Mock,
    ) -> None:
        """Ctrl-D exits without traceback."""
        run_interactive()  # should not raise
        mock_run.assert_not_called()

    @mock.patch("agent.main.run")
    @mock.patch("builtins.input", side_effect=KeyboardInterrupt)
    def test_ctrl_c_exits_gracefully(
        self, mock_input: mock.Mock, mock_run: mock.Mock,
    ) -> None:
        """Ctrl-C exits without traceback."""
        run_interactive()  # should not raise
        mock_run.assert_not_called()

    @mock.patch("agent.main.run")
    @mock.patch("builtins.input", side_effect=["", "  ", "退出"])
    def test_blank_input_skipped(
        self, mock_input: mock.Mock, mock_run: mock.Mock,
    ) -> None:
        """Empty / whitespace-only input is ignored."""
        run_interactive()
        mock_run.assert_not_called()

    def test_all_exit_commands_recognised(self) -> None:
        """Verify the exit-command set covers expected keywords."""
        expected = {"exit", "quit", "bye", "q", "退出", "结束"}
        assert _EXIT_COMMANDS == expected
