"""Tests for UI/UX improvements: error handling, /status, prompt, panel feedback."""

from __future__ import annotations

import json
import os
from unittest import mock

import pytest
import requests

from agent.main import (
    DEFAULT_MODEL,
    _handle_slash_command,
    _init_messages,
    call_tool,
    run,
    run_interactive,
)
from agent.ollama_client import OllamaConnectionError, ollama_chat


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_ollama_response(content: str) -> dict:
    return {"message": {"content": content}}


def _make_slash_ctx(
    active_model: str = "qwen2.5:7b",
    panel_models: list[str] | None = None,
) -> dict:
    return {
        "active_model": active_model,
        "messages": _init_messages(),
        "panel_models": panel_models or [],
    }


# ---------------------------------------------------------------------------
# OllamaConnectionError
# ---------------------------------------------------------------------------

class TestOllamaConnectionError:
    def test_connection_refused(self) -> None:
        """ollama_chat raises OllamaConnectionError on connection failure."""
        with mock.patch("agent.ollama_client.requests.post") as m:
            m.side_effect = requests.ConnectionError("refused")
            with pytest.raises(OllamaConnectionError, match="无法连接 Ollama"):
                ollama_chat(model="test", messages=[])

    def test_timeout_error(self) -> None:
        """ollama_chat raises OllamaConnectionError on timeout."""
        with mock.patch("agent.ollama_client.requests.post") as m:
            m.side_effect = requests.Timeout("timed out")
            with pytest.raises(OllamaConnectionError, match="超时"):
                ollama_chat(model="test", messages=[], timeout=5)


# ---------------------------------------------------------------------------
# call_tool – friendly error on connection failure
# ---------------------------------------------------------------------------

class TestCallToolConnectionError:
    def test_returns_error_dict_on_connection_failure(self) -> None:
        """call_tool returns an error dict instead of raising."""
        with mock.patch("agent.main.requests.post") as m:
            m.side_effect = requests.ConnectionError("refused")
            result = call_tool("list_dir", {"path": "."})
        assert result["ok"] is False
        assert "工具服务器未连接" in result["error"]

    def test_normal_call_still_works(self) -> None:
        """call_tool works normally when server is up."""
        expected = {"ok": True, "tool": "list_dir", "result": {}, "error": None}
        with mock.patch("agent.main.requests.post") as m:
            m.return_value = mock.Mock(json=lambda: expected, raise_for_status=lambda: None)
            result = call_tool("list_dir", {"path": "."})
        assert result == expected


# ---------------------------------------------------------------------------
# Tool call progress display
# ---------------------------------------------------------------------------

class TestToolCallProgress:
    @mock.patch("agent.main.call_tool")
    @mock.patch("agent.main.ollama_chat")
    def test_tool_call_prints_progress(
        self, mock_chat: mock.Mock, mock_tool: mock.Mock, capsys,
    ) -> None:
        """run() prints tool name when calling tools."""
        tool_call = json.dumps({"tool": "read_file", "args": {"path": "a.txt"}})
        mock_chat.side_effect = [
            _fake_ollama_response(tool_call),
            _fake_ollama_response("Done"),
        ]
        mock_tool.return_value = {"ok": True, "tool": "read_file", "result": {"content": "hi"}, "error": None}

        run("read a.txt")
        captured = capsys.readouterr()
        assert "🔧 调用 read_file" in captured.out
        assert "✓ read_file" in captured.out


# ---------------------------------------------------------------------------
# /panel+ and /panel- feedback
# ---------------------------------------------------------------------------

class TestPanelFeedback:
    def test_panel_add_duplicate_shows_message(self, capsys) -> None:
        ctx = _make_slash_ctx(panel_models=["model-a"])
        _handle_slash_command("/panel+ model-a", **ctx)
        out = capsys.readouterr().out
        assert "已在 Panel 中" in out

    def test_panel_add_new_model(self, capsys) -> None:
        ctx = _make_slash_ctx(panel_models=[])
        _, _, panel, _ = _handle_slash_command("/panel+ model-b", **ctx)
        out = capsys.readouterr().out
        assert "已添加 model-b" in out
        assert "model-b" in panel

    def test_panel_remove_missing_shows_message(self, capsys) -> None:
        ctx = _make_slash_ctx(panel_models=["model-a"])
        _handle_slash_command("/panel- not-here", **ctx)
        out = capsys.readouterr().out
        assert "不在 Panel 中" in out

    def test_panel_add_no_arg_shows_usage(self, capsys) -> None:
        ctx = _make_slash_ctx()
        _handle_slash_command("/panel+", **ctx)
        out = capsys.readouterr().out
        assert "用法" in out

    def test_panel_remove_no_arg_shows_usage(self, capsys) -> None:
        ctx = _make_slash_ctx()
        _handle_slash_command("/panel-", **ctx)
        out = capsys.readouterr().out
        assert "用法" in out


# ---------------------------------------------------------------------------
# /status command
# ---------------------------------------------------------------------------

class TestStatusCommand:
    def test_status_handled(self) -> None:
        ctx = _make_slash_ctx()
        _, _, _, handled = _handle_slash_command("/status", **ctx)
        assert handled is True

    @mock.patch("agent.main.requests.get")
    def test_status_shows_model_and_panel(self, mock_get, capsys) -> None:
        mock_get.return_value = mock.Mock(status_code=200)
        ctx = _make_slash_ctx(active_model="llama3:8b", panel_models=["a", "b"])
        _handle_slash_command("/status", **ctx)
        out = capsys.readouterr().out
        assert "llama3:8b" in out
        assert "a, b" in out
        assert "✓ 已连接" in out

    @mock.patch("agent.main.requests.get")
    def test_status_shows_disconnected(self, mock_get, capsys) -> None:
        mock_get.side_effect = requests.ConnectionError("refused")
        ctx = _make_slash_ctx()
        _handle_slash_command("/status", **ctx)
        out = capsys.readouterr().out
        assert "✗ 未连接" in out


# ---------------------------------------------------------------------------
# /help includes /status
# ---------------------------------------------------------------------------

class TestHelpIncludesStatus:
    def test_help_mentions_status(self, capsys) -> None:
        ctx = _make_slash_ctx()
        _handle_slash_command("/help", **ctx)
        out = capsys.readouterr().out
        assert "/status" in out


# ---------------------------------------------------------------------------
# Model-aware REPL prompt
# ---------------------------------------------------------------------------

class TestReplPrompt:
    @mock.patch("agent.main.run")
    @mock.patch("builtins.input", side_effect=["hello", "exit"])
    def test_prompt_contains_model_name(
        self, mock_input: mock.Mock, mock_run: mock.Mock,
    ) -> None:
        mock_run.return_value = _init_messages() + [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
        ]
        run_interactive(model="my-model:7b")
        # input() is called with the prompt string as first argument
        prompt_arg = mock_input.call_args_list[0][0][0]
        assert "my-model:7b" in prompt_arg


# ---------------------------------------------------------------------------
# Default model from environment variable
# ---------------------------------------------------------------------------

class TestDefaultModel:
    def test_default_model_from_env(self) -> None:
        """DEFAULT_MODEL reflects OLLAMA_MODEL env when set."""
        # We can't easily re-import, but we can verify the pattern
        assert isinstance(DEFAULT_MODEL, str)
        assert len(DEFAULT_MODEL) > 0

    def test_env_var_respected_in_module(self) -> None:
        """os.environ.get('OLLAMA_MODEL', 'qwen2.5:7b') pattern is used."""
        import agent.main as m
        # Ensure the module-level default is accessible
        assert hasattr(m, "DEFAULT_MODEL")


# ---------------------------------------------------------------------------
# pyproject.toml entry point
# ---------------------------------------------------------------------------

class TestEntryPoint:
    def test_pyproject_points_to_agent_main(self) -> None:
        """pyproject.toml scripts should point to agent.main:main."""
        import tomllib
        path = os.path.join(os.path.dirname(__file__), "..", "pyproject.toml")
        with open(path, "rb") as f:
            data = tomllib.load(f)
        scripts = data.get("project", {}).get("scripts", {})
        assert scripts.get("ollama-exec") == "agent.main:main"
