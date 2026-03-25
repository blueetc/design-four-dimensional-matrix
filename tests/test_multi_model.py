"""Tests for multi-model support: model listing, switching, /ask, /panel."""

from __future__ import annotations

import json
from io import StringIO
from unittest import mock

import pytest

from agent.main import (
    DEFAULT_PANEL_MODELS,
    _format_model_list,
    _handle_slash_command,
    _init_messages,
    _resolve_panel_models,
    run,
    run_interactive,
)
from agent.ollama_client import list_models


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAKE_MODELS = [
    {"name": "qwen2.5:32b", "size": 19 * 1024**3, "modified_at": "2025-01-01"},
    {"name": "llama3.3:70b", "size": 42 * 1024**3, "modified_at": "2025-01-01"},
    {"name": "deepseek-r1:32b", "size": 19 * 1024**3, "modified_at": "2025-01-01"},
    {"name": "nomic-embed-text:latest", "size": 274 * 1024**2, "modified_at": "2025-01-01"},
    {"name": "gpt-oss:20b-cloud", "size": 0, "modified_at": "2025-01-01"},
]


def _fake_ollama_response(content: str) -> dict:
    return {"message": {"content": content}}


# ---------------------------------------------------------------------------
# list_models
# ---------------------------------------------------------------------------

class TestListModels:
    @mock.patch("agent.ollama_client.requests.get")
    def test_returns_models(self, mock_get: mock.Mock) -> None:
        mock_get.return_value.json.return_value = {"models": FAKE_MODELS}
        mock_get.return_value.raise_for_status = mock.Mock()
        result = list_models()
        assert len(result) == 5
        assert result[0]["name"] == "qwen2.5:32b"

    @mock.patch("agent.ollama_client.requests.get")
    def test_returns_empty_on_connection_error(self, mock_get: mock.Mock) -> None:
        import requests as req
        mock_get.side_effect = req.ConnectionError("refused")
        result = list_models()
        assert result == []

    @mock.patch("agent.ollama_client.requests.get")
    def test_returns_empty_on_timeout(self, mock_get: mock.Mock) -> None:
        import requests as req
        mock_get.side_effect = req.Timeout("timed out")
        result = list_models()
        assert result == []


# ---------------------------------------------------------------------------
# _format_model_list
# ---------------------------------------------------------------------------

class TestFormatModelList:
    def test_formats_models(self) -> None:
        output = _format_model_list(FAKE_MODELS)
        assert "qwen2.5:32b" in output
        assert "llama3.3:70b" in output
        assert "274 MB" in output
        assert "cloud" in output  # gpt-oss with size=0

    def test_empty_list(self) -> None:
        output = _format_model_list([])
        assert "无法连接" in output

    def test_gb_formatting(self) -> None:
        models = [{"name": "big:model", "size": 42 * 1024**3}]
        output = _format_model_list(models)
        assert "42.0 GB" in output

    def test_mb_formatting(self) -> None:
        models = [{"name": "small:model", "size": 500 * 1024**2}]
        output = _format_model_list(models)
        assert "500 MB" in output


# ---------------------------------------------------------------------------
# _resolve_panel_models
# ---------------------------------------------------------------------------

class TestResolvePanelModels:
    def test_uses_explicit_panel(self) -> None:
        result = _resolve_panel_models(["a", "b"], "default")
        assert result == ["a", "b"]

    @mock.patch("agent.main.list_models")
    def test_auto_discovers_from_ollama(self, mock_lm: mock.Mock) -> None:
        mock_lm.return_value = FAKE_MODELS
        result = _resolve_panel_models([], "default")
        assert len(result) == 5
        assert result[0] == "qwen2.5:32b"

    @mock.patch("agent.main.list_models")
    def test_falls_back_to_active(self, mock_lm: mock.Mock) -> None:
        mock_lm.return_value = []
        result = _resolve_panel_models([], "fallback:7b")
        assert result == ["fallback:7b"]


# ---------------------------------------------------------------------------
# _handle_slash_command
# ---------------------------------------------------------------------------

class TestHandleSlashCommand:
    def _call(self, cmd, model="qwen2.5:7b", messages=None, panel=None):
        if messages is None:
            messages = _init_messages()
        if panel is None:
            panel = []
        return _handle_slash_command(
            cmd,
            active_model=model,
            messages=messages,
            panel_models=panel,
        )

    @mock.patch("agent.main.list_models", return_value=FAKE_MODELS)
    def test_models_command(self, mock_lm: mock.Mock) -> None:
        model, msgs, panel, handled = self._call("/models")
        assert handled is True

    def test_model_show_current(self) -> None:
        model, msgs, panel, handled = self._call("/model")
        assert handled is True
        assert model == "qwen2.5:7b"

    def test_model_switch(self) -> None:
        model, msgs, panel, handled = self._call("/model llama3.3:70b")
        assert handled is True
        assert model == "llama3.3:70b"

    @mock.patch("agent.main.run")
    def test_ask_command(self, mock_run: mock.Mock) -> None:
        msgs = _init_messages()
        mock_run.return_value = msgs + [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
        ]
        model, returned_msgs, panel, handled = self._call(
            "/ask deepseek-r1:32b 你好", messages=msgs,
        )
        assert handled is True
        assert model == "qwen2.5:7b"  # active model unchanged
        mock_run.assert_called_once_with("你好", model="deepseek-r1:32b", messages=msgs)

    def test_ask_no_args(self) -> None:
        model, msgs, panel, handled = self._call("/ask")
        assert handled is True  # prints usage, no crash

    @mock.patch("agent.main.ollama_chat")
    @mock.patch("agent.main._resolve_panel_models")
    def test_panel_command(
        self, mock_resolve: mock.Mock, mock_chat: mock.Mock,
    ) -> None:
        mock_resolve.return_value = ["a:7b", "b:7b"]
        mock_chat.return_value = _fake_ollama_response("answer")
        msgs = _init_messages()
        model, returned_msgs, panel, handled = self._call(
            "/panel 什么是AI？", messages=msgs,
        )
        assert handled is True
        assert mock_chat.call_count == 2
        # Messages should contain panel Q&A for both models
        panel_msgs = [m for m in returned_msgs if "[panel:" in m.get("content", "") or m.get("content", "").startswith("[")]
        assert len(panel_msgs) >= 2

    def test_panel_show_empty(self) -> None:
        model, msgs, panel, handled = self._call("/panel")
        assert handled is True

    def test_panel_add(self) -> None:
        panel = []
        model, msgs, panel, handled = self._call(
            "/panel+ llama3.3:70b", panel=panel,
        )
        assert handled is True
        assert "llama3.3:70b" in panel

    def test_panel_add_no_duplicates(self) -> None:
        panel = ["llama3.3:70b"]
        model, msgs, panel, handled = self._call(
            "/panel+ llama3.3:70b", panel=panel,
        )
        assert panel.count("llama3.3:70b") == 1

    def test_panel_remove(self) -> None:
        panel = ["llama3.3:70b", "qwen2.5:32b"]
        model, msgs, panel, handled = self._call(
            "/panel- llama3.3:70b", panel=panel,
        )
        assert "llama3.3:70b" not in panel
        assert "qwen2.5:32b" in panel

    def test_help_command(self) -> None:
        model, msgs, panel, handled = self._call("/help")
        assert handled is True

    def test_unknown_slash_not_handled(self) -> None:
        model, msgs, panel, handled = self._call("/unknown_cmd")
        assert handled is False


# ---------------------------------------------------------------------------
# REPL integration with slash commands
# ---------------------------------------------------------------------------

class TestReplWithSlashCommands:
    @mock.patch("agent.main.list_models", return_value=FAKE_MODELS)
    @mock.patch("builtins.input", side_effect=["/models", "exit"])
    def test_models_in_repl(
        self, mock_input: mock.Mock, mock_lm: mock.Mock,
    ) -> None:
        """``/models`` is handled without calling the LLM."""
        with mock.patch("agent.main.run") as mock_run:
            run_interactive()
            mock_run.assert_not_called()

    @mock.patch("agent.main.run")
    @mock.patch("builtins.input", side_effect=["/model llama3.3:70b", "你好", "quit"])
    def test_model_switch_in_repl(
        self, mock_input: mock.Mock, mock_run: mock.Mock,
    ) -> None:
        """``/model`` switches, then next input uses the new model."""
        base = _init_messages()
        mock_run.return_value = base + [
            {"role": "user", "content": "你好"},
            {"role": "assistant", "content": "hi"},
        ]
        run_interactive()
        # run() should have been called with the switched model
        assert mock_run.call_count == 1
        call_kwargs = mock_run.call_args
        assert call_kwargs[1]["model"] == "llama3.3:70b" or call_kwargs[0][1] == "llama3.3:70b"

    @mock.patch("agent.main.run")
    @mock.patch("builtins.input", side_effect=["/help", "退出"])
    def test_help_in_repl(
        self, mock_input: mock.Mock, mock_run: mock.Mock,
    ) -> None:
        """``/help`` is handled without calling the LLM."""
        run_interactive()
        mock_run.assert_not_called()

    @mock.patch("agent.main.ollama_chat")
    @mock.patch("agent.main._resolve_panel_models", return_value=["m1", "m2"])
    @mock.patch("builtins.input", side_effect=["/panel 测试问题", "exit"])
    def test_panel_in_repl(
        self, mock_input: mock.Mock, mock_resolve: mock.Mock,
        mock_chat: mock.Mock,
    ) -> None:
        mock_chat.return_value = _fake_ollama_response("回答")
        run_interactive()
        assert mock_chat.call_count == 2


# ---------------------------------------------------------------------------
# Panel with chat failure
# ---------------------------------------------------------------------------

class TestPanelFailure:
    @mock.patch("agent.main.ollama_chat")
    @mock.patch("agent.main._resolve_panel_models")
    def test_panel_handles_model_failure(
        self, mock_resolve: mock.Mock, mock_chat: mock.Mock,
    ) -> None:
        """If one model in the panel fails, others still respond."""
        mock_resolve.return_value = ["good:7b", "bad:7b"]
        mock_chat.side_effect = [
            _fake_ollama_response("好的回答"),
            Exception("connection refused"),
        ]
        msgs = _init_messages()
        model, returned_msgs, panel, handled = _handle_slash_command(
            "/panel 问题",
            active_model="default",
            messages=msgs,
            panel_models=[],
        )
        assert handled is True
        # Should have messages from both attempts
        contents = [m["content"] for m in returned_msgs if m["role"] == "assistant"]
        assert any("好的回答" in c for c in contents)
        assert any("调用失败" in c for c in contents)
