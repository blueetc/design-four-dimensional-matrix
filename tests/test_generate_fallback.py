"""Tests for the /api/chat → /api/generate automatic fallback."""

from __future__ import annotations

from unittest import mock

import pytest
import requests

from agent.ollama_client import (
    OllamaConnectionError,
    _generate_fallback,
    _messages_to_prompt,
    ollama_chat,
)


# ---------------------------------------------------------------------------
# _messages_to_prompt
# ---------------------------------------------------------------------------

class TestMessagesToPrompt:
    def test_separates_system_from_others(self) -> None:
        msgs = [
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "Hi"},
            {"role": "assistant", "content": "Hello"},
        ]
        system, prompt = _messages_to_prompt(msgs)
        assert "You are helpful." in system
        assert "User: Hi" in prompt
        assert "Assistant: Hello" in prompt
        # system text must NOT leak into prompt
        assert "You are helpful." not in prompt

    def test_empty_messages(self) -> None:
        system, prompt = _messages_to_prompt([])
        assert system == ""
        assert prompt == ""

    def test_only_system(self) -> None:
        msgs = [
            {"role": "system", "content": "sys1"},
            {"role": "system", "content": "sys2"},
        ]
        system, prompt = _messages_to_prompt(msgs)
        assert "sys1" in system
        assert "sys2" in system
        assert prompt == ""

    def test_only_user(self) -> None:
        msgs = [{"role": "user", "content": "question"}]
        system, prompt = _messages_to_prompt(msgs)
        assert system == ""
        assert "User: question" in prompt


# ---------------------------------------------------------------------------
# _generate_fallback – normalised response shape
# ---------------------------------------------------------------------------

class TestGenerateFallback:
    @mock.patch("agent.ollama_client.requests.post")
    def test_returns_chat_shaped_response(self, mock_post: mock.Mock) -> None:
        mock_post.return_value = mock.Mock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {"model": "m", "response": "ok"},
        )
        result = _generate_fallback("m", [{"role": "user", "content": "hi"}], 0.2, 60)
        assert result["message"]["role"] == "assistant"
        assert result["message"]["content"] == "ok"
        assert result["model"] == "m"

    @mock.patch("agent.ollama_client.requests.post")
    def test_sends_system_when_present(self, mock_post: mock.Mock) -> None:
        mock_post.return_value = mock.Mock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {"model": "m", "response": "ok"},
        )
        msgs = [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "q"},
        ]
        _generate_fallback("m", msgs, 0.2, 60)
        sent_json = mock_post.call_args.kwargs.get("json") or mock_post.call_args[1].get("json")
        assert "system" in sent_json
        assert sent_json["system"] == "be brief"


# ---------------------------------------------------------------------------
# ollama_chat – 404 triggers fallback
# ---------------------------------------------------------------------------

class TestOllamaChatFallback:
    @mock.patch("agent.ollama_client._generate_fallback")
    @mock.patch("agent.ollama_client.requests.post")
    def test_fallback_on_404(
        self, mock_post: mock.Mock, mock_fallback: mock.Mock,
    ) -> None:
        """When /api/chat returns 404, fallback is invoked."""
        mock_post.return_value = mock.Mock(status_code=404)
        mock_fallback.return_value = {
            "model": "m",
            "message": {"role": "assistant", "content": "from generate"},
        }
        result = ollama_chat("m", [{"role": "user", "content": "hi"}])
        mock_fallback.assert_called_once()
        assert result["message"]["content"] == "from generate"

    @mock.patch("agent.ollama_client.requests.post")
    def test_no_fallback_on_200(self, mock_post: mock.Mock) -> None:
        """Normal 200 from /api/chat should be returned directly."""
        mock_post.return_value = mock.Mock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {"message": {"role": "assistant", "content": "chat ok"}},
        )
        result = ollama_chat("m", [{"role": "user", "content": "hi"}])
        assert result["message"]["content"] == "chat ok"

    @mock.patch("agent.ollama_client.requests.post")
    def test_connection_error_still_raises(self, mock_post: mock.Mock) -> None:
        mock_post.side_effect = requests.ConnectionError("refused")
        with pytest.raises(OllamaConnectionError, match="无法连接"):
            ollama_chat("m", [{"role": "user", "content": "hi"}])

    @mock.patch("agent.ollama_client.requests.post")
    def test_timeout_still_raises(self, mock_post: mock.Mock) -> None:
        mock_post.side_effect = requests.Timeout("slow")
        with pytest.raises(OllamaConnectionError, match="超时"):
            ollama_chat("m", [{"role": "user", "content": "hi"}])

    @mock.patch("agent.ollama_client.requests.post")
    def test_other_http_errors_propagate(self, mock_post: mock.Mock) -> None:
        """Non-404 HTTP errors should still raise normally."""
        resp_mock = mock.Mock(status_code=500)
        resp_mock.raise_for_status.side_effect = requests.HTTPError("server error")
        mock_post.return_value = resp_mock
        with pytest.raises(requests.HTTPError):
            ollama_chat("m", [{"role": "user", "content": "hi"}])
