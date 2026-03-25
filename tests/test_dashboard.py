"""Tests for the toolserver web dashboard and chat API."""

from __future__ import annotations

from unittest import mock

import pytest

from toolserver.dashboard import TOOLS, render_dashboard


# ---------------------------------------------------------------------------
# render_dashboard – pure function
# ---------------------------------------------------------------------------

class TestRenderDashboard:
    def test_returns_non_empty_html(self) -> None:
        html = render_dashboard()
        assert isinstance(html, str)
        assert len(html) > 500

    def test_contains_doctype_and_closing_html(self) -> None:
        html = render_dashboard()
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html

    def test_contains_title(self) -> None:
        html = render_dashboard()
        assert "Ollama Local Agent" in html

    def test_contains_all_tool_names(self) -> None:
        html = render_dashboard()
        for name, _, _ in TOOLS:
            assert name in html, f"Tool '{name}' missing from dashboard"

    def test_contains_api_docs_link(self) -> None:
        html = render_dashboard()
        assert 'href="/docs"' in html

    def test_contains_openapi_link(self) -> None:
        html = render_dashboard()
        assert 'href="/openapi.json"' in html

    def test_contains_chat_input(self) -> None:
        """Chat input field is present."""
        html = render_dashboard()
        assert 'id="chat-input"' in html

    def test_contains_send_button(self) -> None:
        html = render_dashboard()
        assert "sendMessage" in html

    def test_contains_model_selector(self) -> None:
        html = render_dashboard()
        assert 'id="model-select"' in html

    def test_contains_welcome_message(self) -> None:
        html = render_dashboard()
        assert "你好" in html

    def test_contains_example_buttons(self) -> None:
        html = render_dashboard()
        assert "sendExample" in html

    def test_tool_count_displayed(self) -> None:
        html = render_dashboard()
        assert f"可用工具 ({len(TOOLS)})" in html

    def test_calls_stream_api(self) -> None:
        """JavaScript references the streaming endpoint."""
        html = render_dashboard()
        assert "/api/chat/stream" in html

    def test_calls_reset_api(self) -> None:
        """JavaScript references the chat reset endpoint."""
        html = render_dashboard()
        assert "/api/chat/reset" in html

    def test_contains_stop_button(self) -> None:
        """Stop button for task cancellation is present."""
        html = render_dashboard()
        assert 'id="stop-btn"' in html
        assert "cancelTask" in html

    def test_contains_progress_step_styles(self) -> None:
        """Progress step CSS is present for step-by-step display."""
        html = render_dashboard()
        assert "progress-step" in html

    def test_calls_cancel_api(self) -> None:
        """JavaScript references the cancel endpoint."""
        html = render_dashboard()
        assert "/api/chat/cancel" in html

    def test_handles_sse_events(self) -> None:
        """JavaScript has handleSSE function for streaming events."""
        html = render_dashboard()
        assert "handleSSE" in html

    def test_step_detail_toggle(self) -> None:
        """Toggle function for expanding tool call details."""
        html = render_dashboard()
        assert "toggleDetail" in html


# ---------------------------------------------------------------------------
# TOOLS metadata integrity
# ---------------------------------------------------------------------------

class TestToolsMetadata:
    def test_all_entries_are_triples(self) -> None:
        for entry in TOOLS:
            assert len(entry) == 3

    def test_endpoints_start_with_post(self) -> None:
        for name, endpoint, _ in TOOLS:
            assert endpoint.startswith("POST /tool/"), f"{name}: bad endpoint"

    def test_no_duplicate_names(self) -> None:
        names = [name for name, _, _ in TOOLS]
        assert len(names) == len(set(names))


# ---------------------------------------------------------------------------
# Chat API models
# ---------------------------------------------------------------------------

class TestChatModels:
    """Test the Pydantic request/response models for chat API."""

    def test_chat_in_defaults(self) -> None:
        from toolserver.server import ChatIn
        inp = ChatIn(message="hi")
        assert inp.message == "hi"
        assert inp.model == ""
        assert inp.session_id == "default"

    def test_chat_out_fields(self) -> None:
        from toolserver.server import ChatOut
        out = ChatOut(reply="ok", model="m", session_id="s")
        assert out.reply == "ok"
        assert out.tool_calls == []
        assert out.error is None


# ---------------------------------------------------------------------------
# Cancel mechanism (unit tests, no HTTP needed)
# ---------------------------------------------------------------------------

class TestCancelMechanism:
    """Test the cancel request set used by the streaming endpoint."""

    def test_cancel_adds_to_set(self) -> None:
        from toolserver.server import _CANCEL_REQUESTS, _CHAT_LOCK
        with _CHAT_LOCK:
            _CANCEL_REQUESTS.add("test-cancel-123")
        assert "test-cancel-123" in _CANCEL_REQUESTS
        with _CHAT_LOCK:
            _CANCEL_REQUESTS.discard("test-cancel-123")

    def test_cancel_discard_is_safe(self) -> None:
        from toolserver.server import _CANCEL_REQUESTS, _CHAT_LOCK
        with _CHAT_LOCK:
            _CANCEL_REQUESTS.discard("nonexistent-session")


# ---------------------------------------------------------------------------
# FastAPI endpoint integration (via TestClient)
# ---------------------------------------------------------------------------

class TestDashboardEndpoint:
    """Test the GET / route returns a proper HTML response."""

    @pytest.fixture(autouse=True)
    def _setup_client(self) -> None:
        try:
            from starlette.testclient import TestClient
        except (ImportError, RuntimeError):
            pytest.skip("starlette TestClient not available (install httpx)")

        # Patch Ollama calls that happen at import time inside server.py
        with mock.patch("agent.ollama_client.list_models", return_value=[]):
            from toolserver.server import app
        self.client = TestClient(app)

    def test_root_returns_200(self) -> None:
        resp = self.client.get("/")
        assert resp.status_code == 200

    def test_root_is_html(self) -> None:
        resp = self.client.get("/")
        assert "text/html" in resp.headers.get("content-type", "")

    def test_root_contains_title(self) -> None:
        resp = self.client.get("/")
        assert "Ollama Local Agent" in resp.text

    def test_docs_still_accessible(self) -> None:
        resp = self.client.get("/docs")
        assert resp.status_code == 200

    def test_chat_models_endpoint(self) -> None:
        with mock.patch("toolserver.server._list_ollama_models", return_value=[
            {"name": "test-model", "size": 100},
        ]):
            resp = self.client.get("/api/chat/models")
        assert resp.status_code == 200
        data = resp.json()
        assert "models" in data
        assert "test-model" in data["models"]

    def test_chat_reset_endpoint(self) -> None:
        resp = self.client.post("/api/chat/reset?session_id=test-sess")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_chat_cancel_endpoint(self) -> None:
        resp = self.client.post("/api/chat/cancel?session_id=test-cancel")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_chat_stream_endpoint_exists(self) -> None:
        """Verify the streaming endpoint responds (even with Ollama down)."""
        with mock.patch(
            "toolserver.server.ollama_chat",
            side_effect=Exception("test skip"),
        ):
            resp = self.client.post(
                "/api/chat/stream",
                json={"message": "test", "session_id": "stream-test"},
            )
        assert resp.status_code == 200
        assert "text/event-stream" in resp.headers.get("content-type", "")
