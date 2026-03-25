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

    # --- Professional UX features ---

    def test_contains_theme_toggle(self) -> None:
        """Light/dark theme toggle button is present."""
        html = render_dashboard()
        assert "toggleTheme" in html
        assert 'id="theme-btn"' in html

    def test_contains_light_theme_css(self) -> None:
        """Light theme CSS variables are defined."""
        html = render_dashboard()
        assert "html.light" in html

    def test_contains_keyboard_shortcuts_overlay(self) -> None:
        """Keyboard shortcuts help overlay is present."""
        html = render_dashboard()
        assert 'id="shortcut-overlay"' in html
        assert "toggleShortcuts" in html

    def test_contains_ctrl_k_shortcut(self) -> None:
        """Ctrl+K focus shortcut is implemented."""
        html = render_dashboard()
        assert 'e.key === "k"' in html

    def test_contains_input_history(self) -> None:
        """Input history (up/down arrow) is implemented."""
        html = render_dashboard()
        assert "inputHistory" in html
        assert "historyIdx" in html

    def test_contains_message_copy_button(self) -> None:
        """Copy-to-clipboard action on messages is present."""
        html = render_dashboard()
        assert "navigator.clipboard" in html
        assert "msg-action" in html

    def test_contains_retry_on_error(self) -> None:
        """Retry button for failed messages is present."""
        html = render_dashboard()
        assert "retryText" in html
        assert "🔄" in html

    def test_contains_response_time(self) -> None:
        """Response time display (elapsed) is present."""
        html = render_dashboard()
        assert "formatElapsed" in html
        assert "elapsed" in html

    def test_contains_aria_attributes(self) -> None:
        """Accessibility: ARIA attributes are present."""
        html = render_dashboard()
        assert 'aria-label' in html
        assert 'aria-live="polite"' in html
        assert 'role="log"' in html
        assert 'role="status"' in html

    def test_contains_focus_visible(self) -> None:
        """Accessibility: focus-visible styles are present."""
        html = render_dashboard()
        assert ":focus-visible" in html

    def test_contains_aria_expanded(self) -> None:
        """Accessibility: sidebar sections have aria-expanded."""
        html = render_dashboard()
        assert 'aria-expanded' in html

    def test_contains_keyboard_hints(self) -> None:
        """Input hint bar with keyboard shortcuts is present."""
        html = render_dashboard()
        assert "input-hint" in html
        assert "Ctrl+K" in html

    def test_contains_capability_badges(self) -> None:
        """Welcome screen has capability badges for features."""
        html = render_dashboard()
        assert "cap-badge" in html
        assert "命令执行" in html
        assert "本地安全" in html

    def test_contains_categorized_examples(self) -> None:
        """Welcome screen has categorized example sections."""
        html = render_dashboard()
        assert "example-cat-title" in html
        assert "快速开始" in html
        assert "数据库" in html
        assert "分析" in html

    def test_contains_message_timestamps(self) -> None:
        """Messages display timestamps."""
        html = render_dashboard()
        assert "toLocaleTimeString" in html
        assert "msg-meta" in html

    def test_contains_responsive_mobile_styles(self) -> None:
        """Responsive mobile breakpoint styles are present."""
        html = render_dashboard()
        assert "@media (max-width: 600px)" in html

    def test_sidebar_uses_nav_element(self) -> None:
        """Sidebar uses semantic <nav> element."""
        html = render_dashboard()
        assert "<nav" in html
        assert 'aria-label="参考信息"' in html

    def test_header_uses_banner_role(self) -> None:
        """Header has banner role for accessibility."""
        html = render_dashboard()
        assert 'role="banner"' in html

    def test_contains_escape_cancel_shortcut(self) -> None:
        """Escape key triggers cancel during execution."""
        html = render_dashboard()
        assert '"Escape"' in html


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
