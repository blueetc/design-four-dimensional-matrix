"""Tests for the toolserver web dashboard at GET /."""

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

    def test_contains_swagger_link(self) -> None:
        html = render_dashboard()
        assert 'href="/docs"' in html

    def test_contains_openapi_link(self) -> None:
        html = render_dashboard()
        assert 'href="/openapi.json"' in html

    def test_contains_try_panel(self) -> None:
        html = render_dashboard()
        assert "tool-select" in html
        assert "runTool" in html

    def test_tool_count_displayed(self) -> None:
        html = render_dashboard()
        assert f"可用工具 ({len(TOOLS)})" in html


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
