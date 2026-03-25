"""Ollama HTTP API thin wrapper."""

from __future__ import annotations

import requests

OLLAMA_BASE = "http://127.0.0.1:11434"


class OllamaConnectionError(RuntimeError):
    """Raised when the Ollama service is unreachable."""


def ollama_chat(
    model: str,
    messages: list[dict],
    temperature: float = 0.2,
    timeout: int = 300,
) -> dict:
    """Send a chat completion request to a local Ollama instance."""
    try:
        resp = requests.post(
            f"{OLLAMA_BASE}/api/chat",
            json={
                "model": model,
                "messages": messages,
                "stream": False,
                "options": {"temperature": temperature},
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.ConnectionError:
        raise OllamaConnectionError(
            f"⚠️  无法连接 Ollama ({OLLAMA_BASE})。\n"
            "   请确认已安装并运行 Ollama：https://ollama.com\n"
            "   启动命令：ollama serve"
        ) from None
    except requests.Timeout:
        raise OllamaConnectionError(
            f"⚠️  Ollama 请求超时（{timeout}s）。模型 '{model}' 可能正在加载，请稍后重试。"
        ) from None


def list_models(timeout: int = 10) -> list[dict]:
    """Return the list of models available on the local Ollama instance.

    Each entry contains at least ``name``, ``size``, and ``modified_at``.
    Returns an empty list on connection failure.
    """
    try:
        resp = requests.get(f"{OLLAMA_BASE}/api/tags", timeout=timeout)
        resp.raise_for_status()
        return resp.json().get("models", [])
    except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
        return []
