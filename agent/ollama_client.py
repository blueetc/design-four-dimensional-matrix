"""Ollama HTTP API thin wrapper."""

from __future__ import annotations

import requests

OLLAMA_BASE = "http://127.0.0.1:11434"


def ollama_chat(
    model: str,
    messages: list[dict],
    temperature: float = 0.2,
    timeout: int = 300,
) -> dict:
    """Send a chat completion request to a local Ollama instance."""
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
