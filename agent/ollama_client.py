"""Ollama HTTP API thin wrapper.

Supports automatic fallback from ``/api/chat`` to ``/api/generate`` for
older Ollama versions that do not expose the chat endpoint (HTTP 404).
"""

from __future__ import annotations

import requests

OLLAMA_BASE = "http://127.0.0.1:11434"


class OllamaConnectionError(RuntimeError):
    """Raised when the Ollama service is unreachable."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _messages_to_prompt(messages: list[dict]) -> tuple[str, str]:
    """Convert a chat-style *messages* list into a ``(system, prompt)`` pair.

    The *system* string collects all ``role=system`` messages.  Everything
    else is merged into *prompt* with lightweight role prefixes so the
    model still sees the conversational structure.
    """
    system_parts: list[str] = []
    prompt_parts: list[str] = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if role == "system":
            system_parts.append(content)
        elif role == "assistant":
            prompt_parts.append(f"Assistant: {content}")
        else:
            prompt_parts.append(f"User: {content}")
    return "\n".join(system_parts), "\n".join(prompt_parts)


def _generate_fallback(
    model: str,
    messages: list[dict],
    temperature: float,
    timeout: int,
) -> dict:
    """Call ``/api/generate`` and reshape the response to look like ``/api/chat``."""
    system, prompt = _messages_to_prompt(messages)
    payload: dict = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperature},
    }
    if system:
        payload["system"] = system

    resp = requests.post(
        f"{OLLAMA_BASE}/api/generate",
        json=payload,
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    # Normalize to the same shape returned by /api/chat so callers are
    # unaffected by which endpoint was actually used.
    return {
        "model": data.get("model", model),
        "message": {
            "role": "assistant",
            "content": data.get("response", ""),
        },
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ollama_chat(
    model: str,
    messages: list[dict],
    temperature: float = 0.2,
    timeout: int = 300,
) -> dict:
    """Send a chat completion request to a local Ollama instance.

    If the ``/api/chat`` endpoint returns **HTTP 404** (older Ollama
    versions), the request is transparently retried via ``/api/generate``
    so the rest of the agent code does not need to care about the
    server version.
    """
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
        if resp.status_code == 404:
            # /api/chat not available – fall back to /api/generate
            return _generate_fallback(model, messages, temperature, timeout)
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
