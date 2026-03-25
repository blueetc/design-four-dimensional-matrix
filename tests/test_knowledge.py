"""Tests for the KNOWLEDGE_PROMPT and /knowledge slash command."""

from __future__ import annotations

from unittest import mock

import pytest

from agent.main import (
    _handle_slash_command,
    _init_messages,
    run_interactive,
)
from agent.prompts import KNOWLEDGE_PROMPT


# ---------------------------------------------------------------------------
# KNOWLEDGE_PROMPT content validation
# ---------------------------------------------------------------------------

class TestKnowledgePrompt:
    def test_prompt_is_non_empty_string(self) -> None:
        assert isinstance(KNOWLEDGE_PROMPT, str)
        assert len(KNOWLEDGE_PROMPT) > 500  # substantial content

    def test_covers_command_section(self) -> None:
        assert "终端命令" in KNOWLEDGE_PROMPT or "命令速查" in KNOWLEDGE_PROMPT

    def test_covers_programming_section(self) -> None:
        assert "编程" in KNOWLEDGE_PROMPT or "Python" in KNOWLEDGE_PROMPT

    def test_covers_database_section(self) -> None:
        assert "数据库" in KNOWLEDGE_PROMPT or "SQLite" in KNOWLEDGE_PROMPT

    def test_covers_reasoning_section(self) -> None:
        assert "推理" in KNOWLEDGE_PROMPT or "思维链" in KNOWLEDGE_PROMPT

    def test_covers_skills_section(self) -> None:
        assert "技能" in KNOWLEDGE_PROMPT or "操作技能" in KNOWLEDGE_PROMPT

    def test_mentions_key_tools(self) -> None:
        """Knowledge prompt references tools available in the agent."""
        for tool in ("analyze_fields", "design_wide_table", "visualize_3d"):
            assert tool in KNOWLEDGE_PROMPT

    def test_mentions_security_policy(self) -> None:
        assert "policy" in KNOWLEDGE_PROMPT.lower() or "策略" in KNOWLEDGE_PROMPT


# ---------------------------------------------------------------------------
# _init_messages includes KNOWLEDGE_PROMPT
# ---------------------------------------------------------------------------

class TestInitMessagesIncludesKnowledge:
    def test_three_system_messages(self) -> None:
        msgs = _init_messages()
        system_msgs = [m for m in msgs if m["role"] == "system"]
        assert len(system_msgs) == 3

    def test_knowledge_prompt_is_second(self) -> None:
        msgs = _init_messages()
        system_msgs = [m for m in msgs if m["role"] == "system"]
        assert "知识库" in system_msgs[1]["content"] or "命令" in system_msgs[1]["content"]


# ---------------------------------------------------------------------------
# /knowledge slash command
# ---------------------------------------------------------------------------

class TestKnowledgeSlashCommand:
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

    def test_knowledge_command_handled(self) -> None:
        model, msgs, panel, handled = self._call("/knowledge")
        assert handled is True

    def test_knowledge_preserves_model(self) -> None:
        model, msgs, panel, handled = self._call("/knowledge", model="llama3.3:70b")
        assert model == "llama3.3:70b"


# ---------------------------------------------------------------------------
# /help mentions /knowledge
# ---------------------------------------------------------------------------

class TestHelpMentionsKnowledge:
    def test_help_includes_knowledge(self, capsys: pytest.CaptureFixture) -> None:
        msgs = _init_messages()
        _handle_slash_command(
            "/help",
            active_model="qwen2.5:7b",
            messages=msgs,
            panel_models=[],
        )
        captured = capsys.readouterr()
        assert "/knowledge" in captured.out


# ---------------------------------------------------------------------------
# /knowledge in REPL
# ---------------------------------------------------------------------------

class TestKnowledgeInRepl:
    @mock.patch("builtins.input", side_effect=["/knowledge", "exit"])
    def test_knowledge_in_repl(self, mock_input: mock.Mock) -> None:
        """/knowledge is handled without calling the LLM."""
        with mock.patch("agent.main.run") as mock_run:
            run_interactive()
            mock_run.assert_not_called()
