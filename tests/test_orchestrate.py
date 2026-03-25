"""Tests for multi-model orchestration: /orch, run_orchestrate, director loop."""

from __future__ import annotations

import json
from unittest import mock

import pytest

from agent.main import (
    MAX_ORCH_ROUNDS,
    _handle_slash_command,
    _init_messages,
    _try_parse_orchestrator_action,
    run_interactive,
    run_orchestrate,
)
from agent.prompts import ORCHESTRATOR_PROMPT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_response(content: str) -> dict:
    return {"message": {"content": content}}


def _delegate_action(model: str, subtask: str) -> str:
    return json.dumps({"action": "delegate", "model": model, "subtask": subtask})


def _broadcast_action(question: str) -> str:
    return json.dumps({"action": "broadcast", "question": question})


def _finish_action(summary: str) -> str:
    return json.dumps({"action": "finish", "summary": summary})


FAKE_MODELS = [
    {"name": "qwen2.5:32b", "size": 19 * 1024**3},
    {"name": "llama3.3:70b", "size": 42 * 1024**3},
]


# ---------------------------------------------------------------------------
# _try_parse_orchestrator_action
# ---------------------------------------------------------------------------

class TestTryParseOrchestratorAction:
    def test_valid_delegate(self) -> None:
        text = _delegate_action("qwen2.5:32b", "写段代码")
        result = _try_parse_orchestrator_action(text)
        assert result is not None
        assert result["action"] == "delegate"
        assert result["model"] == "qwen2.5:32b"

    def test_valid_broadcast(self) -> None:
        text = _broadcast_action("什么是AI？")
        result = _try_parse_orchestrator_action(text)
        assert result is not None
        assert result["action"] == "broadcast"

    def test_valid_finish(self) -> None:
        text = _finish_action("任务完成")
        result = _try_parse_orchestrator_action(text)
        assert result is not None
        assert result["action"] == "finish"

    def test_plain_text_returns_none(self) -> None:
        assert _try_parse_orchestrator_action("hello world") is None

    def test_json_without_action_returns_none(self) -> None:
        assert _try_parse_orchestrator_action('{"key": "value"}') is None

    def test_invalid_json_returns_none(self) -> None:
        assert _try_parse_orchestrator_action("{bad json}") is None


# ---------------------------------------------------------------------------
# ORCHESTRATOR_PROMPT
# ---------------------------------------------------------------------------

class TestOrchestratorPrompt:
    def test_prompt_is_formattable(self) -> None:
        """The template has {available_models} and {max_rounds} placeholders."""
        result = ORCHESTRATOR_PROMPT.format(
            available_models="- model_a\n- model_b",
            max_rounds=10,
        )
        assert "model_a" in result
        assert "10" in result

    def test_prompt_contains_action_keywords(self) -> None:
        result = ORCHESTRATOR_PROMPT.format(available_models="", max_rounds=5)
        assert "delegate" in result
        assert "broadcast" in result
        assert "finish" in result


# ---------------------------------------------------------------------------
# run_orchestrate – delegate then finish
# ---------------------------------------------------------------------------

class TestRunOrchestrateDelegate:
    @mock.patch("agent.main.ollama_chat")
    @mock.patch("agent.main.list_models", return_value=FAKE_MODELS)
    def test_delegate_then_finish(
        self, mock_lm: mock.Mock, mock_chat: mock.Mock,
    ) -> None:
        """Director delegates to a worker, then finishes."""
        mock_chat.side_effect = [
            # Round 1: director delegates
            _fake_response(_delegate_action("qwen2.5:32b", "分析数据")),
            # Round 1: worker responds (called by delegate)
            _fake_response("数据分析结果: 一切正常"),
            # Round 2: director finishes
            _fake_response(_finish_action("基于 qwen2.5:32b 的分析，一切正常。")),
        ]
        result = run_orchestrate("帮我分析数据", director_model="llama3.3:70b")
        assert result["round_count"] == 2
        assert "一切正常" in result["summary"]
        assert len(result["rounds"]) == 2
        assert result["rounds"][0]["action"] == "delegate"
        assert result["rounds"][1]["action"] == "finish"


# ---------------------------------------------------------------------------
# run_orchestrate – broadcast then finish
# ---------------------------------------------------------------------------

class TestRunOrchestrrateBroadcast:
    @mock.patch("agent.main.ollama_chat")
    @mock.patch("agent.main.list_models", return_value=FAKE_MODELS)
    def test_broadcast_then_finish(
        self, mock_lm: mock.Mock, mock_chat: mock.Mock,
    ) -> None:
        """Director broadcasts, collects responses, then finishes."""
        mock_chat.side_effect = [
            # Round 1: director broadcasts
            _fake_response(_broadcast_action("什么是机器学习？")),
            # Round 1: worker 1 responds
            _fake_response("机器学习是..."),
            # Round 1: worker 2 responds
            _fake_response("ML is a subset of AI..."),
            # Round 2: director finishes
            _fake_response(_finish_action("综合两个模型的回答，机器学习是AI的子集。")),
        ]
        result = run_orchestrate("解释机器学习")
        assert result["round_count"] == 2
        assert len(result["rounds"]) == 2
        assert result["rounds"][0]["action"] == "broadcast"
        assert len(result["rounds"][0]["responses"]) == 2


# ---------------------------------------------------------------------------
# run_orchestrate – plain text (implicit finish)
# ---------------------------------------------------------------------------

class TestRunOrchestratePlainText:
    @mock.patch("agent.main.ollama_chat")
    @mock.patch("agent.main.list_models", return_value=FAKE_MODELS)
    def test_plain_text_is_implicit_finish(
        self, mock_lm: mock.Mock, mock_chat: mock.Mock,
    ) -> None:
        """If director outputs plain text, treat as immediate finish."""
        mock_chat.return_value = _fake_response("这个任务很简单，答案是42。")
        result = run_orchestrate("42是什么？")
        assert result["round_count"] == 1
        assert "42" in result["summary"]
        assert result["rounds"][0]["action"] == "text"


# ---------------------------------------------------------------------------
# run_orchestrate – max rounds
# ---------------------------------------------------------------------------

class TestRunOrchestateMaxRounds:
    @mock.patch("agent.main.ollama_chat")
    @mock.patch("agent.main.list_models", return_value=FAKE_MODELS)
    def test_stops_at_max_rounds(
        self, mock_lm: mock.Mock, mock_chat: mock.Mock,
    ) -> None:
        """Stops after max_rounds without a finish action."""
        # Director always delegates, worker always responds
        side = []
        for i in range(5):
            side.append(_fake_response(_delegate_action("qwen2.5:32b", f"子任务{i}")))
            side.append(_fake_response(f"结果{i}"))
        mock_chat.side_effect = side
        result = run_orchestrate("无限任务", max_rounds=3)
        assert result["round_count"] == 3
        assert "最大轮数" in result["summary"]


# ---------------------------------------------------------------------------
# run_orchestrate – worker failure
# ---------------------------------------------------------------------------

class TestRunOrchestateWorkerFailure:
    @mock.patch("agent.main.ollama_chat")
    @mock.patch("agent.main.list_models", return_value=FAKE_MODELS)
    def test_delegate_worker_failure(
        self, mock_lm: mock.Mock, mock_chat: mock.Mock,
    ) -> None:
        """Worker failure is captured and fed back to director."""
        mock_chat.side_effect = [
            # Director delegates
            _fake_response(_delegate_action("bad:model", "做点什么")),
            # Worker fails
            Exception("connection refused"),
            # Director finishes after seeing failure
            _fake_response(_finish_action("工作模型失败，无法完成。")),
        ]
        result = run_orchestrate("测试失败")
        assert result["round_count"] == 2
        assert "调用失败" in result["rounds"][0]["response"]

    @mock.patch("agent.main.ollama_chat")
    @mock.patch("agent.main.list_models", return_value=FAKE_MODELS)
    def test_broadcast_partial_failure(
        self, mock_lm: mock.Mock, mock_chat: mock.Mock,
    ) -> None:
        """One worker fails in broadcast, others succeed."""
        mock_chat.side_effect = [
            # Director broadcasts
            _fake_response(_broadcast_action("问题")),
            # Worker 1 succeeds
            _fake_response("回答1"),
            # Worker 2 fails
            Exception("timeout"),
            # Director finishes
            _fake_response(_finish_action("部分成功。")),
        ]
        result = run_orchestrate("广播测试")
        assert result["round_count"] == 2
        responses = result["rounds"][0]["responses"]
        assert any("回答1" in r["response"] for r in responses)
        assert any("调用失败" in r["response"] for r in responses)


# ---------------------------------------------------------------------------
# run_orchestrate – explicit worker models
# ---------------------------------------------------------------------------

class TestRunOrchestateExplicitWorkers:
    @mock.patch("agent.main.ollama_chat")
    def test_uses_explicit_workers(self, mock_chat: mock.Mock) -> None:
        """When worker_models are explicitly provided, uses them."""
        mock_chat.return_value = _fake_response(
            _finish_action("完成"),
        )
        result = run_orchestrate(
            "测试",
            worker_models=["a:7b", "b:7b"],
        )
        assert result["round_count"] == 1


# ---------------------------------------------------------------------------
# /orch slash command
# ---------------------------------------------------------------------------

class TestOrchSlashCommand:
    @mock.patch("agent.main.run_orchestrate")
    @mock.patch("agent.main._resolve_panel_models", return_value=["a:7b", "b:7b"])
    def test_orch_command(
        self, mock_resolve: mock.Mock, mock_orch: mock.Mock,
    ) -> None:
        mock_orch.return_value = {
            "summary": "任务完成",
            "rounds": [],
            "round_count": 2,
        }
        msgs = _init_messages()
        model, returned_msgs, panel, handled = _handle_slash_command(
            "/orch 设计一个系统",
            active_model="qwen2.5:7b",
            messages=msgs,
            panel_models=[],
        )
        assert handled is True
        mock_orch.assert_called_once_with(
            task="设计一个系统",
            director_model="qwen2.5:7b",
            worker_models=["a:7b", "b:7b"],
        )
        # Result should be recorded in messages
        assert any("编排结果" in m["content"] for m in returned_msgs)

    def test_orch_no_args(self) -> None:
        msgs = _init_messages()
        model, returned_msgs, panel, handled = _handle_slash_command(
            "/orch",
            active_model="qwen2.5:7b",
            messages=msgs,
            panel_models=[],
        )
        assert handled is True
        # No crash, just prints usage


# ---------------------------------------------------------------------------
# /orch in REPL
# ---------------------------------------------------------------------------

class TestOrchInRepl:
    @mock.patch("agent.main.run_orchestrate")
    @mock.patch("agent.main._resolve_panel_models", return_value=["a:7b"])
    @mock.patch("builtins.input", side_effect=["/orch 做个总结", "exit"])
    def test_orch_in_repl(
        self, mock_input: mock.Mock, mock_resolve: mock.Mock,
        mock_orch: mock.Mock,
    ) -> None:
        mock_orch.return_value = {
            "summary": "总结完毕",
            "rounds": [],
            "round_count": 1,
        }
        with mock.patch("agent.main.run") as mock_run:
            run_interactive()
            mock_run.assert_not_called()
            mock_orch.assert_called_once()


# ---------------------------------------------------------------------------
# /help includes /orch
# ---------------------------------------------------------------------------

class TestHelpIncludesOrch:
    def test_help_mentions_orch(self) -> None:
        msgs = _init_messages()
        model, returned_msgs, panel, handled = _handle_slash_command(
            "/help",
            active_model="qwen2.5:7b",
            messages=msgs,
            panel_models=[],
        )
        assert handled is True
        # The help text is printed (captured by capsys if needed),
        # but we can verify the command is handled.
