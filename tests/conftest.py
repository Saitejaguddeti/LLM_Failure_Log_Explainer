from __future__ import annotations

import json
from pathlib import Path

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from pydantic import Field

SAMPLES = Path(__file__).resolve().parents[1] / "samples"

TIMEOUT_NOTE = json.dumps(
    {
        "summary": "merge_trades was killed by its execution_timeout while the Snowflake MERGE was still running.",
        "root_cause": "The log shows the MERGE starting at 06:00:06 and the timeout firing at 06:30:04. "
        "Nothing failed inside Snowflake; the statement was still waiting when Airflow stopped the task.",
        "evidence": [
            "airflow.exceptions.AirflowTaskTimeout: Timeout, PID: 48211",
            "WARN the warehouse was suspended at 06:12",  # not in the log; should be dropped
        ],
        "fix": ["Check the query history for the MERGE.", "Compare the staging row count with earlier days."],
        "category": "timeout",
        "confidence": "Medium",
    }
)


class ScriptedLLM(BaseChatModel):
    """Chat model stand-in: returns canned replies in order and keeps every prompt it was sent."""

    replies: list[str]
    calls: list = Field(default_factory=list)

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        self.calls.append(messages)
        if len(self.calls) > len(self.replies):
            raise AssertionError(f"unexpected model call #{len(self.calls)}")
        reply = AIMessage(self.replies[len(self.calls) - 1])
        return ChatResult(generations=[ChatGeneration(message=reply)])
