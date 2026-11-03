import json

import pytest
from conftest import SAMPLES, TIMEOUT_NOTE, ScriptedLLM

from log_explainer.core import explain_file


def test_model_note_is_parsed_and_made_up_evidence_is_dropped():
    llm = ScriptedLLM(replies=[f"Here you go:\n```json\n{TIMEOUT_NOTE}\n```"])
    note, _ = explain_file(SAMPLES / "airflow_task_timeout.log", llm, "scripted")
    assert note.category == "timeout"
    assert note.confidence == "medium"
    assert note.evidence == ["airflow.exceptions.AirflowTaskTimeout: Timeout, PID: 48211"]
    assert note.written_by == "scripted"


def test_secrets_never_reach_the_model():
    llm = ScriptedLLM(replies=[TIMEOUT_NOTE])
    explain_file(SAMPLES / "airflow_db_auth_failure.log", llm, "scripted")
    sent = "\n".join(message.content for message in llm.calls[0])
    assert "Sup3rS3cret" not in sent
    assert "acme.example" not in sent
    assert "password authentication failed" in sent


def test_a_bad_reply_gets_one_retry():
    llm = ScriptedLLM(replies=["It probably timed out.", TIMEOUT_NOTE])
    note, _ = explain_file(SAMPLES / "airflow_task_timeout.log", llm, "scripted")
    assert note.written_by == "scripted"
    assert "could not be used" in llm.calls[1][-1].content


def test_falls_back_to_rules_when_the_model_keeps_failing():
    llm = ScriptedLLM(replies=["not json", "still not json"])
    note, _ = explain_file(SAMPLES / "spark_executor_oom.log", llm, "scripted")
    assert note.written_by == "rules"
    assert note.category == "memory"


def test_odd_category_and_confidence_are_normalized():
    data = json.loads(TIMEOUT_NOTE) | {"category": "cosmic rays", "confidence": "certain"}
    note, _ = explain_file(SAMPLES / "airflow_task_timeout.log", ScriptedLLM(replies=[json.dumps(data)]), "scripted")
    assert (note.category, note.confidence) == ("unknown", "low")


@pytest.mark.parametrize(
    "name,category",
    [
        ("airflow_task_timeout.log", "timeout"),
        ("airflow_db_auth_failure.log", "permissions"),
        ("spark_executor_oom.log", "memory"),
        ("spark_analysis_exception.log", "schema"),
    ],
)
def test_rule_based_notes(name, category):
    note, excerpt = explain_file(SAMPLES / name)
    assert note.category == category
    assert note.summary.startswith(f"{excerpt.label} failed because")
    assert note.evidence and note.fix
    assert note.written_by == "rules"
