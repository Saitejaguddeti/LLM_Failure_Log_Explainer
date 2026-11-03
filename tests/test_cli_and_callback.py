import json
import os
import time
from types import SimpleNamespace

from conftest import SAMPLES, TIMEOUT_NOTE, ScriptedLLM

from log_explainer.airflow_callback import default_log_folder, find_task_log, make_failure_callback
from log_explainer.cli import main

RUN_ID = "scheduled__2026-10-11T06:00:00+00:00"


# ---------------------------------------------------------------------------------------------- CLI
def test_markdown_note_without_a_model(capsys):
    assert main([str(SAMPLES / "spark_executor_oom.log"), "--no-llm"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("## Failure note: Spark app trades_daily_aggregates")
    assert "failed because a Spark executor ran out of memory" in out
    assert "Written by built-in rules (no LLM)" in out


def test_json_notes_written_to_a_folder(tmp_path):
    main([str(SAMPLES / "airflow_task_timeout.log"), "--no-llm", "--format", "json", "--out", str(tmp_path)])
    data = json.loads((tmp_path / "airflow_task_timeout.rootcause.json").read_text())
    assert data["label"] == "daily_trades_load.merge_trades (try 2)"
    assert data["category"] == "timeout"
    assert data["matched_patterns"] == ["airflow_task_timeout"]


def test_wildcards_are_expanded_by_the_tool(tmp_path):
    # PowerShell passes samples/*.log through unexpanded, so the CLI has to do it
    out = tmp_path / "notes"
    assert main([str(SAMPLES / "*.log"), "--no-llm", "--out", str(out)]) == 0
    written = sorted(p.name for p in out.iterdir())
    assert len(written) == len(list(SAMPLES.glob("*.log"))) >= 4
    assert "spark_executor_oom.rootcause.md" in written


def test_missing_file_is_reported(tmp_path, capsys):
    assert main([str(tmp_path / "nope.log"), "--no-llm"]) == 1
    assert "no such file" in capsys.readouterr().err


def test_show_excerpt_prints_the_redacted_text(capsys):
    main([str(SAMPLES / "airflow_db_auth_failure.log"), "--show-excerpt"])
    out = capsys.readouterr().out
    assert "postgresql://etl_user:[REDACTED]@" in out
    assert "Sup3rS3cret" not in out


def test_model_comes_from_llm_model(monkeypatch, capsys):
    llm = ScriptedLLM(replies=[TIMEOUT_NOTE])
    monkeypatch.setenv("LLM_MODEL", "scripted:test")
    monkeypatch.setattr("langchain.chat_models.init_chat_model", lambda name, **kwargs: llm)
    main([str(SAMPLES / "airflow_task_timeout.log")])
    out = capsys.readouterr().out
    assert "still running" in out
    assert "Written by scripted:test" in out


def test_missing_file_sets_the_exit_code(capsys):
    assert main(["does-not-exist.log", "--no-llm"]) == 1
    assert "no such file" in capsys.readouterr().err


# ------------------------------------------------------------------------------------ Airflow callback
def task_instance(**overrides):
    fields = {"dag_id": "daily_trades_load", "task_id": "merge_trades", "run_id": RUN_ID, "map_index": -1}
    return SimpleNamespace(**(fields | overrides))


def write_attempt(base, attempt, text, map_index=None):
    folder = base / "dag_id=daily_trades_load" / f"run_id={RUN_ID}" / "task_id=merge_trades"
    if map_index is not None:
        folder = folder / f"map_index={map_index}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"attempt={attempt}.log"
    path.write_text(text)
    return path


def test_callback_writes_a_note_next_to_the_newest_attempt(tmp_path, monkeypatch):
    monkeypatch.delenv("LLM_MODEL", raising=False)
    write_attempt(tmp_path, 1, "[2026-10-12, 05:00:00 UTC] {x.py:1} ERROR - first attempt failed differently")
    newest = write_attempt(tmp_path, 2, (SAMPLES / "airflow_task_timeout.log").read_text())
    os.utime(newest, (time.time() + 5, time.time() + 5))

    received = []
    callback = make_failure_callback(log_folder=tmp_path, on_note=lambda note, markdown: received.append(note))
    target = callback({"ti": task_instance()})

    assert target == newest.with_name("attempt=2.rootcause.md")
    assert "failed because the task ran past its execution_timeout" in target.read_text()
    assert received[0].category == "timeout"


def test_mapped_task_logs_are_found(tmp_path):
    path = write_attempt(tmp_path, 1, "log", map_index=3)
    assert find_task_log(tmp_path, "daily_trades_load", RUN_ID, "merge_trades", map_index=3) == path


def test_callback_never_raises(tmp_path):
    callback = make_failure_callback(log_folder=tmp_path)
    assert callback({"ti": task_instance()}) is None  # no log file yet
    assert callback({}) is None  # context without a task instance


def test_default_log_folder_follows_airflow_settings(monkeypatch, tmp_path):
    monkeypatch.setenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", str(tmp_path))
    assert default_log_folder() == tmp_path
    monkeypatch.delenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER")
    monkeypatch.setenv("AIRFLOW_HOME", "/opt/airflow")
    assert str(default_log_folder()) == "/opt/airflow/logs"
