"""Hook the explainer into Airflow as an on_failure_callback.

    from log_explainer.airflow_callback import make_failure_callback

    default_args = {"on_failure_callback": make_failure_callback()}

When a task fails, the callback finds the task's log file, writes a root-cause note next to it
(attempt=2.log -> attempt=2.rootcause.md), and hands the note to `on_note` if you pass one, for example
to post it to Slack. It never raises: a broken callback must not hide the task's real error.

It reads task logs from the local log folder, which is Airflow's default. With remote logging
(S3, CloudWatch, Elasticsearch), fetch the log text yourself and call core.explain_text().
This module does not import Airflow, so it can be tested without it.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .core import explain_file, load_model
from .explainer import RootCauseNote
from .report import to_markdown

log = logging.getLogger(__name__)


def default_log_folder() -> Path:
    folder = os.getenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER")
    if folder:
        return Path(folder)
    return Path(os.getenv("AIRFLOW_HOME", Path.home() / "airflow")) / "logs"


def find_task_log(log_folder: Path, dag_id: str, run_id: str, task_id: str, map_index: int = -1) -> Path | None:
    """Newest attempt log for the task, using Airflow's default log_filename_template layout.

    The newest file is used instead of trusting ti.try_number, whose meaning changed between Airflow versions.
    """
    folder = Path(log_folder) / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}"
    if map_index is not None and map_index >= 0:
        folder = folder / f"map_index={map_index}"
    attempts = sorted(folder.glob("attempt=*.log"), key=lambda p: p.stat().st_mtime)
    return attempts[-1] if attempts else None


def make_failure_callback(
    log_folder: str | Path | None = None,
    output_dir: str | Path | None = None,
    model: str | None = None,
    on_note: Callable[[RootCauseNote, str], Any] | None = None,
) -> Callable[[dict], Path | None]:
    """Build an on_failure_callback. With no model (and no LLM_MODEL set) it uses the built-in rules."""

    def callback(context: dict) -> Path | None:
        try:
            ti = context["ti"]
            path = find_task_log(
                Path(log_folder) if log_folder else default_log_folder(),
                ti.dag_id,
                ti.run_id,
                ti.task_id,
                getattr(ti, "map_index", -1),
            )
            if path is None:
                log.warning("explain-log: no log file found for %s.%s (%s)", ti.dag_id, ti.task_id, ti.run_id)
                return None
            llm, model_name = load_model(model)
            note, excerpt = explain_file(path, llm, model_name)
            markdown = to_markdown(note, excerpt)
            target_dir = Path(output_dir) if output_dir else path.parent
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / f"{path.stem}.rootcause.md"
            target.write_text(markdown, encoding="utf-8")
            log.info("explain-log: %s", note.summary)
            if on_note:
                on_note(note, markdown)
            return target
        except Exception:
            log.exception("explain-log: could not write a failure note")
            return None

    return callback
