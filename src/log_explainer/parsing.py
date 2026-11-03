"""Read an Airflow or Spark log and cut it down to the part that explains the failure.

A failed run's log can be thousands of lines, most of them routine. The excerpt keeps what a person
would look at: who/what/when, the known failure patterns that matched, the lines just before the first
error, the error lines with repeats collapsed, the exception and its root cause, a trimmed stack trace,
any Spark settings, and the end of the log.
"""

from __future__ import annotations

import gzip
import re
from dataclasses import dataclass, field
from pathlib import Path

from .patterns import Match, find_matches, matches_any

_AIRFLOW_MARKERS = ("taskinstance.py", "AIRFLOW_CTX_", "Marking task as", "airflow.exceptions", "standard_task_runner")
_SPARK_MARKERS = (
    "org.apache.spark",
    "SparkContext",
    "TaskSetManager",
    "DAGScheduler",
    "py4j",
    "pyspark",
    "YarnScheduler",
)

# Timestamps and logger prefixes at the start of a line
_PREFIX = re.compile(
    r"^\[\d{4}-\d{2}-\d{2},? [\d:.,]+(?: \w+)?\]\s*(?:\{[^}]*\}\s*)?"  # Airflow: [2026-10-12, 06:00:03 UTC] {x.py:12}
    r"|^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\s*"  # log4j / Spark: 26/10/12 02:31:44
    r"|^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[.,\d]*Z?\s*"  # ISO: 2026-10-12 02:31:44,123
)
_LEVEL = re.compile(r"(?:^|\s)(?:ERROR|FATAL|CRITICAL)\b")
_JAVA_EXCEPTION = re.compile(r"^(?:Caused by: |: )?(?:[a-z_$][\w$]*\.)+[A-Z][\w$]*(?:Exception|Error)\b")
_JAVA_FRAME = re.compile(r"^\s+at [\w$.<>/]+\(|^\s+\.\.\. \d+ more")
_SPARK_SETTING = re.compile(r"^\s*(spark\.[\w.]+)=(\S+)\s*$|--conf\s+(spark\.[\w.]+)=(\S+)")

MAX_LINE = 300


@dataclass
class Excerpt:
    name: str
    source: str  # "airflow", "spark", "airflow+spark", or "unknown"
    total_lines: int
    metadata: dict[str, str] = field(default_factory=dict)
    matches: list[Match] = field(default_factory=list)
    exception: str = ""
    root_cause: str = ""
    text: str = ""

    @property
    def label(self) -> str:
        m = self.metadata
        if "dag_id" in m and "task_id" in m:
            tries = f" (try {m['try_number']})" if m.get("try_number") else ""
            return f"{m['dag_id']}.{m['task_id']}{tries}"
        if "app_name" in m or "app_id" in m:
            return f"Spark app {m.get('app_name') or m['app_id']}"
        return self.name


def read_log(path: str | Path) -> str:
    path = Path(path)
    if path.suffix == ".gz":  # EMR and YARN logs are often gzipped
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
            return f.read()
    return path.read_text(encoding="utf-8", errors="replace")


def build_excerpt(text: str, name: str = "log", max_chars: int = 12_000) -> Excerpt:
    lines = text.splitlines()
    source = _detect_source(text)
    matches = find_matches(lines)
    meta = _metadata(text)

    tracebacks = _python_tracebacks(lines)
    java_header, java_frames, causes = _java_trace(lines)
    exception = "\n".join(tracebacks[-1][2]) if tracebacks else (java_header or "")
    root_cause = causes[-1] if causes else ""
    important = _important_lines(lines)

    sections: list[tuple[str, list[str]]] = []
    header = [f"Log file: {name} ({len(lines)} lines)", f"Looks like: {source}"]
    header += [f"{k}: {v}" for k, v in meta.items()]
    sections.append(("", header))

    if matches:
        sections.append(
            (
                "Known failure patterns that matched (hints, not conclusions)",
                [f"- {m.signature.title} [{m.signature.id}], first on line {m.line_no}" for m in matches],
            )
        )

    settings = [ln.strip() for ln in lines if _SPARK_SETTING.search(ln)][:15]
    if settings:
        sections.append(("Spark settings in the log", settings))

    if important:
        first = important[0][0]
        before = [
            f"L{i}: {_clip(lines[i - 1])}"
            for i in range(max(1, first - 12), first)
            if _is_plain(lines[i - 1]) and not _SPARK_SETTING.search(lines[i - 1])
        ]
        if before:
            sections.append(("Lines just before the first error", before))
        sections.append(
            (
                "Error lines (repeats collapsed)",
                [f"L{no}: {line}" + (f"   [same message {n} times]" if n > 1 else "") for no, line, n in important],
            )
        )

    if exception:
        sections.append(("Exception", exception.splitlines()))
    if root_cause:
        sections.append(("Deepest 'Caused by'", [root_cause]))
    if tracebacks:
        sections.append(("Python traceback (trimmed)", _trim_frames(tracebacks[-1][1])))
    if java_frames:
        sections.append((f"Java stack trace under: {java_header[:120]}", java_frames))

    shown = {i for i, _, _ in important}
    in_exception = set(exception.splitlines())
    tail = [
        f"L{i}: {_clip(lines[i - 1])}"
        for i in range(max(1, len(lines) - 7), len(lines) + 1)
        if _is_plain(lines[i - 1]) and i not in shown and _clip(lines[i - 1]) not in in_exception
    ]
    if tail:
        sections.append(("End of the log", tail))

    rendered = _render(sections)
    if len(rendered) > max_chars:
        # first squeeze the long lists, then hard-cut as a last resort
        sections = [(title, body[:12]) for title, body in sections]
        rendered = _render(sections)
        if len(rendered) > max_chars:
            rendered = rendered[: max_chars - 30] + "\n[excerpt truncated]"

    return Excerpt(name, source, len(lines), meta, matches, exception, root_cause, rendered)


# --------------------------------------------------------------------------- helpers
def _detect_source(text: str) -> str:
    airflow = any(m in text for m in _AIRFLOW_MARKERS)
    spark = any(m in text for m in _SPARK_MARKERS)
    return {(True, True): "airflow+spark", (True, False): "airflow", (False, True): "spark"}.get(
        (airflow, spark), "unknown"
    )


def _first(pattern: str, text: str) -> str | None:
    m = re.search(pattern, text)
    return m.group(1) if m else None


def _metadata(text: str) -> dict[str, str]:
    meta: dict[str, str] = {}
    fields = {
        "dag_id": [r"AIRFLOW_CTX_DAG_ID='([^']+)'", r"dag_id=([^,\s]+)"],
        "task_id": [r"AIRFLOW_CTX_TASK_ID='([^']+)'", r"task_id=([^,\s]+)"],
        "run_id": [r"AIRFLOW_CTX_DAG_RUN_ID='([^']+)'", r"run_id=([^,\s]+)"],
        "try_number": [r"AIRFLOW_CTX_TRY_NUMBER='(\d+)'", r"Starting attempt (\d+) of"],
        "max_tries": [r"Starting attempt \d+ of (\d+)"],
        "operator": [r"Executing <Task\((\w+)\)"],
        "app_name": [r"Submitted application: (\S+)"],
        "app_id": [r"\b(application_\d+_\d+)\b", r"\b(app-\d{14}-\d{4})\b"],
    }
    for key, patterns in fields.items():
        for p in patterns:
            value = _first(p, text)
            if value:
                meta[key] = value
                break
    failed = re.search(r"Task (\d+) in stage ([\d.]+) failed (\d+) times", text)
    if failed:
        meta["failed_stage"] = f"stage {failed.group(2)}, task {failed.group(1)} failed {failed.group(3)} times"
    return meta


def _strip_prefix(line: str) -> str:
    return _PREFIX.sub("", line)


def _important_lines(lines: list[str]) -> list[tuple[int, str, int]]:
    """(first line number, line, how many times it repeated) for error-level or known-pattern lines."""
    seen: dict[str, list] = {}
    for no, line in enumerate(lines, start=1):
        if line.startswith((" ", "\t")):  # stack frames are shown with the trace, not here
            continue
        if _LEVEL.search(line) or matches_any(line):
            key = re.sub(r"\d+", "#", _strip_prefix(line)).strip()
            if key in seen:
                seen[key][2] += 1
            else:
                seen[key] = [no, _clip(line), 1]
    return [tuple(v) for v in seen.values()][:25]


def _python_tracebacks(lines: list[str]) -> list[tuple[int, list[str], list[str]]]:
    """Every Python traceback as (start line number, frame lines, exception lines)."""
    found = []
    i, n = 0, len(lines)
    while i < n:
        if not lines[i].startswith("Traceback (most recent call last)"):
            i += 1
            continue
        start, frames, exc = i + 1, [], []
        i += 1
        while i < n and lines[i].startswith((" ", "\t")):
            frames.append(lines[i])
            i += 1
        # the exception line, plus any continuation lines (Spark plans, py4j's ": org.apache..." line)
        while i < n and lines[i].strip() and not _PREFIX.match(lines[i]) and len(exc) < 6:
            if _JAVA_FRAME.match(lines[i]) or lines[i].startswith("Traceback"):
                break
            exc.append(_clip(lines[i]))
            i += 1
        found.append((start, frames, exc))
    return found


def _trim_frames(frames: list[str], keep_tail: int = 10) -> list[str]:
    """Drop caret lines, keep the last frames and every frame from the job's own code."""
    frames = [f.rstrip() for f in frames if f.strip() and set(f.strip()) - set("^~")]
    if len(frames) <= keep_tail + 4:
        return frames
    own = [
        i
        for i, f in enumerate(frames)
        if f.lstrip().startswith("File ") and "site-packages" not in f and "/usr/lib/spark" not in f
    ]
    keep = set(range(len(frames) - keep_tail, len(frames)))
    for i in own:
        keep |= {i, i + 1}
    out, last = [], -1
    for i in sorted(k for k in keep if k < len(frames)):
        if i != last + 1:
            out.append("  ...")
        out.append(frames[i])
        last = i
    return out


def _java_trace(lines: list[str]) -> tuple[str, list[str], list[str]]:
    """First Java exception that has 'at' frames under it, its first frames, and all 'Caused by' lines."""
    causes = [_clip(ln) for ln in lines if ln.startswith("Caused by:")]
    for i, line in enumerate(lines):
        if not _JAVA_EXCEPTION.match(line):
            continue
        j = i + 1
        if j < len(lines) and lines[j].startswith("Driver stacktrace"):
            j += 1
        if j < len(lines) and _JAVA_FRAME.match(lines[j]):
            frames = []
            while j < len(lines) and (_JAVA_FRAME.match(lines[j]) or lines[j].startswith("Caused by:")):
                if len(frames) < 8 and not lines[j].startswith("Caused by:"):
                    frames.append(lines[j].rstrip())
                j += 1
            return _clip(line), frames, causes
    return "", [], causes


def _is_plain(line: str) -> bool:
    """A normal log line: not blank, not a stack frame, not the start of a traceback."""
    return bool(line.strip()) and not line.startswith((" ", "\t", "Traceback"))


def _clip(line: str) -> str:
    line = line.rstrip()
    return line if len(line) <= MAX_LINE else line[: MAX_LINE - 3] + "..."


def _render(sections: list[tuple[str, list[str]]]) -> str:
    parts = []
    for title, body in sections:
        if title:
            parts.append(f"\n## {title}")
        parts.extend(body)
    return "\n".join(parts).strip()
