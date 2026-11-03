"""Render a root-cause note as Markdown (for people) or JSON (for alerts and tickets)."""

from __future__ import annotations

import json

from .explainer import RootCauseNote
from .parsing import Excerpt


def to_markdown(note: RootCauseNote, excerpt: Excerpt) -> str:
    meta = excerpt.metadata
    lines = [f"## Failure note: {excerpt.label}", ""]
    if meta.get("run_id"):
        lines += [f"Run: `{meta['run_id']}`", ""]
    elif meta.get("app_id"):
        lines += [f"Application: `{meta['app_id']}`", ""]
    lines += [f"**What happened:** {note.summary}", "", f"**Likely root cause:** {note.root_cause}", ""]
    if note.evidence:
        lines += ["**Evidence from the log:**", "", "```"] + note.evidence + ["```", ""]
    if note.fix:
        lines += ["**What to try:**", ""] + [f"{i}. {step}" for i, step in enumerate(note.fix, start=1)] + [""]
    written_by = "built-in rules (no LLM)" if note.written_by == "rules" else note.written_by
    lines.append(
        f"_Category: {note.category}. Confidence: {note.confidence}. "
        f"Log: {excerpt.name}, {excerpt.total_lines} lines. Written by {written_by}._"
    )
    return "\n".join(lines) + "\n"


def to_json(note: RootCauseNote, excerpt: Excerpt) -> str:
    payload = {
        "log": excerpt.name,
        "source": excerpt.source,
        "label": excerpt.label,
        "metadata": excerpt.metadata,
        "matched_patterns": [m.signature.id for m in excerpt.matches],
        **note.model_dump(),
    }
    return json.dumps(payload, indent=2)
