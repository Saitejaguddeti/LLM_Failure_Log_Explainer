"""Turn an excerpt into a root-cause note, with a chat model or with the built-in rules.

The model's reply is checked before it is used: it must be valid JSON in the expected shape, and every
evidence line it quotes has to actually appear in the log excerpt. Quotes it made up are dropped.
If the model call fails or the reply is unusable twice in a row, the rule-based note is returned instead,
so a failure callback never ends up with nothing.
"""

from __future__ import annotations

import json
import logging
import re

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from pydantic import BaseModel, Field, ValidationError

from .parsing import Excerpt

log = logging.getLogger(__name__)

CATEGORIES = {
    "memory", "shuffle", "schema", "data", "timeout", "upstream", "connectivity", "permissions",
    "environment", "database", "performance", "code", "infrastructure", "unknown",
}  # fmt: skip

SYSTEM_PROMPT = """You are the on-call data engineer. You get an excerpt from the log of a failed Airflow task or \
Spark job. Explain the failure to a teammate in plain English.

Reply with only a JSON object with these keys:
  "summary": one sentence: what failed and the most likely reason.
  "root_cause": two to four sentences. Say what the log shows, then what you infer from it, and keep the two apart.
  "evidence": one to four lines copied exactly from the excerpt that support the root cause.
  "fix": one to four concrete next steps, most likely first.
  "category": one of memory, shuffle, schema, data, timeout, upstream, connectivity, permissions, environment, \
database, performance, code, infrastructure, unknown.
  "confidence": "high" only if the log states the cause directly, "medium" if it strongly suggests it, "low" if \
you are guessing.

The "known failure patterns" in the excerpt come from simple regex rules. Treat them as hints and check them \
against the log. Do not invent setting names, values, file names, or numbers that are not in the excerpt. If the \
excerpt is not enough to tell, say what extra information would settle it."""

RETRY_PROMPT = "That reply could not be used ({problem}). Reply with only the JSON object described above."


class RootCauseNote(BaseModel):
    summary: str
    root_cause: str
    evidence: list[str] = Field(default_factory=list)
    fix: list[str] = Field(default_factory=list)
    category: str = "unknown"
    confidence: str = "low"
    written_by: str = "rules"


def explain(excerpt: Excerpt, llm: BaseChatModel | None = None, model_name: str | None = None) -> RootCauseNote:
    if llm is None:
        return rule_based_note(excerpt)
    try:
        note = _ask_model(llm, excerpt)
    except Exception as e:  # network error, bad reply twice, provider outage
        log.warning("Model call failed (%s: %s); using the rule-based note instead", type(e).__name__, e)
        return rule_based_note(excerpt)
    note.evidence = [line for line in note.evidence if _appears_in(line, excerpt.text)]
    note.written_by = model_name or "LLM"
    return note


def rule_based_note(excerpt: Excerpt) -> RootCauseNote:
    """A note built only from the known failure patterns. Used without a model, or when the model fails."""
    if excerpt.matches:
        sig = excerpt.matches[0].signature
        evidence = [m.line.strip() for m in excerpt.matches[:3]]
        first_exception_line = excerpt.exception.splitlines()[0] if excerpt.exception else ""
        if first_exception_line and first_exception_line not in evidence:
            evidence.append(first_exception_line)
        return RootCauseNote(
            summary=f"{excerpt.label} failed because {sig.title}.",
            root_cause=sig.explanation,
            evidence=evidence[:4],
            fix=list(sig.fixes),
            category=sig.category,
            confidence="medium",
        )
    exception = excerpt.exception.splitlines()[0] if excerpt.exception else ""
    return RootCauseNote(
        summary=f"{excerpt.label} failed" + (f" with {exception}" if exception else "") + ". No known pattern matched.",
        root_cause="None of the built-in failure patterns matched this log. Start from the exception and the error "
        "lines in the excerpt.",
        evidence=[exception] if exception else [],
        fix=["Read the stack trace from the bottom up; the last frame in your own code is usually the place to look."],
        category="unknown",
        confidence="low",
    )


def _ask_model(llm: BaseChatModel, excerpt: Excerpt) -> RootCauseNote:
    messages: list[BaseMessage] = [SystemMessage(SYSTEM_PROMPT), HumanMessage(excerpt.text)]
    reply = llm.invoke(messages)
    try:
        return _parse(reply.text)
    except ValueError as e:
        # one more try, telling the model what was wrong with its answer
        messages += [AIMessage(reply.text), HumanMessage(RETRY_PROMPT.format(problem=e))]
        return _parse(llm.invoke(messages).text)


def _parse(text: str) -> RootCauseNote:
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end <= start:
        raise ValueError("no JSON object found")
    try:
        data = json.loads(text[start : end + 1])
        note = RootCauseNote.model_validate(data)
    except (json.JSONDecodeError, ValidationError) as e:
        raise ValueError(str(e).splitlines()[0]) from e
    note.category = note.category.lower() if note.category.lower() in CATEGORIES else "unknown"
    note.confidence = note.confidence.lower() if note.confidence.lower() in {"low", "medium", "high"} else "low"
    note.evidence, note.fix = note.evidence[:4], note.fix[:4]
    return note


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().strip("`'\"").strip()


def _appears_in(quote: str, excerpt_text: str) -> bool:
    """True if the quoted line (minus an 'L12:' prefix) is really in the excerpt."""
    quote = _normalize(re.sub(r"^L\d+:\s*", "", quote.strip()))
    return len(quote) >= 12 and quote in _normalize(excerpt_text)
