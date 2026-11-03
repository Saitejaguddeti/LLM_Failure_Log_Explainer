"""The pipeline in one place: read -> redact -> excerpt -> explain."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from langchain_core.language_models import BaseChatModel

from .explainer import RootCauseNote, explain
from .parsing import Excerpt, build_excerpt, read_log
from .redact import redact

log = logging.getLogger(__name__)


def load_model(model: str | None = None) -> tuple[BaseChatModel | None, str | None]:
    """Chat model from a "provider:model" string or the LLM_MODEL variable; (None, None) means use the rules."""
    name = model or os.getenv("LLM_MODEL")
    if not name:
        return None, None
    from langchain.chat_models import init_chat_model  # imported late so the rules work without a provider

    return init_chat_model(name), name


def explain_text(
    text: str,
    name: str = "log",
    llm: BaseChatModel | None = None,
    model_name: str | None = None,
    max_chars: int = 12_000,
) -> tuple[RootCauseNote, Excerpt]:
    masked, counts = redact(text)
    if counts:
        log.info("Masked %s in %s before analysis", dict(counts), name)
    excerpt = build_excerpt(masked, name=name, max_chars=max_chars)
    return explain(excerpt, llm, model_name), excerpt


def explain_file(
    path: str | Path,
    llm: BaseChatModel | None = None,
    model_name: str | None = None,
    max_chars: int = 12_000,
) -> tuple[RootCauseNote, Excerpt]:
    path = Path(path)
    return explain_text(read_log(path), path.name, llm, model_name, max_chars)
