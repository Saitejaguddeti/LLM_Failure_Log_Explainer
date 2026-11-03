"""Mask credentials and personal data before any log text leaves the machine.

Logs pick up secrets in all sorts of ways: a connection string printed by a DAG, a token in a curl
command, an access key in an environment dump. Everything sent to the model goes through redact() first.
"""

from __future__ import annotations

import re
from collections import Counter

_RULES: list[tuple[str, re.Pattern[str], str]] = [
    # user:password@ inside any URL or connection string
    ("url_password", re.compile(r"(\b[a-zA-Z][a-zA-Z0-9+.-]*://[^:/\s@]+):([^@\s]+)@"), r"\1:[REDACTED]@"),
    ("aws_access_key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), "[AWS_ACCESS_KEY]"),
    (
        "secret_value",
        re.compile(
            # the lookbehind (instead of \b) also catches names like AWS_SECRET_ACCESS_KEY and AUTH_TOKEN
            r"(?i)(?<![a-z0-9])(password|passwd|pwd|secret|secret_access_key|api[_-]?key|access[_-]?token|token)"
            r"(\s*[=:]\s*['\"]?)([^\s'\",;&]+)"
        ),
        r"\1\2[REDACTED]",
    ),
    ("bearer_token", re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{8,}"), "Bearer [REDACTED]"),
    ("email", re.compile(r"\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b"), "[EMAIL]"),
    # 9+ digit runs that stand alone look like account or card numbers; ids inside words are left alone
    ("long_number", re.compile(r"(?<![\w.:/-])\d{9,}(?![\w.:/-])"), "[NUMBER]"),
]


def redact(text: str) -> tuple[str, Counter]:
    """Return the masked text and how many of each kind of value were masked."""
    counts: Counter = Counter()
    for name, pattern, replacement in _RULES:
        text, n = pattern.subn(replacement, text)
        if n:
            counts[name] += n
    return text, counts
