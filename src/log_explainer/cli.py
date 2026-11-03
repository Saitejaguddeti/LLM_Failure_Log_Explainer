"""Command line: explain-log path/to/failed_task.log [more logs...]"""

from __future__ import annotations

import argparse
import glob
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from .core import explain_file, load_model
from .report import to_json, to_markdown


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="explain-log",
        description="Read Airflow or Spark failure logs and write a plain-English root-cause note.",
    )
    parser.add_argument("logs", nargs="+", help="log files (.log, .txt, or .gz); wildcards such as samples/*.log work")
    parser.add_argument("--model", help="chat model as provider:model; overrides LLM_MODEL")
    parser.add_argument("--no-llm", action="store_true", help="use only the built-in failure patterns")
    parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    parser.add_argument("--out", type=Path, help="write one note per log into this folder instead of printing")
    parser.add_argument("--max-chars", type=int, default=12_000, help="size cap for the excerpt sent to the model")
    parser.add_argument("--show-excerpt", action="store_true", help="print the redacted excerpt and stop")
    args = parser.parse_args(argv)

    load_dotenv()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    llm, model_name = (None, None) if args.no_llm or args.show_excerpt else load_model(args.model)
    if args.out:
        args.out.mkdir(parents=True, exist_ok=True)

    status = 0
    for path in _expand(args.logs):
        if not path.is_file():
            print(f"explain-log: {path}: no such file", file=sys.stderr)
            status = 1
            continue
        note, excerpt = explain_file(path, llm, model_name, args.max_chars)
        if args.show_excerpt:
            print(excerpt.text)
            continue
        rendered = to_json(note, excerpt) if args.format == "json" else to_markdown(note, excerpt)
        if args.out:
            stem = path.name.removesuffix(".gz").removesuffix(".log")
            target = args.out / f"{stem}.rootcause.{'json' if args.format == 'json' else 'md'}"
            target.write_text(rendered, encoding="utf-8")
            print(f"{path} -> {target}")
        else:
            print(rendered)
    return status


def _expand(patterns: list[str]) -> list[Path]:
    """Expand wildcards here, because PowerShell and cmd pass samples/*.log through as-is."""
    paths: list[Path] = []
    for pattern in patterns:
        hits = sorted(glob.glob(pattern)) if any(ch in pattern for ch in "*?[") and not Path(pattern).exists() else []
        paths += [Path(hit) for hit in hits] or [Path(pattern)]
    return paths


if __name__ == "__main__":
    sys.exit(main())
