"""Read Airflow and Spark failure logs and write plain-English root-cause notes."""

from .core import explain_file, explain_text, load_model
from .explainer import RootCauseNote, rule_based_note
from .parsing import Excerpt, build_excerpt, read_log

__all__ = [
    "Excerpt",
    "RootCauseNote",
    "build_excerpt",
    "explain_file",
    "explain_text",
    "load_model",
    "read_log",
    "rule_based_note",
]
__version__ = "0.1.0"
