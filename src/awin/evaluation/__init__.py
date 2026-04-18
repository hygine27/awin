"""Evaluation helpers for awin."""

from awin.evaluation.day_summary import build_day_summary, build_day_summary_json, build_day_summary_markdown
from awin.evaluation.outcomes import compute_post_trade_outcomes, load_post_trade_outcomes
from awin.evaluation.parity import build_parity_report_markdown, compare_v1_v2_snapshots

__all__ = [
    "build_day_summary",
    "build_day_summary_json",
    "build_day_summary_markdown",
    "compute_post_trade_outcomes",
    "load_post_trade_outcomes",
    "build_parity_report_markdown",
    "compare_v1_v2_snapshots",
]
"""Evaluation layer for awin."""
