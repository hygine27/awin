"""ths_market_overview interface.

Reads the local THS market-overview snapshot file and derives the market tape
used by regime, breadth and freshness judgment.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from awin.adapters.base import FileBackedAdapter
from awin.adapters.contracts import SourceHealth
from awin.config import get_app_config


MAX_EFFECTIVE_LAG_SECONDS = 900


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def _series_trend_label(values: list[float], *, higher_is_better: bool) -> str:
    if len(values) < 2:
        return "横向"
    first = float(values[0])
    last = float(values[-1])
    delta = last - first
    if abs(delta) < 1e-9:
        return "横向"
    if higher_is_better:
        return "走强" if delta > 0 else "走弱"
    return "走弱" if delta > 0 else "走强"


def _parse_ts(value) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def derive_market_tape(payload: dict) -> dict:
    if not payload:
        return {}

    summary = payload.get("summary") or {}
    external_reference = payload.get("external_reference") or {}
    distribution = payload.get("up_down_distribution") or {}
    segments = distribution.get("segments") or {}
    panel = payload.get("intraday_panel") or {}
    metrics = panel.get("metrics") or {}

    up = int(summary.get("up") or 0)
    down = int(summary.get("down") or 0)
    implied_flat = int(distribution.get("implied_flat_count") or 0)
    total = up + down + implied_flat
    down_pressure = int(segments.get("-8%~跌停", 0) + segments.get("-8%~-6%", 0) + segments.get("-6%~-4%", 0))
    right_tail = int(segments.get("2%~4%", 0) + segments.get("4%~6%", 0) + segments.get("6%~8%", 0) + segments.get("8%~涨停", 0))
    up_limit = int(summary.get("up_limit") or 0)
    down_limit = int(summary.get("down_limit") or 0)
    ylimit_return = float(summary.get("yesterday_limitup_return_pct") or 0.0)

    captured_at = _parse_ts(payload.get("captured_at"))
    series_asof = _parse_ts(payload.get("series_asof"))
    if captured_at and series_asof:
        effective_lag_seconds = int((captured_at - series_asof).total_seconds())
    else:
        effective_lag_seconds = None
    freshness_status = "fresh" if effective_lag_seconds is not None and effective_lag_seconds <= MAX_EFFECTIVE_LAG_SECONDS else "stale"
    regime_actionable = freshness_status == "fresh" and total > 0

    up_ratio = _safe_ratio(up, total) or 0.0
    down_ratio = _safe_ratio(down, total) or 0.0
    deep_down_ratio = _safe_ratio(down_pressure, total) or 0.0
    right_tail_ratio = _safe_ratio(right_tail, total) or 0.0

    if down > up * 4 and down_limit > max(up_limit * 2, 60) and ylimit_return < -0.5:
        market_regime = "broad_risk_off"
        market_regime_label = "普跌风险释放"
    elif down > up * 2 and ylimit_return < 0 and right_tail_ratio < 0.04:
        market_regime = "weak_market_relative_strength"
        market_regime_label = "弱市中的局部活口"
    elif up > down and ylimit_return > 0 and up_limit >= down_limit:
        market_regime = "trend_expansion"
        market_regime_label = "赚钱效应扩散"
    elif abs(up - down) <= max(300, int(total * 0.08)):
        market_regime = "split_market"
        market_regime_label = "分化震荡"
    else:
        market_regime = "mixed_tape"
        market_regime_label = "分化偏弱"

    return {
        "market_regime": market_regime,
        "market_regime_label": market_regime_label,
        "regime_actionable": regime_actionable,
        "freshness": {
            "status": freshness_status,
            "effective_lag_seconds": effective_lag_seconds,
            "max_usable_lag_seconds": MAX_EFFECTIVE_LAG_SECONDS,
        },
        "breadth": {
            "up": up,
            "down": down,
            "implied_flat": implied_flat,
            "total": total,
            "up_ratio": up_ratio,
            "down_ratio": down_ratio,
            "deep_down_ratio": deep_down_ratio,
            "right_tail_ratio": right_tail_ratio,
        },
        "limit_tape": {
            "up_limit": up_limit,
            "down_limit": down_limit,
            "up_limit_trend": _series_trend_label([int(v) for v in metrics.get("today_up_limit", []) or []], higher_is_better=True),
            "down_limit_trend": _series_trend_label([int(v) for v in metrics.get("today_down_limit", []) or []], higher_is_better=False),
        },
        "yesterday_limitup_return": {
            "current_pct": ylimit_return,
            "trend_label": _series_trend_label([float(v) for v in metrics.get("yesterday_limitup_return_pct", []) or []], higher_is_better=True),
        },
        "external_reference": {
            "market_rating_score": external_reference.get("market_rating_score", summary.get("market_rating_score")),
            "investment_advice": external_reference.get("investment_advice", summary.get("investment_advice")),
        },
    }


class ThsMarketOverviewAdapter(FileBackedAdapter):
    """Load the local THS market-overview file and derive market tape fields."""

    source_name = "ths_market_overview"

    def __init__(self, path: Path | None = None) -> None:
        super().__init__(path or get_app_config().ths_market_overview_path)

    def health(self) -> SourceHealth:
        if not self.root.exists():
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=f"missing file: {self.root}")
        try:
            payload = json.loads(self.root.read_text(encoding="utf-8"))
        except Exception as exc:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=str(exc))
        market_tape = derive_market_tape(payload)
        freshness = (market_tape.get("freshness") or {}).get("status")
        if freshness == "fresh":
            return SourceHealth(source_name=self.source_name, source_status="ready", detail="ok")
        return SourceHealth(source_name=self.source_name, source_status="degraded", detail=freshness or "no_freshness")

    def load_payload(self) -> dict:
        if not self.root.exists():
            return {}
        try:
            return json.loads(self.root.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def load_market_tape(self) -> dict:
        return derive_market_tape(self.load_payload())
