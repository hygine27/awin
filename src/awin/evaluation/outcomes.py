from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from statistics import mean

from awin.adapters import QmtBar1dAdapter, QmtBar1dRow
from awin.adapters.contracts import SourceHealth


def _parse_date(text: str) -> date:
    return date.fromisoformat(text)


def _build_range(trade_date: str, lookahead_days: int = 10) -> tuple[str, str]:
    start = _parse_date(trade_date)
    end = start + timedelta(days=lookahead_days * 3)
    return trade_date, end.isoformat()


def _state_label(symbol_row: dict) -> str:
    latest_display_bucket = symbol_row.get("latest_display_bucket")
    latest_risk_tag = symbol_row.get("latest_risk_tag")
    if latest_display_bucket in {"core_anchor", "new_long", "catchup"}:
        return str(latest_display_bucket)
    if latest_risk_tag:
        return "risk"
    return "exited"


def _avg(values: list[float | None]) -> float | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return float(mean(filtered))


def _safe_ret(base: float | None, target: float | None) -> float | None:
    if base in {None, 0} or target is None:
        return None
    return (float(target) / float(base)) - 1.0


def _series_to_outcome(rows: list[QmtBar1dRow], trade_date: str) -> dict[str, float | str | None]:
    ordered = sorted(rows, key=lambda item: item.trade_date)
    dates = [item.trade_date for item in ordered]
    if trade_date not in dates:
        return {"outcome_status": "missing_trigger_bar"}

    base_idx = dates.index(trade_date)
    base_row = ordered[base_idx]
    next_idx = base_idx + 1

    def _close_ret(offset: int) -> float | None:
        target_idx = base_idx + offset
        if target_idx >= len(ordered):
            return None
        return _safe_ret(base_row.close_price, ordered[target_idx].close_price)

    next_open_ret = None
    if next_idx < len(ordered):
        next_open_ret = _safe_ret(base_row.close_price, ordered[next_idx].open_price)

    close_ret_1d = _close_ret(1)
    close_ret_3d = _close_ret(3)
    close_ret_5d = _close_ret(5)
    has_future_window = any(value is not None for value in [next_open_ret, close_ret_1d, close_ret_3d, close_ret_5d])

    return {
        "outcome_status": "ready" if has_future_window else "trigger_only",
        "entry_close_price": base_row.close_price,
        "next_open_ret": next_open_ret,
        "close_ret_1d": close_ret_1d,
        "close_ret_3d": close_ret_3d,
        "close_ret_5d": close_ret_5d,
    }


def compute_post_trade_outcomes(
    trade_date: str,
    active_symbols: list[dict],
    bar_rows: list[QmtBar1dRow],
    *,
    source_health: SourceHealth | None = None,
) -> dict:
    by_symbol: dict[str, list[QmtBar1dRow]] = defaultdict(list)
    for row in bar_rows:
        by_symbol[row.symbol].append(row)

    symbol_outcomes: dict[str, dict] = {}
    enriched_symbols: list[dict] = []
    for item in active_symbols:
        symbol = item["symbol"]
        outcome = _series_to_outcome(by_symbol.get(symbol, []), trade_date)
        symbol_outcomes[symbol] = outcome
        enriched_symbols.append({**item, **outcome})

    cohorts: dict[str, list[dict]] = defaultdict(list)
    cohorts["all_active"] = enriched_symbols
    cohorts["persistent"] = [item for item in enriched_symbols if item.get("mention_count", 0) >= 2]
    for item in enriched_symbols:
        cohorts[_state_label(item)].append(item)

    cohort_summaries = []
    for cohort_name in ["all_active", "persistent", "core_anchor", "new_long", "catchup", "risk", "exited"]:
        items = cohorts.get(cohort_name, [])
        if not items:
            continue
        cohort_summaries.append(
            {
                "cohort": cohort_name,
                "sample_count": len(items),
                "trigger_count": sum(1 for item in items if item.get("entry_close_price") is not None),
                "next_open_count": sum(1 for item in items if item.get("next_open_ret") is not None),
                "close_1d_count": sum(1 for item in items if item.get("close_ret_1d") is not None),
                "close_3d_count": sum(1 for item in items if item.get("close_ret_3d") is not None),
                "close_5d_count": sum(1 for item in items if item.get("close_ret_5d") is not None),
                "avg_next_open_ret": _avg([item.get("next_open_ret") for item in items]),
                "avg_close_ret_1d": _avg([item.get("close_ret_1d") for item in items]),
                "avg_close_ret_3d": _avg([item.get("close_ret_3d") for item in items]),
                "avg_close_ret_5d": _avg([item.get("close_ret_5d") for item in items]),
            }
        )

    status = source_health.source_status if source_health is not None else ("ready" if bar_rows else "missing")
    detail = source_health.detail if source_health is not None else ("ok" if bar_rows else "no_daily_bar_rows")
    return {
        "source_status": status,
        "detail": detail,
        "bar_row_count": len(bar_rows),
        "symbol_count": len(active_symbols),
        "symbols_with_trigger_bar": sum(1 for item in enriched_symbols if item.get("entry_close_price") is not None),
        "symbols_with_next_open": sum(1 for item in enriched_symbols if item.get("next_open_ret") is not None),
        "symbols_with_close_1d": sum(1 for item in enriched_symbols if item.get("close_ret_1d") is not None),
        "symbols_with_close_3d": sum(1 for item in enriched_symbols if item.get("close_ret_3d") is not None),
        "symbols_with_close_5d": sum(1 for item in enriched_symbols if item.get("close_ret_5d") is not None),
        "cohort_summaries": cohort_summaries,
        "symbol_outcomes": symbol_outcomes,
        "active_symbols": enriched_symbols,
    }


def load_post_trade_outcomes(trade_date: str, active_symbols: list[dict]) -> dict:
    start_date, end_date = _build_range(trade_date)
    symbols = sorted({str(item["symbol"]) for item in active_symbols if item.get("symbol")})
    adapter = QmtBar1dAdapter()
    bar_rows, health = adapter.load_rows_with_health(symbols, start_date, end_date)
    return compute_post_trade_outcomes(trade_date, active_symbols, bar_rows, source_health=health)
