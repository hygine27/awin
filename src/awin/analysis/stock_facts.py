from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from awin.adapters.contracts import DcfSnapshotRow, QmtBar1dRow, QmtSnapshotRow, ResearchCoverageRow, StockMasterRow, ThsConceptRow
from awin.market_understanding.engine import load_style_baskets
from awin.utils.symbols import normalize_stock_code


@dataclass(slots=True)
class StockFact:
    symbol: str
    stock_code: str
    stock_name: str
    exchange: str | None
    market: str | None
    industry: str | None
    last_price: float | None
    last_close: float | None
    open_price: float | None
    high_price: float | None
    low_price: float | None
    volume: float | None
    amount: float | None
    bid_volume1: float | None
    ask_volume1: float | None
    bid_ask_imbalance: float | None
    pct_chg_prev_close: float | None
    open_ret: float | None
    range_position: float | None
    turnover_rate: float | None
    volume_ratio: float | None
    amplitude: float | None
    float_mkt_cap: float | None
    total_mkt_cap: float | None
    avg_amount_20d: float | None
    elapsed_ratio: float | None
    money_pace_ratio: float | None
    main_net_inflow: float | None
    super_net: float | None
    large_net: float | None
    flow_ratio: float | None
    super_flow_ratio: float | None
    large_flow_ratio: float | None
    ret_3d: float | None
    ret_5d: float | None
    ret_10d: float | None
    ret_20d: float | None
    meta_themes: list[str] = field(default_factory=list)
    concepts: list[str] = field(default_factory=list)
    style_names: list[str] = field(default_factory=list)
    best_meta_theme: str | None = None
    best_concept: str | None = None
    research_coverage_score: float = 0.0
    onepage_path: str | None = None
    company_card_path: str | None = None
    company_card_quality_score: float = 0.0
    company_card_tracking_recommendation: str | None = None
    recent_intel_mentions: int = 0
    research_hooks: list[str] = field(default_factory=list)
    amount_rank: float = 0.0
    float_mkt_cap_rank: float = 0.0
    pct_chg_rank: float = 0.0
    volume_ratio_rank: float = 0.0
    turnover_rate_rank: float = 0.0
    amplitude_rank: float = 0.0
    main_flow_rank: float = 0.0
    super_flow_rank: float = 0.0
    large_flow_rank: float = 0.0
    ret_3d_rank: float = 0.0
    ret_10d_rank: float = 0.0
    ret_20d_rank: float = 0.0
    research_rank: float = 0.0


def _safe_div(numerator: float | int | None, denominator: float | int | None) -> float | None:
    if numerator is None or denominator in {None, 0, 0.0}:
        return None
    return float(numerator) / float(denominator)


def _to_float(value) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct_chg(base: float | None, ref: float | None) -> float | None:
    if base is None or ref in {None, 0, 0.0}:
        return None
    return float(base) / float(ref) - 1.0


def _range_position(last_price: float | None, high_price: float | None, low_price: float | None) -> float | None:
    if last_price is None or high_price is None or low_price is None:
        return None
    spread = float(high_price) - float(low_price)
    if spread <= 0:
        return None
    value = (float(last_price) - float(low_price)) / spread
    return max(0.0, min(1.0, value))


def _style_matches(style_rule: dict, master: StockMasterRow) -> bool:
    industries = set(style_rule.get("industries", []))
    market_types = set(style_rule.get("market_types", []))
    return (
        (master.industry in industries if industries else False)
        or (master.market in market_types if market_types else False)
    )


def _bid_ask_imbalance(bid_volume1: float | None, ask_volume1: float | None) -> float | None:
    if bid_volume1 is None or ask_volume1 is None:
        return None
    denom = float(bid_volume1) + float(ask_volume1)
    if denom == 0:
        return 0.0
    return (float(bid_volume1) - float(ask_volume1)) / denom


def _elapsed_ratio(snapshot_time: str | None) -> float | None:
    text = str(snapshot_time or "").strip()
    if not text:
        return None
    clock = datetime.strptime(text, "%H:%M:%S")
    morning_start = clock.replace(hour=9, minute=30, second=0)
    morning_end = clock.replace(hour=11, minute=30, second=0)
    afternoon_start = clock.replace(hour=13, minute=0, second=0)
    afternoon_end = clock.replace(hour=15, minute=0, second=0)

    elapsed = timedelta(0)
    if clock <= morning_start:
        elapsed = timedelta(0)
    elif clock <= morning_end:
        elapsed = clock - morning_start
    elif clock <= afternoon_start:
        elapsed = morning_end - morning_start
    elif clock <= afternoon_end:
        elapsed = (morning_end - morning_start) + (clock - afternoon_start)
    else:
        elapsed = (morning_end - morning_start) + (afternoon_end - afternoon_start)
    full_day = (morning_end - morning_start) + (afternoon_end - afternoon_start)
    ratio = elapsed.total_seconds() / full_day.total_seconds() if full_day.total_seconds() else 0.0
    return max(0.05, min(1.0, ratio))


def _build_avg_amount_20d_map(qmt_bars_1d: list[QmtBar1dRow], trade_date: str) -> dict[str, float]:
    grouped: dict[str, list[float]] = {}
    for row in qmt_bars_1d:
        if str(row.trade_date) >= trade_date:
            continue
        if row.amount is None:
            continue
        grouped.setdefault(str(row.symbol), []).append(float(row.amount))
    out: dict[str, float] = {}
    for symbol, amounts in grouped.items():
        if not amounts:
            continue
        tail = amounts[-20:]
        out[symbol] = sum(tail) / len(tail)
    return out


def _build_intraday_return_map(
    qmt_bars_1d: list[QmtBar1dRow],
    qmt_snapshot: list[QmtSnapshotRow],
    trade_date: str,
) -> dict[str, dict[int, float]]:
    history_by_symbol: dict[str, list[tuple[str, float]]] = {}
    for row in qmt_bars_1d:
        if str(row.trade_date) >= trade_date:
            continue
        close_price = _to_float(row.close_price)
        if close_price is None or close_price <= 0:
            continue
        history_by_symbol.setdefault(str(row.symbol), []).append((str(row.trade_date), close_price))

    out: dict[str, dict[int, float]] = {}
    for qmt in qmt_snapshot:
        current_price = _to_float(qmt.last_price)
        if current_price is None or current_price <= 0:
            continue
        history = sorted(history_by_symbol.get(qmt.symbol, []), key=lambda item: item[0])
        if not history:
            continue

        horizon_returns: dict[int, float] = {}
        closes = [item[1] for item in history]
        for horizon in (3, 5, 10, 20):
            if len(closes) < horizon:
                continue
            ref_close = closes[-horizon]
            if ref_close <= 0:
                continue
            horizon_returns[horizon] = current_price / ref_close - 1.0
        if horizon_returns:
            out[qmt.symbol] = horizon_returns
    return out


def _assign_rank(facts: list[StockFact], attr_name: str, rank_attr_name: str, *, default_rank: float = 0.0) -> None:
    valid_values = [getattr(fact, attr_name) for fact in facts if getattr(fact, attr_name) is not None]
    if not valid_values:
        for fact in facts:
            setattr(fact, rank_attr_name, default_rank)
        return

    ordered = sorted(float(value) for value in valid_values)
    rank_map: dict[float, float] = {}
    total = len(ordered)
    for idx, value in enumerate(ordered, start=1):
        rank_map[value] = idx / total

    for fact in facts:
        value = getattr(fact, attr_name)
        setattr(fact, rank_attr_name, rank_map.get(float(value), default_rank) if value is not None else default_rank)


def build_stock_facts(
    stock_master: list[StockMasterRow],
    qmt_snapshot: list[QmtSnapshotRow],
    dcf_snapshot: list[DcfSnapshotRow],
    qmt_bars_1d: list[QmtBar1dRow],
    ths_concepts: list[ThsConceptRow],
    research_coverage: list[ResearchCoverageRow],
    *,
    style_baskets_config_path: Path | None = None,
) -> list[StockFact]:
    style_baskets, _ = load_style_baskets(style_baskets_config_path)
    master_by_code = {
        normalize_stock_code(item.stock_code or item.symbol): item
        for item in stock_master
        if item.is_listed and not item.is_st
    }
    dcf_by_symbol = {item.symbol: item for item in dcf_snapshot}
    research_by_symbol = {item.symbol: item for item in research_coverage}
    avg_amount_20d_map = _build_avg_amount_20d_map(qmt_bars_1d, qmt_snapshot[0].trade_date if qmt_snapshot else "")
    intraday_return_map = _build_intraday_return_map(qmt_bars_1d, qmt_snapshot, qmt_snapshot[0].trade_date if qmt_snapshot else "")

    concepts_by_symbol: dict[str, list[ThsConceptRow]] = {}
    for row in ths_concepts:
        concepts_by_symbol.setdefault(row.symbol, []).append(row)

    facts: list[StockFact] = []
    for qmt in qmt_snapshot:
        stock_code = normalize_stock_code(qmt.stock_code or qmt.symbol)
        master = master_by_code.get(stock_code)
        if master is None:
            continue

        dcf = dcf_by_symbol.get(qmt.symbol)
        research = research_by_symbol.get(qmt.symbol)
        concept_rows = concepts_by_symbol.get(qmt.symbol, [])
        meta_themes = sorted({item.meta_theme for item in concept_rows if item.meta_theme})
        concepts = sorted({item.concept_name for item in concept_rows if item.concept_name})
        style_names = sorted([style_name for style_name, style_rule in style_baskets.items() if _style_matches(style_rule, master)])
        elapsed_ratio = _elapsed_ratio(qmt.snapshot_time)
        avg_amount_20d = avg_amount_20d_map.get(qmt.symbol)
        return_map = intraday_return_map.get(qmt.symbol, {})
        pace_base = (avg_amount_20d or 0.0) * (elapsed_ratio or 0.0)

        fact = StockFact(
            symbol=qmt.symbol,
            stock_code=stock_code,
            stock_name=master.stock_name or qmt.symbol,
            exchange=master.exchange,
            market=master.market,
            industry=master.industry,
            last_price=_to_float(qmt.last_price),
            last_close=_to_float(qmt.last_close),
            open_price=_to_float(qmt.open_price),
            high_price=_to_float(qmt.high_price),
            low_price=_to_float(qmt.low_price),
            volume=_to_float(qmt.volume),
            amount=_to_float(qmt.amount),
            bid_volume1=_to_float(qmt.bid_volume1),
            ask_volume1=_to_float(qmt.ask_volume1),
            bid_ask_imbalance=_bid_ask_imbalance(_to_float(qmt.bid_volume1), _to_float(qmt.ask_volume1)),
            pct_chg_prev_close=_pct_chg(_to_float(qmt.last_price), _to_float(qmt.last_close)),
            open_ret=_pct_chg(_to_float(qmt.last_price), _to_float(qmt.open_price)),
            range_position=_range_position(_to_float(qmt.last_price), _to_float(qmt.high_price), _to_float(qmt.low_price)),
            turnover_rate=_to_float(dcf.turnover_rate) if dcf else None,
            volume_ratio=_to_float(dcf.volume_ratio) if dcf else None,
            amplitude=_to_float(dcf.amplitude) if dcf else None,
            float_mkt_cap=_to_float(dcf.float_mkt_cap) if dcf else None,
            total_mkt_cap=_to_float(dcf.total_mkt_cap) if dcf else None,
            avg_amount_20d=avg_amount_20d,
            elapsed_ratio=elapsed_ratio,
            money_pace_ratio=_safe_div(_to_float(qmt.amount), pace_base),
            main_net_inflow=_to_float(dcf.main_net_inflow) if dcf else None,
            super_net=_to_float(dcf.super_net) if dcf else None,
            large_net=_to_float(dcf.large_net) if dcf else None,
            flow_ratio=_safe_div(_to_float(dcf.main_net_inflow) if dcf else None, _to_float(qmt.amount)),
            super_flow_ratio=_safe_div(_to_float(dcf.super_net) if dcf else None, _to_float(qmt.amount)),
            large_flow_ratio=_safe_div(_to_float(dcf.large_net) if dcf else None, _to_float(qmt.amount)),
            ret_3d=return_map.get(3, _to_float(dcf.ret_3d) if dcf else None),
            ret_5d=return_map.get(5, _to_float(dcf.ret_5d) if dcf else None),
            ret_10d=return_map.get(10, _to_float(dcf.ret_10d) if dcf else None),
            ret_20d=return_map.get(20, _to_float(dcf.ret_20d) if dcf else None),
            meta_themes=meta_themes,
            concepts=concepts,
            style_names=style_names,
            best_meta_theme=meta_themes[0] if meta_themes else None,
            best_concept=concepts[0] if concepts else None,
            research_coverage_score=research.research_coverage_score if research else 0.0,
            onepage_path=research.onepage_path if research else None,
            company_card_path=research.company_card_path if research else None,
            company_card_quality_score=research.company_card_quality_score if research else 0.0,
            company_card_tracking_recommendation=research.company_card_tracking_recommendation if research else None,
            recent_intel_mentions=int(research.recent_intel_mentions) if research else 0,
            research_hooks=list(research.research_hooks) if research else [],
        )
        facts.append(fact)

    _assign_rank(facts, "amount", "amount_rank")
    _assign_rank(facts, "float_mkt_cap", "float_mkt_cap_rank")
    _assign_rank(facts, "pct_chg_prev_close", "pct_chg_rank")
    _assign_rank(facts, "volume_ratio", "volume_ratio_rank")
    _assign_rank(facts, "turnover_rate", "turnover_rate_rank")
    _assign_rank(facts, "amplitude", "amplitude_rank")
    _assign_rank(facts, "flow_ratio", "main_flow_rank", default_rank=0.5)
    _assign_rank(facts, "super_flow_ratio", "super_flow_rank", default_rank=0.5)
    _assign_rank(facts, "large_flow_ratio", "large_flow_rank", default_rank=0.5)
    _assign_rank(facts, "ret_3d", "ret_3d_rank")
    _assign_rank(facts, "ret_10d", "ret_10d_rank")
    _assign_rank(facts, "ret_20d", "ret_20d_rank")
    _assign_rank(facts, "research_coverage_score", "research_rank")
    return facts
