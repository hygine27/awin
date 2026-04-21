from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from awin.adapters import (
    DcfHqZjSnapshotAdapter,
    QmtAshareSnapshot5mAdapter,
    QmtBar1dMetricsAdapter,
    ResearchCoverageAdapter,
    SnapshotRequest,
    StockMasterAdapter,
    ThsCliHotConceptAdapter,
    ThsConceptAdapter,
    ThsMarketOverviewAdapter,
    TsDailyBasicAdapter,
    TsFinaIndicatorAdapter,
    TsIndexMemberAllAdapter,
    TsMoneyflowCntThsAdapter,
    TsMoneyflowDcAdapter,
    TsMoneyflowIndThsAdapter,
    TsMoneyflowMktDcAdapter,
    TsMoneyflowThsAdapter,
    TsStockBasicAdapter,
    TsStyleDailyMetricsAdapter,
)
from awin.analysis import StockFact, build_stock_facts
from awin.alerting.diff import build_alert_output
from awin.contracts.m0 import (
    AlertMaterial,
    CandidateItem,
    M0SnapshotBundle,
    MarketEvidenceBundle,
    MarketFundEvidence,
    RunContext,
    StockEvidenceBundle,
    StockEvidenceItem,
    ThemeEvidenceItem,
)
from awin.market_understanding import compute_market_understanding
from awin.opportunity_discovery import PreviousBullState, compute_opportunity_discovery
from awin.risk_surveillance import compute_risk_surveillance
from awin.storage.db import connect_sqlite, init_db
from awin.fund_flow_profile import FundFlowSnapshot, build_fund_flow_snapshot
from awin.style_profile import StyleProfile, build_style_profiles, persist_style_profiles


@dataclass(frozen=True)
class M0BuildResult:
    bundle: M0SnapshotBundle
    stock_facts: list[StockFact]
    style_profiles: list[StyleProfile] = field(default_factory=list)
    fund_flow_snapshot: FundFlowSnapshot | None = None
    source_health: dict[str, dict] = field(default_factory=dict)


def _safe_ratio(numerator: float | int | None, denominator: float | int | None) -> float | None:
    if numerator is None or denominator in {None, 0, 0.0}:
        return None
    return float(numerator) / float(denominator)


def _average(values: list[float | None]) -> float | None:
    valid = [float(value) for value in values if value is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _sum_or_none(values: list[float | None]) -> float | None:
    valid = [float(value) for value in values if value is not None]
    if not valid:
        return None
    return sum(valid)


def _theme_rank_map(market) -> dict[str, int]:
    mapping = {str(item.meta_theme): int(item.rank or 99) for item in market.top_meta_themes if str(item.meta_theme).strip()}
    if mapping:
        return mapping
    return {
        str(item.meta_theme): idx
        for idx, item in enumerate(market.top_meta_themes, start=1)
        if str(item.meta_theme).strip()
    }


def _theme_primary_concepts_map(market) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    for item in market.top_meta_themes:
        theme = str(item.meta_theme or "").strip()
        if not theme:
            continue
        mapping[theme] = {str(concept).strip() for concept in item.strongest_concepts if str(concept).strip()}
    return mapping


def _primary_theme_for_fact(
    fact: StockFact,
    *,
    theme_rank_map: dict[str, int],
    primary_concepts_map: dict[str, set[str]],
) -> str | None:
    if not fact.meta_themes:
        return fact.best_meta_theme

    best_theme: str | None = None
    best_support = -1
    best_rank = 999
    for theme in fact.meta_themes:
        theme_name = str(theme or "").strip()
        if not theme_name:
            continue
        primary_concepts = primary_concepts_map.get(theme_name, set())
        support = sum(1 for concept in fact.concepts if concept in primary_concepts)
        rank = int(theme_rank_map.get(theme_name, 99) or 99)
        if support > best_support or (support == best_support and rank < best_rank):
            best_theme = theme_name
            best_support = support
            best_rank = rank

    return best_theme or fact.best_meta_theme


def _build_primary_theme_groups(stock_facts: list[StockFact], market) -> dict[str, list[StockFact]]:
    theme_rank_map = _theme_rank_map(market)
    primary_concepts_map = _theme_primary_concepts_map(market)
    grouped: dict[str, list[StockFact]] = {}
    for fact in stock_facts:
        primary_theme = _primary_theme_for_fact(
            fact,
            theme_rank_map=theme_rank_map,
            primary_concepts_map=primary_concepts_map,
        )
        if not primary_theme:
            continue
        grouped.setdefault(primary_theme, []).append(fact)
    return grouped


MORNING_SESSION_START_MINUTE = 9 * 60 + 30
MORNING_SESSION_FIRST_SLOT_MINUTE = 9 * 60 + 35
MORNING_SESSION_END_MINUTE = 11 * 60 + 30
AFTERNOON_SESSION_START_MINUTE = 13 * 60
AFTERNOON_SESSION_FIRST_SLOT_MINUTE = 13 * 60 + 5
AFTERNOON_SESSION_END_MINUTE = 15 * 60
THEME_FLOW_WINDOW_MINUTES = 15


def _clock_to_minutes(clock_text: str | None) -> int | None:
    text = str(clock_text or "").strip()
    if not text:
        return None
    parts = text.split(":")
    if len(parts) < 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    return hour * 60 + minute


def _minutes_to_clock(total_minutes: int) -> str:
    hour = max(0, total_minutes // 60)
    minute = max(0, total_minutes % 60)
    return f"{hour:02d}:{minute:02d}:00"


def _theme_flow_comparison_request(request: SnapshotRequest) -> tuple[str | None, SnapshotRequest | None]:
    clock_minutes = _clock_to_minutes(request.snapshot_time)
    if clock_minutes is None:
        return None, None

    if MORNING_SESSION_START_MINUTE < clock_minutes <= MORNING_SESSION_END_MINUTE:
        target_minutes = clock_minutes - THEME_FLOW_WINDOW_MINUTES
        if target_minutes >= MORNING_SESSION_FIRST_SLOT_MINUTE:
            snapshot_time = _minutes_to_clock(target_minutes)
            return (
                f"近{THEME_FLOW_WINDOW_MINUTES}分钟",
                SnapshotRequest(
                    trade_date=request.trade_date,
                    snapshot_time=snapshot_time,
                    analysis_snapshot_ts=f"{request.trade_date}T{snapshot_time}",
                ),
            )
        return None, None

    if AFTERNOON_SESSION_START_MINUTE < clock_minutes <= AFTERNOON_SESSION_END_MINUTE:
        target_minutes = clock_minutes - THEME_FLOW_WINDOW_MINUTES
        if target_minutes >= AFTERNOON_SESSION_FIRST_SLOT_MINUTE:
            snapshot_time = _minutes_to_clock(target_minutes)
            return (
                f"近{THEME_FLOW_WINDOW_MINUTES}分钟",
                SnapshotRequest(
                    trade_date=request.trade_date,
                    snapshot_time=snapshot_time,
                    analysis_snapshot_ts=f"{request.trade_date}T{snapshot_time}",
                ),
            )

        snapshot_time = _minutes_to_clock(MORNING_SESSION_END_MINUTE)
        return (
            "午后以来",
            SnapshotRequest(
                trade_date=request.trade_date,
                snapshot_time=snapshot_time,
                analysis_snapshot_ts=f"{request.trade_date}T{snapshot_time}",
            ),
        )

    return None, None


def _build_t1_market_fund_evidence(fund_flow_snapshot: FundFlowSnapshot | None) -> MarketFundEvidence | None:
    market_flow = fund_flow_snapshot.market_profile if fund_flow_snapshot is not None else None
    if market_flow is None or market_flow.net_amount_1d is None:
        return None
    return MarketFundEvidence(
        scope="t1_market",
        asof=market_flow.trade_date,
        net_amount=market_flow.net_amount_1d,
        net_amount_rate=market_flow.net_amount_rate_1d,
        super_large_net=market_flow.super_large_net_1d,
        large_order_net=market_flow.large_order_net_1d,
        inflow_streak_days=market_flow.inflow_streak_days,
        outflow_streak_days=market_flow.outflow_streak_days,
    )


def _build_intraday_market_fund_evidence(stock_facts: list[StockFact], run_context: RunContext) -> MarketFundEvidence | None:
    amount_sum = _sum_or_none([item.amount for item in stock_facts])
    main_flow_sum = _sum_or_none([item.main_net_inflow for item in stock_facts])
    if amount_sum is None and main_flow_sum is None:
        return None
    flow_covered = [item for item in stock_facts if item.main_net_inflow is not None]
    positive_flow_count = sum(1 for item in flow_covered if float(item.main_net_inflow or 0.0) > 0)
    positive_stock_count = sum(1 for item in stock_facts if item.pct_chg_prev_close is not None and float(item.pct_chg_prev_close) > 0)
    return MarketFundEvidence(
        scope="intraday_market",
        asof=run_context.analysis_snapshot_ts,
        net_amount=main_flow_sum,
        net_amount_rate=_safe_ratio(main_flow_sum, amount_sum),
        super_large_net=_sum_or_none([item.super_net for item in stock_facts]),
        large_order_net=_sum_or_none([item.large_net for item in stock_facts]),
        positive_stock_ratio=_safe_ratio(positive_stock_count, len(stock_facts)),
        positive_main_flow_ratio=_safe_ratio(positive_flow_count, len(flow_covered)),
    )


def _leader_stocks_for_theme(theme_members: list[StockFact]) -> list[str]:
    members = list(theme_members)
    members.sort(
        key=lambda item: (
            float(item.pct_chg_prev_close) if item.pct_chg_prev_close is not None else -999.0,
            float(item.amount) if item.amount is not None else 0.0,
        ),
        reverse=True,
    )
    return [f"{item.stock_name}({item.symbol})" for item in members[:3]]


def _build_theme_evidence_items(
    stock_facts: list[StockFact],
    market,
    fund_flow_snapshot: FundFlowSnapshot | None,
    *,
    comparison_window_label: str | None = None,
    prior_main_net_inflow_by_symbol: dict[str, float] | None = None,
) -> list[ThemeEvidenceItem]:
    concept_flow_map = {
        item.concept_name: item
        for item in (fund_flow_snapshot.concept_profiles if fund_flow_snapshot is not None else [])
        if str(item.concept_name).strip()
    }
    primary_theme_groups = _build_primary_theme_groups(stock_facts, market)
    items: list[ThemeEvidenceItem] = []
    for theme in market.top_meta_themes[:5]:
        members = primary_theme_groups.get(str(theme.meta_theme), [])
        flow_covered = [item for item in members if item.main_net_inflow is not None]
        flow_strength_covered = [
            item for item in members if item.main_net_inflow is not None and item.amount not in {None, 0, 0.0}
        ]
        positive_flow_count = sum(1 for item in flow_covered if float(item.main_net_inflow or 0.0) > 0)
        positive_stock_count = sum(1 for item in members if item.pct_chg_prev_close is not None and float(item.pct_chg_prev_close) > 0)
        comparable_members = [
            item
            for item in members
            if item.main_net_inflow is not None
            and prior_main_net_inflow_by_symbol is not None
            and item.symbol in prior_main_net_inflow_by_symbol
        ]
        comparison_main_net_inflow_delta = None
        if comparable_members:
            current_sum = _sum_or_none([item.main_net_inflow for item in comparable_members])
            prior_sum = _sum_or_none([prior_main_net_inflow_by_symbol.get(item.symbol) for item in comparable_members])
            if current_sum is not None and prior_sum is not None:
                comparison_main_net_inflow_delta = current_sum - prior_sum
        concept_flow_1d_map = {
            concept_name: float(concept_flow_map[concept_name].net_amount_1d)
            for concept_name in theme.strongest_concepts[:3]
            if concept_name in concept_flow_map and concept_flow_map[concept_name].net_amount_1d is not None
        }
        items.append(
            ThemeEvidenceItem(
                meta_theme=theme.meta_theme,
                rank=theme.rank,
                stock_count=len(members),
                avg_pct_chg_prev_close=_average([item.pct_chg_prev_close for item in members]),
                positive_stock_ratio=_safe_ratio(positive_stock_count, len(members)),
                current_main_net_inflow_sum=_sum_or_none([item.main_net_inflow for item in members]),
                current_main_flow_rate=_safe_ratio(
                    _sum_or_none([item.main_net_inflow for item in flow_strength_covered]),
                    _sum_or_none([item.amount for item in flow_strength_covered]),
                ),
                current_positive_main_flow_ratio=_safe_ratio(positive_flow_count, len(flow_covered)),
                comparison_window_label=comparison_window_label if comparison_main_net_inflow_delta is not None else None,
                comparison_main_net_inflow_delta=comparison_main_net_inflow_delta,
                strongest_concepts=list(theme.strongest_concepts[:3]),
                strongest_concept_flow_1d_map=concept_flow_1d_map,
                leader_stocks=_leader_stocks_for_theme(members),
            )
        )
    return items


def _merge_candidate_items(opportunity, risk) -> list[CandidateItem]:
    merged: list[CandidateItem] = []
    seen_symbols: set[str] = set()
    for items in (
        opportunity.core_anchor_watchlist,
        opportunity.new_long_watchlist,
        opportunity.catchup_watchlist,
        risk.short_watchlist,
    ):
        for item in items:
            if item.symbol in seen_symbols:
                continue
            seen_symbols.add(item.symbol)
            merged.append(item)
    return merged


def _stock_role(item: CandidateItem) -> str:
    if item.risk_tag:
        return str(item.risk_tag)
    return str(item.display_bucket)


def _build_stock_evidence_bundle(stock_facts: list[StockFact], market, opportunity, risk) -> StockEvidenceBundle:
    fact_by_symbol = {item.symbol: item for item in stock_facts}
    focus_stocks: list[StockEvidenceItem] = []
    for item in _merge_candidate_items(opportunity, risk):
        fact = fact_by_symbol.get(item.symbol)
        if fact is None:
            continue
        focus_stocks.append(
            StockEvidenceItem(
                symbol=item.symbol,
                stock_name=item.stock_name,
                role=_stock_role(item),
                display_bucket=item.display_bucket,
                confidence_score=float(item.confidence_score),
                best_meta_theme=item.best_meta_theme or fact.best_meta_theme,
                best_concept=item.best_concept or fact.best_concept,
                theme_rank=market.meta_theme_rank_map.get(item.best_meta_theme or fact.best_meta_theme or ""),
                concept_overlay_rank=market.concept_overlay_rank_map.get(item.best_concept or fact.best_concept or ""),
                risk_tag=item.risk_tag,
                reason=item.reason,
                themes=list(item.themes or fact.meta_themes[:3]),
                style_names=list(fact.style_names),
                composite_style_labels=list(fact.composite_style_labels),
                pct_chg_prev_close=fact.pct_chg_prev_close,
                open_ret=fact.open_ret,
                range_position=fact.range_position,
                amount=fact.amount,
                money_pace_ratio=fact.money_pace_ratio,
                volume_ratio=fact.volume_ratio,
                turnover_rate=fact.turnover_rate,
                amplitude=fact.amplitude,
                main_net_inflow=fact.main_net_inflow,
                super_net=fact.super_net,
                large_net=fact.large_net,
                ret_3d=fact.ret_3d,
                ret_10d=fact.ret_10d,
                ret_20d=fact.ret_20d,
                main_net_amount_1d=fact.main_net_amount_1d,
                main_net_amount_5d_sum=fact.main_net_amount_5d_sum,
                outflow_streak_days=int(fact.outflow_streak_days or 0),
                price_flow_divergence_flag=bool(fact.price_flow_divergence_flag),
                research_coverage_score=float(fact.research_coverage_score or 0.0),
                research_hooks=list(item.research_hooks or fact.research_hooks),
                candidate_metadata=dict(item.metadata or {}),
            )
        )
    return StockEvidenceBundle(focus_stocks=focus_stocks)


def _build_market_evidence_bundle(
    run_context: RunContext,
    market,
    stock_facts: list[StockFact],
    fund_flow_snapshot: FundFlowSnapshot | None,
    source_health: dict[str, dict],
    *,
    comparison_window_label: str | None = None,
    prior_main_net_inflow_by_symbol: dict[str, float] | None = None,
) -> MarketEvidenceBundle:
    return MarketEvidenceBundle(
        confirmed_style=market.confirmed_style,
        latest_status=market.latest_status,
        latest_dominant_style=market.latest_dominant_style,
        market_regime=market.market_regime,
        summary_line=market.summary_line,
        evidence_lines=list(market.evidence_lines),
        top_styles=list(market.top_styles),
        top_meta_themes=list(market.top_meta_themes),
        strongest_concepts=list(market.strongest_concepts),
        acceleration_concepts=list(market.acceleration_concepts),
        t1_market_fund=_build_t1_market_fund_evidence(fund_flow_snapshot),
        intraday_market_fund=_build_intraday_market_fund_evidence(stock_facts, run_context),
        theme_evidence=_build_theme_evidence_items(
            stock_facts,
            market,
            fund_flow_snapshot,
            comparison_window_label=comparison_window_label,
            prior_main_net_inflow_by_symbol=prior_main_net_inflow_by_symbol,
        ),
        source_health=source_health,
    )


def build_run_id(trade_date: str, snapshot_time: str, round_seq: int) -> str:
    compact_time = snapshot_time.replace(":", "")
    return f"{trade_date}-{compact_time}-r{round_seq:02d}"


def build_run_context(trade_date: str, snapshot_time: str, round_seq: int) -> RunContext:
    return RunContext(
        run_id=build_run_id(trade_date, snapshot_time, round_seq),
        trade_date=trade_date,
        snapshot_time=snapshot_time,
        analysis_snapshot_ts=f"{trade_date}T{snapshot_time}",
        round_seq=round_seq,
    )


def _run_parallel_loaders(
    loaders: dict[str, Callable[[], object]],
    *,
    max_workers: int = 4,
) -> dict[str, object]:
    if not loaders:
        return {}

    worker_count = max(1, min(len(loaders), max_workers))
    completed_results: dict[str, object] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {executor.submit(loader): name for name, loader in loaders.items()}
        for future in as_completed(future_map):
            name = future_map[future]
            completed_results[name] = future.result()
    return {name: completed_results[name] for name in loaders}


def build_m0_snapshot_bundle(
    request: SnapshotRequest,
    *,
    round_seq: int,
    previous_material: AlertMaterial | None = None,
    previous_bull_state: dict[str, PreviousBullState] | None = None,
) -> M0BuildResult:
    run_context = build_run_context(request.trade_date, request.snapshot_time, round_seq)

    stock_master_adapter = StockMasterAdapter()
    ths_adapter = ThsConceptAdapter()
    ths_cli_hot_adapter = ThsCliHotConceptAdapter()
    research_adapter = ResearchCoverageAdapter()
    qmt_ashare_snapshot_5m_adapter = QmtAshareSnapshot5mAdapter()
    qmt_bar_1d_metrics_adapter = QmtBar1dMetricsAdapter()
    dcf_hq_zj_snapshot_adapter = DcfHqZjSnapshotAdapter()
    market_overview_adapter = ThsMarketOverviewAdapter()
    ts_stock_basic_adapter = TsStockBasicAdapter()
    ts_daily_basic_adapter = TsDailyBasicAdapter()
    ts_index_member_all_adapter = TsIndexMemberAllAdapter()
    ts_style_daily_metrics_adapter = TsStyleDailyMetricsAdapter()
    ts_fina_indicator_adapter = TsFinaIndicatorAdapter()
    ts_moneyflow_ths_adapter = TsMoneyflowThsAdapter()
    ts_moneyflow_dc_adapter = TsMoneyflowDcAdapter()
    ts_moneyflow_cnt_ths_adapter = TsMoneyflowCntThsAdapter()
    ts_moneyflow_ind_ths_adapter = TsMoneyflowIndThsAdapter()
    ts_moneyflow_mkt_dc_adapter = TsMoneyflowMktDcAdapter()

    stock_master = stock_master_adapter.load_rows()
    ths_concepts = ths_adapter.load_rows(request)
    ths_hot_concepts = ths_cli_hot_adapter.load_rows(request)
    research = research_adapter.load_rows(request)
    qmt_rows = qmt_ashare_snapshot_5m_adapter.load_rows(request)
    dcf_rows, dcf_health = dcf_hq_zj_snapshot_adapter.load_rows_with_health(request)
    market_tape = market_overview_adapter.load_market_tape()
    ts_stock_basic_rows, ts_stock_basic_health = ts_stock_basic_adapter.load_rows_with_health()
    ts_daily_basic_rows, ts_daily_basic_health = ts_daily_basic_adapter.load_rows_with_health(request.trade_date)
    ts_index_member_all_rows, ts_index_member_all_health = ts_index_member_all_adapter.load_rows_with_health(request.trade_date)
    ts_style_daily_metric_rows, ts_style_daily_metrics_health = ts_style_daily_metrics_adapter.load_rows_with_health(
        request.trade_date
    )
    ts_fina_indicator_rows, ts_fina_indicator_health = ts_fina_indicator_adapter.load_rows_with_health(request.trade_date)
    ts_moneyflow_ths_rows, ts_moneyflow_ths_health = ts_moneyflow_ths_adapter.load_rows_with_health(request.trade_date)
    ts_moneyflow_dc_rows, ts_moneyflow_dc_health = ts_moneyflow_dc_adapter.load_rows_with_health(request.trade_date)
    ts_moneyflow_cnt_ths_rows, ts_moneyflow_cnt_ths_health = ts_moneyflow_cnt_ths_adapter.load_rows_with_health(request.trade_date)
    ts_moneyflow_ind_ths_rows, ts_moneyflow_ind_ths_health = ts_moneyflow_ind_ths_adapter.load_rows_with_health(request.trade_date)
    ts_moneyflow_mkt_dc_rows, ts_moneyflow_mkt_dc_health = ts_moneyflow_mkt_dc_adapter.load_rows_with_health(request.trade_date)

    trade_day = date.fromisoformat(request.trade_date)
    derived_results = _run_parallel_loaders(
        {
            "qmt_bar_1d_metrics": lambda: qmt_bar_1d_metrics_adapter.load_rows_with_health(
                [item.symbol for item in qmt_rows],
                start_date=(trade_day - timedelta(days=45)).isoformat(),
                trade_date=request.trade_date,
            ),
            "style_profiles": lambda: build_style_profiles(
                ts_stock_basic_rows,
                ts_daily_basic_rows,
                ts_index_member_all_rows,
                daily_metric_rows=ts_style_daily_metric_rows,
                fina_indicator_rows=ts_fina_indicator_rows,
                trade_date=request.trade_date.replace("-", ""),
            ),
            "fund_flow_snapshot": lambda: build_fund_flow_snapshot(
                ts_moneyflow_ths_rows,
                ts_moneyflow_dc_rows,
                ts_moneyflow_cnt_ths_rows,
                ts_moneyflow_ind_ths_rows,
                ts_moneyflow_mkt_dc_rows,
            ),
        }
    )

    qmt_bar_1d_metric_rows, qmt_bar_1d_metrics_health = derived_results["qmt_bar_1d_metrics"]
    style_profiles = derived_results["style_profiles"]
    fund_flow_snapshot = derived_results["fund_flow_snapshot"]
    style_profile_rows = [item.to_dict() for item in style_profiles]
    comparison_window_label, comparison_request = _theme_flow_comparison_request(request)
    prior_main_net_inflow_by_symbol: dict[str, float] | None = None
    if comparison_request is not None:
        prior_dcf_rows, _ = dcf_hq_zj_snapshot_adapter.load_rows_with_health(comparison_request)
        prior_main_net_inflow_by_symbol = {
            str(item.symbol): float(item.main_net_inflow)
            for item in prior_dcf_rows
            if str(item.symbol).strip() and item.main_net_inflow is not None
        }

    analysis_results = _run_parallel_loaders(
        {
            "market_understanding": lambda: compute_market_understanding(
                stock_master,
                qmt_rows,
                dcf_rows,
                ths_concepts,
                ths_hot_concepts=ths_hot_concepts,
                market_tape=market_tape,
                style_profiles=style_profile_rows,
                fund_flow_snapshot=fund_flow_snapshot,
            ),
            "stock_facts": lambda: build_stock_facts(
                stock_master,
                qmt_rows,
                dcf_rows,
                [],
                ths_concepts,
                research,
                qmt_bar_metrics=qmt_bar_1d_metric_rows,
                style_profiles=style_profile_rows,
                fund_flow_snapshot=fund_flow_snapshot,
            ),
        }
    )

    market = analysis_results["market_understanding"]
    stock_facts = analysis_results["stock_facts"]

    decision_results = _run_parallel_loaders(
        {
            "opportunity_discovery": lambda: compute_opportunity_discovery(
                stock_facts,
                market,
                previous_state=previous_bull_state,
            ),
            "risk_surveillance": lambda: compute_risk_surveillance(stock_facts, market),
        }
    )
    opportunity = decision_results["opportunity_discovery"]
    risk = decision_results["risk_surveillance"]
    alert_output = build_alert_output(run_context, market, opportunity, risk, previous_material=previous_material)
    source_health = {
        "stock_master": stock_master_adapter.health().to_dict(),
        "ths_concepts": ths_adapter.health().to_dict(),
        "ths_cli_hot_concept": ths_cli_hot_adapter.health().to_dict(),
        "research": research_adapter.health().to_dict(),
        "qmt_ashare_snapshot_5m": qmt_ashare_snapshot_5m_adapter.health().to_dict(),
        "qmt_bar_1d_metrics": qmt_bar_1d_metrics_health.to_dict(),
        "dcf_hq_zj_snapshot": dcf_health.to_dict(),
        "ths_market_overview": market_overview_adapter.health().to_dict(),
        "ts_stock_basic": ts_stock_basic_health.to_dict(),
        "ts_daily_basic": ts_daily_basic_health.to_dict(),
        "ts_index_member_all": ts_index_member_all_health.to_dict(),
        "ts_style_daily_metrics": ts_style_daily_metrics_health.to_dict(),
        "ts_fina_indicator": ts_fina_indicator_health.to_dict(),
        "ts_moneyflow_ths": ts_moneyflow_ths_health.to_dict(),
        "ts_moneyflow_dc": ts_moneyflow_dc_health.to_dict(),
        "ts_moneyflow_cnt_ths": ts_moneyflow_cnt_ths_health.to_dict(),
        "ts_moneyflow_ind_ths": ts_moneyflow_ind_ths_health.to_dict(),
        "ts_moneyflow_mkt_dc": ts_moneyflow_mkt_dc_health.to_dict(),
    }

    bundle = M0SnapshotBundle(
        run_context=run_context,
        market_understanding=market,
        opportunity_discovery=opportunity,
        risk_surveillance=risk,
        alert_output=alert_output,
        market_evidence_bundle=_build_market_evidence_bundle(
            run_context,
            market,
            stock_facts,
            fund_flow_snapshot,
            source_health,
            comparison_window_label=comparison_window_label,
            prior_main_net_inflow_by_symbol=prior_main_net_inflow_by_symbol,
        ),
        stock_evidence_bundle=_build_stock_evidence_bundle(stock_facts, market, opportunity, risk),
    )
    return M0BuildResult(
        bundle=bundle,
        stock_facts=stock_facts,
        style_profiles=style_profiles,
        fund_flow_snapshot=fund_flow_snapshot,
        source_health=source_health,
    )


def _aggregate_source_status(source_health: dict[str, dict]) -> tuple[str, int, int | None, float | None]:
    statuses = [str(item.get("source_status") or "").lower() for item in source_health.values()]
    fallback_used = int(any(bool(item.get("fallback_used")) for item in source_health.values()))
    freshness_values = [item.get("freshness_seconds") for item in source_health.values() if item.get("freshness_seconds") is not None]
    coverage_values = [item.get("coverage_ratio") for item in source_health.values() if item.get("coverage_ratio") is not None]

    if any(status == "missing" for status in statuses):
        source_status = "MISSING"
    elif any(status == "degraded" for status in statuses):
        source_status = "DEGRADED"
    elif any(status == "fallback" for status in statuses):
        source_status = "FALLBACK"
    else:
        source_status = "READY"
    freshness_seconds = max(int(value) for value in freshness_values) if freshness_values else None
    coverage_ratio = min(float(value) for value in coverage_values) if coverage_values else None
    return source_status, fallback_used, freshness_seconds, coverage_ratio


def load_previous_alert_material(db_path: Path, current_run_id: str, current_analysis_snapshot_ts: str) -> AlertMaterial | None:
    if not db_path.exists():
        return None
    init_db(db_path)

    with connect_sqlite(db_path) as connection:
        row = connection.execute(
            """
            SELECT run_id, confirmed_style, latest_status, latest_dominant_style, market_regime, top_meta_themes_json
            FROM monitor_run
            WHERE run_id <> ?
              AND analysis_snapshot_ts < ?
            ORDER BY trade_date DESC, snapshot_time DESC, round_seq DESC
            LIMIT 1
            """,
            (current_run_id, current_analysis_snapshot_ts),
        ).fetchone()
        if row is None:
            return None

        bucket_rows = connection.execute(
            """
            SELECT symbol, display_bucket, risk_tag, confidence_score
            FROM stock_snapshot
            WHERE run_id = ?
            ORDER BY confidence_score DESC, symbol ASC
            """,
            (row["run_id"],),
        ).fetchall()

    top_meta_themes = json.loads(row["top_meta_themes_json"] or "[]")
    core_anchor_symbols = [item["symbol"] for item in bucket_rows if item["display_bucket"] == "core_anchor"][:5]
    new_long_symbols = [item["symbol"] for item in bucket_rows if item["display_bucket"] == "new_long"][:5]
    catchup_symbols = [item["symbol"] for item in bucket_rows if item["display_bucket"] == "catchup"][:5]
    short_symbols = [item["symbol"] for item in bucket_rows if item["risk_tag"] is not None][:5]

    return AlertMaterial(
        confirmed_style=row["confirmed_style"],
        latest_status=row["latest_status"],
        latest_dominant_style=row["latest_dominant_style"],
        market_regime=row["market_regime"],
        top_meta_themes=top_meta_themes,
        core_anchor_symbols=core_anchor_symbols,
        new_long_symbols=new_long_symbols,
        short_symbols=short_symbols,
        catchup_symbols=catchup_symbols,
    )


def load_previous_bull_state(db_path: Path, current_run_id: str, current_analysis_snapshot_ts: str) -> dict[str, PreviousBullState]:
    if not db_path.exists():
        return {}
    init_db(db_path)

    with connect_sqlite(db_path) as connection:
        row = connection.execute(
            """
            SELECT run_id, trade_date, round_seq
            FROM monitor_run
            WHERE run_id <> ?
              AND analysis_snapshot_ts < ?
            ORDER BY trade_date DESC, snapshot_time DESC, round_seq DESC
            LIMIT 1
            """,
            (current_run_id, current_analysis_snapshot_ts),
        ).fetchone()
        if row is None:
            return {}

        trade_date = str(row["trade_date"])
        current_round_seq = int(row["round_seq"] or 1) + 1

    return load_previous_bull_state_history(
        db_path,
        current_run_id,
        current_analysis_snapshot_ts,
        trade_date=trade_date,
        current_round_seq=current_round_seq,
    )


def load_previous_bull_state_history(
    db_path: Path,
    current_run_id: str,
    current_analysis_snapshot_ts: str,
    *,
    trade_date: str,
    current_round_seq: int,
) -> dict[str, PreviousBullState]:
    if not db_path.exists():
        return {}
    init_db(db_path)

    with connect_sqlite(db_path) as connection:
        run_rows = connection.execute(
            """
            SELECT run_id, round_seq
            FROM monitor_run
            WHERE trade_date = ?
              AND run_id <> ?
              AND analysis_snapshot_ts < ?
            ORDER BY analysis_snapshot_ts ASC, round_seq ASC
            """,
            (trade_date, current_run_id, current_analysis_snapshot_ts),
        ).fetchall()
        if not run_rows:
            return {}

        run_ids = [str(item["run_id"]) for item in run_rows]
        round_seq_map: dict[str, int] = {}
        for idx, item in enumerate(run_rows, start=1):
            round_seq_map[str(item["run_id"])] = int(item["round_seq"] or idx)

        placeholders = ",".join("?" for _ in run_ids)
        bucket_rows = connection.execute(
            f"""
            SELECT s.run_id, s.symbol, s.display_bucket, s.confidence_score, s.best_meta_theme, s.best_concept, m.round_seq
            FROM stock_snapshot s
            JOIN monitor_run m ON m.run_id = s.run_id
            WHERE s.run_id IN ({placeholders})
              AND s.display_bucket IN ('core_anchor', 'new_long', 'catchup')
            ORDER BY m.analysis_snapshot_ts ASC, m.round_seq ASC, s.symbol ASC
            """,
            tuple(run_ids),
        ).fetchall()

    grouped: dict[str, list[dict[str, str | float | int | None]]] = {}
    for item in bucket_rows:
        symbol = str(item["symbol"])
        grouped.setdefault(symbol, []).append(
            {
                "run_id": str(item["run_id"]),
                "round_seq": round_seq_map[str(item["run_id"])],
                "display_bucket": item["display_bucket"],
                "confidence_score": item["confidence_score"],
                "best_meta_theme": item["best_meta_theme"],
                "best_concept": item["best_concept"],
            }
        )

    previous_state: dict[str, PreviousBullState] = {}
    for symbol, history in grouped.items():
        appearances = len(history)
        last_item = history[-1]
        streak = 1
        for idx in range(len(history) - 2, -1, -1):
            if int(history[idx + 1]["round_seq"]) - int(history[idx]["round_seq"]) == 1:
                streak += 1
                continue
            break

        last_round_seq = int(last_item["round_seq"])
        round_gap = max(1, current_round_seq - last_round_seq)
        previous_state[symbol] = PreviousBullState(
            symbol=symbol,
            display_bucket=str(last_item["display_bucket"]),
            confidence_score=float(last_item["confidence_score"] or 0.0),
            best_meta_theme=str(last_item["best_meta_theme"] or "") or None,
            best_concept=str(last_item["best_concept"] or "") or None,
            appearances=appearances,
            streak=streak,
            round_gap=round_gap,
            recent_repeat=round_gap <= 2,
            consecutive_repeat=round_gap == 1,
        )
    return previous_state


def persist_m0_snapshot_bundle(db_path: Path, build_result: M0BuildResult) -> None:
    init_db(db_path)
    persist_style_profiles(db_path, build_result.style_profiles)
    bundle = build_result.bundle
    run_context = bundle.run_context
    market = bundle.market_understanding
    opportunity = bundle.opportunity_discovery
    risk = bundle.risk_surveillance

    candidate_by_symbol = {}
    bucket_priority = {"core_anchor": 3, "new_long": 2, "catchup": 1, "warning": 0}

    def _merge_candidate(item) -> None:
        existing = candidate_by_symbol.get(item.symbol)
        if existing is None:
            candidate_by_symbol[item.symbol] = item
            return

        existing_priority = bucket_priority.get(existing.display_bucket or "", -1)
        current_priority = bucket_priority.get(item.display_bucket or "", -1)
        if current_priority > existing_priority:
            merged = item
        else:
            merged = existing

        if item.risk_tag and not merged.risk_tag:
            merged = CandidateItem(
                symbol=merged.symbol,
                stock_name=merged.stock_name,
                display_bucket=merged.display_bucket,
                confidence_score=merged.confidence_score,
                themes=merged.themes,
                reason=merged.reason,
                display_line=merged.display_line,
                best_meta_theme=merged.best_meta_theme,
                best_concept=merged.best_concept,
                risk_tag=item.risk_tag,
                research_hooks=merged.research_hooks,
                metadata={**merged.metadata, **item.metadata},
            )
        candidate_by_symbol[item.symbol] = merged

    for item in opportunity.core_anchor_watchlist:
        _merge_candidate(item)
    for item in opportunity.new_long_watchlist:
        _merge_candidate(item)
    for item in opportunity.catchup_watchlist:
        _merge_candidate(item)
    for item in risk.short_watchlist:
        _merge_candidate(item)

    source_status, fallback_used, freshness_seconds, coverage_ratio = _aggregate_source_status(build_result.source_health)
    top_meta_themes_json = json.dumps([item.meta_theme for item in market.top_meta_themes[:5]], ensure_ascii=False)
    core_anchor_members = {item.symbol for item in opportunity.core_anchor_watchlist}
    new_long_members = {item.symbol for item in opportunity.new_long_watchlist}
    catchup_members = {item.symbol for item in opportunity.catchup_watchlist}
    short_members = {item.symbol for item in risk.short_watchlist}
    attack_lines: list[str] = []
    for concept_name in list(market.acceleration_concepts[:3]) + list(market.strongest_concepts[:5]):
        text = str(concept_name or "").strip()
        if text and text not in attack_lines:
            attack_lines.append(text)
        if len(attack_lines) >= 3:
            break
    top_attack_lines = " / ".join(attack_lines)
    alert_level = "WARN" if risk.short_watchlist else "INFO"

    with connect_sqlite(db_path) as connection:
        connection.execute("DELETE FROM stock_snapshot WHERE run_id = ?", (run_context.run_id,))
        connection.execute("DELETE FROM alert_log WHERE run_id = ?", (run_context.run_id,))

        connection.execute(
            """
            INSERT INTO monitor_run (
                run_id,
                trade_date,
                snapshot_time,
                analysis_snapshot_ts,
                round_seq,
                source_status,
                fallback_used,
                freshness_seconds,
                coverage_ratio,
                market_regime,
                style_state,
                top_attack_lines,
                has_update,
                alert_level,
                stock_count,
                confirmed_style,
                latest_status,
                latest_dominant_style,
                top_meta_themes_json,
                summary_line,
                alert_decision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                analysis_snapshot_ts = excluded.analysis_snapshot_ts,
                round_seq = excluded.round_seq,
                source_status = excluded.source_status,
                fallback_used = excluded.fallback_used,
                freshness_seconds = excluded.freshness_seconds,
                coverage_ratio = excluded.coverage_ratio,
                market_regime = excluded.market_regime,
                style_state = excluded.style_state,
                top_attack_lines = excluded.top_attack_lines,
                has_update = excluded.has_update,
                alert_level = excluded.alert_level,
                stock_count = excluded.stock_count,
                confirmed_style = excluded.confirmed_style,
                latest_status = excluded.latest_status,
                latest_dominant_style = excluded.latest_dominant_style,
                top_meta_themes_json = excluded.top_meta_themes_json,
                summary_line = excluded.summary_line,
                alert_decision = excluded.alert_decision
            """,
            (
                run_context.run_id,
                run_context.trade_date,
                run_context.snapshot_time,
                run_context.analysis_snapshot_ts,
                run_context.round_seq,
                source_status,
                fallback_used,
                freshness_seconds,
                coverage_ratio,
                market.market_regime,
                market.latest_status,
                top_attack_lines,
                1 if bundle.alert_output.diff_result.decision == "UPDATE" else 0,
                alert_level,
                len(build_result.stock_facts),
                market.confirmed_style,
                market.latest_status,
                market.latest_dominant_style,
                top_meta_themes_json,
                market.summary_line,
                bundle.alert_output.diff_result.decision,
            ),
        )

        for fact in build_result.stock_facts:
            candidate = candidate_by_symbol.get(fact.symbol)
            signal_state = "neutral"
            display_bucket = None
            risk_tag = None
            confidence_score = None
            is_watchlist = 0
            is_warning = 0
            is_core_anchor = 1 if fact.symbol in core_anchor_members else 0
            is_new_long = 1 if fact.symbol in new_long_members else 0
            is_catchup = 1 if fact.symbol in catchup_members else 0
            is_short = 1 if fact.symbol in short_members else 0
            if candidate is not None:
                display_bucket = candidate.display_bucket
                risk_tag = candidate.risk_tag
                confidence_score = candidate.confidence_score
                if is_core_anchor or is_new_long or is_catchup:
                    signal_state = "bull" if candidate.display_bucket in {"core_anchor", "new_long"} else "catchup"
                    is_watchlist = 1
                if is_short:
                    signal_state = "warning"
                    is_warning = 1
            stored_best_meta_theme = candidate.best_meta_theme if candidate and candidate.best_meta_theme else fact.best_meta_theme
            stored_best_concept = candidate.best_concept if candidate and candidate.best_concept else fact.best_concept

            connection.execute(
                """
                INSERT INTO stock_snapshot (
                    run_id,
                    trade_date,
                    snapshot_time,
                    analysis_snapshot_ts,
                    symbol,
                    stock_code,
                    stock_name,
                    exchange,
                    market,
                    industry,
                    last_price,
                    last_close,
                    open_price,
                    high_price,
                    low_price,
                    volume,
                    bid_volume1,
                    ask_volume1,
                    pct_chg_prev_close,
                    open_ret,
                    range_position,
                    amount,
                    turnover_rate,
                    volume_ratio,
                    amplitude,
                    float_mkt_cap,
                    total_mkt_cap,
                    money_pace_ratio,
                    main_net_inflow,
                    super_net,
                    large_net,
                    ret_3d,
                    ret_5d,
                    ret_10d,
                    ret_20d,
                    dividend_value_score,
                    quality_growth_score,
                    high_beta_attack_score,
                    low_vol_defensive_score,
                    dividend_style,
                    valuation_style,
                    growth_style,
                    quality_style,
                    volatility_style,
                    ownership_style,
                    capacity_bucket,
                    composite_style_labels_json,
                    main_net_amount_5d_sum,
                    inflow_streak_days,
                    outflow_streak_days,
                    flow_acceleration_3d,
                    price_flow_divergence_flag,
                    style_bucket,
                    best_meta_theme,
                    best_concept,
                    theme_names_json,
                    style_names_json,
                    signal_state,
                    display_bucket,
                    risk_tag,
                    confidence_score,
                    research_coverage_score,
                    research_hooks_json,
                    source_status,
                    fallback_used,
                    freshness_seconds,
                    coverage_ratio,
                    is_core_anchor,
                    is_new_long,
                    is_catchup,
                    is_short,
                    is_watchlist,
                    is_warning
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_context.run_id,
                    run_context.trade_date,
                    run_context.snapshot_time,
                    run_context.analysis_snapshot_ts,
                    fact.symbol,
                    fact.stock_code,
                    fact.stock_name,
                    fact.exchange,
                    fact.market,
                    fact.industry,
                    fact.last_price,
                    fact.last_close,
                    fact.open_price,
                    fact.high_price,
                    fact.low_price,
                    fact.volume,
                    fact.bid_volume1,
                    fact.ask_volume1,
                    fact.pct_chg_prev_close,
                    fact.open_ret,
                    fact.range_position,
                    fact.amount,
                    fact.turnover_rate,
                    fact.volume_ratio,
                    fact.amplitude,
                    fact.float_mkt_cap,
                    fact.total_mkt_cap,
                    fact.money_pace_ratio,
                    fact.main_net_inflow,
                    fact.super_net,
                    fact.large_net,
                    fact.ret_3d,
                    fact.ret_5d,
                    fact.ret_10d,
                    fact.ret_20d,
                    fact.dividend_value_score,
                    fact.quality_growth_score,
                    fact.high_beta_attack_score,
                    fact.low_vol_defensive_score,
                    fact.dividend_style,
                    fact.valuation_style,
                    fact.growth_style,
                    fact.quality_style,
                    fact.volatility_style,
                    fact.ownership_style,
                    fact.capacity_bucket,
                    json.dumps(fact.composite_style_labels, ensure_ascii=False),
                    fact.main_net_amount_5d_sum,
                    fact.inflow_streak_days,
                    fact.outflow_streak_days,
                    fact.flow_acceleration_3d,
                    1 if fact.price_flow_divergence_flag else 0,
                    market.confirmed_style,
                    stored_best_meta_theme,
                    stored_best_concept,
                    json.dumps(fact.meta_themes, ensure_ascii=False),
                    json.dumps(fact.style_names, ensure_ascii=False),
                    signal_state,
                    display_bucket,
                    risk_tag,
                    confidence_score,
                    fact.research_coverage_score,
                    json.dumps(fact.research_hooks, ensure_ascii=False),
                    source_status,
                    fallback_used,
                    freshness_seconds,
                    coverage_ratio,
                    is_core_anchor,
                    is_new_long,
                    is_catchup,
                    is_short,
                    is_watchlist,
                    is_warning,
                ),
            )

        if bundle.alert_output.diff_result.decision == "UPDATE":
            connection.execute(
                """
                INSERT INTO alert_log (
                    run_id,
                    alert_type,
                    severity,
                    entity_type,
                    entity_key,
                    title,
                    message
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_context.run_id,
                    "m0_snapshot_update",
                    alert_level,
                    "market_snapshot",
                    run_context.run_id,
                    market.summary_line,
                    bundle.alert_output.alert_body,
                ),
            )
        connection.commit()
