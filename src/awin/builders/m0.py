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
from awin.contracts.m0 import AlertMaterial, CandidateItem, M0SnapshotBundle, RunContext
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

    bundle = M0SnapshotBundle(
        run_context=run_context,
        market_understanding=market,
        opportunity_discovery=opportunity,
        risk_surveillance=risk,
        alert_output=alert_output,
    )
    return M0BuildResult(
        bundle=bundle,
        stock_facts=stock_facts,
        style_profiles=style_profiles,
        fund_flow_snapshot=fund_flow_snapshot,
        source_health={
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
        },
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
