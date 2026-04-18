from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from awin.adapters import (
    DcfSnapshotAdapter,
    QmtBar1dAdapter,
    QmtSnapshotAdapter,
    ResearchCoverageAdapter,
    SnapshotRequest,
    StockMasterAdapter,
    ThsAppHotConceptAdapter,
    ThsCliHotConceptAdapter,
    ThsConceptAdapter,
    ThsMarketOverviewAdapter,
)
from awin.analysis import StockFact, build_stock_facts
from awin.alerting.diff import build_alert_output
from awin.contracts.m0 import AlertMaterial, CandidateItem, M0SnapshotBundle, RunContext
from awin.market_understanding import compute_market_understanding
from awin.opportunity_discovery import PreviousBullState, compute_opportunity_discovery
from awin.risk_surveillance import compute_risk_surveillance
from awin.storage.db import connect_sqlite, init_db


@dataclass(frozen=True)
class M0BuildResult:
    bundle: M0SnapshotBundle
    stock_facts: list[StockFact]
    source_health: dict[str, dict]


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
    ths_app_hot_adapter = ThsAppHotConceptAdapter()
    ths_cli_hot_adapter = ThsCliHotConceptAdapter()
    research_adapter = ResearchCoverageAdapter()
    qmt_adapter = QmtSnapshotAdapter()
    qmt_bar_1d_adapter = QmtBar1dAdapter()
    dcf_adapter = DcfSnapshotAdapter()
    market_overview_adapter = ThsMarketOverviewAdapter()

    stock_master = stock_master_adapter.load_rows()
    ths_concepts = ths_adapter.load_rows(request)
    ths_hot_concepts = ths_app_hot_adapter.load_rows(request) + ths_cli_hot_adapter.load_rows(request)
    research = research_adapter.load_rows(request)
    qmt_rows = qmt_adapter.load_rows(request)
    trade_day = date.fromisoformat(request.trade_date)
    qmt_bar_1d_rows, qmt_bar_1d_health = qmt_bar_1d_adapter.load_rows_with_health(
        [item.symbol for item in qmt_rows],
        start_date=(trade_day - timedelta(days=45)).isoformat(),
        end_date=request.trade_date,
    )
    dcf_rows, dcf_health = dcf_adapter.load_rows_with_health(request)
    market_tape = market_overview_adapter.load_market_tape()

    market = compute_market_understanding(
        stock_master,
        qmt_rows,
        dcf_rows,
        ths_concepts,
        ths_hot_concepts=ths_hot_concepts,
        market_tape=market_tape,
    )
    stock_facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_1d_rows, ths_concepts, research)
    opportunity = compute_opportunity_discovery(stock_facts, market, previous_state=previous_bull_state)
    risk = compute_risk_surveillance(stock_facts, market)
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
        source_health={
            "stock_master": stock_master_adapter.health().to_dict(),
            "ths_concepts": ths_adapter.health().to_dict(),
            "ths_app_hot_concept": ths_app_hot_adapter.health().to_dict(),
            "ths_cli_hot_concept": ths_cli_hot_adapter.health().to_dict(),
            "research": research_adapter.health().to_dict(),
            "qmt": qmt_adapter.health().to_dict(),
            "qmt_bar_1d": qmt_bar_1d_health.to_dict(),
            "dcf": dcf_health.to_dict(),
            "ths_market_overview": market_overview_adapter.health().to_dict(),
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
