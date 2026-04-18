from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.adapters import SnapshotRequest
from awin.builders.m0 import build_m0_snapshot_bundle, load_previous_alert_material, load_previous_bull_state_history
from awin.opportunity_discovery.engine import (
    _catchup_target_concepts,
    _clamp01,
    _concept_priority,
    _derive_selected_focus_concepts,
    _long_score_breakdown,
    _novelty_score,
    _repeat_adjustment,
    _resolve_theme_context,
    _safe,
)


DEFAULT_TARGETS = [
    "300033.SZ",
    "300058.SZ",
    "300364.SZ",
    "300418.SZ",
    "300442.SZ",
    "300476.SZ",
    "600105.SH",
    "601138.SH",
    "688111.SH",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Debug awin new_long / catchup selection for specific symbols.")
    parser.add_argument("--trade-date", default="2026-04-16")
    parser.add_argument("--snapshot-time", default="10:35:00")
    parser.add_argument("--round-seq", type=int, default=3)
    parser.add_argument("--db-path", type=Path, default=Path("/tmp/awin_step_20260416_1035_v11.db"))
    parser.add_argument(
        "--targets",
        default=",".join(DEFAULT_TARGETS),
        help="Comma-separated symbol list",
    )
    args = parser.parse_args()

    trade_date = args.trade_date
    snapshot_time = args.snapshot_time if len(args.snapshot_time) == 8 else f"{args.snapshot_time}:00"
    round_seq = args.round_seq
    db_path = args.db_path
    targets = {item.strip() for item in args.targets.split(",") if item.strip()}
    request = SnapshotRequest(
        trade_date=trade_date,
        snapshot_time=snapshot_time,
        analysis_snapshot_ts=f"{trade_date}T{snapshot_time}",
    )
    run_id = f"{trade_date}-{snapshot_time.replace(':', '')}-r{round_seq:02d}"
    previous_material = load_previous_alert_material(db_path, run_id, request.analysis_snapshot_ts)
    previous_state = load_previous_bull_state_history(
        db_path,
        run_id,
        request.analysis_snapshot_ts,
        trade_date=trade_date,
        current_round_seq=round_seq,
    )
    result = build_m0_snapshot_bundle(
        request,
        round_seq=round_seq,
        previous_material=previous_material,
        previous_bull_state=previous_state,
    )
    market = result.bundle.market_understanding
    concept_priority = _concept_priority(market)
    acceleration_concepts = set(market.acceleration_concepts[:3])
    focus_concepts = acceleration_concepts | set(market.strongest_concepts[:3])
    focus_themes = {item.meta_theme for item in market.top_meta_themes[:3] if item.meta_theme}
    dominant_theme = str(market.top_meta_themes[0].meta_theme).strip() if market.top_meta_themes else None
    selected_focus_concepts = _derive_selected_focus_concepts(
        [(item.confidence_score, 0.0, item) for item in result.bundle.opportunity_discovery.core_anchor_watchlist],
        [(item.confidence_score, 0.0, item) for item in result.bundle.opportunity_discovery.new_long_watchlist],
        dominant_theme=dominant_theme,
    )
    fallback_catchup_focus_concepts = set(_catchup_target_concepts(market))
    catchup_focus_concepts = selected_focus_concepts or fallback_catchup_focus_concepts

    print("DOMINANT_THEME", dominant_theme)
    print("STRONGEST", market.strongest_concepts[:10])
    print("ACCEL", market.acceleration_concepts[:10])
    print("SELECTED_FOCUS", sorted(selected_focus_concepts))
    print("FALLBACK_FOCUS", sorted(fallback_catchup_focus_concepts))
    print("ACTIVE_FOCUS", sorted(catchup_focus_concepts))
    print("FINAL_NEW", [(item.symbol, item.best_concept) for item in result.bundle.opportunity_discovery.new_long_watchlist])
    print("FINAL_CATCHUP", [(item.symbol, item.best_concept, item.metadata.get("catchup_score_raw")) for item in result.bundle.opportunity_discovery.catchup_watchlist])

    for fact in result.stock_facts:
        if fact.symbol not in targets:
            continue
        context = _resolve_theme_context(fact, market, concept_priority)
        best_theme = str(context.get("best_theme") or "") or None
        best_concept = str(context.get("best_concept") or "") or None
        previous_bucket = previous_state.get(fact.symbol).display_bucket if fact.symbol in previous_state else None
        long_score, _ = _long_score_breakdown(fact, context)
        novelty_score = (
            1.0
            if fact.symbol not in previous_state
            else _novelty_score(
                fact,
                score_10=long_score,
                previous=previous_state.get(fact.symbol),
                theme_name=best_theme,
                concept_name=best_concept,
            )
        )
        repeat_context = _repeat_adjustment(
            previous_state.get(fact.symbol),
            novelty_score=novelty_score,
            long_score=long_score,
        )
        pct = _safe(fact.pct_chg_prev_close)
        range_position = _clamp01(fact.range_position)
        base_focus = bool((best_theme and best_theme in focus_themes) or (best_concept and best_concept in focus_concepts))
        long_ok = (
            base_focus
            and pct >= -0.02
            and pct <= 0.10
            and _safe(fact.ret_3d, -999.0) <= 0.15
            and _safe(fact.ret_5d, -999.0) <= 0.20
            and _safe(fact.ret_10d, -999.0) <= 0.28
            and range_position >= 0.35
            and _safe(fact.money_pace_ratio) >= 0.9
            and _safe(fact.open_ret) >= -0.02
            and _safe(fact.amount) >= 3e8
            and (long_score >= 8.2 or (previous_bucket in {"core_anchor", "new_long"} and long_score >= 6.8))
        )

        catchup_focus_support = sum(1 for concept in fact.concepts if concept in catchup_focus_concepts)
        catchup_reset_score = 0.55 * (1.0 - fact.ret_3d_rank) + 0.45 * (1.0 - fact.ret_10d_rank)
        catchup_amount_norm = min(1.0, _safe(fact.amount) / 8e9)
        catchup_flow_norm = min(1.0, max(0.0, _safe(fact.main_net_inflow)) / 2e8)
        best_concept_focus = bool(best_concept and best_concept in catchup_focus_concepts)
        catchup_focus_bonus = 0.30 if best_concept_focus else 0.15 if catchup_focus_support >= 2 else 0.0
        catchup_best_concept_penalty = 0.25 if best_concept and best_concept not in catchup_focus_concepts else 0.0
        money_pace_ratio = _safe(fact.money_pace_ratio)
        catchup_pace_penalty = 0.0
        if money_pace_ratio < 0.75:
            catchup_pace_penalty = 0.60
        elif money_pace_ratio < 0.90:
            catchup_pace_penalty = 0.35
        company_card_quality_score = _safe(getattr(fact, "company_card_quality_score", None))
        tracking_text = str(getattr(fact, "company_card_tracking_recommendation", "") or "")
        catchup_score = (
            min(4.0, money_pace_ratio) * 0.28
            + _clamp01(fact.range_position) * 1.20
            + company_card_quality_score * 1.10
            + catchup_amount_norm * 0.90
            + catchup_flow_norm * 0.80
            + (0.08 - max(0.0, min(0.08, _safe(fact.ret_3d)))) * 5.0
            + catchup_focus_bonus
            - catchup_best_concept_penalty
            - catchup_pace_penalty
        )
        if tracking_text and "否" in tracking_text:
            catchup_score -= 0.35
        catchup_ok = (
            catchup_focus_support > 0
            and 0.01 <= pct < 0.07
            and _safe(fact.ret_3d, -999.0) <= 0.10
            and _safe(fact.ret_5d, -999.0) <= 0.16
            and range_position >= 0.35
            and (
                _safe(fact.money_pace_ratio) >= 1.0
                or _safe(fact.volume_ratio) >= 1.2
                or _safe(fact.main_net_inflow) > 0
            )
            and catchup_score >= 2.40
        )
        print(
            {
                "symbol": fact.symbol,
                "stock": fact.stock_name,
                "concepts": fact.concepts,
                "best_theme": best_theme,
                "best_concept": best_concept,
                "previous_bucket": previous_bucket,
                "long_score": round(long_score, 3),
                "novelty_score": round(novelty_score, 3),
                "recent_repeat": bool(repeat_context.get("recent_repeat")),
                "repeat_penalty": repeat_context.get("repeat_penalty"),
                "evidence_bonus": repeat_context.get("evidence_bonus"),
                "rank_score": repeat_context.get("rank_score"),
                "long_ok": long_ok,
                "catchup_ok": catchup_ok,
                "focus_support": catchup_focus_support,
                "best_concept_focus": best_concept_focus,
                "company_card_quality_score": round(company_card_quality_score, 3),
                "tracking": tracking_text or None,
                "pct": round(pct, 4),
                "ret_3d": round(_safe(fact.ret_3d), 4),
                "ret_5d": round(_safe(fact.ret_5d), 4),
                "ret_10d": round(_safe(fact.ret_10d), 4),
                "money_pace_ratio": round(_safe(fact.money_pace_ratio), 3),
                "range_position": round(range_position, 3),
                "amount_e": round(_safe(fact.amount) / 1e8, 2),
                "main_flow_e": round(_safe(fact.main_net_inflow) / 1e8, 3),
                "catchup_score": round(catchup_score, 3),
                "catchup_reset_score": round(catchup_reset_score, 3),
                "focus_bonus": round(catchup_focus_bonus, 3),
                "focus_penalty": round(catchup_best_concept_penalty, 3),
                "pace_penalty": round(catchup_pace_penalty, 3),
            }
        )


if __name__ == "__main__":
    main()
