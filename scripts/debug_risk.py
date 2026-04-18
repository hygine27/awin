from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.adapters import SnapshotRequest
from awin.builders.m0 import build_m0_snapshot_bundle, load_previous_alert_material, load_previous_bull_state_history
from awin.risk_surveillance.engine import _build_theme_stats, _clamp01, _max_theme_score, _theme_priority


TARGETS = {
    "300857.SZ",
    "002990.SZ",
    "301282.SZ",
    "301382.SZ",
    "301396.SZ",
    "688227.SH",
    "002025.SZ",
    "002580.SZ",
    "301603.SZ",
    "600666.SH",
    "688668.SH",
}


def main() -> None:
    trade_date = "2026-04-16"
    snapshot_time = "10:35:00"
    round_seq = 3
    db_path = Path("/tmp/awin_step_20260416_1035_v8.db")
    request = SnapshotRequest(
        trade_date=trade_date,
        snapshot_time=snapshot_time,
        analysis_snapshot_ts=f"{trade_date}T{snapshot_time}",
    )
    run_id = f"{trade_date}-103500-r03"
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
    theme_priority = _theme_priority(market)
    strongest_concepts = set(market.strongest_concepts[:5]) | set(market.acceleration_concepts[:3])
    theme_stats = _build_theme_stats(result.stock_facts)

    overheat_intraday_threshold = 0.10
    overheat_relative_threshold = 0.05
    weak_intraday_threshold = -0.03
    weak_relative_threshold = -0.03
    weak_from_open_threshold = -0.02
    if market.market_regime == "trend_expansion":
        overheat_intraday_threshold = 0.08
        overheat_relative_threshold = 0.04
    elif market.market_regime == "weak_market_relative_strength":
        weak_intraday_threshold = -0.02
        weak_relative_threshold = -0.02

    print("REGIME", market.market_regime)
    print("STRONGEST", market.strongest_concepts[:5])
    print("ACCEL", market.acceleration_concepts[:3])
    for fact in result.stock_facts:
        if fact.symbol not in TARGETS:
            continue
        theme_name, theme_score = _max_theme_score(fact.meta_themes, theme_priority)
        concept_support = sum(1 for concept in fact.concepts if concept in strongest_concepts)
        theme_stat = theme_stats.get(theme_name or "", {})
        relative_to_theme = None
        if fact.pct_chg_prev_close is not None and theme_stat.get("avg_pct") is not None:
            relative_to_theme = float(fact.pct_chg_prev_close) - float(theme_stat["avg_pct"])
        flow_score = 0.60 * fact.main_flow_rank + 0.25 * fact.super_flow_rank + 0.15 * fact.large_flow_rank
        pct = fact.pct_chg_prev_close or 0.0
        pct_from_open = fact.open_ret if fact.open_ret is not None else 0.0
        range_position = _clamp01(fact.range_position)
        money_pace_ratio = float(fact.money_pace_ratio or 0.0)
        turnover_rate = float(fact.turnover_rate or 0.0)
        amplitude = float(fact.amplitude or 0.0)
        main_net_inflow = float(fact.main_net_inflow or 0.0)
        large_flow_net = float(fact.super_net or 0.0) + float(fact.large_net or 0.0)

        overheat_regime = min(2.5, (1.5 if theme_score >= 0.85 else 1.1 if theme_score >= 0.70 else 0.7) + (0.4 if concept_support >= 2 else 0.2))
        overheat_stretch = 0.0
        overheat_stretch += 1.3 if pct >= 0.15 else 1.0 if pct >= 0.10 else 0.6 if pct >= overheat_intraday_threshold else 0.0
        overheat_stretch += 0.5 if (fact.ret_3d or -999.0) >= 0.12 else 0.0
        overheat_stretch += 0.4 if (fact.ret_5d or -999.0) >= 0.18 else 0.0
        overheat_stretch += 0.3 if (fact.ret_10d or -999.0) >= 0.28 else 0.0
        overheat_stretch += 0.3 if turnover_rate >= 0.20 else 0.0
        overheat_stretch += 0.2 if amplitude >= 0.12 else 0.0
        overheat_stretch = min(2.5, overheat_stretch)

        overheat_relative = 0.0
        overheat_relative += 1.0 if (relative_to_theme or 0.0) >= 0.08 else 0.7 if (relative_to_theme or 0.0) >= overheat_relative_threshold else 0.4
        overheat_relative += 0.5 if concept_support >= 4 else 0.2 if concept_support >= 1 else 0.0
        overheat_relative = min(1.5, overheat_relative)

        overheat_research = 0.0
        overheat_research += 0.55 if fact.onepage_path else 0.2
        overheat_research += 0.45 if fact.company_card_path else 0.1
        overheat_research += min(0.55, fact.research_coverage_score * 0.65)
        overheat_research += min(0.7, float(fact.recent_intel_mentions) / 180.0)
        overheat_research = min(1.5, overheat_research)

        overheat_tape = 0.0
        overheat_tape += 0.9 if abs(pct - pct_from_open) <= 0.01 and range_position >= 0.85 else 0.6 if pct_from_open < pct * 0.7 else 0.4
        overheat_tape += 0.6 if (fact.bid_ask_imbalance or 0.0) <= -0.3 else 0.4 if (fact.bid_ask_imbalance or 0.0) <= 0.1 else 0.2
        overheat_tape += 0.5 if money_pace_ratio >= 3.0 else 0.3 if money_pace_ratio >= 1.5 else 0.1
        overheat_tape += 0.3 if (main_net_inflow < 0 or large_flow_net < 0) else 0.0
        overheat_tape = min(2.0, overheat_tape)
        overheat_score = (overheat_regime + overheat_stretch + overheat_relative + overheat_research + overheat_tape) / 10.0
        overheat_ok = (
            theme_score >= 0.55
            and (
                pct >= overheat_intraday_threshold
                or (fact.ret_3d or -999.0) >= 0.12
                or (fact.ret_5d or -999.0) >= 0.18
                or (fact.ret_10d or -999.0) >= 0.28
            )
            and (relative_to_theme or 0.0) >= overheat_relative_threshold
            and overheat_score >= 0.52
        )

        print(
            {
                "symbol": fact.symbol,
                "meta_themes": fact.meta_themes,
                "concepts": fact.concepts,
                "theme": theme_name,
                "concept": fact.best_concept,
                "theme_score": round(theme_score, 3),
                "concept_support": concept_support,
                "pct": round(pct, 4),
                "open_ret": round(pct_from_open, 4),
                "relative_to_theme": None if relative_to_theme is None else round(relative_to_theme, 4),
                "ret_3d": round(fact.ret_3d or 0.0, 4),
                "ret_5d": round(fact.ret_5d or 0.0, 4),
                "ret_10d": round(fact.ret_10d or 0.0, 4),
                "ret_20d": round(fact.ret_20d or 0.0, 4),
                "pace": round(money_pace_ratio, 3),
                "turnover": round(turnover_rate, 4),
                "amplitude": round(amplitude, 4),
                "overheat_score": round(overheat_score, 3),
                "overheat_ok": overheat_ok,
                "stretch": round(overheat_stretch, 3),
                "relative": round(overheat_relative, 3),
                "tape": round(overheat_tape, 3),
            }
        )

    print("FINAL_SHORT", [(item.symbol, item.risk_tag, item.confidence_score) for item in result.bundle.risk_surveillance.short_watchlist])


if __name__ == "__main__":
    main()
