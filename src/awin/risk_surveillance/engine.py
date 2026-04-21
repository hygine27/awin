from __future__ import annotations

from collections import defaultdict

from awin.analysis import StockFact
from awin.contracts.m0 import CandidateItem, MarketUnderstandingOutput, RiskSurveillanceOutput
from awin.risk_surveillance.config import load_risk_rules


RISK_RULES = load_risk_rules()
THEME_PRIORITY_RULES = RISK_RULES["theme_priority"]
RISK_THRESHOLDS = RISK_RULES["thresholds"]
RISK_WEIGHTS = RISK_RULES["weights"]
RISK_QUOTA = RISK_RULES["quota"]
OVERHEAT_RULES = RISK_RULES["overheat_rules"]
OVERHEAT_POSITIVE_FLOW_SUPPORT_OFFSET = 0.25
WEAKENING_SIGNAL_MIN_COUNT = 2


def _theme_priority(market: MarketUnderstandingOutput) -> dict[str, float]:
    priority: dict[str, float] = {}
    start = float(THEME_PRIORITY_RULES["start"])
    step = float(THEME_PRIORITY_RULES["step"])
    floor = float(THEME_PRIORITY_RULES["floor"])
    for idx, item in enumerate(market.top_meta_themes, start=1):
        priority[item.meta_theme] = max(floor, start - (idx - 1) * step)
    return priority


def _max_theme_score(values: list[str], priority: dict[str, float]) -> tuple[str | None, float]:
    best_name = None
    best_score = 0.0
    for value in values:
        score = priority.get(value, 0.0)
        if score > best_score:
            best_name = value
            best_score = score
    return best_name, best_score


def _clamp01(value: float | None) -> float:
    if value is None:
        return 0.0
    return max(0.0, min(1.0, float(value)))


def _safe(value: float | None, default: float = 0.0) -> float:
    return default if value is None else float(value)


def _score_by_min(value: float, bands: list[dict[str, float]]) -> float:
    for band in bands:
        if value >= float(band["min"]):
            return float(band["score"])
    return 0.0


def _score_by_max(value: float, bands: list[dict[str, float]]) -> float:
    for band in bands:
        if value <= float(band["max"]):
            return float(band["score"])
    return 0.0


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _avg(values: list[float | None]) -> float | None:
    valid = [float(value) for value in values if value is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _build_theme_stats(stock_facts: list[StockFact]) -> dict[str, dict[str, float | None]]:
    grouped: dict[str, list[StockFact]] = defaultdict(list)
    for fact in stock_facts:
        for theme in fact.meta_themes:
            grouped[theme].append(fact)

    stats: dict[str, dict[str, float | None]] = {}
    for theme, members in grouped.items():
        stats[theme] = {
            "avg_pct": _avg([item.pct_chg_prev_close for item in members]),
            "avg_ret_3d": _avg([item.ret_3d for item in members]),
            "avg_ret_10d": _avg([item.ret_10d for item in members]),
        }
    return stats


def _build_candidate(
    fact: StockFact,
    *,
    score: float,
    risk_tag: str,
    reason: str,
    theme_name: str | None,
    relative_to_theme: float | None,
) -> CandidateItem:
    return CandidateItem(
        symbol=fact.symbol,
        stock_name=fact.stock_name,
        display_bucket="warning",
        confidence_score=round(min(10.0, max(0.0, score * 10.0)), 1),
        themes=fact.meta_themes[:3] or fact.concepts[:3],
        reason=reason,
        display_line=f"{fact.stock_name}({fact.symbol})｜{risk_tag}｜涨幅{_fmt_pct(fact.pct_chg_prev_close)}｜日内位置{_clamp01(fact.range_position):.2f}",
        best_meta_theme=theme_name or fact.best_meta_theme,
        best_concept=fact.best_concept,
        risk_tag=risk_tag,
        research_hooks=fact.research_hooks[:4],
        metadata={
            "pct_chg_prev_close": fact.pct_chg_prev_close,
            "range_position": fact.range_position,
            "flow_ratio": fact.flow_ratio,
            "main_net_inflow": fact.main_net_inflow,
            "super_net": fact.super_net,
            "large_net": fact.large_net,
            "ret_10d": fact.ret_10d,
            "ret_20d": fact.ret_20d,
            "amplitude": fact.amplitude,
            "main_net_amount_1d": fact.main_net_amount_1d,
            "main_net_amount_3d_sum": fact.main_net_amount_3d_sum,
            "main_net_amount_5d_sum": fact.main_net_amount_5d_sum,
            "outflow_streak_days": fact.outflow_streak_days,
            "flow_acceleration_3d": fact.flow_acceleration_3d,
            "price_flow_divergence_flag": fact.price_flow_divergence_flag,
            "high_beta_attack_score": fact.high_beta_attack_score,
            "relative_to_theme": relative_to_theme,
        },
    )


def _overheat_risk_tag(
    *,
    same_day_weak_signals: int,
    main_net_inflow: float,
    large_flow_net: float,
    money_pace_ratio: float,
    range_position: float,
) -> str:
    strong_support = (
        main_net_inflow > 0
        and large_flow_net > 0
        and money_pace_ratio >= 1.2
        and range_position >= 0.7
    )
    if strong_support and same_day_weak_signals == 0:
        return "overheat_supported"
    return "overheat_fading"


def compute_risk_surveillance(
    stock_facts: list[StockFact],
    market: MarketUnderstandingOutput,
    *,
    risk_limit: int = 5,
) -> RiskSurveillanceOutput:
    risk_limit = int(RISK_THRESHOLDS.get("risk_limit", risk_limit))
    theme_priority = _theme_priority(market)
    overheat_regime_rules = OVERHEAT_RULES["regime"]
    overheat_stretch_rules = OVERHEAT_RULES["stretch"]
    overheat_relative_rules = OVERHEAT_RULES["relative"]
    overheat_research_rules = OVERHEAT_RULES["research"]
    overheat_tape_rules = OVERHEAT_RULES["tape"]
    overheat_persistence_rules = OVERHEAT_RULES["persistence"]
    overheat_entry_gates = OVERHEAT_RULES["entry_gates"]
    strongest_concepts = set(market.strongest_concepts[:5]) | set(market.acceleration_concepts[:3])
    theme_stats = _build_theme_stats(stock_facts)

    overheat_candidates: list[tuple[float, float, CandidateItem]] = []
    weak_candidates: list[tuple[float, float, CandidateItem]] = []

    overheat_intraday_threshold = float(RISK_THRESHOLDS["overheat_intraday_threshold"])
    overheat_relative_threshold = float(RISK_THRESHOLDS["overheat_relative_threshold"])
    weak_intraday_threshold = float(RISK_THRESHOLDS["weak_intraday_threshold"])
    weak_relative_threshold = float(RISK_THRESHOLDS["weak_relative_threshold"])
    weak_from_open_threshold = float(RISK_THRESHOLDS["weak_from_open_threshold"])

    if market.market_regime == "trend_expansion":
        overheat_intraday_threshold = float(RISK_THRESHOLDS["trend_expansion_overheat_intraday_threshold"])
        overheat_relative_threshold = float(RISK_THRESHOLDS["trend_expansion_overheat_relative_threshold"])
    elif market.market_regime == "weak_market_relative_strength":
        weak_intraday_threshold = float(RISK_THRESHOLDS["weak_market_intraday_threshold"])
        weak_relative_threshold = float(RISK_THRESHOLDS["weak_market_relative_threshold"])

    for fact in stock_facts:
        theme_name, theme_score = _max_theme_score(fact.meta_themes, theme_priority)
        concept_support = sum(1 for concept in fact.concepts if concept in strongest_concepts)
        if theme_score <= 0.0 and concept_support <= 0:
            continue

        theme_stat = theme_stats.get(theme_name or "", {})
        relative_to_theme = None
        if fact.pct_chg_prev_close is not None and theme_stat.get("avg_pct") is not None:
            relative_to_theme = float(fact.pct_chg_prev_close) - float(theme_stat["avg_pct"])

        flow_score = (
            float(RISK_WEIGHTS["flow_score_main"]) * fact.main_flow_rank
            + float(RISK_WEIGHTS["flow_score_super"]) * fact.super_flow_rank
            + float(RISK_WEIGHTS["flow_score_large"]) * fact.large_flow_rank
        )
        imbalance = _clamp01((0.5 - (fact.bid_ask_imbalance or 0.0) / 2.0))
        pct = fact.pct_chg_prev_close or 0.0
        pct_from_open = fact.open_ret if fact.open_ret is not None else 0.0
        range_position = _clamp01(fact.range_position)
        money_pace_ratio = float(fact.money_pace_ratio or 0.0)
        turnover_rate = float(fact.turnover_rate or 0.0)
        amplitude = float(fact.amplitude or 0.0)
        main_net_inflow = float(fact.main_net_inflow or 0.0)
        large_flow_net = float(fact.super_net or 0.0) + float(fact.large_net or 0.0)
        same_day_weak_signals = 0
        if main_net_inflow < 0:
            same_day_weak_signals += 1
        if large_flow_net < 0:
            same_day_weak_signals += 1
        if pct_from_open <= weak_from_open_threshold:
            same_day_weak_signals += 1
        if range_position <= float(overheat_entry_gates["warning_range_position_max"]):
            same_day_weak_signals += 1
        if fact.price_flow_divergence_flag:
            same_day_weak_signals += 1

        overheat_regime = min(
            float(overheat_regime_rules["cap"]),
            _score_by_min(theme_score, overheat_regime_rules["theme_score_bands"])
            + _score_by_min(float(concept_support), overheat_regime_rules["concept_support_bands"]),
        )
        overheat_stretch = 0.0
        pct_bands = []
        for band in overheat_stretch_rules["pct_bands"]:
            patched_band = dict(band)
            if float(band["min"]) <= 0.0:
                patched_band["min"] = overheat_intraday_threshold
            pct_bands.append(patched_band)
        overheat_stretch += _score_by_min(pct, pct_bands)
        overheat_stretch += (
            float(overheat_stretch_rules["ret_3d_bonus"]["score"])
            if (fact.ret_3d or -999.0) >= float(overheat_stretch_rules["ret_3d_bonus"]["threshold"])
            else 0.0
        )
        overheat_stretch += (
            float(overheat_stretch_rules["ret_5d_bonus"]["score"])
            if (fact.ret_5d or -999.0) >= float(overheat_stretch_rules["ret_5d_bonus"]["threshold"])
            else 0.0
        )
        overheat_stretch += (
            float(overheat_stretch_rules["ret_10d_bonus"]["score"])
            if (fact.ret_10d or -999.0) >= float(overheat_stretch_rules["ret_10d_bonus"]["threshold"])
            else 0.0
        )
        overheat_stretch += _score_by_min(float(fact.ret_20d or -999.0), overheat_stretch_rules["ret_20d_bands"])
        overheat_stretch += (
            float(overheat_stretch_rules["turnover_bonus"]["score"])
            if turnover_rate >= float(overheat_stretch_rules["turnover_bonus"]["threshold"])
            else 0.0
        )
        overheat_stretch += (
            float(overheat_stretch_rules["amplitude_bonus"]["score"])
            if amplitude >= float(overheat_stretch_rules["amplitude_bonus"]["threshold"])
            else 0.0
        )
        overheat_stretch = min(float(overheat_stretch_rules["cap"]), overheat_stretch)

        overheat_relative = 0.0
        relative_bands = []
        for band in overheat_relative_rules["relative_to_theme_bands"]:
            patched_band = dict(band)
            if float(band["min"]) == 0.0:
                patched_band["min"] = overheat_relative_threshold
            relative_bands.append(patched_band)
        overheat_relative += _score_by_min((relative_to_theme or 0.0), relative_bands)
        overheat_relative += _score_by_min(float(concept_support), overheat_relative_rules["concept_support_bands"])
        overheat_relative = min(float(overheat_relative_rules["cap"]), overheat_relative)

        overheat_research = 0.0
        overheat_research += (
            float(overheat_research_rules["onepage_present"])
            if fact.onepage_path
            else float(overheat_research_rules["onepage_absent"])
        )
        overheat_research += (
            float(overheat_research_rules["company_card_present"])
            if fact.company_card_path
            else float(overheat_research_rules["company_card_absent"])
        )
        overheat_research += min(
            float(overheat_research_rules["coverage_cap"]),
            fact.research_coverage_score * float(overheat_research_rules["coverage_multiplier"]),
        )
        overheat_research += min(
            float(overheat_research_rules["intel_mentions_cap"]),
            float(fact.recent_intel_mentions) / float(overheat_research_rules["intel_mentions_divisor"]),
        )
        overheat_research = min(float(overheat_research_rules["cap"]), overheat_research)

        overheat_tape = 0.0
        extension_gate = overheat_tape_rules["extension_gate"]
        if (
            abs(pct - pct_from_open) <= float(extension_gate["pct_vs_open_abs_max"])
            and range_position >= float(extension_gate["range_position_min"])
        ):
            overheat_tape += float(extension_gate["score"])
        elif pct_from_open < pct * float(overheat_tape_rules["open_lag_ratio_max"]):
            overheat_tape += float(overheat_tape_rules["open_lag_score"])
        else:
            overheat_tape += float(overheat_tape_rules["fallback_score"])
        overheat_tape += _score_by_max(float(fact.bid_ask_imbalance or 0.0), overheat_tape_rules["bid_ask_imbalance_bands"])
        overheat_tape += _score_by_min(money_pace_ratio, overheat_tape_rules["money_pace_bands"])
        overheat_tape += (
            float(overheat_tape_rules["negative_flow_bonus"])
            if (main_net_inflow < 0 or large_flow_net < 0)
            else 0.0
        )
        overheat_tape += (
            float(overheat_tape_rules["negative_history_flow_bonus"])
            if (_clamp01(1.0 if _safe(fact.main_net_amount_5d_sum) < 0 else 0.0) > 0 or int(fact.outflow_streak_days or 0) >= 2)
            else 0.0
        )
        overheat_tape += float(overheat_tape_rules["divergence_bonus"]) if fact.price_flow_divergence_flag else 0.0
        if (
            main_net_inflow > 0
            and large_flow_net > 0
            and money_pace_ratio >= 1.2
            and range_position >= 0.7
        ):
            overheat_tape -= OVERHEAT_POSITIVE_FLOW_SUPPORT_OFFSET
        overheat_tape = max(0.0, overheat_tape)
        overheat_tape = min(float(overheat_tape_rules["cap"]), overheat_tape)

        overheat_persistence = 0.0
        ret20 = float(fact.ret_20d or 0.0)
        overheat_persistence += _score_by_min(ret20, overheat_persistence_rules["ret_20d_bands"])
        overheat_persistence += _score_by_min(turnover_rate, overheat_persistence_rules["turnover_bands"])
        overheat_persistence += _score_by_min(amplitude, overheat_persistence_rules["amplitude_bands"])
        overheat_persistence += _score_by_min(float(fact.outflow_streak_days or 0), overheat_persistence_rules["outflow_streak_bands"])
        overheat_persistence += _score_by_min(float(fact.high_beta_attack_score or 0.0), overheat_persistence_rules["high_beta_attack_bands"])

        calm_reset_gate = overheat_persistence_rules["calm_reset_gate"]
        if (
            ret20 < float(calm_reset_gate["ret_20d_max"])
            and turnover_rate < float(calm_reset_gate["turnover_max"])
            and amplitude < float(calm_reset_gate["amplitude_max"])
        ):
            overheat_persistence -= float(calm_reset_gate["penalty"])

        best_concept_name = str(fact.best_concept or "").strip()
        for adjustment in overheat_persistence_rules["concept_adjustments"]:
            if best_concept_name != str(adjustment["concept_name"]):
                continue
            if "concept_support_max" in adjustment and concept_support <= int(adjustment["concept_support_max"]):
                overheat_persistence -= float(adjustment["penalty"])
            elif "ret_20d_max" in adjustment and ret20 < float(adjustment["ret_20d_max"]):
                overheat_persistence -= float(adjustment["penalty"])

        overheat_score = (overheat_regime + overheat_stretch + overheat_relative + overheat_research + overheat_tape) / 10.0
        overheat_score = max(0.0, overheat_score + overheat_persistence)

        weakness_score = (
            float(RISK_WEIGHTS["weakness_theme"]) * theme_score
            + float(RISK_WEIGHTS["weakness_pct_rank"]) * (1.0 - fact.pct_chg_rank)
            + float(RISK_WEIGHTS["weakness_range_position"]) * (1.0 - range_position)
            + float(RISK_WEIGHTS["weakness_flow_score"]) * (1.0 - flow_score)
            + float(RISK_WEIGHTS["weakness_amplitude_rank"]) * fact.amplitude_rank
            + float(RISK_WEIGHTS["weakness_concept_support"]) * min(1.0, concept_support / 3.0)
            + float(RISK_WEIGHTS["weakness_flow_divergence"]) * (1.0 if fact.price_flow_divergence_flag else 0.0)
        )

        if (
            theme_score >= float(overheat_entry_gates["theme_score_min"])
            and (
                pct >= overheat_intraday_threshold
                or (fact.ret_3d or -999.0) >= float(overheat_stretch_rules["ret_3d_bonus"]["threshold"])
                or (fact.ret_5d or -999.0) >= float(overheat_stretch_rules["ret_5d_bonus"]["threshold"])
                or (fact.ret_10d or -999.0) >= float(overheat_stretch_rules["ret_10d_bonus"]["threshold"])
            )
            and (relative_to_theme or 0.0) >= overheat_relative_threshold
            and overheat_score >= float(RISK_THRESHOLDS["overheat_score_gate"])
        ):
            overheat_tag = _overheat_risk_tag(
                same_day_weak_signals=same_day_weak_signals,
                main_net_inflow=main_net_inflow,
                large_flow_net=large_flow_net,
                money_pace_ratio=money_pace_ratio,
                range_position=range_position,
            )
            if overheat_tag == "overheat_supported":
                reason = (
                    f"{theme_name or '热门主线'}内明显超涨，较主题均值偏离{_fmt_pct(relative_to_theme)}，"
                    f"短线透支抬升，但盘中承接仍强，更适合做过热管理"
                )
            else:
                reason = (
                    f"{theme_name or '热门主线'}内明显超涨，较主题均值偏离{_fmt_pct(relative_to_theme)}，"
                    f"短线透支和拥挤度抬升，且盘中承接开始松动，更适合做过热管理"
                )
            overheat_candidates.append(
                (
                    overheat_score,
                    float(fact.amount or 0.0),
                    _build_candidate(
                        fact,
                        score=overheat_score,
                        risk_tag=overheat_tag,
                        reason=reason,
                        theme_name=theme_name,
                        relative_to_theme=relative_to_theme,
                    ),
                )
            )
            continue

        if (
            theme_score >= float(overheat_entry_gates["theme_score_min"])
            and pct <= weak_intraday_threshold
            and pct_from_open <= weak_from_open_threshold
            and range_position <= float(overheat_entry_gates["warning_range_position_max"])
            and (relative_to_theme or 0.0) <= weak_relative_threshold
            and flow_score <= float(overheat_entry_gates["warning_flow_score_max"])
            and same_day_weak_signals >= WEAKENING_SIGNAL_MIN_COUNT
            and weakness_score >= float(RISK_THRESHOLDS["warning_score_gate"])
        ):
            reason = (
                f"{theme_name or '热门主线'}内部分化，这只票相对主题落后{_fmt_pct(abs(relative_to_theme or 0.0))}，"
                f"日内承接开始走弱，需要纳入转弱预警"
            )
            weak_candidates.append(
                (
                    weakness_score,
                    float(fact.amount or 0.0),
                    _build_candidate(
                        fact,
                        score=weakness_score,
                        risk_tag="weakening",
                        reason=reason,
                        theme_name=theme_name,
                        relative_to_theme=relative_to_theme,
                    ),
                )
            )
            continue

        if (
            pct < float(overheat_entry_gates["weak_pct_max"])
            and pct_from_open <= float(overheat_entry_gates["weak_open_ret_max"])
            and range_position <= float(overheat_entry_gates["weak_range_position_max"])
            and flow_score <= float(overheat_entry_gates["weak_flow_score_max"])
            and weakness_score >= float(RISK_THRESHOLDS["weak_score_gate"])
        ):
            reason = (
                f"价格和资金同步转弱，涨幅{_fmt_pct(fact.pct_chg_prev_close)}，"
                f"日内位置偏低，属于弱势回避区"
            )
            weak_candidates.append(
                (
                    weakness_score,
                    float(fact.amount or 0.0),
                    _build_candidate(
                        fact,
                        score=weakness_score,
                        risk_tag="weak",
                        reason=reason,
                        theme_name=theme_name,
                        relative_to_theme=relative_to_theme,
                    ),
                )
            )

    overheat_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    weak_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)

    if market.market_regime == "trend_expansion" and len(overheat_candidates) >= risk_limit:
        return RiskSurveillanceOutput(short_watchlist=[item for _, _, item in overheat_candidates[:risk_limit]])

    picked: list[CandidateItem] = []
    used_symbols: set[str] = set()
    overheat_quota = min(
        int(RISK_QUOTA["trend_expansion_overheat_quota"])
        if market.market_regime == "trend_expansion"
        else max(int(RISK_QUOTA["default_overheat_quota_floor"]), risk_limit // 2),
        risk_limit,
    )

    for bucket, quota in ((overheat_candidates, overheat_quota), (weak_candidates, risk_limit)):
        for _, _, item in bucket:
            if item.symbol in used_symbols:
                continue
            if bucket is overheat_candidates and sum(
                1 for candidate in picked if candidate.risk_tag in {"overheat", "overheat_supported", "overheat_fading"}
            ) >= quota:
                break
            if bucket is weak_candidates and len(picked) >= max(overheat_quota, min(risk_limit, len(overheat_candidates))):
                break
            picked.append(item)
            used_symbols.add(item.symbol)
            if len(picked) >= risk_limit:
                break
        if len(picked) >= risk_limit:
            break

    return RiskSurveillanceOutput(short_watchlist=picked[:risk_limit])
