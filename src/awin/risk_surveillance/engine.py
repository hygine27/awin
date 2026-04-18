from __future__ import annotations

from collections import defaultdict

from awin.analysis import StockFact
from awin.contracts.m0 import CandidateItem, MarketUnderstandingOutput, RiskSurveillanceOutput


def _theme_priority(market: MarketUnderstandingOutput) -> dict[str, float]:
    priority: dict[str, float] = {}
    for idx, item in enumerate(market.top_meta_themes, start=1):
        priority[item.meta_theme] = max(0.25, 1.0 - (idx - 1) * 0.15)
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
        confidence_score=round(score * 10.0, 1),
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
            "ret_10d": fact.ret_10d,
            "ret_20d": fact.ret_20d,
            "amplitude": fact.amplitude,
            "relative_to_theme": relative_to_theme,
        },
    )


def compute_risk_surveillance(
    stock_facts: list[StockFact],
    market: MarketUnderstandingOutput,
    *,
    risk_limit: int = 5,
) -> RiskSurveillanceOutput:
    theme_priority = _theme_priority(market)
    strongest_concepts = set(market.strongest_concepts[:5]) | set(market.acceleration_concepts[:3])
    theme_stats = _build_theme_stats(stock_facts)

    overheat_candidates: list[tuple[float, float, CandidateItem]] = []
    weak_candidates: list[tuple[float, float, CandidateItem]] = []

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

    for fact in stock_facts:
        theme_name, theme_score = _max_theme_score(fact.meta_themes, theme_priority)
        concept_support = sum(1 for concept in fact.concepts if concept in strongest_concepts)
        if theme_score <= 0.0 and concept_support <= 0:
            continue

        theme_stat = theme_stats.get(theme_name or "", {})
        relative_to_theme = None
        if fact.pct_chg_prev_close is not None and theme_stat.get("avg_pct") is not None:
            relative_to_theme = float(fact.pct_chg_prev_close) - float(theme_stat["avg_pct"])

        flow_score = 0.60 * fact.main_flow_rank + 0.25 * fact.super_flow_rank + 0.15 * fact.large_flow_rank
        imbalance = _clamp01((0.5 - (fact.bid_ask_imbalance or 0.0) / 2.0))
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
        overheat_stretch += 0.6 if (fact.ret_20d or -999.0) >= 0.50 else 0.4 if (fact.ret_20d or -999.0) >= 0.40 else 0.2 if (fact.ret_20d or -999.0) >= 0.25 else 0.0
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

        overheat_persistence = 0.0
        ret20 = float(fact.ret_20d or 0.0)
        if ret20 >= 0.50:
            overheat_persistence += 0.18
        elif ret20 >= 0.40:
            overheat_persistence += 0.12
        elif ret20 >= 0.30:
            overheat_persistence += 0.08

        if turnover_rate >= 0.20:
            overheat_persistence += 0.12
        elif turnover_rate >= 0.12:
            overheat_persistence += 0.06

        if amplitude >= 0.14:
            overheat_persistence += 0.08
        elif amplitude >= 0.10:
            overheat_persistence += 0.04

        if ret20 < 0.30 and turnover_rate < 0.12 and amplitude < 0.12:
            overheat_persistence -= 0.12

        best_concept_name = str(fact.best_concept or "").strip()
        if best_concept_name == "AIGC概念":
            if concept_support <= 1:
                overheat_persistence -= 0.14
            elif ret20 < 0.20:
                overheat_persistence -= 0.12

        overheat_score = (overheat_regime + overheat_stretch + overheat_relative + overheat_research + overheat_tape) / 10.0
        overheat_score = max(0.0, overheat_score + overheat_persistence)

        weakness_score = (
            0.26 * theme_score
            + 0.22 * (1.0 - fact.pct_chg_rank)
            + 0.18 * (1.0 - range_position)
            + 0.16 * (1.0 - flow_score)
            + 0.10 * fact.amplitude_rank
            + 0.08 * min(1.0, concept_support / 3.0)
        )

        if (
            theme_score >= 0.55
            and (
                pct >= overheat_intraday_threshold
                or (fact.ret_3d or -999.0) >= 0.12
                or (fact.ret_5d or -999.0) >= 0.18
                or (fact.ret_10d or -999.0) >= 0.28
            )
            and (relative_to_theme or 0.0) >= overheat_relative_threshold
            and overheat_score >= 0.52
        ):
            reason = (
                f"{theme_name or '热门主线'}内明显超涨，较主题均值偏离{_fmt_pct(relative_to_theme)}，"
                f"短线透支和拥挤度抬升，更适合做过热管理"
            )
            overheat_candidates.append(
                (
                    overheat_score,
                    float(fact.amount or 0.0),
                    _build_candidate(
                        fact,
                        score=overheat_score,
                        risk_tag="overheat",
                        reason=reason,
                        theme_name=theme_name,
                        relative_to_theme=relative_to_theme,
                    ),
                )
            )
            continue

        if (
            theme_score >= 0.55
            and pct <= weak_intraday_threshold
            and pct_from_open <= weak_from_open_threshold
            and range_position <= 0.35
            and (relative_to_theme or 0.0) <= weak_relative_threshold
            and flow_score <= 0.40
            and weakness_score >= 0.62
        ):
            reason = (
                f"{theme_name or '热门主线'}内部分化，这只票相对主题落后{_fmt_pct(abs(relative_to_theme or 0.0))}，"
                f"日内位置偏低，资金承接也偏弱，需要纳入 warning"
            )
            weak_candidates.append(
                (
                    weakness_score,
                    float(fact.amount or 0.0),
                    _build_candidate(
                        fact,
                        score=weakness_score,
                        risk_tag="warning",
                        reason=reason,
                        theme_name=theme_name,
                        relative_to_theme=relative_to_theme,
                    ),
                )
            )
            continue

        if (
            pct < -0.04
            and pct_from_open <= -0.03
            and range_position <= 0.20
            and flow_score <= 0.30
            and weakness_score >= 0.64
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
    overheat_quota = min(4 if market.market_regime == "trend_expansion" else max(3, risk_limit // 2), risk_limit)

    for bucket, quota in ((overheat_candidates, overheat_quota), (weak_candidates, risk_limit)):
        for _, _, item in bucket:
            if item.symbol in used_symbols:
                continue
            if bucket is overheat_candidates and sum(1 for candidate in picked if candidate.risk_tag == "overheat") >= quota:
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
