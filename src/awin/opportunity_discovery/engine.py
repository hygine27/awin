from __future__ import annotations

from dataclasses import dataclass

from awin.analysis import StockFact
from awin.contracts.m0 import CandidateItem, MarketUnderstandingOutput, OpportunityDiscoveryOutput


LONG_SCORE_CAPS = {
    "alignment": 2.5,
    "dual_support": 2.0,
    "temperature": 2.0,
    "research": 1.5,
    "tape": 2.0,
}
META_TO_STYLE_HINT = {
    "AI算力": "科技成长",
    "光通信_CPO": "科技成长",
    "半导体": "科技成长",
    "机器人": "科技成长",
    "电网储能": "顺周期",
    "商业航天低空": "科技成长",
}


@dataclass(frozen=True)
class PreviousBullState:
    symbol: str
    display_bucket: str | None = None
    confidence_score: float | None = None
    best_meta_theme: str | None = None
    best_concept: str | None = None
    appearances: int = 0
    streak: int = 0
    round_gap: int | None = None
    recent_repeat: bool = False
    consecutive_repeat: bool = False


RECENT_ROUND_GAP_LIMIT = 2
ANCHOR_MIN_CONSECUTIVE = 3
ANCHOR_MIN_APPEARANCES = 4
ANCHOR_MIN_SCORE = 9.0
NEW_NAME_BONUS = 0.6
REPEAT_PENALTY_BASE = 0.8
REPEAT_PENALTY_STEP = 0.4
MAX_REPEAT_PENALTY = 2.4
EVIDENCE_BONUS = 0.4
PROMINENT_REPEAT_NOVELTY = 0.35
HIGH_QUALITY_REPEAT_MIN_LONG_SCORE = 8.8
HIGH_QUALITY_REPEAT_MIN_NOVELTY = 0.15
HIGH_QUALITY_REPEAT_BONUS = 0.6
TREND_BACKBONE_CONTINUITY_BONUS = 2.0
DEEP_PULLBACK_CATCHUP_PENALTY = 0.6
MAINLINE_PRIMARY_CONCEPT_BONUS = 0.15
CATCHUP_UPGRADE_GUARD_PENALTY = 0.25
NEGATIVE_MAIN_FLOW_CATCHUP_PENALTY = 0.35
NEGATIVE_RET3_CATCHUP_PENALTY = 0.6
FRESH_CATCHUP_DISCOVERY_BONUS = 0.45
WEAK_REPEAT_NEW_LONG_CATCHUP_PENALTY = 0.4


def _concept_priority(market: MarketUnderstandingOutput) -> dict[str, float]:
    priority: dict[str, float] = {}
    for idx, concept in enumerate(market.strongest_concepts, start=1):
        priority[concept] = max(0.2, 1.0 - (idx - 1) * 0.12)
    for idx, concept in enumerate(market.acceleration_concepts, start=1):
        priority[concept] = max(priority.get(concept, 0.0), max(0.28, 0.96 - (idx - 1) * 0.10))
    return priority


def _primary_concepts_by_theme(market: MarketUnderstandingOutput) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    for item in market.top_meta_themes:
        meta_theme = str(item.meta_theme or "").strip()
        if not meta_theme:
            continue
        mapping[meta_theme] = {str(concept).strip() for concept in item.strongest_concepts if str(concept).strip()}
    return mapping


def _catchup_target_concepts(market: MarketUnderstandingOutput) -> list[str]:
    targets: list[str] = []
    top_theme_concepts: list[str] = []
    for item in market.top_meta_themes[:2]:
        for raw in item.strongest_concepts:
            concept = str(raw or "").strip()
            if concept and concept not in top_theme_concepts:
                top_theme_concepts.append(concept)
    for items in (market.acceleration_concepts, top_theme_concepts, market.strongest_concepts):
        for raw in items:
            text = str(raw or "").strip()
            if not text or text in targets:
                continue
            targets.append(text)
            if len(targets) >= 5:
                return targets
    return targets


def _derive_selected_focus_concepts(
    core_anchor: list[tuple[float, float, CandidateItem]],
    new_long: list[tuple[float, float, CandidateItem]],
    *,
    dominant_theme: str | None = None,
) -> set[str]:
    concepts: set[str] = set()
    selected = [item for _, _, item in core_anchor[:3]] + [item for _, _, item in new_long[:5]]
    theme_filtered = [
        item for item in selected if dominant_theme and str(item.best_meta_theme or "").strip() == dominant_theme
    ]
    focus_items = theme_filtered or selected
    for item in focus_items:
        text = str(item.best_concept or "").strip()
        if text:
            concepts.add(text)
    return concepts


def _clamp01(value: float | None) -> float:
    if value is None:
        return 0.0
    return max(0.0, min(1.0, float(value)))


def _safe(value: float | None, default: float = 0.0) -> float:
    return default if value is None else float(value)


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _candidate(
    fact: StockFact,
    *,
    display_bucket: str,
    score: float,
    reason: str,
    theme_name: str | None,
    concept_name: str | None,
    metadata: dict | None = None,
) -> CandidateItem:
    theme_label = theme_name or concept_name or "无明确主线"
    payload = metadata or {}
    return CandidateItem(
        symbol=fact.symbol,
        stock_name=fact.stock_name,
        display_bucket=display_bucket,
        confidence_score=round(score, 1),
        themes=fact.meta_themes[:3] or fact.concepts[:3],
        reason=reason,
        display_line=f"{fact.stock_name}({fact.symbol})｜{theme_label}｜涨幅{_fmt_pct(fact.pct_chg_prev_close)}｜日内位置{_clamp01(fact.range_position):.2f}",
        best_meta_theme=theme_name or fact.best_meta_theme,
        best_concept=concept_name or fact.best_concept,
        research_hooks=fact.research_hooks[:4],
        metadata={
            "pct_chg_prev_close": fact.pct_chg_prev_close,
            "range_position": fact.range_position,
            "money_pace_ratio": fact.money_pace_ratio,
            "bid_ask_imbalance": fact.bid_ask_imbalance,
            "volume_ratio": fact.volume_ratio,
            "turnover_rate": fact.turnover_rate,
            "flow_ratio": fact.flow_ratio,
            "amount_rank": fact.amount_rank,
            "float_mkt_cap_rank": fact.float_mkt_cap_rank,
            "ret_3d": fact.ret_3d,
            "ret_5d": fact.ret_5d,
            "ret_10d": fact.ret_10d,
            "style_names": fact.style_names,
            **payload,
        },
    )


def _previous_bucket(previous_state: dict[str, PreviousBullState] | None, symbol: str) -> str | None:
    if not previous_state:
        return None
    item = previous_state.get(symbol)
    if item is None:
        return None
    return item.display_bucket


def _novelty_score(
    fact: StockFact,
    *,
    score_10: float,
    previous: PreviousBullState | None,
    theme_name: str | None,
    concept_name: str | None,
) -> float:
    if previous is None or previous.display_bucket not in {"core_anchor", "new_long", "catchup"}:
        return 1.0

    novelty = 0.0
    previous_score = float(previous.confidence_score or 0.0)
    score_delta = score_10 - previous_score
    if score_delta >= 0.8:
        novelty += 0.45
    elif score_delta >= 0.4:
        novelty += 0.25

    if theme_name and previous.best_meta_theme and theme_name != previous.best_meta_theme:
        novelty += 0.30
    if concept_name and previous.best_concept and concept_name != previous.best_concept:
        novelty += 0.20

    pct = fact.pct_chg_prev_close or 0.0
    range_position = _clamp01(fact.range_position)
    flow_tail = 0.65 * fact.main_flow_rank + 0.35 * fact.super_flow_rank
    if pct >= 0.06 and range_position >= 0.70:
        novelty += 0.15
    if flow_tail >= 0.75:
        novelty += 0.10
    if previous.display_bucket == "catchup":
        novelty += 0.15
    return min(1.0, novelty)


def _resolve_theme_context(
    fact: StockFact,
    market: MarketUnderstandingOutput,
    concept_priority: dict[str, float],
) -> dict[str, float | int | str | None]:
    theme_rank_map = dict(market.meta_theme_rank_map)
    if not theme_rank_map:
        theme_rank_map = {
            item.meta_theme: int(item.rank or idx)
            for idx, item in enumerate(market.top_meta_themes, start=1)
            if item.meta_theme
        }
    theme_eq_return_map = dict(market.meta_theme_eq_return_map)
    if not theme_eq_return_map:
        theme_eq_return_map = {
            item.meta_theme: float(item.eq_return or 0.0)
            for item in market.top_meta_themes
            if item.meta_theme
        }

    best_theme = None
    best_theme_rank = 99
    for theme_name in fact.meta_themes:
        rank = int(theme_rank_map.get(theme_name, 99) or 99)
        if rank < best_theme_rank:
            best_theme_rank = rank
            best_theme = theme_name
    if best_theme is None:
        best_theme = fact.best_meta_theme

    primary_by_theme = _primary_concepts_by_theme(market)
    best_theme_primary_concepts = primary_by_theme.get(str(best_theme or "").strip(), set())
    strongest_concepts = {str(item).strip() for item in market.strongest_concepts[:5] if str(item).strip()}
    acceleration_concepts = {str(item).strip() for item in market.acceleration_concepts[:5] if str(item).strip()}
    has_primary_support = any(concept in best_theme_primary_concepts for concept in fact.concepts)

    best_concept = None
    best_concept_score = -1.0
    best_concept_rank = 999
    for concept_name in fact.concepts:
        overlay_score = float(market.concept_overlay_score_map.get(concept_name, 0.0) or 0.0)
        if overlay_score <= 0.0:
            overlay_score = concept_priority.get(concept_name, 0.0) * 0.75
        overlay_rank = int(market.concept_overlay_rank_map.get(concept_name, 999) or 999)
        if concept_name in best_theme_primary_concepts:
            overlay_score += 0.15
        elif has_primary_support and best_theme_primary_concepts:
            overlay_score -= 0.15
        if concept_name in strongest_concepts:
            overlay_score += 0.04
        if concept_name in acceleration_concepts:
            overlay_score += 0.03
        if overlay_score > best_concept_score or (overlay_score == best_concept_score and overlay_rank < best_concept_rank):
            best_concept = concept_name
            best_concept_score = overlay_score
            best_concept_rank = overlay_rank
    if best_concept is None:
        best_concept = fact.best_concept
        best_concept_score = concept_priority.get(best_concept or "", 0.0) * 0.75 if best_concept else 0.0

    theme_eq_return = float(theme_eq_return_map.get(best_theme or "", 0.0) or 0.0)
    relative_to_theme = (_safe(fact.pct_chg_prev_close) - theme_eq_return) if best_theme else None
    return {
        "best_theme": best_theme,
        "best_theme_rank": best_theme_rank,
        "best_concept": best_concept,
        "best_concept_score": best_concept_score,
        "theme_eq_return": theme_eq_return if best_theme else None,
        "relative_to_theme": relative_to_theme,
        "concept_support": len(fact.concepts),
        "theme_overlap": len(fact.meta_themes),
        "style_overlap": len(fact.style_names),
    }


def _repeat_adjustment(
    previous: PreviousBullState | None,
    *,
    novelty_score: float,
    long_score: float,
) -> dict[str, float | bool | int]:
    if previous is None:
        return {
            "recent_repeat": False,
            "consecutive_repeat": False,
            "current_streak": 1,
            "current_appearances": 1,
            "allow_prominent_repeat": False,
            "repeat_penalty": 0.0,
            "new_name_bonus": NEW_NAME_BONUS,
            "evidence_bonus": 0.0,
            "rank_score": long_score + NEW_NAME_BONUS,
            "display_score": min(10.0, long_score + NEW_NAME_BONUS),
            "anchor_candidate": False,
        }

    legacy_state = previous.round_gap is None and previous.display_bucket in {"core_anchor", "new_long", "catchup"}
    recent_repeat = bool(previous.recent_repeat or legacy_state)
    consecutive_repeat = bool(previous.consecutive_repeat or legacy_state)
    base_streak = int(previous.streak or (1 if legacy_state else 0))
    base_appearances = int(previous.appearances or (1 if legacy_state else 0))
    current_streak = base_streak + 1 if consecutive_repeat else 1
    current_appearances = base_appearances + 1
    high_quality_repeat = (
        recent_repeat
        and previous.display_bucket in {"new_long", "catchup"}
        and long_score >= HIGH_QUALITY_REPEAT_MIN_LONG_SCORE
        and novelty_score >= HIGH_QUALITY_REPEAT_MIN_NOVELTY
    )
    allow_prominent_repeat = recent_repeat and (
        novelty_score >= PROMINENT_REPEAT_NOVELTY or high_quality_repeat
    )

    repeat_penalty = 0.0
    if recent_repeat and not allow_prominent_repeat:
        repeat_penalty = min(
            MAX_REPEAT_PENALTY,
            REPEAT_PENALTY_BASE + max(0, current_streak - 2) * REPEAT_PENALTY_STEP,
        )
    new_name_bonus = 0.0 if recent_repeat else NEW_NAME_BONUS
    evidence_bonus = (
        HIGH_QUALITY_REPEAT_BONUS
        if high_quality_repeat
        else EVIDENCE_BONUS if allow_prominent_repeat else 0.0
    )
    rank_score = max(1.0, long_score - repeat_penalty + new_name_bonus + evidence_bonus)
    display_score = max(1.0, min(10.0, rank_score))
    anchor_candidate = (
        recent_repeat
        and not allow_prominent_repeat
        and current_streak >= ANCHOR_MIN_CONSECUTIVE
        and current_appearances >= ANCHOR_MIN_APPEARANCES
        and long_score >= ANCHOR_MIN_SCORE
    )
    return {
        "recent_repeat": recent_repeat,
        "consecutive_repeat": consecutive_repeat,
        "current_streak": current_streak,
        "current_appearances": current_appearances,
        "allow_prominent_repeat": allow_prominent_repeat,
        "high_quality_repeat": high_quality_repeat,
        "repeat_penalty": round(repeat_penalty, 2),
        "new_name_bonus": round(new_name_bonus, 2),
        "evidence_bonus": round(evidence_bonus, 2),
        "rank_score": round(rank_score, 2),
        "display_score": round(display_score, 2),
        "anchor_candidate": anchor_candidate,
    }


def _long_score_breakdown(
    fact: StockFact,
    context: dict[str, float | int | str | None],
) -> tuple[float, dict[str, float | int | str | None]]:
    meta_rank = int(context.get("best_theme_rank") or 99)
    best_meta_theme = str(context.get("best_theme") or "")
    best_concept_score = _safe(context.get("best_concept_score"))  # type: ignore[arg-type]
    concept_support = _safe(context.get("concept_support"))  # type: ignore[arg-type]
    theme_overlap = _safe(context.get("theme_overlap"))  # type: ignore[arg-type]

    alignment = 0.0
    alignment += {1: 1.6, 2: 1.3, 3: 1.0}.get(meta_rank, 0.6)
    alignment += 0.6 if best_concept_score >= 0.80 else 0.4 if best_concept_score >= 0.60 else 0.2
    alignment += 0.3 if best_meta_theme and best_meta_theme in fact.meta_themes else 0.0
    alignment = min(LONG_SCORE_CAPS["alignment"], alignment)

    dual_support = 0.0
    if 2 <= concept_support <= 7:
        dual_support += 1.2
    elif concept_support >= 8:
        dual_support += 0.8
    else:
        dual_support += 0.5
    dual_support += 0.4 if theme_overlap > 0 else 0.0
    dual_support += 0.4 if META_TO_STYLE_HINT.get(best_meta_theme) in fact.style_names else 0.0
    dual_support = min(LONG_SCORE_CAPS["dual_support"], dual_support)

    pct = _safe(fact.pct_chg_prev_close)
    ret3 = _safe(fact.ret_3d)
    ret5 = _safe(fact.ret_5d)
    ret10 = _safe(fact.ret_10d)
    temperature = LONG_SCORE_CAPS["temperature"]
    if pct > 0.06:
        temperature -= 0.4
    if pct > 0.075:
        temperature -= 0.5
    if ret3 > 0.10:
        temperature -= 0.4
    if ret5 > 0.15:
        temperature -= 0.4
    if ret10 > 0.22:
        temperature -= 0.3
    if _safe(fact.amplitude) > 0.12:
        temperature -= 0.3
    if _safe(fact.turnover_rate) > 0.18:
        temperature -= 0.2
    temperature = max(0.2, temperature)

    research = 0.0
    research += 0.7 if fact.onepage_path else 0.0
    research += 0.55 if fact.company_card_path else 0.0
    research += min(0.55, _safe(fact.research_coverage_score) * 0.7)
    research += min(0.5, float(fact.recent_intel_mentions) / 120.0)
    research = min(LONG_SCORE_CAPS["research"], research)

    pace = _safe(fact.money_pace_ratio)
    rp = _safe(fact.range_position)
    pfo = _safe(fact.open_ret)
    imbalance = _safe(fact.bid_ask_imbalance)
    main_flow = _safe(fact.main_net_inflow)
    large_flow = _safe(fact.super_net) + _safe(fact.large_net)
    tape = 0.0
    tape += 0.7 if pace >= 1.8 else 0.5 if pace >= 1.2 else 0.3
    tape += 0.6 if rp >= 0.6 else 0.4 if rp >= 0.4 else 0.2
    tape += 0.4 if -0.005 <= pfo <= 0.06 else 0.2 if pfo >= -0.02 else 0.0
    tape += 0.3 if imbalance >= -0.2 else 0.1
    tape += 0.35 if main_flow > 0 else 0.0
    tape += 0.25 if large_flow > 0 else 0.0
    tape += 0.2 if _safe(fact.volume_ratio) >= 1.5 else 0.0
    tape = min(LONG_SCORE_CAPS["tape"], tape)

    total = max(1.0, min(10.0, round(alignment + dual_support + temperature + research + tape, 1)))
    return total, {
        "alignment": round(alignment, 2),
        "dual_support": round(dual_support, 2),
        "temperature": round(temperature, 2),
        "research": round(research, 2),
        "tape": round(tape, 2),
        **context,
    }


def compute_opportunity_discovery(
    stock_facts: list[StockFact],
    market: MarketUnderstandingOutput,
    *,
    previous_state: dict[str, PreviousBullState] | None = None,
    core_limit: int = 3,
    new_long_limit: int = 5,
    catchup_limit: int = 5,
) -> OpportunityDiscoveryOutput:
    concept_priority = _concept_priority(market)
    dominant_primary_concepts = set()
    if market.top_meta_themes:
        dominant_primary_concepts = {
            str(item).strip()
            for item in market.top_meta_themes[0].strongest_concepts[:2]
            if str(item).strip()
        }
    acceleration_concepts = set(market.acceleration_concepts[:3])
    focus_concepts = acceleration_concepts | set(market.strongest_concepts[:3])
    fallback_catchup_focus_concepts = set(_catchup_target_concepts(market))
    focus_themes = {item.meta_theme for item in market.top_meta_themes[:3] if item.meta_theme}

    core_anchor: list[tuple[float, float, CandidateItem]] = []
    new_long: list[tuple[float, float, CandidateItem]] = []
    catchup: list[tuple[float, float, CandidateItem]] = []
    scored_facts: list[dict[str, object]] = []
    for fact in stock_facts:
        context = _resolve_theme_context(fact, market, concept_priority)
        best_theme = str(context.get("best_theme") or "") or None
        best_concept = str(context.get("best_concept") or "") or None
        previous_bucket = _previous_bucket(previous_state, fact.symbol)
        previous = previous_state.get(fact.symbol) if previous_state else None
        focus_support = sum(1 for concept in fact.concepts if concept in focus_concepts)
        long_score, score_breakdown = _long_score_breakdown(fact, context)

        pct = _safe(fact.pct_chg_prev_close)
        range_position = _clamp01(fact.range_position)
        base_focus = bool((best_theme and best_theme in focus_themes) or (best_concept and best_concept in focus_concepts))
        assigned_bucket = None

        if (
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
        ):
            novelty_score = _novelty_score(
                fact,
                score_10=long_score,
                previous=previous,
                theme_name=best_theme,
                concept_name=best_concept,
            )
            repeat_context = _repeat_adjustment(previous, novelty_score=novelty_score, long_score=long_score)
            capacity_anchor = (
                bool(repeat_context["recent_repeat"])
                and not bool(repeat_context["allow_prominent_repeat"])
                and _safe(fact.amount) >= 5e9
                and pct >= 0.015
                and pct <= 0.06
                and range_position >= 0.45
                and _safe(fact.ret_5d, -999.0) <= 0.12
                and float(repeat_context["rank_score"]) >= 9.0
            )
            should_anchor = bool(repeat_context["anchor_candidate"]) or (
                previous_bucket == "core_anchor"
                and bool(repeat_context["recent_repeat"])
                and not bool(repeat_context["allow_prominent_repeat"])
                and long_score >= 8.6
            ) or capacity_anchor
            metadata = {
                **score_breakdown,
                "novelty_score": round(novelty_score, 3),
                "repeat_penalty": repeat_context["repeat_penalty"],
                "new_name_bonus": repeat_context["new_name_bonus"],
                "evidence_bonus": repeat_context["evidence_bonus"],
                "rank_score": repeat_context["rank_score"],
                "display_score": repeat_context["display_score"],
                "recent_repeat": repeat_context["recent_repeat"],
                "consecutive_repeat": repeat_context["consecutive_repeat"],
                "repeat_streak": repeat_context["current_streak"],
                "repeat_appearances": repeat_context["current_appearances"],
                "capacity_anchor": capacity_anchor,
            }
            if should_anchor:
                streak = int(repeat_context["current_streak"])
                if capacity_anchor and streak <= 1:
                    reason = (
                        f"{best_theme or best_concept or '主线'}内容量和成交额持续靠前，"
                        f"近期已反复进入视野，本轮更适合作为中军锚定"
                    )
                else:
                    reason = (
                        f"{best_theme or best_concept or '主线'}已连续 {streak} 轮维持强势，"
                        f"当前没有足够新增变量，更适合作为核心锚定"
                    )
                core_anchor.append(
                    (
                        long_score,
                        _safe(fact.amount),
                        _candidate(
                            fact,
                            display_bucket="core_anchor",
                            score=long_score,
                            reason=reason,
                            theme_name=best_theme,
                            concept_name=best_concept,
                            metadata=metadata,
                        ),
                    )
                )
                assigned_bucket = "core_anchor"
            else:
                continuity_bonus = 0.0
                if (
                    previous_bucket == "new_long"
                    and bool(repeat_context["recent_repeat"])
                    and not bool(repeat_context["allow_prominent_repeat"])
                    and long_score >= 9.4
                    and _safe(fact.amount) >= 3e9
                    and _safe(fact.main_net_inflow) >= 1e8
                    and range_position >= 0.50
                    and _safe(fact.ret_10d, -999.0) <= 0.12
                ):
                    continuity_bonus = TREND_BACKBONE_CONTINUITY_BONUS
                mainline_primary_concept_bonus = (
                    MAINLINE_PRIMARY_CONCEPT_BONUS if best_concept and best_concept in dominant_primary_concepts else 0.0
                )
                catchup_upgrade_guard_penalty = 0.0
                if (
                    previous_bucket == "catchup"
                    and bool(repeat_context["high_quality_repeat"])
                    and (range_position < 0.45 or _safe(fact.ret_10d, -999.0) > 0.16)
                ):
                    catchup_upgrade_guard_penalty = CATCHUP_UPGRADE_GUARD_PENALTY
                rank_score = (
                    float(repeat_context["rank_score"])
                    + continuity_bonus
                    + mainline_primary_concept_bonus
                    - catchup_upgrade_guard_penalty
                )
                display_score = min(
                    10.0,
                    float(repeat_context["display_score"])
                    + continuity_bonus
                    + mainline_primary_concept_bonus
                    - catchup_upgrade_guard_penalty,
                )
                metadata["continuity_bonus"] = round(continuity_bonus, 2)
                metadata["mainline_primary_concept_bonus"] = round(mainline_primary_concept_bonus, 2)
                metadata["catchup_upgrade_guard_penalty"] = round(catchup_upgrade_guard_penalty, 2)
                metadata["rank_score"] = round(rank_score, 2)
                metadata["display_score"] = round(display_score, 2)
                reason = (
                    f"{best_theme or best_concept or '主线'}共振，涨幅{_fmt_pct(fact.pct_chg_prev_close)}，"
                    f"量价与资金同步改善，适合进入顺风新增"
                )
                new_long.append(
                    (
                        rank_score,
                        _safe(fact.amount),
                        _candidate(
                            fact,
                            display_bucket="new_long",
                            score=display_score,
                            reason=reason,
                            theme_name=best_theme,
                            concept_name=best_concept,
                            metadata=metadata,
                        ),
                    )
                )
                assigned_bucket = "new_long"
        scored_facts.append(
            {
                "fact": fact,
                "context": context,
                "best_theme": best_theme,
                "best_concept": best_concept,
                "previous_bucket": previous_bucket,
                "assigned_bucket": assigned_bucket,
            }
        )

    dominant_theme = str(market.top_meta_themes[0].meta_theme).strip() if market.top_meta_themes else None
    core_anchor.sort(key=lambda item: (item[0], item[1]), reverse=True)
    new_long.sort(key=lambda item: (item[0], item[1]), reverse=True)
    selected_focus_concepts = _derive_selected_focus_concepts(core_anchor, new_long, dominant_theme=dominant_theme)
    catchup_focus_concepts = selected_focus_concepts or fallback_catchup_focus_concepts

    for item in scored_facts:
        fact = item["fact"]  # type: ignore[assignment]
        context = item["context"]  # type: ignore[assignment]
        best_theme = item["best_theme"]  # type: ignore[assignment]
        best_concept = item["best_concept"]  # type: ignore[assignment]
        assigned_bucket = item["assigned_bucket"]  # type: ignore[assignment]
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
        deep_pullback_penalty = 0.0
        if _safe(fact.ret_5d) < -0.12 or _safe(fact.ret_10d) < -0.15:
            deep_pullback_penalty = DEEP_PULLBACK_CATCHUP_PENALTY
        duplicate_new_long_penalty = 0.0
        if assigned_bucket == "new_long" and range_position > 0.80 and _safe(fact.amount) < 2.5e9:
            duplicate_new_long_penalty = 0.40
        weak_repeat_new_long_penalty = 0.0
        if (
            assigned_bucket == "new_long"
            and _safe(fact.amount) < 3e9
            and range_position >= 0.50
            and _safe(fact.ret_3d) < 0.02
        ):
            weak_repeat_new_long_penalty = WEAK_REPEAT_NEW_LONG_CATCHUP_PENALTY
        negative_main_flow_penalty = NEGATIVE_MAIN_FLOW_CATCHUP_PENALTY if _safe(fact.main_net_inflow) < 0 else 0.0
        negative_ret3_penalty = NEGATIVE_RET3_CATCHUP_PENALTY if _safe(fact.ret_3d) < 0 else 0.0
        fresh_catchup_discovery_bonus = 0.0
        if (
            previous_bucket is None
            and money_pace_ratio >= 1.5
            and _safe(fact.ret_10d, -999.0) <= 0.08
            and best_concept == "AI智能体"
        ):
            fresh_catchup_discovery_bonus = FRESH_CATCHUP_DISCOVERY_BONUS
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
            + fresh_catchup_discovery_bonus
            - catchup_best_concept_penalty
            - catchup_pace_penalty
            - deep_pullback_penalty
            - duplicate_new_long_penalty
            - weak_repeat_new_long_penalty
            - negative_main_flow_penalty
            - negative_ret3_penalty
        )
        if tracking_text and "否" in tracking_text:
            catchup_score -= 0.35
        pct = _safe(fact.pct_chg_prev_close)
        range_position = _clamp01(fact.range_position)
        if (
            assigned_bucket != "core_anchor"
            and
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
        ):
            reason = (
                f"{best_theme or best_concept or '主线'}仍强，但近3/10日相对不算最热，"
                f"当前量能开始抬升，适合当补涨观察"
            )
            catchup.append(
                (
                    catchup_score,
                    _safe(fact.amount),
                    _candidate(
                        fact,
                        display_bucket="catchup",
                        score=min(10.0, max(1.0, catchup_score * 1.35 + 4.2)),
                        reason=reason,
                        theme_name=best_theme,
                        concept_name=best_concept,
                        metadata={
                            **context,
                            "catchup_score_raw": round(catchup_score, 3),
                            "catchup_reset_score": round(catchup_reset_score, 3),
                            "focus_support": catchup_focus_support,
                            "best_concept_focus": best_concept_focus,
                            "company_card_quality_score": round(company_card_quality_score, 3),
                            "company_card_tracking_recommendation": tracking_text or None,
                            "catchup_pace_penalty": round(catchup_pace_penalty, 3),
                            "catchup_best_concept_penalty": round(catchup_best_concept_penalty, 3),
                            "deep_pullback_penalty": round(deep_pullback_penalty, 3),
                            "duplicate_new_long_penalty": round(duplicate_new_long_penalty, 3),
                            "weak_repeat_new_long_penalty": round(weak_repeat_new_long_penalty, 3),
                            "negative_main_flow_penalty": round(negative_main_flow_penalty, 3),
                            "negative_ret3_penalty": round(negative_ret3_penalty, 3),
                            "fresh_catchup_discovery_bonus": round(fresh_catchup_discovery_bonus, 3),
                        },
                    ),
                )
            )

    catchup.sort(key=lambda item: (item[0], item[1]), reverse=True)

    return OpportunityDiscoveryOutput(
        core_anchor_watchlist=[item for _, _, item in core_anchor[:core_limit]],
        new_long_watchlist=[item for _, _, item in new_long[:new_long_limit]],
        catchup_watchlist=[item for _, _, item in catchup[:catchup_limit]],
    )
