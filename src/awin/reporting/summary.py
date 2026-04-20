from __future__ import annotations

from awin.builders.m0 import M0BuildResult
from awin.contracts.m0 import CandidateItem, MarketUnderstandingOutput, OpportunityDiscoveryOutput, RiskSurveillanceOutput
from awin.opportunity_discovery.config import load_opportunity_rules


STATUS_TEXT = {
    "stable": "维持领先",
    "observation": "进入观察",
    "confirmation": "继续强化",
    "backswitch": "发生切换",
}

SOURCE_STATUS_TEXT = {
    "READY": "可用",
    "DEGRADED": "降级",
    "MISSING": "缺失",
    "FALLBACK": "回退",
    "UNKNOWN": "未知",
}

REGIME_TEXT = {
    "trend_expansion": "赚钱效应扩散",
    "mixed_rotation": "混合轮动",
    "weak_market_relative_strength": "弱市中的相对强势",
    "mixed_tape": "盘面分化",
}

BUCKET_TEXT = {
    "core_anchor": "核心锚定",
    "new_long": "顺风看多",
    "catchup": "潜在补涨",
}

RISK_TEXT = {
    "overheat": "过热",
    "weak": "偏弱",
    "warning": "预警",
}

OPPORTUNITY_RULES = load_opportunity_rules()
LONG_SCORE_CAPS = {key: float(value) for key, value in OPPORTUNITY_RULES["long_score_caps"].items()}
MODULE_SCORE_CAP_TOTAL = round(sum(LONG_SCORE_CAPS.values()), 2)
CATCHUP_RULES = OPPORTUNITY_RULES["catchup_rules"]
CATCHUP_SCORE_FORMULA = CATCHUP_RULES["score_formula"]
CATCHUP_FOCUS_BONUS_RULES = CATCHUP_RULES["focus_bonus"]


def _fmt_percent(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.{digits}f}%"


def _fmt_minutes(seconds: int | None) -> str:
    if seconds is None:
        return "n/a"
    minutes = max(0, int(round(seconds / 60.0)))
    return f"{minutes} 分钟"


def _fmt_multiple(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}x"


def _fmt_position(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{max(0.0, min(1.0, float(value))):.2f}"


def _fmt_amount_yy(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{float(value) / 100000000:.2f} 亿"


def _fmt_signed_amount_yy(value: float | None) -> str:
    if value is None:
        return "n/a"
    number = float(value) / 100000000
    sign = "+" if number > 0 else ""
    return f"{sign}{number:.2f} 亿"


def _fmt_signed_amount_wy(value: float | None) -> str:
    if value is None:
        return "n/a"
    number = float(value) / 10000
    sign = "+" if number > 0 else ""
    return f"{sign}{number:.0f} 万"


def _fmt_signed_amount_auto(value: float | None) -> str:
    if value is None:
        return "n/a"
    amount = float(value)
    if abs(amount) >= 100000000:
        return _fmt_signed_amount_yy(amount)
    if abs(amount) >= 10000:
        return f"{amount / 10000:+.1f} 万".replace("+", "+")
    return f"{amount:+.0f} 元".replace("+", "+")


def _fmt_score(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def _theme_list(market: MarketUnderstandingOutput, limit: int = 3) -> str:
    names = [item.meta_theme for item in market.top_meta_themes[:limit] if item.meta_theme]
    return " / ".join(names) if names else "暂无"


def _source_status_text(value: object) -> str:
    status = str(value or "UNKNOWN").upper()
    return SOURCE_STATUS_TEXT.get(status, status)


def _translate_line(text: str) -> str:
    rendered = str(text)
    for raw, label in REGIME_TEXT.items():
        rendered = rendered.replace(raw, label)
    return rendered


def _title_line(market: MarketUnderstandingOutput, trade_date: str, snapshot_time: str) -> str:
    status_text = STATUS_TEXT.get(market.latest_status, "延续")
    return f"{trade_date} {snapshot_time[:5]} {market.confirmed_style or '暂无主风格'}{status_text}，{_theme_list(market)}活跃"


def _source_evidence_lines(build_result: M0BuildResult) -> list[str]:
    lines: list[str] = []
    qmt = build_result.source_health.get("qmt_ashare_snapshot_5m", {})
    dcf = build_result.source_health.get("dcf_hq_zj_snapshot", {})
    ths_cli = build_result.source_health.get("ths_cli_hot_concept", {})

    if qmt:
        lines.append(
            "- QMT 5 分钟快照：{status}｜覆盖率 {coverage}｜新鲜度 {freshness}。".format(
                status=_source_status_text(qmt.get("source_status")),
                coverage=_fmt_percent(qmt.get("coverage_ratio"), 1),
                freshness=_fmt_minutes(qmt.get("freshness_seconds")),
            )
        )
    if dcf:
        lines.append(
            "- DCF 行情/资金增强：{status}｜覆盖率 {coverage}｜新鲜度 {freshness}。".format(
                status=_source_status_text(dcf.get("source_status")),
                coverage=_fmt_percent(dcf.get("coverage_ratio"), 1),
                freshness=_fmt_minutes(dcf.get("freshness_seconds")),
            )
        )
    if ths_cli:
        lines.append(
            "- THS 热概念快照：{status}｜新鲜度 {freshness}。".format(
                status=_source_status_text(ths_cli.get("source_status")),
                freshness=_fmt_minutes(ths_cli.get("freshness_seconds")),
            )
        )
    return lines


def _render_candidate(item: CandidateItem, *, prefix: str | None = None) -> str:
    theme_label = " / ".join(item.themes[:3]) if item.themes else "无明确主线"
    head = prefix or BUCKET_TEXT.get(item.display_bucket, item.display_bucket)
    score_text = _candidate_score_text(item)
    return f"- {head}：{item.stock_name}（{item.symbol}）｜{theme_label}｜{score_text}｜{item.reason}"


def _render_risk(item: CandidateItem) -> str:
    risk_label = RISK_TEXT.get(item.risk_tag or "", item.risk_tag or "风险")
    theme_label = " / ".join(item.themes[:3]) if item.themes else "无明确主线"
    score_text = _candidate_score_text(item)
    return f"- {risk_label}：{item.stock_name}（{item.symbol}）｜{theme_label}｜{score_text}｜{item.reason}"


def _numeric_meta(item: CandidateItem, field_name: str) -> float | None:
    value = item.metadata.get(field_name) if item.metadata else None
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _module_raw_total(item: CandidateItem) -> float | None:
    fields = ("alignment", "dual_support", "temperature", "research", "tape", "profile")
    values = [_numeric_meta(item, field_name) for field_name in fields]
    if any(value is None for value in values):
        return None
    return round(sum(value or 0.0 for value in values), 2)


def _candidate_score_text(item: CandidateItem) -> str:
    raw_module_total = _module_raw_total(item)
    normalized_module_score = _normalize_score_10(raw_module_total, MODULE_SCORE_CAP_TOTAL)
    if normalized_module_score is not None:
        return f"模块强度{_fmt_score(normalized_module_score, 1)}/10"

    catchup_score = _numeric_meta(item, "catchup_score_raw")
    if catchup_score is not None:
        return f"补涨原始分{_fmt_score(catchup_score, 2)}"

    if item.risk_tag is not None:
        relative_to_theme = _numeric_meta(item, "relative_to_theme")
        if relative_to_theme is not None:
            return f"相对主题偏离{_fmt_percent(relative_to_theme)}"
        return "风险观察"

    return f"{item.confidence_score:.1f}/10"


def _catchup_breakdown(item: CandidateItem) -> dict[str, float]:
    money_pace_ratio = _numeric_meta(item, "money_pace_ratio") or 0.0
    range_position = _numeric_meta(item, "range_position") or 0.0
    company_card_quality = _numeric_meta(item, "company_card_quality_score") or 0.0
    amount = _numeric_meta(item, "amount") or 0.0
    main_net_inflow = _numeric_meta(item, "main_net_inflow") or 0.0
    ret_3d = _numeric_meta(item, "ret_3d") or 0.0
    focus_support = _numeric_meta(item, "focus_support") or 0.0
    best_concept_focus = bool(item.metadata.get("best_concept_focus")) if item.metadata else False
    tracking_text = str(item.metadata.get("company_card_tracking_recommendation") or "") if item.metadata else ""

    money_pace_component = min(float(CATCHUP_SCORE_FORMULA["money_pace_cap"]), money_pace_ratio) * float(
        CATCHUP_SCORE_FORMULA["money_pace_weight"]
    )
    range_position_component = range_position * float(CATCHUP_SCORE_FORMULA["range_position_weight"])
    research_component = company_card_quality * float(CATCHUP_SCORE_FORMULA["company_card_quality_weight"])
    amount_component = min(1.0, amount / float(CATCHUP_RULES["amount_norm_divisor"])) * float(
        CATCHUP_SCORE_FORMULA["amount_norm_weight"]
    )
    flow_component = min(1.0, max(0.0, main_net_inflow) / float(CATCHUP_RULES["flow_norm_divisor"])) * float(
        CATCHUP_SCORE_FORMULA["flow_norm_weight"]
    )
    reset_component = (
        float(CATCHUP_SCORE_FORMULA["ret_3d_anchor"])
        - max(0.0, min(float(CATCHUP_SCORE_FORMULA["ret_3d_clip_max"]), ret_3d))
    ) * float(CATCHUP_SCORE_FORMULA["ret_3d_weight"])
    focus_bonus = (
        float(CATCHUP_FOCUS_BONUS_RULES["best_concept"])
        if best_concept_focus
        else float(CATCHUP_FOCUS_BONUS_RULES["support_ge_2"])
        if focus_support >= 2
        else 0.0
    )
    fresh_bonus = _numeric_meta(item, "fresh_catchup_discovery_bonus") or 0.0
    penalties = sum(
        max(0.0, _numeric_meta(item, field_name) or 0.0)
        for field_name in (
            "catchup_best_concept_penalty",
            "catchup_pace_penalty",
            "deep_pullback_penalty",
            "duplicate_new_long_penalty",
            "weak_repeat_new_long_penalty",
            "negative_main_flow_penalty",
            "negative_ret3_penalty",
        )
    )
    if tracking_text and "否" in tracking_text:
        penalties += float(CATCHUP_SCORE_FORMULA["tracking_block_penalty"])

    return {
        "money_pace_component": round(money_pace_component, 2),
        "range_position_component": round(range_position_component, 2),
        "research_component": round(research_component, 2),
        "amount_component": round(amount_component, 2),
        "flow_component": round(flow_component, 2),
        "reset_component": round(reset_component, 2),
        "focus_bonus": round(focus_bonus, 2),
        "fresh_bonus": round(fresh_bonus, 2),
        "penalties": round(penalties, 2),
    }


def _normalize_score_10(value: float | None, score_max: float) -> float | None:
    if value is None or score_max <= 0:
        return None
    return round(float(value) / float(score_max) * 10.0, 2)


def _score_level(value: float | None, score_max: float) -> str:
    if value is None or score_max <= 0:
        return "n/a"
    ratio = float(value) / float(score_max)
    if ratio >= 0.9:
        return "高"
    if ratio >= 0.7:
        return "中上"
    if ratio >= 0.5:
        return "中等"
    if ratio >= 0.3:
        return "偏弱"
    return "低"


def _temperature_level(value: float | None, score_max: float) -> str:
    if value is None or score_max <= 0:
        return "n/a"
    ratio = float(value) / float(score_max)
    if ratio >= 0.9:
        return "健康"
    if ratio >= 0.7:
        return "可控"
    if ratio >= 0.5:
        return "中性"
    return "偏热"


def _component_line(label: str, field_name: str, item: CandidateItem, *, semantic: str = "generic") -> str:
    current = _numeric_meta(item, field_name)
    score_max = LONG_SCORE_CAPS[field_name]
    if semantic == "temperature":
        level = _temperature_level(current, score_max)
    else:
        level = _score_level(current, score_max)
    return f"{label} {_fmt_score(current)}/{_fmt_score(score_max)}（{level}）"


def _risk_flow_explanation(item: CandidateItem) -> str:
    amount_1d = _numeric_meta(item, "main_net_amount_1d")
    amount_3d = _numeric_meta(item, "main_net_amount_3d_sum")
    amount_5d = _numeric_meta(item, "main_net_amount_5d_sum")
    outflow_streak_days = _numeric_meta(item, "outflow_streak_days")
    flow_acceleration_3d = _numeric_meta(item, "flow_acceleration_3d")
    divergence_flag = bool(item.metadata.get("price_flow_divergence_flag")) if item.metadata else False

    parts: list[str] = []
    if amount_1d is not None:
        parts.append(f"近1日主力净额 {_fmt_signed_amount_auto(amount_1d)}")
    if amount_5d is not None:
        parts.append(f"近5日累计 {_fmt_signed_amount_auto(amount_5d)}")
    elif amount_3d is not None:
        parts.append(f"近3日累计 {_fmt_signed_amount_auto(amount_3d)}")

    if outflow_streak_days is not None and outflow_streak_days >= 2:
        parts.append(f"已连续流出 {int(outflow_streak_days)} 天")
    elif flow_acceleration_3d is not None and flow_acceleration_3d <= -5000:
        parts.append("近3日边际转弱")

    if divergence_flag:
        parts.append("呈现价强资弱")

    if parts:
        return "，".join(parts)
    return "资金侧暂无额外恶化证据"


def _candidate_rank_key(item: CandidateItem) -> tuple[float, float, float, float]:
    display_score = _numeric_meta(item, "display_score")
    rank_score = _numeric_meta(item, "rank_score")
    amount = _numeric_meta(item, "amount")
    return (
        display_score if display_score is not None else float(item.confidence_score),
        rank_score if rank_score is not None else float(item.confidence_score),
        float(item.confidence_score),
        amount if amount is not None else 0.0,
    )


def _sorted_bullish_items(opportunity: OpportunityDiscoveryOutput) -> list[CandidateItem]:
    items = list(opportunity.core_anchor_watchlist) + list(opportunity.new_long_watchlist)
    items.sort(key=_candidate_rank_key, reverse=True)
    return items


def _lead_candidate_lines(item: CandidateItem, *, section_name: str) -> list[str]:
    theme_label = " / ".join(item.themes[:3]) if item.themes else "无明确主线"
    best_theme_rank = _numeric_meta(item, "best_theme_rank")
    alignment = _numeric_meta(item, "alignment")
    tape = _numeric_meta(item, "tape")
    profile = _numeric_meta(item, "profile")
    research = _numeric_meta(item, "research")
    money_pace = _numeric_meta(item, "money_pace_ratio")
    range_position = _numeric_meta(item, "range_position")
    volume_ratio = _numeric_meta(item, "volume_ratio")
    amount = _numeric_meta(item, "amount")
    main_net_inflow = _numeric_meta(item, "main_net_inflow")
    ret_3d = _numeric_meta(item, "ret_3d")
    ret_10d = _numeric_meta(item, "ret_10d")
    catchup_score = _numeric_meta(item, "catchup_score_raw")
    relative_to_theme = _numeric_meta(item, "relative_to_theme")
    amplitude = _numeric_meta(item, "amplitude")
    ret_20d = _numeric_meta(item, "ret_20d")
    dual_support = _numeric_meta(item, "dual_support")
    temperature = _numeric_meta(item, "temperature")
    rank_score = _numeric_meta(item, "rank_score")
    new_name_bonus = _numeric_meta(item, "new_name_bonus")
    mainline_primary_concept_bonus = _numeric_meta(item, "mainline_primary_concept_bonus")
    raw_module_total = _module_raw_total(item)
    normalized_module_score = _normalize_score_10(raw_module_total, MODULE_SCORE_CAP_TOTAL)

    lines = [
        f"- 本节首选：{item.stock_name}（{item.symbol}）｜{theme_label}｜{item.reason}"
    ]
    if section_name == "bullish":
        lines.append(
            "- 首选依据：所属主线当前排第 {rank}；模块强度 {module_score}/10（原始 {raw_total}/{raw_max}）；内部排序分 {rank_score}。".format(
                rank=int(best_theme_rank) if best_theme_rank is not None else "n/a",
                module_score=_fmt_score(normalized_module_score, 1),
                raw_total=_fmt_score(raw_module_total),
                raw_max=_fmt_score(MODULE_SCORE_CAP_TOTAL),
                rank_score=_fmt_score(rank_score),
            )
        )
        lines.append(
            "- 模块拆解：{alignment}｜{dual_support}｜{temperature}｜{research}｜{tape}｜{profile}。".format(
                alignment=_component_line("主线一致性", "alignment", item),
                dual_support=_component_line("双重支撑", "dual_support", item),
                temperature=_component_line("温度", "temperature", item, semantic="temperature"),
                research=_component_line("研究覆盖", "research", item),
                tape=_component_line("盘口节奏", "tape", item),
                profile=_component_line("风格画像", "profile", item),
            )
        )
        lines.append(
            "- 排序补充：新晋加分 {new_bonus}｜主概念加分 {concept_bonus}｜资金节奏 {pace}，日内位置 {position}，量比 {volume}，成交额 {amount}，主力净流入 {flow}。".format(
                new_bonus=f"+{_fmt_score(new_name_bonus)}" if new_name_bonus is not None else "n/a",
                concept_bonus=f"+{_fmt_score(mainline_primary_concept_bonus)}" if mainline_primary_concept_bonus is not None else "n/a",
                pace=_fmt_multiple(money_pace),
                position=_fmt_position(range_position),
                volume=_fmt_multiple(volume_ratio),
                amount=_fmt_amount_yy(amount),
                flow=_fmt_signed_amount_yy(main_net_inflow),
            )
        )
    elif section_name == "catchup":
        catchup_parts = _catchup_breakdown(item)
        lines.append(
            "- 首选依据：补涨原始分 {score}（同类排序用）；所属主线当前排第 {rank}；近3日 {ret3} / 10日 {ret10}，"
            "资金节奏 {pace}，日内位置 {position}，量比 {volume}，主力净流入 {flow}。".format(
                score=f"{catchup_score:.2f}" if catchup_score is not None else "n/a",
                rank=int(best_theme_rank) if best_theme_rank is not None else "n/a",
                ret3=_fmt_percent(ret_3d),
                ret10=_fmt_percent(ret_10d),
                pace=_fmt_multiple(money_pace),
                position=_fmt_position(range_position),
                volume=_fmt_multiple(volume_ratio),
                flow=_fmt_signed_amount_yy(main_net_inflow),
            )
        )
        lines.append(
            "- 补涨拆解：盘口转强 {money_pace}｜位置强度 {range_position}｜研究质量 {research}｜成交承接 {amount}｜资金承接 {flow}｜相对滞涨 {reset}｜焦点奖励 +{focus_bonus}｜新发现奖励 +{fresh_bonus}｜惩罚 {penalties}。".format(
                money_pace=_fmt_score(catchup_parts["money_pace_component"]),
                range_position=_fmt_score(catchup_parts["range_position_component"]),
                research=_fmt_score(catchup_parts["research_component"]),
                amount=_fmt_score(catchup_parts["amount_component"]),
                flow=_fmt_score(catchup_parts["flow_component"]),
                reset=_fmt_score(catchup_parts["reset_component"]),
                focus_bonus=_fmt_score(catchup_parts["focus_bonus"]),
                fresh_bonus=_fmt_score(catchup_parts["fresh_bonus"]),
                penalties=_fmt_score(catchup_parts["penalties"]),
            )
        )
    else:
        lines.append(
            "- 风险依据：相对主题偏离 {relative}，近10日 {ret10} / 20日 {ret20}，振幅 {amplitude}，"
            "{flow_signal}。".format(
                relative=_fmt_percent(relative_to_theme),
                ret10=_fmt_percent(ret_10d),
                ret20=_fmt_percent(ret_20d),
                amplitude=_fmt_percent(amplitude),
                flow_signal=_risk_flow_explanation(item),
            )
        )
    return lines


def _section_or_placeholder(title: str, lines: list[str], placeholder: str) -> list[str]:
    section = [title]
    if lines:
        section.extend(lines)
    else:
        section.append(f"- {placeholder}")
    return section


def _score_legend_lines() -> list[str]:
    return [
        "## 评分说明",
        f"- 模块强度分：把 6 个模块原始和按理论上限 {_fmt_score(MODULE_SCORE_CAP_TOTAL)} 归一化到 10 分，便于业务阅读。",
        "- 内部排序分：在模块强度之外，再叠加新晋加分、主概念加分等排序项；只用于同类候选排序，可以高于 10。",
        f"- 主线一致性 alignment：0-{_fmt_score(LONG_SCORE_CAPS['alignment'])}，越高越说明股票与当前强主线、强概念越一致。",
        f"- 双重支撑 dual_support：0-{_fmt_score(LONG_SCORE_CAPS['dual_support'])}，越高越说明有多概念、多主题或风格标签共同支持。",
        f"- 温度 temperature：约 0.2-{_fmt_score(LONG_SCORE_CAPS['temperature'])}，越高越说明不过热；越低说明短线涨幅、换手或振幅已偏拥挤。",
        f"- 研究覆盖 research：0-{_fmt_score(LONG_SCORE_CAPS['research'])}，越高越说明 onepage、公司卡、情报覆盖越充分。",
        f"- 盘口节奏 tape：0-{_fmt_score(LONG_SCORE_CAPS['tape'])}，越高越说明资金节奏、日内位置、量比和主力流向更健康；满分只代表健康，不代表全市场最强。",
        f"- 风格画像 profile：0-{_fmt_score(LONG_SCORE_CAPS['profile'])}，越高越说明慢变量风格与历史资金画像更支持当前机会。",
        "- 新晋加分：当前轮新进入重点视野时给予的排序加分，当前规则默认 +0.60，仅用于排序。",
        "- 主概念加分：命中当前主线核心概念时给予的排序加分，当前规则默认 +0.15，仅用于排序。",
        "- 补涨原始分：只在 catchup 候选内部排序时使用，不是 10 分制；越高越说明这只票既有盘口转强和资金承接，又没有短线涨得太透支。",
        "- 补涨拆解：默认拆成盘口转强、位置强度、研究质量、成交承接、资金承接、相对滞涨、焦点奖励、新发现奖励、惩罚项。",
    ]


def render_intraday_summary(build_result: M0BuildResult) -> str:
    bundle = build_result.bundle
    run_context = bundle.run_context
    market = bundle.market_understanding
    opportunity = bundle.opportunity_discovery
    risk = bundle.risk_surveillance

    lines = [
        _title_line(market, run_context.trade_date, run_context.snapshot_time),
        "",
        f"时间：{run_context.trade_date} {run_context.snapshot_time[:5]}",
        "",
        "## 结论与证据",
        f"- {_translate_line(market.summary_line)}",
    ]
    lines.extend(_source_evidence_lines(build_result))
    lines.extend(_translate_line(line) for line in market.evidence_lines)
    lines.append("")
    bullish_items = _sorted_bullish_items(opportunity)
    bullish_lines: list[str] = []
    if bullish_items:
        bullish_lines.extend(_lead_candidate_lines(bullish_items[0], section_name="bullish"))
        for item in bullish_items[1:5]:
            bullish_lines.append(_render_candidate(item, prefix=BUCKET_TEXT.get(item.display_bucket, "顺风看多")))
    lines.extend(_section_or_placeholder("## 顺风看多观察", bullish_lines, "当前无明确顺风看多观察。"))
    lines.append("")
    catchup_lines: list[str] = []
    if opportunity.catchup_watchlist:
        catchup_lines.extend(_lead_candidate_lines(opportunity.catchup_watchlist[0], section_name="catchup"))
        for item in opportunity.catchup_watchlist[1:5]:
            catchup_lines.append(_render_candidate(item, prefix="潜在补涨"))
    lines.extend(
        _section_or_placeholder(
            "## 潜在补涨观察",
            catchup_lines,
            "当前无明确补涨候选。",
        )
    )
    lines.append("")
    risk_lines: list[str] = []
    if risk.short_watchlist:
        risk_lines.extend(_lead_candidate_lines(risk.short_watchlist[0], section_name="risk"))
        for item in risk.short_watchlist[1:5]:
            risk_lines.append(_render_risk(item))
    lines.extend(
        _section_or_placeholder(
            "## 偏空 / 过热预警",
            risk_lines,
            "当前无明确风险预警。",
        )
    )
    lines.append("")
    lines.extend(_score_legend_lines())
    return "\n".join(lines).strip()
