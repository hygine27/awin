from __future__ import annotations

from awin.contracts.m0 import (
    AlertChange,
    AlertDiffResult,
    AlertMaterial,
    AlertOutput,
    MarketUnderstandingOutput,
    OpportunityDiscoveryOutput,
    RiskSurveillanceOutput,
    RunContext,
)


def _unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        value = str(raw).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _candidate_symbols(items, max_names: int) -> list[str]:
    return _unique_preserve_order([item.symbol for item in items])[:max_names]


def _format_value(value) -> str:
    if value is None:
        return "none"
    if isinstance(value, list):
        return " / ".join(str(item) for item in value) if value else "none"
    text = str(value).strip()
    return text or "none"


def build_alert_material(
    market: MarketUnderstandingOutput,
    opportunity: OpportunityDiscoveryOutput,
    risk: RiskSurveillanceOutput,
    *,
    top_theme_limit: int = 3,
    watchlist_limit: int = 5,
) -> AlertMaterial:
    return AlertMaterial(
        confirmed_style=market.confirmed_style,
        latest_status=market.latest_status,
        latest_dominant_style=market.latest_dominant_style,
        market_regime=market.market_regime,
        top_meta_themes=[item.meta_theme for item in market.top_meta_themes[:top_theme_limit]],
        core_anchor_symbols=_candidate_symbols(opportunity.core_anchor_watchlist, watchlist_limit),
        new_long_symbols=_candidate_symbols(opportunity.new_long_watchlist, watchlist_limit),
        short_symbols=_candidate_symbols(risk.short_watchlist, watchlist_limit),
        catchup_symbols=_candidate_symbols(opportunity.catchup_watchlist, watchlist_limit),
    )


def diff_alert_material(previous: AlertMaterial, current: AlertMaterial) -> AlertDiffResult:
    changes: list[AlertChange] = []

    comparable_fields = [
        "confirmed_style",
        "latest_status",
        "latest_dominant_style",
        "market_regime",
        "top_meta_themes",
        "core_anchor_symbols",
        "new_long_symbols",
        "short_symbols",
        "catchup_symbols",
    ]

    for field_name in comparable_fields:
        previous_value = getattr(previous, field_name)
        current_value = getattr(current, field_name)
        if previous_value == current_value:
            continue

        change_type = "set_change" if isinstance(current_value, list) else "value_change"
        changes.append(
            AlertChange(
                field_name=field_name,
                previous=previous_value,
                current=current_value,
                change_type=change_type,
            )
        )

    decision = "UPDATE" if changes else "NO_UPDATE"
    return AlertDiffResult(decision=decision, changes=changes)


def render_alert_body(
    run_context: RunContext,
    market: MarketUnderstandingOutput,
    material: AlertMaterial,
    diff_result: AlertDiffResult,
) -> str:
    if diff_result.decision == "NO_UPDATE":
        return "NO_UPDATE"

    timestamp = f"{run_context.trade_date} {run_context.snapshot_time[:5]}"
    change_summary = "；".join(
        f"{change.field_name}: {_format_value(change.previous)} -> {_format_value(change.current)}"
        for change in diff_result.changes[:6]
    )

    lines = [f"[{timestamp}] 盘中风格监控有更新：{change_summary}。"]
    lines.append(
        "主风格 {style}｜状态 {status}｜主导方向 {direction}。".format(
            style=_format_value(market.confirmed_style),
            status=_format_value(market.latest_status),
            direction=_format_value(market.latest_dominant_style),
        )
    )

    if material.top_meta_themes:
        lines.append(f"最强主题：{_format_value(material.top_meta_themes)}。")
    if material.new_long_symbols:
        lines.append(f"顺风观察：{_format_value(material.new_long_symbols)}。")
    if material.short_symbols:
        lines.append(f"风险预警：{_format_value(material.short_symbols)}。")
    if material.catchup_symbols:
        lines.append(f"潜在补涨：{_format_value(material.catchup_symbols)}。")

    return "\n".join(lines)


def build_alert_output(
    run_context: RunContext,
    market: MarketUnderstandingOutput,
    opportunity: OpportunityDiscoveryOutput,
    risk: RiskSurveillanceOutput,
    previous_material: AlertMaterial | None = None,
) -> AlertOutput:
    material = build_alert_material(market, opportunity, risk)
    baseline = previous_material or AlertMaterial()
    diff_result = diff_alert_material(baseline, material)
    alert_body = render_alert_body(run_context, market, material, diff_result)
    return AlertOutput(material=material, diff_result=diff_result, alert_body=alert_body)
