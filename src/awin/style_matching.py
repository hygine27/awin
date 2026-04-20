from __future__ import annotations

from typing import Any, Mapping


SUPPORTED_STYLE_BASKET_KEYS = {
    "industries",
    "market_types",
    "ownership_styles",
    "size_bucket_abs_in",
    "size_bucket_pct_in",
    "capacity_bucket_in",
    "composite_labels",
    "match_mode",
}
SUPPORTED_STYLE_MATCH_MODES = {"any", "all"}


def _normalize_set(values: object) -> set[str]:
    if not isinstance(values, list):
        return set()
    return {str(item).strip() for item in values if str(item).strip()}


def style_rule_matches(
    style_rule: Mapping[str, Any],
    *,
    industry: str | None = None,
    market_type: str | None = None,
    ownership_style: str | None = None,
    size_bucket_abs: str | None = None,
    size_bucket_pct: str | None = None,
    capacity_bucket: str | None = None,
    composite_labels: list[str] | None = None,
) -> bool:
    match_mode = str(style_rule.get("match_mode") or "any").strip().lower()
    if match_mode not in SUPPORTED_STYLE_MATCH_MODES:
        return False

    checks: list[bool] = []
    composite_label_set = {str(item).strip() for item in composite_labels or [] if str(item).strip()}

    industries = _normalize_set(style_rule.get("industries"))
    if industries:
        checks.append(bool(industry and industry in industries))

    market_types = _normalize_set(style_rule.get("market_types"))
    if market_types:
        checks.append(bool(market_type and market_type in market_types))

    ownership_styles = _normalize_set(style_rule.get("ownership_styles"))
    if ownership_styles:
        checks.append(bool(ownership_style and ownership_style in ownership_styles))

    size_bucket_abs_in = _normalize_set(style_rule.get("size_bucket_abs_in"))
    if size_bucket_abs_in:
        checks.append(bool(size_bucket_abs and size_bucket_abs in size_bucket_abs_in))

    size_bucket_pct_in = _normalize_set(style_rule.get("size_bucket_pct_in"))
    if size_bucket_pct_in:
        checks.append(bool(size_bucket_pct and size_bucket_pct in size_bucket_pct_in))

    capacity_bucket_in = _normalize_set(style_rule.get("capacity_bucket_in"))
    if capacity_bucket_in:
        checks.append(bool(capacity_bucket and capacity_bucket in capacity_bucket_in))

    required_composite_labels = _normalize_set(style_rule.get("composite_labels"))
    if required_composite_labels:
        checks.append(bool(composite_label_set & required_composite_labels))

    if not checks:
        return False
    return all(checks) if match_mode == "all" else any(checks)
