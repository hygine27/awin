from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping

from awin.market_understanding.engine import load_style_baskets
from awin.style_profile.config import load_style_profile_rules


@dataclass(slots=True)
class StyleProfile:
    trade_date: str
    symbol: str
    market_type_label: str | None = None
    exchange_label: str | None = None
    ownership_style: str | None = None
    legacy_industry_label: str | None = None
    sw_l1_code: str | None = None
    sw_l1_name: str | None = None
    sw_l2_code: str | None = None
    sw_l2_name: str | None = None
    sw_l3_code: str | None = None
    sw_l3_name: str | None = None
    float_mv: float | None = None
    total_mv: float | None = None
    size_bucket_pct: str | None = None
    size_bucket_abs: str | None = None
    capacity_bucket: str | None = None
    composite_style_labels: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clean_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _to_float(value: object) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_symbol(raw: Mapping[str, Any]) -> str | None:
    for key in ("ts_code", "symbol"):
        value = _clean_text(raw.get(key))
        if value:
            return value
    return None


def _normalize_ownership_style(act_ent_type: str | None, ownership_config: Mapping[str, Any]) -> str:
    text = str(act_ent_type or "").strip()
    if not text:
        return str(ownership_config["default_label"])
    for rule in ownership_config["rules"]:
        if any(str(token).strip() and str(token).strip() in text for token in rule.get("match_any", [])):
            return str(rule["label"])
    return str(ownership_config["default_label"])


def _classify_by_min_threshold(
    value: float | None,
    rules: list[Mapping[str, Any]],
    *,
    threshold_key: str,
) -> str | None:
    if value is None:
        return None
    for rule in rules:
        threshold = _to_float(rule.get(threshold_key))
        if threshold is None:
            continue
        if value >= threshold:
            return _clean_text(rule.get("label"))
    return None


def _latest_trade_date(daily_basic_rows: list[Mapping[str, Any]]) -> str | None:
    dates = sorted({_clean_text(row.get("trade_date")) for row in daily_basic_rows if _clean_text(row.get("trade_date"))})
    return dates[-1] if dates else None


def _select_daily_basic_rows(
    daily_basic_rows: list[Mapping[str, Any]],
    *,
    trade_date: str | None,
) -> dict[str, Mapping[str, Any]]:
    selected_trade_date = trade_date or _latest_trade_date(daily_basic_rows)
    out: dict[str, Mapping[str, Any]] = {}
    if selected_trade_date is None:
        return out
    for row in daily_basic_rows:
        if _clean_text(row.get("trade_date")) != selected_trade_date:
            continue
        symbol = _normalize_symbol(row)
        if symbol is None:
            continue
        out[symbol] = row
    return out


def _is_active_member(row: Mapping[str, Any], trade_date: str) -> bool:
    out_date = _clean_text(row.get("out_date"))
    return out_date is None or out_date == "" or out_date >= trade_date.replace("-", "")


def _select_industry_members(
    index_member_rows: list[Mapping[str, Any]],
    *,
    trade_date: str,
) -> dict[str, Mapping[str, Any]]:
    selected: dict[str, Mapping[str, Any]] = {}
    for row in index_member_rows:
        symbol = _normalize_symbol(row)
        if symbol is None or not _is_active_member(row, trade_date):
            continue
        current = selected.get(symbol)
        if current is None:
            selected[symbol] = row
            continue
        current_in_date = _clean_text(current.get("in_date")) or ""
        row_in_date = _clean_text(row.get("in_date")) or ""
        if row_in_date >= current_in_date:
            selected[symbol] = row
    return selected


def _assign_pct_buckets(profiles: list[StyleProfile], pct_rules: list[Mapping[str, Any]]) -> None:
    ordered = sorted(profile.float_mv for profile in profiles if profile.float_mv is not None)
    if not ordered:
        return
    total = len(ordered)
    rank_map: dict[float, float] = {}
    for idx, value in enumerate(ordered, start=1):
        rank_map[value] = idx / total
    for profile in profiles:
        if profile.float_mv is None:
            profile.size_bucket_pct = None
            continue
        profile.size_bucket_pct = _classify_by_min_threshold(
            rank_map.get(profile.float_mv),
            pct_rules,
            threshold_key="min_rank_pct",
        )


def _derive_composite_labels(
    profile: StyleProfile,
    style_baskets: dict[str, dict[str, list[str]]],
    composite_label_rules: list[Mapping[str, Any]],
) -> list[str]:
    labels: list[str] = []
    market_type = profile.market_type_label or ""
    industry = profile.legacy_industry_label or ""
    sw_l1_name = profile.sw_l1_name or ""

    for style_name, style_rule in style_baskets.items():
        industries = set(style_rule.get("industries", []))
        market_types = set(style_rule.get("market_types", []))
        if industry in industries or market_type in market_types:
            labels.append(style_name)

    for rule in composite_label_rules:
        label = _clean_text(rule.get("label"))
        if not label or label in labels:
            continue
        sw_l1_names = {str(item).strip() for item in rule.get("sw_l1_names", []) if str(item).strip()}
        size_bucket_pct_in = {str(item).strip() for item in rule.get("size_bucket_pct_in", []) if str(item).strip()}
        capacity_bucket_in = {str(item).strip() for item in rule.get("capacity_bucket_in", []) if str(item).strip()}

        if sw_l1_names and sw_l1_name in sw_l1_names:
            labels.append(label)
            continue
        if size_bucket_pct_in and capacity_bucket_in:
            if profile.size_bucket_pct in size_bucket_pct_in and profile.capacity_bucket in capacity_bucket_in:
                labels.append(label)
    return labels


def build_style_profiles(
    stock_basic_rows: list[Mapping[str, Any]],
    daily_basic_rows: list[Mapping[str, Any]],
    index_member_rows: list[Mapping[str, Any]],
    *,
    trade_date: str | None = None,
) -> list[StyleProfile]:
    latest_trade_date = trade_date or _latest_trade_date(daily_basic_rows) or ""
    if not latest_trade_date:
        return []

    style_profile_rules = load_style_profile_rules()
    daily_basic_by_symbol = _select_daily_basic_rows(daily_basic_rows, trade_date=latest_trade_date)
    industry_member_by_symbol = _select_industry_members(index_member_rows, trade_date=latest_trade_date)

    profiles: list[StyleProfile] = []
    for row in stock_basic_rows:
        symbol = _normalize_symbol(row)
        if symbol is None:
            continue

        daily_basic = daily_basic_by_symbol.get(symbol, {})
        industry_member = industry_member_by_symbol.get(symbol, {})

        profile = StyleProfile(
            trade_date=latest_trade_date,
            symbol=symbol,
            market_type_label=_clean_text(row.get("market")),
            exchange_label=_clean_text(row.get("exchange")),
            ownership_style=_normalize_ownership_style(_clean_text(row.get("act_ent_type")), style_profile_rules["ownership"]),
            legacy_industry_label=_clean_text(row.get("industry")),
            sw_l1_code=_clean_text(industry_member.get("l1_code")),
            sw_l1_name=_clean_text(industry_member.get("l1_name")),
            sw_l2_code=_clean_text(industry_member.get("l2_code")),
            sw_l2_name=_clean_text(industry_member.get("l2_name")),
            sw_l3_code=_clean_text(industry_member.get("l3_code")),
            sw_l3_name=_clean_text(industry_member.get("l3_name")),
            float_mv=_to_float(daily_basic.get("circ_mv")),
            total_mv=_to_float(daily_basic.get("total_mv")),
        )
        profile.size_bucket_abs = _classify_by_min_threshold(
            profile.float_mv,
            style_profile_rules["size_bucket_abs_rules"],
            threshold_key="min_float_mv",
        )
        profile.capacity_bucket = _classify_by_min_threshold(
            profile.float_mv,
            style_profile_rules["capacity_bucket_rules"],
            threshold_key="min_float_mv",
        )
        profiles.append(profile)

    _assign_pct_buckets(profiles, style_profile_rules["size_bucket_pct_rules"])
    style_baskets, _ = load_style_baskets()
    for profile in profiles:
        profile.composite_style_labels = _derive_composite_labels(
            profile,
            style_baskets,
            style_profile_rules["composite_label_rules"],
        )
    profiles.sort(key=lambda item: item.symbol)
    return profiles
