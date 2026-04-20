from __future__ import annotations

import statistics
from dataclasses import asdict, dataclass, field
from typing import Any, Mapping

from awin.market_understanding.engine import load_style_baskets
from awin.style_profile.config import load_style_profile_rules
from awin.style_matching import style_rule_matches


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
    free_float_share: float | None = None
    float_mv: float | None = None
    total_mv: float | None = None
    avg_amount_20d: float | None = None
    size_bucket_pct: str | None = None
    size_bucket_abs: str | None = None
    capacity_bucket: str | None = None
    dividend_value_score: float | None = None
    growth_valuation_score: float | None = None
    quality_growth_score: float | None = None
    sales_growth_score: float | None = None
    profit_growth_score: float | None = None
    low_vol_defensive_score: float | None = None
    high_beta_attack_score: float | None = None
    dividend_style: str | None = None
    valuation_style: str | None = None
    growth_style: str | None = None
    quality_style: str | None = None
    volatility_style: str | None = None
    composite_style_labels: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _clean_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_trade_date(value: object) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    return text.replace("-", "")


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
    dates = sorted({_normalize_trade_date(row.get("trade_date")) for row in daily_basic_rows if _normalize_trade_date(row.get("trade_date"))})
    return dates[-1] if dates else None


def _select_daily_basic_rows(
    daily_basic_rows: list[Mapping[str, Any]],
    *,
    trade_date: str | None,
) -> dict[str, Mapping[str, Any]]:
    selected_trade_date = _normalize_trade_date(trade_date) or _latest_trade_date(daily_basic_rows)
    out: dict[str, Mapping[str, Any]] = {}
    if selected_trade_date is None:
        return out
    for row in daily_basic_rows:
        if _normalize_trade_date(row.get("trade_date")) != selected_trade_date:
            continue
        symbol = _normalize_symbol(row)
        if symbol is None:
            continue
        out[symbol] = row
    return out


def _is_active_member(row: Mapping[str, Any], trade_date: str) -> bool:
    out_date = _normalize_trade_date(row.get("out_date"))
    return out_date is None or out_date == "" or out_date >= trade_date


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
        current_in_date = _normalize_trade_date(current.get("in_date")) or ""
        row_in_date = _normalize_trade_date(row.get("in_date")) or ""
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


def _daily_returns(prices: list[float]) -> list[float]:
    out: list[float] = []
    for prev, curr in zip(prices[:-1], prices[1:]):
        if prev <= 0:
            continue
        out.append(curr / prev - 1.0)
    return out


def _window_return(prices: list[float], window: int) -> float | None:
    if len(prices) <= window:
        return None
    base = prices[-window - 1]
    if base <= 0:
        return None
    return prices[-1] / base - 1.0


def _window_vol(prices: list[float], window: int) -> float | None:
    returns = _daily_returns(prices)
    if len(returns) < window:
        return None
    try:
        return float(statistics.pstdev(returns[-window:]))
    except statistics.StatisticsError:
        return None


def _window_max_drawdown(prices: list[float], window: int) -> float | None:
    if len(prices) <= window:
        return None
    series = prices[-window - 1 :]
    peak = series[0]
    max_drawdown = 0.0
    for price in series:
        if price > peak:
            peak = price
        if peak <= 0:
            continue
        drawdown = price / peak - 1.0
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return abs(max_drawdown)


def _average_tail(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    tail = values[-window:]
    return float(sum(tail) / len(tail)) if tail else None


def _build_daily_series_by_symbol(
    daily_rows: list[Mapping[str, Any]],
    adj_factor_rows: list[Mapping[str, Any]],
) -> dict[str, dict[str, list[float]]]:
    adj_factor_by_key: dict[tuple[str, str], float] = {}
    for row in adj_factor_rows:
        symbol = _normalize_symbol(row)
        trade_date = _normalize_trade_date(row.get("trade_date"))
        adj_factor = _to_float(row.get("adj_factor"))
        if symbol is None or trade_date is None or adj_factor is None:
            continue
        adj_factor_by_key[(symbol, trade_date)] = adj_factor

    grouped: dict[str, list[tuple[str, float, float]]] = {}
    for row in daily_rows:
        symbol = _normalize_symbol(row)
        trade_date = _normalize_trade_date(row.get("trade_date"))
        close_price = _to_float(row.get("close"))
        amount = _to_float(row.get("amount"))
        if symbol is None or trade_date is None or close_price is None:
            continue
        adj_factor = adj_factor_by_key.get((symbol, trade_date))
        if adj_factor is None:
            continue
        grouped.setdefault(symbol, []).append((trade_date, close_price * adj_factor, amount or 0.0))

    out: dict[str, dict[str, list[float]]] = {}
    for symbol, rows in grouped.items():
        rows.sort(key=lambda item: item[0])
        out[symbol] = {
            "adj_close": [item[1] for item in rows],
            "amount": [item[2] for item in rows],
        }
    return out


def _build_daily_series_from_metric_rows(
    daily_metric_rows: list[Mapping[str, Any]],
) -> dict[str, dict[str, list[float]]]:
    out: dict[str, dict[str, list[float]]] = {}
    for row in daily_metric_rows:
        symbol = _normalize_symbol(row)
        if symbol is None:
            continue
        adj_close_series = row.get("adj_close_series")
        amount_series = row.get("amount_series")
        if not isinstance(adj_close_series, list) or not isinstance(amount_series, list):
            continue
        out[symbol] = {
            "adj_close": [float(value) for value in adj_close_series if _to_float(value) is not None],
            "amount": [float(value) for value in amount_series if _to_float(value) is not None],
        }
    return out


def _build_daily_metric_map(
    daily_metric_rows: list[Mapping[str, Any]],
) -> dict[str, dict[str, float | None]]:
    out: dict[str, dict[str, float | None]] = {}
    for row in daily_metric_rows:
        symbol = _normalize_symbol(row)
        if symbol is None:
            continue
        if "ret_20d" not in row and "vol_20d" not in row and "max_drawdown_20d" not in row:
            continue
        out[symbol] = {
            "avg_amount_20d": _to_float(row.get("avg_amount_20d")),
            "ret_20d": _to_float(row.get("ret_20d")),
            "ret_60d": _to_float(row.get("ret_60d")),
            "vol_20d": _to_float(row.get("vol_20d")),
            "vol_60d": _to_float(row.get("vol_60d")),
            "max_drawdown_20d": _to_float(row.get("max_drawdown_20d")),
            "max_drawdown_60d": _to_float(row.get("max_drawdown_60d")),
        }
    return out


def _select_fina_rows(
    fina_indicator_rows: list[Mapping[str, Any]],
    *,
    trade_date: str,
) -> dict[str, Mapping[str, Any]]:
    selected: dict[str, Mapping[str, Any]] = {}
    for row in fina_indicator_rows:
        symbol = _normalize_symbol(row)
        ann_date = _normalize_trade_date(row.get("ann_date"))
        end_date = _normalize_trade_date(row.get("end_date"))
        if symbol is None or ann_date is None or ann_date >= trade_date:
            continue
        current = selected.get(symbol)
        if current is None:
            selected[symbol] = row
            continue
        current_ann_date = _normalize_trade_date(current.get("ann_date")) or ""
        current_end_date = _normalize_trade_date(current.get("end_date")) or ""
        if (ann_date, end_date or "") >= (current_ann_date, current_end_date):
            selected[symbol] = row
    return selected


def _rank_map(values: dict[str, float | None], *, higher_better: bool) -> dict[str, float]:
    valid = [(symbol, value) for symbol, value in values.items() if value is not None]
    if not valid:
        return {}
    valid.sort(key=lambda item: float(item[1]), reverse=not higher_better)
    total = len(valid)
    out: dict[str, float] = {}
    for idx, (symbol, _) in enumerate(valid, start=1):
        out[symbol] = idx / total
    return out


def _assign_score(
    raw_metrics: dict[str, dict[str, float | None]],
    factor_weights: Mapping[str, float],
) -> dict[str, float | None]:
    rank_maps: dict[str, dict[str, float]] = {}
    for factor_name in factor_weights:
        if factor_name == "small_cap":
            values = {symbol: metrics.get("float_mv") for symbol, metrics in raw_metrics.items()}
            rank_maps[factor_name] = _rank_map(values, higher_better=False)
            continue
        higher_better = not factor_name.endswith("_inverse")
        base_factor_name = factor_name[: -len("_inverse")] if factor_name.endswith("_inverse") else factor_name
        values = {symbol: metrics.get(base_factor_name) for symbol, metrics in raw_metrics.items()}
        rank_maps[factor_name] = _rank_map(values, higher_better=higher_better)

    out: dict[str, float | None] = {}
    for symbol in raw_metrics:
        numerator = 0.0
        denominator = 0.0
        for factor_name, weight in factor_weights.items():
            rank_value = rank_maps.get(factor_name, {}).get(symbol)
            if rank_value is None:
                continue
            numerator += rank_value * float(weight)
            denominator += float(weight)
        out[symbol] = round(numerator / denominator, 4) if denominator > 0 else None
    return out


def _assign_profile_score(
    profiles: list[StyleProfile],
    raw_metrics: dict[str, dict[str, float | None]],
    *,
    target_attr: str,
    factor_weights: Mapping[str, float],
) -> None:
    score_map = _assign_score(raw_metrics, factor_weights)
    for profile in profiles:
        setattr(profile, target_attr, score_map.get(profile.symbol))


def _resolve_profile_score(profile: StyleProfile, score_name: str, auxiliary_scores: Mapping[str, dict[str, float | None]]) -> float | None:
    direct_value = getattr(profile, score_name, None)
    if isinstance(direct_value, (float, int)):
        return float(direct_value)
    return auxiliary_scores.get(score_name, {}).get(profile.symbol)


def _classify_business_label(
    profile: StyleProfile,
    *,
    source_score: str,
    bands: list[Mapping[str, Any]],
    auxiliary_scores: Mapping[str, dict[str, float | None]],
) -> str | None:
    score_value = _resolve_profile_score(profile, source_score, auxiliary_scores)
    if score_value is None:
        return None
    for band in bands:
        label = _clean_text(band.get("label"))
        threshold = _to_float(band.get("min_score"))
        if label is None or threshold is None:
            continue
        if score_value >= threshold:
            return label
    return None


def _derive_composite_labels(
    profile: StyleProfile,
    style_baskets: dict[str, dict[str, list[str]]],
    composite_label_rules: list[Mapping[str, Any]],
) -> list[str]:
    labels: list[str] = []
    sw_l1_name = profile.sw_l1_name or ""

    for style_name, style_rule in style_baskets.items():
        if style_rule_matches(
            style_rule,
            industry=profile.legacy_industry_label,
            market_type=profile.market_type_label,
            ownership_style=profile.ownership_style,
            size_bucket_abs=profile.size_bucket_abs,
            size_bucket_pct=profile.size_bucket_pct,
            capacity_bucket=profile.capacity_bucket,
            composite_labels=labels,
        ):
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
    daily_metric_rows: list[Mapping[str, Any]] | None = None,
    daily_rows: list[Mapping[str, Any]] | None = None,
    adj_factor_rows: list[Mapping[str, Any]] | None = None,
    fina_indicator_rows: list[Mapping[str, Any]] | None = None,
    trade_date: str | None = None,
) -> list[StyleProfile]:
    latest_trade_date = _normalize_trade_date(trade_date) or _latest_trade_date(daily_basic_rows) or ""
    if not latest_trade_date:
        return []

    style_profile_rules = load_style_profile_rules()
    daily_basic_by_symbol = _select_daily_basic_rows(daily_basic_rows, trade_date=latest_trade_date)
    industry_member_by_symbol = _select_industry_members(index_member_rows, trade_date=latest_trade_date)
    fina_by_symbol = _select_fina_rows(fina_indicator_rows or [], trade_date=latest_trade_date)
    precomputed_metric_by_symbol = _build_daily_metric_map(daily_metric_rows or [])
    daily_series_by_symbol = (
        _build_daily_series_from_metric_rows(daily_metric_rows or [])
        if daily_metric_rows and not precomputed_metric_by_symbol
        else _build_daily_series_by_symbol(daily_rows or [], adj_factor_rows or [])
    )
    history_windows = style_profile_rules["history_windows"]

    raw_metrics: dict[str, dict[str, float | None]] = {}
    profiles: list[StyleProfile] = []
    for row in stock_basic_rows:
        symbol = _normalize_symbol(row)
        if symbol is None:
            continue

        daily_basic = daily_basic_by_symbol.get(symbol, {})
        industry_member = industry_member_by_symbol.get(symbol, {})
        fina_indicator = fina_by_symbol.get(symbol, {})
        precomputed_metrics = precomputed_metric_by_symbol.get(symbol, {})
        series = daily_series_by_symbol.get(symbol, {})
        adj_close = list(series.get("adj_close", []))
        amount_series = [value for value in series.get("amount", []) if value is not None]

        avg_amount_20d = precomputed_metrics.get("avg_amount_20d")
        ret_20d = precomputed_metrics.get("ret_20d")
        ret_60d = precomputed_metrics.get("ret_60d")
        vol_20d = precomputed_metrics.get("vol_20d")
        vol_60d = precomputed_metrics.get("vol_60d")
        max_drawdown_20d = precomputed_metrics.get("max_drawdown_20d")
        max_drawdown_60d = precomputed_metrics.get("max_drawdown_60d")

        if avg_amount_20d is None:
            avg_amount_20d = _average_tail(amount_series, history_windows["amount_20d"])
        if ret_20d is None:
            ret_20d = _window_return(adj_close, history_windows["ret_20d"])
        if ret_60d is None:
            ret_60d = _window_return(adj_close, history_windows["ret_60d"])
        if vol_20d is None:
            vol_20d = _window_vol(adj_close, history_windows["vol_20d"])
        if vol_60d is None:
            vol_60d = _window_vol(adj_close, history_windows["vol_60d"])
        if max_drawdown_20d is None:
            max_drawdown_20d = _window_max_drawdown(adj_close, history_windows["max_drawdown_20d"])
        if max_drawdown_60d is None:
            max_drawdown_60d = _window_max_drawdown(adj_close, history_windows["max_drawdown_60d"])

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
            free_float_share=_to_float(daily_basic.get("free_share")),
            float_mv=_to_float(daily_basic.get("circ_mv")),
            total_mv=_to_float(daily_basic.get("total_mv")),
            avg_amount_20d=avg_amount_20d,
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

        raw_metrics[symbol] = {
            "float_mv": profile.float_mv,
            "turnover_rate_f": _to_float(daily_basic.get("turnover_rate_f")),
            "pe_ttm": _to_float(daily_basic.get("pe_ttm")),
            "pb": _to_float(daily_basic.get("pb")),
            "ps_ttm": _to_float(daily_basic.get("ps_ttm")),
            "dv_ratio": _to_float(daily_basic.get("dv_ratio")),
            "dv_ttm": _to_float(daily_basic.get("dv_ttm")),
            "roe_yearly": _to_float(fina_indicator.get("roe_yearly")),
            "roic": _to_float(fina_indicator.get("roic")),
            "debt_to_assets": _to_float(fina_indicator.get("debt_to_assets")),
            "q_ocf_to_sales": _to_float(fina_indicator.get("q_ocf_to_sales")),
            "tr_yoy": _to_float(fina_indicator.get("tr_yoy")),
            "or_yoy": _to_float(fina_indicator.get("or_yoy")),
            "q_sales_yoy": _to_float(fina_indicator.get("q_sales_yoy")),
            "netprofit_yoy": _to_float(fina_indicator.get("netprofit_yoy")),
            "dt_netprofit_yoy": _to_float(fina_indicator.get("dt_netprofit_yoy")),
            "op_yoy": _to_float(fina_indicator.get("op_yoy")),
            "ret_20d": ret_20d,
            "ret_60d": ret_60d,
            "vol_20d": vol_20d,
            "vol_60d": vol_60d,
            "max_drawdown_20d": max_drawdown_20d,
            "max_drawdown_60d": max_drawdown_60d,
        }

    _assign_pct_buckets(profiles, style_profile_rules["size_bucket_pct_rules"])

    score_attr_map = {
        "dividend_value": "dividend_value_score",
        "growth_valuation": "growth_valuation_score",
        "quality_growth": "quality_growth_score",
        "sales_growth": "sales_growth_score",
        "profit_growth": "profit_growth_score",
        "low_vol_defensive": "low_vol_defensive_score",
        "high_beta_attack": "high_beta_attack_score",
    }
    for score_name, target_attr in score_attr_map.items():
        factor_weights = style_profile_rules["score_weights"].get(score_name, {})
        if factor_weights:
            _assign_profile_score(profiles, raw_metrics, target_attr=target_attr, factor_weights=factor_weights)

    auxiliary_scores: dict[str, dict[str, float | None]] = {}
    for score_name, factor_weights in style_profile_rules["business_label_score_weights"].items():
        auxiliary_scores[score_name] = _assign_score(raw_metrics, factor_weights)

    for profile in profiles:
        for rule in style_profile_rules["business_label_rules"]:
            setattr(
                profile,
                str(rule["field"]),
                _classify_business_label(
                    profile,
                    source_score=str(rule["source_score"]),
                    bands=list(rule["bands"]),
                    auxiliary_scores=auxiliary_scores,
                ),
            )

    style_baskets, _ = load_style_baskets()
    for profile in profiles:
        profile.composite_style_labels = _derive_composite_labels(
            profile,
            style_baskets,
            style_profile_rules["composite_label_rules"],
        )
    profiles.sort(key=lambda item: item.symbol)
    return profiles
