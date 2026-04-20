from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


def _trade_date_key(value: Any) -> str:
    return str(value or "").strip()


def _to_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sum_tail(values: list[float], window: int) -> float | None:
    if not values:
        return None
    tail = values[-window:]
    if not tail:
        return None
    return float(sum(tail))


def _streak(values: list[float], *, positive: bool) -> int:
    count = 0
    for value in reversed(values):
        if positive and value > 0:
            count += 1
            continue
        if (not positive) and value < 0:
            count += 1
            continue
        break
    return count


def _acceleration_3d(values: list[float]) -> float | None:
    if len(values) < 4:
        return None
    recent = values[-3:]
    previous = values[-6:-3]
    if not previous:
        return None
    return float(sum(recent) / len(recent) - sum(previous) / len(previous))


@dataclass(slots=True)
class SerializableDataclass:
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class StockFundFlowProfile(SerializableDataclass):
    symbol: str
    main_net_amount_1d: float | None = None
    main_net_amount_3d_sum: float | None = None
    main_net_amount_5d_sum: float | None = None
    main_net_amount_10d_sum: float | None = None
    main_net_amount_rate_1d: float | None = None
    ths_net_d5_amount: float | None = None
    super_large_net_1d: float | None = None
    large_order_net_1d: float | None = None
    inflow_streak_days: int = 0
    outflow_streak_days: int = 0
    flow_acceleration_3d: float | None = None
    price_flow_divergence_flag: bool = False


@dataclass(slots=True)
class ConceptFundFlowProfile(SerializableDataclass):
    concept_code: str
    concept_name: str
    net_amount_1d: float | None = None
    net_amount_3d_sum: float | None = None
    net_amount_5d_sum: float | None = None
    pct_change_1d: float | None = None
    flow_acceleration_3d: float | None = None


@dataclass(slots=True)
class IndustryFundFlowProfile(SerializableDataclass):
    industry_code: str
    industry_name: str
    net_amount_1d: float | None = None
    net_amount_3d_sum: float | None = None
    net_amount_5d_sum: float | None = None
    pct_change_1d: float | None = None
    flow_acceleration_3d: float | None = None


@dataclass(slots=True)
class MarketFundFlowProfile(SerializableDataclass):
    trade_date: str
    net_amount_1d: float | None = None
    net_amount_rate_1d: float | None = None
    super_large_net_1d: float | None = None
    large_order_net_1d: float | None = None
    inflow_streak_days: int = 0
    outflow_streak_days: int = 0


@dataclass(slots=True)
class FundFlowSnapshot(SerializableDataclass):
    stock_profiles: list[StockFundFlowProfile] = field(default_factory=list)
    concept_profiles: list[ConceptFundFlowProfile] = field(default_factory=list)
    industry_profiles: list[IndustryFundFlowProfile] = field(default_factory=list)
    market_profile: MarketFundFlowProfile | None = None


def build_fund_flow_snapshot(
    moneyflow_ths_rows: list[dict[str, object]],
    moneyflow_dc_rows: list[dict[str, object]],
    moneyflow_cnt_rows: list[dict[str, object]],
    moneyflow_ind_rows: list[dict[str, object]],
    moneyflow_mkt_rows: list[dict[str, object]],
) -> FundFlowSnapshot:
    aggregated_ths_by_symbol: dict[str, dict[str, float | int | bool | None]] = {}
    stock_histories: dict[str, list[tuple[str, float, float | None, float | None]]] = {}
    for row in moneyflow_ths_rows:
        symbol = str(row.get("ts_code") or "").strip()
        if not symbol:
            continue
        if "main_net_amount_3d_sum" in row:
            pct_change_1d = _to_float(row.get("pct_change"))
            main_net_amount_1d = _to_float(row.get("main_net_amount_1d"))
            aggregated_ths_by_symbol[symbol] = {
                "main_net_amount_1d": main_net_amount_1d,
                "main_net_amount_3d_sum": _to_float(row.get("main_net_amount_3d_sum")),
                "main_net_amount_5d_sum": _to_float(row.get("main_net_amount_5d_sum")),
                "main_net_amount_10d_sum": _to_float(row.get("main_net_amount_10d_sum")),
                "ths_net_d5_amount": _to_float(row.get("ths_net_d5_amount")),
                "inflow_streak_days": int(_to_float(row.get("inflow_streak_days")) or 0),
                "outflow_streak_days": int(_to_float(row.get("outflow_streak_days")) or 0),
                "flow_acceleration_3d": _to_float(row.get("flow_acceleration_3d")),
                "price_flow_divergence_flag": bool(
                    pct_change_1d is not None
                    and main_net_amount_1d is not None
                    and ((pct_change_1d > 0 and main_net_amount_1d < 0) or (pct_change_1d < 0 and main_net_amount_1d > 0))
                ),
            }
            continue
        stock_histories.setdefault(symbol, []).append(
            (
                _trade_date_key(row.get("trade_date")),
                _to_float(row.get("net_amount")) or 0.0,
                _to_float(row.get("pct_change")),
                _to_float(row.get("net_d5_amount")),
            )
        )

    stock_dc_latest: dict[str, tuple[str, dict[str, float | None]]] = {}
    for row in moneyflow_dc_rows:
        symbol = str(row.get("ts_code") or "").strip()
        if not symbol:
            continue
        row_trade_date = _trade_date_key(row.get("trade_date"))
        current = stock_dc_latest.get(symbol)
        if current is None or row_trade_date >= current[0]:
            stock_dc_latest[symbol] = (
                row_trade_date,
                {
                    "main_net_amount_rate_1d": _to_float(row.get("net_amount_rate")),
                    "super_large_net_1d": _to_float(row.get("buy_elg_amount")),
                    "large_order_net_1d": _to_float(row.get("buy_lg_amount")),
                },
            )

    stock_profiles: list[StockFundFlowProfile] = []
    all_symbols = sorted(set(stock_histories) | set(aggregated_ths_by_symbol) | set(stock_dc_latest))
    for symbol in all_symbols:
        aggregated = aggregated_ths_by_symbol.get(symbol)
        if aggregated is None:
            ordered_stock_rows = sorted(stock_histories.get(symbol, []), key=lambda item: item[0])
            main_series = [item[1] for item in ordered_stock_rows]
            latest_stock_row = ordered_stock_rows[-1] if ordered_stock_rows else None
            latest_ths = {
                "main_net_amount_1d": latest_stock_row[1] if latest_stock_row else None,
                "ths_net_d5_amount": latest_stock_row[3] if latest_stock_row else None,
                "pct_change_1d": latest_stock_row[2] if latest_stock_row else None,
            }
            pct_change_1d = latest_ths.get("pct_change_1d")
            main_net_amount_1d = latest_ths.get("main_net_amount_1d")
            aggregated = {
                "main_net_amount_1d": main_net_amount_1d,
                "main_net_amount_3d_sum": _sum_tail(main_series, 3),
                "main_net_amount_5d_sum": _sum_tail(main_series, 5),
                "main_net_amount_10d_sum": _sum_tail(main_series, 10),
                "ths_net_d5_amount": latest_ths.get("ths_net_d5_amount"),
                "inflow_streak_days": _streak(main_series, positive=True),
                "outflow_streak_days": _streak(main_series, positive=False),
                "flow_acceleration_3d": _acceleration_3d(main_series),
                "price_flow_divergence_flag": bool(
                    pct_change_1d is not None
                    and main_net_amount_1d is not None
                    and ((pct_change_1d > 0 and main_net_amount_1d < 0) or (pct_change_1d < 0 and main_net_amount_1d > 0))
                ),
            }
        stock_profiles.append(
            StockFundFlowProfile(
                symbol=symbol,
                main_net_amount_1d=_to_float(aggregated.get("main_net_amount_1d")),
                main_net_amount_3d_sum=_to_float(aggregated.get("main_net_amount_3d_sum")),
                main_net_amount_5d_sum=_to_float(aggregated.get("main_net_amount_5d_sum")),
                main_net_amount_10d_sum=_to_float(aggregated.get("main_net_amount_10d_sum")),
                main_net_amount_rate_1d=stock_dc_latest.get(symbol, ("", {}))[1].get("main_net_amount_rate_1d"),
                ths_net_d5_amount=_to_float(aggregated.get("ths_net_d5_amount")),
                super_large_net_1d=stock_dc_latest.get(symbol, ("", {}))[1].get("super_large_net_1d"),
                large_order_net_1d=stock_dc_latest.get(symbol, ("", {}))[1].get("large_order_net_1d"),
                inflow_streak_days=int(aggregated.get("inflow_streak_days") or 0),
                outflow_streak_days=int(aggregated.get("outflow_streak_days") or 0),
                flow_acceleration_3d=_to_float(aggregated.get("flow_acceleration_3d")),
                price_flow_divergence_flag=bool(aggregated.get("price_flow_divergence_flag")),
            )
        )

    concept_series: dict[tuple[str, str], list[tuple[str, float]]] = {}
    concept_latest_pct: dict[tuple[str, str], tuple[str, float | None]] = {}
    for row in moneyflow_cnt_rows:
        key = (str(row.get("ts_code") or "").strip(), str(row.get("name") or "").strip())
        if not key[0]:
            continue
        trade_date = _trade_date_key(row.get("trade_date"))
        concept_series.setdefault(key, []).append((trade_date, _to_float(row.get("net_amount")) or 0.0))
        current = concept_latest_pct.get(key)
        if current is None or trade_date >= current[0]:
            concept_latest_pct[key] = (trade_date, _to_float(row.get("pct_change")))
    concept_profiles = [
        ConceptFundFlowProfile(
            concept_code=code,
            concept_name=name,
            net_amount_1d=ordered_series[-1] if ordered_series else None,
            net_amount_3d_sum=_sum_tail(ordered_series, 3),
            net_amount_5d_sum=_sum_tail(ordered_series, 5),
            pct_change_1d=concept_latest_pct.get((code, name), ("", None))[1],
            flow_acceleration_3d=_acceleration_3d(ordered_series),
        )
        for (code, name), series in sorted(concept_series.items())
        for ordered_series in [[amount for _, amount in sorted(series, key=lambda item: item[0])]]
    ]

    industry_series: dict[tuple[str, str], list[tuple[str, float]]] = {}
    industry_latest_pct: dict[tuple[str, str], tuple[str, float | None]] = {}
    for row in moneyflow_ind_rows:
        key = (str(row.get("ts_code") or "").strip(), str(row.get("industry") or "").strip())
        if not key[0]:
            continue
        trade_date = _trade_date_key(row.get("trade_date"))
        industry_series.setdefault(key, []).append((trade_date, _to_float(row.get("net_amount")) or 0.0))
        current = industry_latest_pct.get(key)
        if current is None or trade_date >= current[0]:
            industry_latest_pct[key] = (trade_date, _to_float(row.get("pct_change")))
    industry_profiles = [
        IndustryFundFlowProfile(
            industry_code=code,
            industry_name=name,
            net_amount_1d=ordered_series[-1] if ordered_series else None,
            net_amount_3d_sum=_sum_tail(ordered_series, 3),
            net_amount_5d_sum=_sum_tail(ordered_series, 5),
            pct_change_1d=industry_latest_pct.get((code, name), ("", None))[1],
            flow_acceleration_3d=_acceleration_3d(ordered_series),
        )
        for (code, name), series in sorted(industry_series.items())
        for ordered_series in [[amount for _, amount in sorted(series, key=lambda item: item[0])]]
    ]

    market_profile = None
    if moneyflow_mkt_rows:
        ordered_rows = sorted(moneyflow_mkt_rows, key=lambda row: _trade_date_key(row.get("trade_date")))
        net_series = [_to_float(row.get("net_amount")) or 0.0 for row in ordered_rows]
        latest = ordered_rows[-1]
        market_profile = MarketFundFlowProfile(
            trade_date=str(latest.get("trade_date") or ""),
            net_amount_1d=_to_float(latest.get("net_amount")),
            net_amount_rate_1d=_to_float(latest.get("net_amount_rate")),
            super_large_net_1d=_to_float(latest.get("buy_elg_amount")),
            large_order_net_1d=_to_float(latest.get("buy_lg_amount")),
            inflow_streak_days=_streak(net_series, positive=True),
            outflow_streak_days=_streak(net_series, positive=False),
        )

    return FundFlowSnapshot(
        stock_profiles=stock_profiles,
        concept_profiles=concept_profiles,
        industry_profiles=industry_profiles,
        market_profile=market_profile,
    )
