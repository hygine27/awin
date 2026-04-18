from __future__ import annotations

from decimal import Decimal

from awin.adapters.base import DbBackedAdapter
from awin.adapters.contracts import QmtBar1dRow, SourceHealth
from awin.config import get_app_config


def _to_float(value):
    if value in {None, ""}:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


class QmtBar1dAdapter(DbBackedAdapter):
    source_name = "qmt_bar_1d"

    def __init__(self) -> None:
        super().__init__(db_config=get_app_config().qt_db, dsn_label="qt")

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def build_query(self, symbols: list[str], start_date: str, end_date: str) -> tuple[str, dict[str, object]]:
        sql = """
        select
          code as symbol,
          split_part(code, '.', 1) as stock_code,
          trade_date::text as trade_date,
          open as open_price,
          high as high_price,
          low as low_price,
          close as close_price,
          volume,
          amount,
          pre_close
        from stg.qmt_bar_1d
        where code = any(%(symbols)s)
          and trade_date >= %(start_date)s::date
          and trade_date <= %(end_date)s::date
          and coalesce(suspend_flag, 0) = 0
        order by code, trade_date
        """
        return sql, {
            "symbols": symbols,
            "start_date": start_date,
            "end_date": end_date,
        }

    def load_rows_with_health(self, symbols: list[str], start_date: str, end_date: str) -> tuple[list[QmtBar1dRow], SourceHealth]:
        if not symbols:
            return [], SourceHealth(source_name=self.source_name, source_status="degraded", detail="empty_symbol_set")

        _, error = self._connect_with_error()
        if error is not None:
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=error)

        sql, params = self.build_query(symbols, start_date, end_date)
        try:
            result = self._query_rows(sql, params) or []
        except Exception as exc:  # pragma: no cover - exercised through live runtime
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=str(exc))

        rows: list[QmtBar1dRow] = []
        health = SourceHealth(
            source_name=self.source_name,
            source_status="ready" if result else "degraded",
            detail="ok" if result else "no_rows_in_range",
        )
        for payload in result:
            rows.append(
                QmtBar1dRow(
                    symbol=str(payload["symbol"]),
                    stock_code=str(payload["stock_code"]),
                    trade_date=str(payload["trade_date"]),
                    open_price=_to_float(payload.get("open_price")),
                    high_price=_to_float(payload.get("high_price")),
                    low_price=_to_float(payload.get("low_price")),
                    close_price=_to_float(payload.get("close_price")),
                    volume=_to_float(payload.get("volume")),
                    amount=_to_float(payload.get("amount")),
                    pre_close=_to_float(payload.get("pre_close")),
                    source_health=health,
                )
            )
        return rows, health

    def load_rows(self, symbols: list[str], start_date: str, end_date: str) -> list[QmtBar1dRow]:
        rows, _ = self.load_rows_with_health(symbols, start_date, end_date)
        return rows
