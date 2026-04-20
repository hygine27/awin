"""ts_moneyflow_ind_ths adapter.

读取 `stg.ts_moneyflow_ind_ths` 的 T-1 及更早同花顺行业资金流历史，
作为大风格确认和行业层资金解释来源。
"""

from __future__ import annotations

from datetime import date, timedelta

from awin.adapters.base import DbBackedAdapter
from awin.adapters.contracts import SourceHealth
from awin.config import get_app_config


class TsMoneyflowIndThsAdapter(DbBackedAdapter):
    """Load historical THS industry moneyflow rows from `stg.ts_moneyflow_ind_ths`."""

    source_name = "ts_moneyflow_ind_ths"

    def __init__(self, lookback_days: int = 45) -> None:
        super().__init__(db_config=get_app_config().qt_db, dsn_label="qt")
        self.lookback_days = lookback_days

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def build_query(self, trade_date: str) -> tuple[str, dict[str, object]]:
        end_date = date.fromisoformat(str(trade_date).replace("/", "-"))
        start_date = end_date - timedelta(days=self.lookback_days)
        sql = """
        select
          trade_date::text as trade_date,
          ts_code,
          industry,
          lead_stock,
          close,
          pct_change,
          company_num,
          pct_change_stock,
          close_price,
          net_buy_amount,
          net_sell_amount,
          net_amount
        from stg.ts_moneyflow_ind_ths
        where trade_date >= %(start_date)s::date
          and trade_date < %(trade_date)s::date
        """
        return sql, {
            "start_date": start_date.isoformat(),
            "trade_date": end_date.isoformat(),
        }

    def load_rows_with_health(self, trade_date: str) -> tuple[list[dict[str, object]], SourceHealth]:
        _, error = self._connect_with_error()
        if error is not None:
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        sql, params = self.build_query(trade_date)
        try:
            rows = self._query_rows(sql, params) or []
        except Exception as exc:  # pragma: no cover - exercised through live runtime
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=str(exc))
        health = SourceHealth(source_name=self.source_name, source_status="ready" if rows else "degraded", detail="ok" if rows else "no_moneyflow_ind_ths_rows")
        return rows, health

    def load_rows(self, trade_date: str) -> list[dict[str, object]]:
        rows, _ = self.load_rows_with_health(trade_date)
        return rows
