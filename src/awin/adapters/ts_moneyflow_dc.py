"""ts_moneyflow_dc adapter.

读取 `stg.ts_moneyflow_dc` 的最近有效交易日个股资金结构快照，
作为超大单/大单等资金结构画像的基础来源。
"""

from __future__ import annotations

from datetime import date

from awin.adapters.base import DbBackedAdapter
from awin.adapters.contracts import SourceHealth
from awin.config import get_app_config


class TsMoneyflowDcAdapter(DbBackedAdapter):
    """Load historical DC stock moneyflow rows from `stg.ts_moneyflow_dc`."""

    source_name = "ts_moneyflow_dc"

    def __init__(self) -> None:
        super().__init__(db_config=get_app_config().qt_db, dsn_label="qt")

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def build_query(self, trade_date: str) -> tuple[str, dict[str, object]]:
        end_date = date.fromisoformat(str(trade_date).replace("/", "-"))
        sql = """
        with latest_trade_date as (
          select max(trade_date) as trade_date
          from stg.ts_moneyflow_dc
          where trade_date < %(trade_date)s::date
        )
        select
          ts_code,
          trade_date::text as trade_date,
          name,
          pct_change,
          close,
          net_amount,
          net_amount_rate,
          buy_elg_amount,
          buy_elg_amount_rate,
          buy_lg_amount,
          buy_lg_amount_rate,
          buy_md_amount,
          buy_md_amount_rate,
          buy_sm_amount,
          buy_sm_amount_rate
        from stg.ts_moneyflow_dc
        where trade_date = (select trade_date from latest_trade_date)
        """
        return sql, {
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
        health = SourceHealth(source_name=self.source_name, source_status="ready" if rows else "degraded", detail="ok" if rows else "no_moneyflow_dc_rows")
        return rows, health

    def load_rows(self, trade_date: str) -> list[dict[str, object]]:
        rows, _ = self.load_rows_with_health(trade_date)
        return rows
