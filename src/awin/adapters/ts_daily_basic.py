"""ts_daily_basic adapter.

读取 `stg.ts_daily_basic` 的最近日度快照，作为风格底座的容量与估值层来源，
补充流通市值、总市值以及分红/估值相关字段。
"""

from __future__ import annotations

from awin.adapters.base import DbBackedAdapter
from awin.adapters.contracts import SourceHealth
from awin.config import get_app_config


def _compact_trade_date(trade_date: str) -> str:
    return str(trade_date or "").replace("-", "").strip()


class TsDailyBasicAdapter(DbBackedAdapter):
    """Load one daily-basic snapshot from `stg.ts_daily_basic` for style profiling."""

    source_name = "ts_daily_basic"

    def __init__(self) -> None:
        super().__init__(db_config=get_app_config().qt_db, dsn_label="qt")

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def build_query(self, trade_date: str) -> tuple[str, dict[str, object]]:
        sql = """
        with latest_trade_date as (
          select max(trade_date) as trade_date
          from stg.ts_daily_basic
          where trade_date < %(trade_date)s::date
        )
        select
          ts_code,
          trade_date::text as trade_date,
          free_share,
          circ_mv,
          total_mv,
          turnover_rate,
          turnover_rate_f,
          volume_ratio,
          pe_ttm,
          pb,
          ps_ttm,
          dv_ratio,
          dv_ttm
        from stg.ts_daily_basic
        where trade_date = (select trade_date from latest_trade_date)
        order by ts_code
        """
        return sql, {"trade_date": _compact_trade_date(trade_date)}

    def load_rows_with_health(self, trade_date: str) -> tuple[list[dict[str, object]], SourceHealth]:
        _, error = self._connect_with_error()
        if error is not None:
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=error)

        sql, params = self.build_query(trade_date)
        try:
            rows = self._query_rows(sql, params) or []
        except Exception as exc:  # pragma: no cover - exercised through live runtime
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=str(exc))

        health = SourceHealth(
            source_name=self.source_name,
            source_status="ready" if rows else "degraded",
            detail="ok" if rows else "no_daily_basic_rows",
        )
        return rows, health

    def load_rows(self, trade_date: str) -> list[dict[str, object]]:
        rows, _ = self.load_rows_with_health(trade_date)
        return rows
