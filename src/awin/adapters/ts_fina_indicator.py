"""ts_fina_indicator adapter.

读取 `stg.ts_fina_indicator` 中在当前交易日之前已经公告的最新财务指标，
用于 style_profile 的质量、收入成长和利润成长评分。
"""

from __future__ import annotations

from awin.adapters.base import DbBackedAdapter
from awin.adapters.contracts import SourceHealth
from awin.config import get_app_config


class TsFinaIndicatorAdapter(DbBackedAdapter):
    """Load latest announced financial indicators before the current trade date."""

    source_name = "ts_fina_indicator"

    def __init__(self) -> None:
        super().__init__(db_config=get_app_config().qt_db, dsn_label="qt")

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def build_query(self, trade_date: str) -> tuple[str, dict[str, object]]:
        sql = """
        select distinct on (ts_code)
          ts_code,
          ann_date::text as ann_date,
          end_date::text as end_date,
          roe_yearly,
          roic,
          debt_to_assets,
          q_ocf_to_sales,
          tr_yoy,
          or_yoy,
          q_sales_yoy,
          netprofit_yoy,
          dt_netprofit_yoy,
          op_yoy
        from stg.ts_fina_indicator
        where coalesce(nullif(ann_date::text, '')::date, '1900-01-01'::date) < %(trade_date)s::date
        order by ts_code, ann_date desc nulls last, end_date desc nulls last
        """
        return sql, {"trade_date": str(trade_date).replace("/", "-")}

    def load_rows_with_health(self, trade_date: str) -> tuple[list[dict[str, object]], SourceHealth]:
        _, error = self._connect_with_error()
        if error is not None:
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        sql, params = self.build_query(trade_date)
        try:
            rows = self._query_rows(sql, params) or []
        except Exception as exc:  # pragma: no cover - exercised through live runtime
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=str(exc))
        health = SourceHealth(source_name=self.source_name, source_status="ready" if rows else "degraded", detail="ok" if rows else "no_fina_rows")
        return rows, health

    def load_rows(self, trade_date: str) -> list[dict[str, object]]:
        rows, _ = self.load_rows_with_health(trade_date)
        return rows
