"""ts_stock_basic adapter.

读取 `stg.ts_stock_basic` 作为风格底座的静态身份层来源，补充：
- 市场层次
- 交易所
- 企业属性
- 基础行业
"""

from __future__ import annotations

from awin.adapters.base import DbBackedAdapter
from awin.adapters.contracts import SourceHealth
from awin.config import get_app_config


class TsStockBasicAdapter(DbBackedAdapter):
    """Load the latest listed A-share static attributes from `stg.ts_stock_basic`."""

    source_name = "ts_stock_basic"

    def __init__(self) -> None:
        super().__init__(db_config=get_app_config().qt_db, dsn_label="qt")

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def build_query(self) -> tuple[str, dict[str, object]]:
        sql = """
        select
          ts_code,
          market,
          exchange,
          industry,
          act_name,
          act_ent_type
        from stg.ts_stock_basic
        order by ts_code
        """
        return sql, {}

    def load_rows_with_health(self) -> tuple[list[dict[str, object]], SourceHealth]:
        _, error = self._connect_with_error()
        if error is not None:
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=error)

        sql, params = self.build_query()
        try:
            rows = self._query_rows(sql, params) or []
        except Exception as exc:  # pragma: no cover - exercised through live runtime
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=str(exc))

        health = SourceHealth(
            source_name=self.source_name,
            source_status="ready" if rows else "degraded",
            detail="ok" if rows else "no_listed_rows",
        )
        return rows, health

    def load_rows(self) -> list[dict[str, object]]:
        rows, _ = self.load_rows_with_health()
        return rows
