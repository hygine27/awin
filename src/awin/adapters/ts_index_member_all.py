"""ts_index_member_all adapter.

读取 `stg.ts_index_member_all` 的当前有效申万行业链，作为风格底座的行业身份层来源，
补充申万一级/二级/三级行业归属。
"""

from __future__ import annotations

from awin.adapters.base import DbBackedAdapter
from awin.adapters.contracts import SourceHealth
from awin.config import get_app_config


class TsIndexMemberAllAdapter(DbBackedAdapter):
    """Load active industry membership rows from `stg.ts_index_member_all`."""

    source_name = "ts_index_member_all"

    def __init__(self) -> None:
        super().__init__(db_config=get_app_config().qt_db, dsn_label="qt")

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def build_query(self, trade_date: str) -> tuple[str, dict[str, object]]:
        sql = """
        select
          ts_code,
          l1_code,
          l1_name,
          l2_code,
          l2_name,
          l3_code,
          l3_name,
          in_date,
          out_date,
          name
        from stg.ts_index_member_all
        where coalesce(nullif(in_date::text, '')::date, '1900-01-01'::date) <= %(trade_date)s::date
          and coalesce(nullif(out_date::text, '')::date, '9999-12-31'::date) >= %(trade_date)s::date
        order by ts_code, in_date desc
        """
        return sql, {"trade_date": str(trade_date or "").strip()}

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
            detail="ok" if rows else "no_index_member_rows",
        )
        return rows, health

    def load_rows(self, trade_date: str) -> list[dict[str, object]]:
        rows, _ = self.load_rows_with_health(trade_date)
        return rows
