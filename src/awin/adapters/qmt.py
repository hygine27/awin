from __future__ import annotations

from awin.adapters.base import DbBackedAdapter, SnapshotRequest
from awin.adapters.contracts import QmtSnapshotRow, SourceHealth
from awin.config import get_app_config


class QmtSnapshotAdapter(DbBackedAdapter):
    source_name = "qmt_snapshot"

    def __init__(self) -> None:
        super().__init__(db_config=get_app_config().qt_db, dsn_label="qt")

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def build_query(self, request: SnapshotRequest) -> tuple[str, dict[str, str]]:
        sql = """
        with latest_cutoff as (
          select distinct on (code)
            code as symbol,
            split_part(code, '.', 1) as stock_code,
            trade_date::text as trade_date,
            to_char(snapshot_time at time zone 'Asia/Shanghai', 'HH24:MI:SS') as snapshot_time,
            last_price,
            last_close,
            open as open_price,
            high as high_price,
            low as low_price,
            volume,
            amount,
            bid_price1,
            ask_price1,
            bid_volume1,
            ask_volume1
          from stg.qmt_ashare_snapshot_5m
          where trade_date = %(trade_date)s
            and snapshot_time <= %(analysis_snapshot_ts)s::timestamptz
          order by code, snapshot_time desc
        )
        select *
        from latest_cutoff
        order by symbol
        """
        return sql, {
            "trade_date": request.trade_date,
            "analysis_snapshot_ts": request.analysis_snapshot_ts,
        }

    def load_rows(self, request: SnapshotRequest) -> list[QmtSnapshotRow]:
        sql, params = self.build_query(request)
        result = self._query_rows(sql, params)
        if result is None:
            return []
        return [QmtSnapshotRow(**payload) for payload in result]
