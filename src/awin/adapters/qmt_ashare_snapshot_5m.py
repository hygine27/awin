"""stg.qmt_ashare_snapshot_5m adapter.

对应 QMT 的 A 股 5 分钟快照表。
用途是按分析时点回看当日最近一批可用快照，作为单轮盘中分析的主行情底座。
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from awin.adapters.base import DbBackedAdapter, SnapshotRequest
from awin.adapters.contracts import QmtSnapshotRow, SourceHealth
from awin.config import get_app_config

SH_TZ = ZoneInfo("Asia/Shanghai")


def _parse_analysis_snapshot_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if ts.tzinfo is None:
        return ts.replace(tzinfo=SH_TZ)
    return ts.astimezone(SH_TZ)


def _parse_local_snapshot_ts(trade_date: str, snapshot_time: str) -> datetime:
    return datetime.fromisoformat(f"{trade_date}T{snapshot_time}").replace(tzinfo=SH_TZ)


class QmtAshareSnapshot5mAdapter(DbBackedAdapter):
    """读取 stg.qmt_ashare_snapshot_5m 的最近可用快照。"""

    source_name = "qmt_ashare_snapshot_5m"

    def __init__(self) -> None:
        super().__init__(db_config=get_app_config().qt_db, dsn_label="qt")
        self._last_health: SourceHealth | None = None

    def health(self) -> SourceHealth:
        if self._last_health is not None:
            return self._last_health
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def build_coverage_query(self, request: SnapshotRequest) -> tuple[str, dict[str, str]]:
        sql = """
        select
          count(distinct code) as total_codes,
          count(distinct case when snapshot_time <= %(analysis_snapshot_ts)s::timestamptz then code end) as covered_codes
        from stg.qmt_ashare_snapshot_5m
        where trade_date = %(trade_date)s
        """
        return sql, {
            "trade_date": request.trade_date,
            "analysis_snapshot_ts": request.analysis_snapshot_ts,
        }

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
            self._last_health = SourceHealth(
                source_name=self.source_name,
                source_status="missing",
                detail="query_unavailable",
            )
            return []
        rows = [QmtSnapshotRow(**payload) for payload in result]

        coverage_ratio = None
        coverage_sql, coverage_params = self.build_coverage_query(request)
        coverage_rows = self._query_rows(coverage_sql, coverage_params) or []
        if coverage_rows:
            total_codes = int(coverage_rows[0].get("total_codes") or 0)
            covered_codes = int(coverage_rows[0].get("covered_codes") or 0)
            if total_codes > 0:
                coverage_ratio = round(covered_codes / total_codes, 4)

        freshness_seconds = None
        if rows:
            latest_snapshot_ts = max(_parse_local_snapshot_ts(item.trade_date, item.snapshot_time) for item in rows)
            cutoff_ts = _parse_analysis_snapshot_ts(request.analysis_snapshot_ts)
            freshness_seconds = max(0, int(round((cutoff_ts - latest_snapshot_ts).total_seconds())))

        self._last_health = SourceHealth(
            source_name=self.source_name,
            source_status="ready" if rows else "degraded",
            freshness_seconds=freshness_seconds,
            coverage_ratio=coverage_ratio,
            detail="ok" if rows else "no_qmt_snapshot_rows",
        )
        return rows
