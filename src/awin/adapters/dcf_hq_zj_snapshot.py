"""dcf_hq_zj_snapshot adapter.

对应东方财富客户端的两张盘中增强表：
- stg.dcf_cli_hq
- stg.dcf_cli_zj

用途是先按同一 batch_ts 配对两张表，再输出一份可直接用于盘中分析的增强快照，
补充换手率、量比、振幅、市值和资金流等字段。
"""

from __future__ import annotations

import math
from datetime import datetime
from zoneinfo import ZoneInfo

from awin.adapters.base import DbBackedAdapter, SnapshotRequest
from awin.adapters.contracts import DcfSnapshotRow, SourceHealth
from awin.config import get_app_config
from awin.utils.symbols import infer_symbol_from_stock_code, normalize_stock_code


SH_TZ = ZoneInfo("Asia/Shanghai")
NUMERIC_FIELDS = {
    "turnover_rate",
    "volume_ratio",
    "amplitude",
    "float_mkt_cap",
    "total_mkt_cap",
    "ret_3d",
    "ret_5d",
    "ret_10d",
    "ret_20d",
    "main_net_inflow",
    "super_net",
    "large_net",
}
PERCENT_RATIO_FIELDS = {
    "turnover_rate",
    "amplitude",
    "ret_3d",
    "ret_5d",
    "ret_10d",
    "ret_20d",
}


def _to_float(value):
    if value in {None, ""}:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    multiplier = 1.0
    unit_map = {
        "万亿": 1e12,
        "亿": 1e8,
        "万": 1e4,
        "%": 1.0,
    }
    for unit, unit_multiplier in unit_map.items():
        if text.endswith(unit):
            multiplier = unit_multiplier
            text = text[: -len(unit)].strip()
            break
    try:
        return float(text) * multiplier
    except (TypeError, ValueError):
        return None


def _normalize_ratio_field(field_name: str, value: float | None) -> float | None:
    if value is None:
        return None
    if field_name not in PERCENT_RATIO_FIELDS:
        return value
    if abs(value) > 1.0:
        return value / 100.0
    return value


class DcfHqZjSnapshotAdapter(DbBackedAdapter):
    """读取 DCF 行情表与资金表配对后的最近完整增强批次。"""

    source_name = "dcf_hq_zj_snapshot"

    def __init__(
        self,
        max_freshness_minutes: float | None = None,
        min_rows_abs: int | None = None,
        min_completeness_ratio: float | None = None,
    ) -> None:
        config = get_app_config()
        super().__init__(db_config=config.fin_db, dsn_label="fin")
        self.max_freshness_minutes = max_freshness_minutes if max_freshness_minutes is not None else config.dcf_max_freshness_minutes
        self.min_rows_abs = min_rows_abs if min_rows_abs is not None else config.dcf_min_rows_abs
        self.min_completeness_ratio = (
            min_completeness_ratio if min_completeness_ratio is not None else config.dcf_min_completeness_ratio
        )

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def normalize_batch_ts(self, batch_ts) -> datetime | None:
        if batch_ts is None:
            return None
        ts = batch_ts
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            return ts.replace(tzinfo=SH_TZ)
        if str(ts.tzinfo) in {str(SH_TZ), "Asia/Shanghai", "CST"}:
            return ts.astimezone(SH_TZ)
        return ts.replace(tzinfo=None).replace(tzinfo=SH_TZ)

    def build_batch_query(self, request: SnapshotRequest) -> tuple[str, dict[str, str]]:
        sql = """
        with hq_batches as (
          select batch_ts, count(*) as hq_rows
          from stg.dcf_cli_hq
          where trade_date = %(trade_date)s
          group by 1
        ), zj_batches as (
          select batch_ts, count(*) as zj_rows
          from stg.dcf_cli_zj
          where trade_date = %(trade_date)s
          group by 1
        ), paired as (
          select
            hq.batch_ts,
            hq.hq_rows,
            zj.zj_rows,
            least(hq.hq_rows, zj.zj_rows) as paired_rows,
            (hq.batch_ts at time zone 'UTC') as wall_clock_ts
          from hq_batches hq
          join zj_batches zj using (batch_ts)
        )
        select *
        from paired
        where wall_clock_ts <= %(cutoff_naive)s::timestamp
        order by batch_ts desc
        """
        cutoff_naive = self.normalize_batch_ts(request.analysis_snapshot_ts)
        assert cutoff_naive is not None
        return sql, {
            "trade_date": request.trade_date,
            "cutoff_naive": cutoff_naive.replace(tzinfo=None).isoformat(sep=" "),
        }

    def build_baseline_query(self, request: SnapshotRequest) -> tuple[str, dict[str, str]]:
        sql = """
        with hq_batches as (
          select batch_ts, count(*) as hq_rows
          from stg.dcf_cli_hq
          where trade_date = %(trade_date)s
          group by 1
        ), zj_batches as (
          select batch_ts, count(*) as zj_rows
          from stg.dcf_cli_zj
          where trade_date = %(trade_date)s
          group by 1
        )
        select max(least(hq.hq_rows, zj.zj_rows)) as baseline_rows
        from hq_batches hq
        join zj_batches zj using (batch_ts)
        """
        return sql, {"trade_date": request.trade_date}

    def build_data_query(self, request: SnapshotRequest, batch_ts: str) -> tuple[str, dict[str, str]]:
        sql = """
        select
          hq.code as symbol,
          hq.trade_date::text as trade_date,
          hq.batch_ts::text as vendor_batch_ts,
          hq.turnover_pct as turnover_rate,
          hq.volume_ratio,
          hq.amplitude_pct as amplitude,
          hq.float_mkt_cap,
          hq.total_mkt_cap,
          hq.change_3d_pct as ret_3d,
          null::real as ret_5d,
          hq.change_6d_pct as ret_10d,
          hq.change_1m_pct as ret_20d,
          zj.main_net_inflow,
          zj.super_net,
          zj.large_net
        from stg.dcf_cli_hq hq
        join stg.dcf_cli_zj zj
          on hq.trade_date = zj.trade_date
         and hq.batch_ts = zj.batch_ts
         and hq.code = zj.code
        where hq.trade_date = %(trade_date)s
          and hq.batch_ts = %(batch_ts)s::timestamptz
        """
        return sql, {"trade_date": request.trade_date, "batch_ts": batch_ts}

    def build_hq_data_query(self, request: SnapshotRequest, batch_ts: str) -> tuple[str, dict[str, str]]:
        sql = """
        select
          code,
          trade_date::text as trade_date,
          batch_ts::text as vendor_batch_ts,
          turnover_pct as turnover_rate,
          volume_ratio,
          amplitude_pct as amplitude,
          float_mkt_cap,
          total_mkt_cap,
          change_3d_pct as ret_3d,
          null::real as ret_5d,
          change_6d_pct as ret_10d,
          change_1m_pct as ret_20d
        from stg.dcf_cli_hq
        where trade_date = %(trade_date)s
          and batch_ts = %(batch_ts)s::timestamptz
        """
        return sql, {"trade_date": request.trade_date, "batch_ts": batch_ts}

    def build_zj_data_query(self, request: SnapshotRequest, batch_ts: str) -> tuple[str, dict[str, str]]:
        sql = """
        select
          code,
          main_net_inflow,
          super_net,
          large_net
        from stg.dcf_cli_zj
        where trade_date = %(trade_date)s
          and batch_ts = %(batch_ts)s::timestamptz
        """
        return sql, {"trade_date": request.trade_date, "batch_ts": batch_ts}

    def evaluate_guard(
        self,
        request: SnapshotRequest,
        batch_rows: list[dict],
        baseline_rows: int,
    ) -> tuple[dict | None, SourceHealth]:
        if not batch_rows:
            return None, SourceHealth(
                source_name=self.source_name,
                source_status="degraded",
                fallback_used=True,
                detail="no_batch_before_cutoff",
            )

        chosen = batch_rows[0]
        batch_local = self.normalize_batch_ts(chosen["batch_ts"])
        cutoff_local = self.normalize_batch_ts(request.analysis_snapshot_ts)
        assert batch_local is not None and cutoff_local is not None

        paired_rows = int(chosen["paired_rows"])
        completeness_ratio = float(paired_rows / baseline_rows) if baseline_rows else None
        freshness_minutes = float((cutoff_local - batch_local).total_seconds() / 60.0)
        min_rows_required = max(self.min_rows_abs, int(math.ceil((baseline_rows or 0) * self.min_completeness_ratio)))
        is_complete = paired_rows >= min_rows_required if baseline_rows else paired_rows >= self.min_rows_abs
        is_fresh = freshness_minutes <= self.max_freshness_minutes

        reason_parts: list[str] = []
        if not is_fresh:
            reason_parts.append("stale_batch")
        if not is_complete:
            reason_parts.append("low_coverage")

        health = SourceHealth(
            source_name=self.source_name,
            source_status="ready" if is_fresh and is_complete else "degraded",
            freshness_seconds=int(round(freshness_minutes * 60.0)),
            coverage_ratio=round(completeness_ratio, 4) if completeness_ratio is not None else None,
            fallback_used=not (is_fresh and is_complete),
            detail="ok" if is_fresh and is_complete else ",".join(reason_parts),
        )
        return chosen, health

    def load_rows_with_health(self, request: SnapshotRequest) -> tuple[list[DcfSnapshotRow], SourceHealth]:
        _, error = self._connect_with_error()
        if error is not None:
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=error)

        batch_sql, batch_params = self.build_batch_query(request)
        baseline_sql, baseline_params = self.build_baseline_query(request)
        batch_rows = self._query_rows(batch_sql, batch_params) or []
        baseline_result = self._query_rows(baseline_sql, baseline_params) or []
        baseline_rows = 0
        if baseline_result and baseline_result[0].get("baseline_rows") is not None:
            baseline_rows = int(baseline_result[0]["baseline_rows"])

        chosen, health = self.evaluate_guard(request, batch_rows, baseline_rows)
        if chosen is None:
            return [], health

        hq_sql, hq_params = self.build_hq_data_query(request, str(chosen["batch_ts"]))
        zj_sql, zj_params = self.build_zj_data_query(request, str(chosen["batch_ts"]))
        hq_rows = self._query_rows(hq_sql, hq_params) or []
        zj_rows = self._query_rows(zj_sql, zj_params) or []
        zj_by_code = {str(item.get("code") or "").strip(): item for item in zj_rows if str(item.get("code") or "").strip()}
        rows = []
        for payload in hq_rows:
            code = str(payload.get("code") or "").strip()
            if not code:
                continue
            payload = dict(payload)
            payload.update(zj_by_code.get(code, {}))
            payload["symbol"] = code
            payload.pop("code", None)
            stock_code = normalize_stock_code(payload.get("symbol"))
            payload["symbol"] = infer_symbol_from_stock_code(stock_code)
            for field_name in NUMERIC_FIELDS:
                payload[field_name] = _normalize_ratio_field(field_name, _to_float(payload.get(field_name)))
            rows.append(DcfSnapshotRow(**payload, source_health=health))
        return rows, health

    def load_rows(self, request: SnapshotRequest) -> list[DcfSnapshotRow]:
        rows, _ = self.load_rows_with_health(request)
        return rows
