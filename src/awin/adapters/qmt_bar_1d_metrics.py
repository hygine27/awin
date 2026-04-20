"""qmt_bar_1d_metrics adapter.

这是一个面向盘中 runtime 的派生接口。

来源：
- `stg.qmt_bar_1d`

用途：
- 将日线明细在数据库侧聚合成盘中分析真正需要的每股一行指标
- 为 `stock_facts` 提供：
  - 近 20 日平均成交额
  - 3/5/10/20 日参考收盘价

说明：
- 评估与后验模块仍可继续使用 `qmt_bar_1d` 原始明细
- 本接口只服务盘中快照构建路径，目标是压缩行数与响应时间
"""

from __future__ import annotations

from awin.adapters.base import DbBackedAdapter
from awin.adapters.contracts import SourceHealth
from awin.config import get_app_config


class QmtBar1dMetricsAdapter(DbBackedAdapter):
    """Load aggregated qmt_bar_1d metrics for runtime stock facts."""

    source_name = "qmt_bar_1d_metrics"

    def __init__(self) -> None:
        super().__init__(db_config=get_app_config().qt_db, dsn_label="qt")

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def build_query(self, symbols: list[str], start_date: str, trade_date: str) -> tuple[str, dict[str, object]]:
        sql = """
        with base as (
          select
            code as symbol,
            trade_date,
            close,
            amount,
            row_number() over (partition by code order by trade_date desc) as rn_desc
          from stg.qmt_bar_1d
          where code = any(%(symbols)s)
            and trade_date >= %(start_date)s::date
            and trade_date < %(trade_date)s::date
            and coalesce(suspend_flag, 0) = 0
        )
        select
          symbol,
          avg(amount) filter (where rn_desc <= 20) as avg_amount_20d,
          max(close) filter (where rn_desc = 3) as close_3d_ago,
          max(close) filter (where rn_desc = 5) as close_5d_ago,
          max(close) filter (where rn_desc = 10) as close_10d_ago,
          max(close) filter (where rn_desc = 20) as close_20d_ago
        from base
        group by symbol
        """
        return sql, {
            "symbols": symbols,
            "start_date": start_date,
            "trade_date": trade_date,
        }

    def load_rows_with_health(self, symbols: list[str], start_date: str, trade_date: str) -> tuple[list[dict[str, object]], SourceHealth]:
        if not symbols:
            return [], SourceHealth(source_name=self.source_name, source_status="degraded", detail="empty_symbol_set")

        _, error = self._connect_with_error()
        if error is not None:
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=error)

        sql, params = self.build_query(symbols, start_date, trade_date)
        try:
            result = self._query_rows(sql, params) or []
        except Exception as exc:  # pragma: no cover - exercised through live runtime
            return [], SourceHealth(source_name=self.source_name, source_status="missing", detail=str(exc))

        health = SourceHealth(
            source_name=self.source_name,
            source_status="ready" if result else "degraded",
            detail="ok" if result else "no_qmt_bar_metric_rows",
        )
        return result, health

    def load_rows(self, symbols: list[str], start_date: str, trade_date: str) -> list[dict[str, object]]:
        rows, _ = self.load_rows_with_health(symbols, start_date, trade_date)
        return rows
