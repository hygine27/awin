"""ts_style_daily_metrics adapter.

这是一个面向 `style_profile` 的派生接口。

来源：
- `stg.ts_daily`
- `stg.ts_adj_factor`

用途：
- 不再把两张明细表的几十万行历史数据完整拉回本地再做组装
- 在数据库侧直接计算 `style_profile` 所需的最终历史指标
- 供 `style_profile` 直接读取：
  - `avg_amount_20d`
  - `ret_20d / ret_60d`
  - `vol_20d / vol_60d`
  - `max_drawdown_20d / max_drawdown_60d`
"""

from __future__ import annotations

from datetime import date, timedelta

from awin.adapters.base import DbBackedAdapter
from awin.adapters.contracts import SourceHealth
from awin.config import get_app_config


class TsStyleDailyMetricsAdapter(DbBackedAdapter):
    """Load aggregated daily metric series for style profiling."""

    source_name = "ts_style_daily_metrics"

    def __init__(self, lookback_days: int = 120) -> None:
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
        with base as (
          select
            d.ts_code,
            d.trade_date,
            (d.close * a.adj_factor)::double precision as adj_close,
            coalesce(d.amount, 0)::double precision as amount,
            row_number() over (partition by d.ts_code order by d.trade_date desc) as rn_desc
          from stg.ts_daily d
          join stg.ts_adj_factor a
            on a.ts_code = d.ts_code
           and a.trade_date = d.trade_date
          where d.trade_date >= %(start_date)s::date
            and d.trade_date < %(trade_date)s::date
        ),
        returns as (
          select
            ts_code,
            trade_date,
            rn_desc,
            adj_close,
            amount,
            case
              when lag(adj_close) over (partition by ts_code order by trade_date) > 0
                then adj_close / lag(adj_close) over (partition by ts_code order by trade_date) - 1.0
              else null
            end as daily_ret
          from base
        ),
        latest as (
          select
            ts_code,
            avg(amount) filter (where rn_desc <= 20) as avg_amount_20d,
            max(adj_close) filter (where rn_desc = 1) as latest_close,
            max(adj_close) filter (where rn_desc = 21) as close_20d_base,
            max(adj_close) filter (where rn_desc = 61) as close_60d_base,
            stddev_pop(daily_ret) filter (where rn_desc <= 20) as vol_20d,
            stddev_pop(daily_ret) filter (where rn_desc <= 60) as vol_60d
          from returns
          group by ts_code
        ),
        drawdown_20_rows as (
          select
            ts_code,
            adj_close / max(adj_close) over (
              partition by ts_code
              order by trade_date
              rows between unbounded preceding and current row
            ) - 1.0 as drawdown_20d
          from returns
          where rn_desc <= 21
        ),
        drawdown_60_rows as (
          select
            ts_code,
            adj_close / max(adj_close) over (
              partition by ts_code
              order by trade_date
              rows between unbounded preceding and current row
            ) - 1.0 as drawdown_60d
          from returns
          where rn_desc <= 61
        ),
        drawdown_20_final as (
          select ts_code, min(drawdown_20d) as max_drawdown_20d
          from drawdown_20_rows
          group by ts_code
        ),
        drawdown_60_final as (
          select ts_code, min(drawdown_60d) as max_drawdown_60d
          from drawdown_60_rows
          group by ts_code
        )
        select
          latest.ts_code,
          latest.avg_amount_20d,
          case
            when latest.close_20d_base > 0 then latest.latest_close / latest.close_20d_base - 1.0
            else null
          end as ret_20d,
          case
            when latest.close_60d_base > 0 then latest.latest_close / latest.close_60d_base - 1.0
            else null
          end as ret_60d,
          latest.vol_20d,
          latest.vol_60d,
          abs(drawdown_20_final.max_drawdown_20d) as max_drawdown_20d,
          abs(drawdown_60_final.max_drawdown_60d) as max_drawdown_60d
        from latest
        left join drawdown_20_final
          on drawdown_20_final.ts_code = latest.ts_code
        left join drawdown_60_final
          on drawdown_60_final.ts_code = latest.ts_code
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
        health = SourceHealth(
            source_name=self.source_name,
            source_status="ready" if rows else "degraded",
            detail="ok" if rows else "no_style_daily_metric_rows",
        )
        return rows, health

    def load_rows(self, trade_date: str) -> list[dict[str, object]]:
        rows, _ = self.load_rows_with_health(trade_date)
        return rows
