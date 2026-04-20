"""ts_moneyflow_ths adapter.

读取 `stg.ts_moneyflow_ths` 的 T-1 及更早个股资金流历史，并在数据库侧聚合为
单股票一行的持续性画像，作为个股历史资金持续性画像的基础来源。
"""

from __future__ import annotations

from datetime import date, timedelta

from awin.adapters.base import DbBackedAdapter
from awin.adapters.contracts import SourceHealth
from awin.config import get_app_config


class TsMoneyflowThsAdapter(DbBackedAdapter):
    """Load historical THS stock moneyflow rows from `stg.ts_moneyflow_ths`."""

    source_name = "ts_moneyflow_ths"

    def __init__(self, lookback_days: int = 45) -> None:
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
            ts_code,
            trade_date,
            name,
            pct_change,
            latest,
            net_amount,
            net_d5_amount
          from stg.ts_moneyflow_ths
          where trade_date >= %(start_date)s::date
            and trade_date < %(trade_date)s::date
        ),
        rolling as (
          select
            ts_code,
            trade_date,
            name,
            pct_change,
            latest,
            net_amount,
            net_d5_amount,
            row_number() over (partition by ts_code order by trade_date desc) as rn_desc,
            sum(net_amount) over (
              partition by ts_code
              order by trade_date
              rows between 2 preceding and current row
            ) as main_net_amount_3d_sum,
            sum(net_amount) over (
              partition by ts_code
              order by trade_date
              rows between 4 preceding and current row
            ) as main_net_amount_5d_sum,
            sum(net_amount) over (
              partition by ts_code
              order by trade_date
              rows between 9 preceding and current row
            ) as main_net_amount_10d_sum,
            avg(net_amount) over (
              partition by ts_code
              order by trade_date
              rows between 2 preceding and current row
            ) as recent_3d_avg,
            avg(net_amount) over (
              partition by ts_code
              order by trade_date
              rows between 5 preceding and 3 preceding
            ) as previous_3d_avg,
            sum(case when net_amount <= 0 then 1 else 0 end) over (
              partition by ts_code
              order by trade_date desc
              rows between unbounded preceding and current row
            ) as positive_breaks,
            sum(case when net_amount >= 0 then 1 else 0 end) over (
              partition by ts_code
              order by trade_date desc
              rows between unbounded preceding and current row
            ) as negative_breaks
          from base
        ),
        streaks as (
          select
            ts_code,
            count(*) filter (where net_amount > 0 and positive_breaks = 0) as inflow_streak_days,
            count(*) filter (where net_amount < 0 and negative_breaks = 0) as outflow_streak_days
          from rolling
          group by ts_code
        )
        select
          rolling.ts_code,
          rolling.trade_date::text as trade_date,
          rolling.name,
          rolling.pct_change,
          rolling.latest,
          rolling.net_amount as main_net_amount_1d,
          rolling.net_d5_amount as ths_net_d5_amount,
          rolling.main_net_amount_3d_sum,
          rolling.main_net_amount_5d_sum,
          rolling.main_net_amount_10d_sum,
          (rolling.recent_3d_avg - rolling.previous_3d_avg) as flow_acceleration_3d,
          coalesce(streaks.inflow_streak_days, 0) as inflow_streak_days,
          coalesce(streaks.outflow_streak_days, 0) as outflow_streak_days
        from rolling
        left join streaks
          on streaks.ts_code = rolling.ts_code
        where rolling.rn_desc = 1
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
        health = SourceHealth(source_name=self.source_name, source_status="ready" if rows else "degraded", detail="ok" if rows else "no_moneyflow_ths_rows")
        return rows, health

    def load_rows(self, trade_date: str) -> list[dict[str, object]]:
        rows, _ = self.load_rows_with_health(trade_date)
        return rows
