from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import psycopg

from awin.adapters import (
    DcfHqZjSnapshotAdapter,
    QmtAshareSnapshot5mAdapter,
    SnapshotRequest,
    ThsAppHotConceptAdapter,
    ThsCliHotConceptAdapter,
    ThsMarketOverviewAdapter,
)
from awin.config import get_app_config


def _to_float(value: object) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_ts(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


@dataclass(slots=True)
class IntradaySourceState:
    request: dict[str, str]
    qmt: dict[str, Any]
    dcf: dict[str, Any]
    ths_cli_hot_concept: dict[str, Any]
    ths_app_hot_concept: dict[str, Any]
    market_overview: dict[str, Any]
    validation: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def collect_intraday_source_state(request: SnapshotRequest) -> IntradaySourceState:
    config = get_app_config()

    qmt_adapter = QmtAshareSnapshot5mAdapter()
    dcf_adapter = DcfHqZjSnapshotAdapter()
    cli_adapter = ThsCliHotConceptAdapter()
    app_adapter = ThsAppHotConceptAdapter()
    market_overview_adapter = ThsMarketOverviewAdapter()

    qmt_rows = qmt_adapter.load_rows(request)
    dcf_rows, dcf_health = dcf_adapter.load_rows_with_health(request)
    cli_rows = cli_adapter.load_rows(request)
    app_rows = app_adapter.load_rows(request)
    market_tape = market_overview_adapter.load_market_tape()

    latest_qmt_snapshot_time = max((str(row.snapshot_time) for row in qmt_rows), default=None)
    latest_dcf_vendor_batch = max((str(row.vendor_batch_ts or "") for row in dcf_rows if row.vendor_batch_ts), default=None) or None
    latest_cli_batch = max((str(row.batch_ts) for row in cli_rows), default=None)
    latest_app_batch = max((str(row.batch_ts) for row in app_rows), default=None)

    qmt_summary = {
        "row_count": len(qmt_rows),
        "latest_snapshot_time": latest_qmt_snapshot_time,
        "health": qmt_adapter.health().to_dict(),
    }
    dcf_summary = {
        "row_count": len(dcf_rows),
        "latest_vendor_batch_ts": latest_dcf_vendor_batch,
        "health": dcf_health.to_dict(),
    }

    cli_summary: dict[str, Any] = {
        "row_count": len(cli_rows),
        "latest_batch_ts": latest_cli_batch,
        "health": cli_adapter.health().to_dict(),
    }
    app_summary: dict[str, Any] = {
        "row_count": len(app_rows),
        "latest_batch_ts": latest_app_batch,
        "health": app_adapter.health().to_dict(),
        "lifecycle_status": getattr(app_adapter, "lifecycle_status", None),
    }

    with psycopg.connect(
        host=config.fin_db.host,
        port=config.fin_db.port,
        dbname=config.fin_db.dbname,
        user=config.fin_db.user,
        password=config.fin_db.password,
    ) as fin_conn:
        with fin_conn.cursor() as cur:
            cur.execute(
                """
                select
                  count(distinct created_at) as batch_count,
                  min(created_at)::text as first_batch_ts,
                  max(created_at)::text as last_batch_ts,
                  count(*) as row_count,
                  sum(case when hot_rank_chg is not null and hot_rank_chg <> 0 then 1 else 0 end) as changed_rank_rows
                from stg.ths_app_hot_concept_trade
                where created_at::date = %(trade_date)s::date
                """,
                {"trade_date": request.trade_date},
            )
            app_daily = cur.fetchone()

            cur.execute(
                """
                select
                  count(distinct batch_ts) as batch_count,
                  min(batch_ts)::text as first_batch_ts,
                  max(batch_ts)::text as last_batch_ts,
                  count(*) as row_count
                from stg.ths_cli_hot_concept
                where trade_date = %(trade_date)s::date
                """,
                {"trade_date": request.trade_date},
            )
            cli_daily = cur.fetchone()

            cur.execute(
                """
                with latest_batch as (
                  select max(batch_ts) as batch_ts
                  from stg.ths_cli_hot_concept
                  where trade_date = %(trade_date)s::date
                    and batch_ts <= %(cutoff_ts)s::timestamp
                )
                select
                  batch_ts::text as batch_ts,
                  concept_name,
                  change_pct,
                  speed_1min,
                  main_net_amount,
                  limit_up_count,
                  leading_stock
                from stg.ths_cli_hot_concept
                where batch_ts = (select batch_ts from latest_batch)
                order by change_pct desc nulls last
                limit 12
                """,
                {
                    "trade_date": request.trade_date,
                    "cutoff_ts": request.analysis_snapshot_ts.replace("T", " "),
                },
            )
            cli_top_rows = [
                {
                    "batch_ts": str(row[0]),
                    "concept_name": row[1],
                    "change_pct": row[2],
                    "speed_1min": row[3],
                    "main_net_amount": row[4],
                    "limit_up_count": row[5],
                    "leading_stock": row[6],
                }
                for row in cur.fetchall()
            ]

            cur.execute(
                """
                with latest_batch as (
                  select max(created_at) as batch_ts
                  from stg.ths_app_hot_concept_trade
                  where created_at::date = %(trade_date)s::date
                    and created_at <= %(cutoff_ts)s::timestamp
                )
                select
                  created_at::text as batch_ts,
                  plate_name,
                  rank,
                  hot_score,
                  hot_rank_chg,
                  limit_up_tag
                from stg.ths_app_hot_concept_trade
                where created_at = (select batch_ts from latest_batch)
                order by rank asc
                limit 12
                """,
                {
                    "trade_date": request.trade_date,
                    "cutoff_ts": request.analysis_snapshot_ts.replace("T", " "),
                },
            )
            app_top_rows = [
                {
                    "batch_ts": str(row[0]),
                    "concept_name": row[1],
                    "concept_rank": row[2],
                    "concept_hot_score": _to_float(row[3]),
                    "concept_rank_change": row[4],
                    "limit_up_tag": row[5],
                }
                for row in cur.fetchall()
            ]

    app_daily_summary = {
        "batch_count": int(app_daily[0] or 0),
        "first_batch_ts": app_daily[1],
        "last_batch_ts": app_daily[2],
        "row_count": int(app_daily[3] or 0),
        "changed_rank_rows": int(app_daily[4] or 0),
        "top_rows": app_top_rows,
    }
    cli_daily_summary = {
        "batch_count": int(cli_daily[0] or 0),
        "first_batch_ts": cli_daily[1],
        "last_batch_ts": cli_daily[2],
        "row_count": int(cli_daily[3] or 0),
        "top_rows": cli_top_rows,
    }

    app_summary["daily"] = app_daily_summary
    cli_summary["daily"] = cli_daily_summary

    app_last_batch = app_daily_summary["last_batch_ts"]
    cli_last_batch = cli_daily_summary["last_batch_ts"]
    lag_minutes: float | None = None
    if app_last_batch and cli_last_batch:
        app_ts = _parse_ts(app_last_batch)
        cli_ts = _parse_ts(cli_last_batch)
        if app_ts is not None and cli_ts is not None:
            lag_minutes = (cli_ts - app_ts).total_seconds() / 60.0

    validation = {
        "ths_app_intraday_usable": bool(
            app_daily_summary["batch_count"] >= 4
            and app_daily_summary["changed_rank_rows"] > 0
            and lag_minutes is not None
            and lag_minutes <= 45.0
        ),
        "ths_app_vs_cli_lag_minutes": lag_minutes,
        "ths_app_deprecation_reason": (
            "app source is deprecated for production scoring; current validation expects stable intraday refresh,"
            " multiple daily batches, and observable rank changes"
        ),
    }

    market_overview_summary = {
        "health": market_overview_adapter.health().to_dict(),
        "market_tape": market_tape,
    }

    return IntradaySourceState(
        request={
            "trade_date": request.trade_date,
            "snapshot_time": request.snapshot_time,
            "analysis_snapshot_ts": request.analysis_snapshot_ts,
        },
        qmt=qmt_summary,
        dcf=dcf_summary,
        ths_cli_hot_concept=cli_summary,
        ths_app_hot_concept=app_summary,
        market_overview=market_overview_summary,
        validation=validation,
    )
