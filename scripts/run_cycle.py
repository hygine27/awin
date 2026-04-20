from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.adapters import SnapshotRequest
from awin.builders.m0 import (
    build_m0_snapshot_bundle,
    load_previous_alert_material,
    load_previous_bull_state_history,
    persist_m0_snapshot_bundle,
)
from awin.config import get_app_config
from awin.reporting import render_intraday_summary
from awin.storage.db import connect_sqlite, init_db


SH_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class RunCycleArgs:
    trade_date: str | None
    snapshot_time: str | None
    floor_minutes: int
    db_path: Path
    dry_run: bool


def parse_args() -> RunCycleArgs:
    parser = argparse.ArgumentParser(description="Run one scheduled awin intraday cycle using the current local clock.")
    parser.add_argument("--trade-date")
    parser.add_argument("--snapshot-time")
    parser.add_argument("--floor-minutes", type=int, default=5)
    parser.add_argument("--db-path", type=Path, default=get_app_config().sqlite_path)
    parser.add_argument("--dry-run", action="store_true")
    parsed = parser.parse_args()
    return RunCycleArgs(
        trade_date=parsed.trade_date,
        snapshot_time=parsed.snapshot_time,
        floor_minutes=parsed.floor_minutes,
        db_path=parsed.db_path,
        dry_run=parsed.dry_run,
    )


def _floor_clock(now: datetime, floor_minutes: int) -> datetime:
    if floor_minutes <= 0:
        raise ValueError("floor_minutes must be positive")
    floored_minute = (now.minute // floor_minutes) * floor_minutes
    return now.replace(minute=floored_minute, second=0, microsecond=0)


def _resolve_slot(args: RunCycleArgs) -> tuple[str, str]:
    now = datetime.now(SH_TZ)
    trade_date = args.trade_date or now.strftime("%Y-%m-%d")
    if args.snapshot_time:
        snapshot_time = args.snapshot_time if len(args.snapshot_time) == 8 else f"{args.snapshot_time}:00"
        return trade_date, snapshot_time
    slot = _floor_clock(now, args.floor_minutes)
    return trade_date, slot.strftime("%H:%M:%S")


def _resolve_round_seq(db_path: Path, trade_date: str, analysis_snapshot_ts: str) -> int:
    init_db(db_path)
    with connect_sqlite(db_path) as connection:
        existing = connection.execute(
            """
            SELECT round_seq
            FROM monitor_run
            WHERE trade_date = ?
              AND analysis_snapshot_ts = ?
            ORDER BY round_seq DESC
            LIMIT 1
            """,
            (trade_date, analysis_snapshot_ts),
        ).fetchone()
        if existing is not None and existing["round_seq"] is not None:
            return int(existing["round_seq"])

        row = connection.execute(
            """
            SELECT COUNT(DISTINCT analysis_snapshot_ts) AS slot_count
            FROM monitor_run
            WHERE trade_date = ?
              AND analysis_snapshot_ts < ?
            """,
            (trade_date, analysis_snapshot_ts),
        ).fetchone()
    slot_count = int(row["slot_count"] or 0) if row is not None else 0
    return slot_count + 1


def main() -> None:
    args = parse_args()
    trade_date, snapshot_time = _resolve_slot(args)
    analysis_snapshot_ts = f"{trade_date}T{snapshot_time}"
    round_seq = _resolve_round_seq(args.db_path, trade_date, analysis_snapshot_ts)

    request = SnapshotRequest(
        trade_date=trade_date,
        snapshot_time=snapshot_time,
        analysis_snapshot_ts=analysis_snapshot_ts,
    )
    current_run_id = f"{trade_date}-{snapshot_time.replace(':', '')}-r{round_seq:02d}"
    previous_material = load_previous_alert_material(args.db_path, current_run_id, analysis_snapshot_ts)
    previous_bull_state = load_previous_bull_state_history(
        args.db_path,
        current_run_id,
        analysis_snapshot_ts,
        trade_date=trade_date,
        current_round_seq=round_seq,
    )
    build_result = build_m0_snapshot_bundle(
        request,
        round_seq=round_seq,
        previous_material=previous_material,
        previous_bull_state=previous_bull_state,
    )

    if args.dry_run:
        print(f"slot={trade_date}T{snapshot_time} round_seq={round_seq:02d} mode=dry-run")
        print(render_intraday_summary(build_result))
        return

    persist_m0_snapshot_bundle(args.db_path, build_result)
    print(f"persisted awin run: {build_result.bundle.run_context.run_id}")
    print(f"slot: {trade_date}T{snapshot_time}")
    print(f"sqlite: {args.db_path}")
    print(render_intraday_summary(build_result))
    print(
        "core={core} new_long={new_long} catchup={catchup} risk={risk} alert={decision}".format(
            core=len(build_result.bundle.opportunity_discovery.core_anchor_watchlist),
            new_long=len(build_result.bundle.opportunity_discovery.new_long_watchlist),
            catchup=len(build_result.bundle.opportunity_discovery.catchup_watchlist),
            risk=len(build_result.bundle.risk_surveillance.short_watchlist),
            decision=build_result.bundle.alert_output.diff_result.decision,
        )
    )


if __name__ == "__main__":
    main()
