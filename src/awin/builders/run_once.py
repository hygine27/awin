from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from awin.adapters import SnapshotRequest
from awin.builders.m0 import (
    build_m0_snapshot_bundle,
    load_previous_alert_material,
    load_previous_bull_state_history,
    persist_m0_snapshot_bundle,
)
from awin.config import get_app_config
from awin.storage.db import connect_sqlite, init_db


@dataclass(frozen=True)
class RunOnceArgs:
    trade_date: str
    snapshot_time: str
    round_seq: int
    db_path: Path
    dry_run: bool
    evidence_only: bool


def _build_run_id(trade_date: str, snapshot_time: str, round_seq: int) -> str:
    compact_time = snapshot_time.replace(":", "")
    return f"{trade_date}-{compact_time}-r{round_seq:02d}"


def parse_args() -> RunOnceArgs:
    config = get_app_config()
    now = datetime.now()

    parser = argparse.ArgumentParser(description="Initialize awin SQLite and write one monitor_run row.")
    parser.add_argument("--trade-date", default=now.strftime("%Y-%m-%d"))
    parser.add_argument("--snapshot-time", default=now.strftime("%H:%M:%S"))
    parser.add_argument("--round-seq", type=int, default=1)
    parser.add_argument("--db-path", type=Path, default=config.sqlite_path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--evidence-only", action="store_true")
    parsed = parser.parse_args()

    return RunOnceArgs(
        trade_date=parsed.trade_date,
        snapshot_time=parsed.snapshot_time,
        round_seq=parsed.round_seq,
        db_path=parsed.db_path,
        dry_run=parsed.dry_run,
        evidence_only=parsed.evidence_only,
    )


def write_monitor_run(args: RunOnceArgs) -> str:
    init_db(args.db_path)

    run_id = _build_run_id(args.trade_date, args.snapshot_time, args.round_seq)
    analysis_snapshot_ts = f"{args.trade_date}T{args.snapshot_time}"

    if args.dry_run:
        return run_id

    with connect_sqlite(args.db_path) as connection:
        connection.execute(
            """
            INSERT INTO monitor_run (
                run_id,
                trade_date,
                snapshot_time,
                analysis_snapshot_ts,
                round_seq,
                market_regime,
                style_state,
                top_attack_lines,
                has_update,
                alert_level,
                source_status,
                stock_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                analysis_snapshot_ts = excluded.analysis_snapshot_ts,
                round_seq = excluded.round_seq,
                source_status = excluded.source_status
            """,
            (
                run_id,
                args.trade_date,
                args.snapshot_time,
                analysis_snapshot_ts,
                args.round_seq,
                None,
                None,
                None,
                0,
                "INFO",
                "SCAFFOLD_ONLY",
                0,
            ),
        )
        connection.commit()

    return run_id


def main() -> None:
    args = parse_args()
    init_db(args.db_path)
    request = SnapshotRequest(
        trade_date=args.trade_date,
        snapshot_time=args.snapshot_time,
        analysis_snapshot_ts=f"{args.trade_date}T{args.snapshot_time}",
    )
    current_run_id = _build_run_id(args.trade_date, args.snapshot_time, args.round_seq)
    previous_material = load_previous_alert_material(args.db_path, current_run_id, request.analysis_snapshot_ts)
    previous_bull_state = load_previous_bull_state_history(
        args.db_path,
        current_run_id,
        request.analysis_snapshot_ts,
        trade_date=args.trade_date,
        current_round_seq=args.round_seq,
    )
    build_result = build_m0_snapshot_bundle(
        request,
        round_seq=args.round_seq,
        previous_material=previous_material,
        previous_bull_state=previous_bull_state,
    )

    if args.dry_run:
        if args.evidence_only:
            payload = {
                "run_context": build_result.bundle.run_context.to_dict(),
                "market_evidence_bundle": build_result.bundle.market_evidence_bundle.to_dict(),
                "stock_evidence_bundle": build_result.bundle.stock_evidence_bundle.to_dict(),
            }
        else:
            payload = build_result.bundle.to_dict()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        persist_m0_snapshot_bundle(args.db_path, build_result)
        print(f"persisted awin run: {build_result.bundle.run_context.run_id}")
        print(f"sqlite: {args.db_path}")
        print(build_result.bundle.market_understanding.summary_line)
        print(
            "core={core} new_long={new_long} catchup={catchup} risk={risk} alert={decision}".format(
                core=len(build_result.bundle.opportunity_discovery.core_anchor_watchlist),
                new_long=len(build_result.bundle.opportunity_discovery.new_long_watchlist),
                catchup=len(build_result.bundle.opportunity_discovery.catchup_watchlist),
                risk=len(build_result.bundle.risk_surveillance.short_watchlist),
                decision=build_result.bundle.alert_output.diff_result.decision,
            )
        )
    if args.dry_run:
        print("mode: dry-run")


if __name__ == "__main__":
    main()
