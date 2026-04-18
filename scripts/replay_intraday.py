from __future__ import annotations

import argparse
import sys
from pathlib import Path


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
from awin.storage.db import init_db


def _parse_times(raw: str) -> list[str]:
    values = [item.strip() for item in raw.split(",")]
    times = [item for item in values if item]
    if not times:
        raise ValueError("at least one snapshot time is required")
    normalized: list[str] = []
    for item in times:
        if len(item) == 5:
            normalized.append(f"{item}:00")
        else:
            normalized.append(item)
    return normalized


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay multiple awin intraday snapshots in one Python process.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--times", required=True, help="Comma-separated clock list, e.g. 09:35,10:00,10:25,10:30,10:35")
    parser.add_argument("--db-path", type=Path, default=get_app_config().sqlite_path)
    parser.add_argument("--round-seq-start", type=int, default=1)
    args = parser.parse_args()

    times = _parse_times(args.times)
    init_db(args.db_path)

    print(f"replay db: {args.db_path}")
    for offset, snapshot_time in enumerate(times):
        round_seq = args.round_seq_start + offset
        analysis_snapshot_ts = f"{args.trade_date}T{snapshot_time}"
        current_run_id = f"{args.trade_date}-{snapshot_time.replace(':', '')}-r{round_seq:02d}"
        request = SnapshotRequest(
            trade_date=args.trade_date,
            snapshot_time=snapshot_time,
            analysis_snapshot_ts=analysis_snapshot_ts,
        )
        previous_material = load_previous_alert_material(args.db_path, current_run_id, analysis_snapshot_ts)
        previous_bull_state = load_previous_bull_state_history(
            args.db_path,
            current_run_id,
            analysis_snapshot_ts,
            trade_date=args.trade_date,
            current_round_seq=round_seq,
        )
        build_result = build_m0_snapshot_bundle(
            request,
            round_seq=round_seq,
            previous_material=previous_material,
            previous_bull_state=previous_bull_state,
        )
        persist_m0_snapshot_bundle(args.db_path, build_result)

        opportunity = build_result.bundle.opportunity_discovery
        risk = build_result.bundle.risk_surveillance
        print(
            "{time} r{round_seq:02d} | {style} | core={core} new={new_long} catchup={catchup} risk={risk_count}".format(
                time=snapshot_time,
                round_seq=round_seq,
                style=build_result.bundle.market_understanding.summary_line,
                core=len(opportunity.core_anchor_watchlist),
                new_long=len(opportunity.new_long_watchlist),
                catchup=len(opportunity.catchup_watchlist),
                risk_count=len(risk.short_watchlist),
            )
        )


if __name__ == "__main__":
    main()
