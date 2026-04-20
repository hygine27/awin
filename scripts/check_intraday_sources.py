from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.adapters import SnapshotRequest
from awin.diagnostics import collect_intraday_source_state


def parse_args() -> argparse.Namespace:
    now = datetime.now()
    parser = argparse.ArgumentParser(description="Check intraday source freshness and source behavior for a given slot.")
    parser.add_argument("--trade-date", default=now.strftime("%Y-%m-%d"))
    parser.add_argument("--snapshot-time", default=now.strftime("%H:%M:%S"))
    parser.add_argument("--json", action="store_true", help="Print full JSON payload.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    request = SnapshotRequest(
        trade_date=args.trade_date,
        snapshot_time=args.snapshot_time,
        analysis_snapshot_ts=f"{args.trade_date}T{args.snapshot_time}",
    )
    payload = collect_intraday_source_state(request).to_dict()

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    validation = payload["validation"]
    cli_daily = payload["ths_cli_hot_concept"]["daily"]
    app_daily = payload["ths_app_hot_concept"]["daily"]
    print(f"slot={request.analysis_snapshot_ts}")
    print(
        "QMT rows={qmt_rows} | DCF rows={dcf_rows} | CLI batches={cli_batches} | APP batches={app_batches}".format(
            qmt_rows=payload["qmt"]["row_count"],
            dcf_rows=payload["dcf"]["row_count"],
            cli_batches=cli_daily["batch_count"],
            app_batches=app_daily["batch_count"],
        )
    )
    print(
        "CLI latest={cli_latest} | APP latest={app_latest} | app_vs_cli_lag_min={lag}".format(
            cli_latest=payload["ths_cli_hot_concept"]["latest_batch_ts"] or "none",
            app_latest=payload["ths_app_hot_concept"]["latest_batch_ts"] or "none",
            lag=validation["ths_app_vs_cli_lag_minutes"],
        )
    )
    print(
        "APP usable for production scoring: {usable} | note: {note}".format(
            usable=validation["ths_app_intraday_usable"],
            note=validation["ths_app_deprecation_reason"],
        )
    )
    print("market_tape:", payload["market_overview"]["market_tape"])
    print("use --json for full payload")


if __name__ == "__main__":
    main()
