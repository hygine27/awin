from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.config import get_app_config
from awin.evaluation.parity import (
    build_parity_report_markdown,
    compare_v1_v2_snapshots,
    load_v1_snapshot,
    load_v2_snapshot,
    locate_v1_snapshot,
    locate_v2_run,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare one V1 snapshot against one awin V2 snapshot.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--snapshot-time", required=True, help="Target clock like 10:35 or 10:35:00")
    parser.add_argument("--v1-root", type=Path, help="V1 state root directory. Required when --v1-state-path is not provided.")
    parser.add_argument("--v1-state-path", type=Path)
    parser.add_argument("--db-path", type=Path, default=get_app_config().sqlite_path)
    parser.add_argument("--v2-run-id")
    parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    args = parser.parse_args()

    if args.v1_state_path is None and args.v1_root is None:
        parser.error("either --v1-state-path or --v1-root must be provided")

    v1_path = args.v1_state_path or locate_v1_snapshot(args.v1_root, args.trade_date, args.snapshot_time)
    v2_run_id = args.v2_run_id or locate_v2_run(args.db_path, args.trade_date, args.snapshot_time)

    payload = compare_v1_v2_snapshots(
        load_v1_snapshot(v1_path),
        load_v2_snapshot(args.db_path, v2_run_id),
    )
    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    print(build_parity_report_markdown(payload))


if __name__ == "__main__":
    main()
