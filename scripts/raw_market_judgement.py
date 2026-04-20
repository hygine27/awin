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
from awin.diagnostics import build_raw_market_report


def parse_args() -> argparse.Namespace:
    now = datetime.now()
    parser = argparse.ArgumentParser(description="Build a raw-data market judgement without using runtime market_understanding output.")
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
    report = build_raw_market_report(request).to_dict()

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return

    print(f"slot={request.analysis_snapshot_ts}")
    print(report["summary_line"])
    print("风格前3：", " / ".join(item["style_name"] for item in report["style_ranking"][:3]))
    print("主线前4：", " / ".join(item["meta_theme"] for item in report["meta_theme_cli_overlay_ranking"][:4]))
    print("原始判断：", report["manual_judgement"]["core_observation"])
    print("use --json for full payload")


if __name__ == "__main__":
    main()
