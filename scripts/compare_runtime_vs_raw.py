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
from awin.builders.m0 import build_m0_snapshot_bundle
from awin.diagnostics import build_raw_market_report


def parse_args() -> argparse.Namespace:
    now = datetime.now()
    parser = argparse.ArgumentParser(description="Compare runtime market output against raw-data judgement for the same slot.")
    parser.add_argument("--trade-date", default=now.strftime("%Y-%m-%d"))
    parser.add_argument("--snapshot-time", default=now.strftime("%H:%M:%S"))
    parser.add_argument("--round-seq", type=int, default=1)
    parser.add_argument("--json", action="store_true", help="Print full JSON payload.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    request = SnapshotRequest(
        trade_date=args.trade_date,
        snapshot_time=args.snapshot_time,
        analysis_snapshot_ts=f"{args.trade_date}T{args.snapshot_time}",
    )
    runtime_bundle = build_m0_snapshot_bundle(request, round_seq=args.round_seq)
    raw_report = build_raw_market_report(request).to_dict()
    runtime_market = runtime_bundle.bundle.market_understanding.to_dict()
    payload = {
        "request": {
            "trade_date": request.trade_date,
            "snapshot_time": request.snapshot_time,
            "analysis_snapshot_ts": request.analysis_snapshot_ts,
            "round_seq": args.round_seq,
        },
        "runtime": {
            "summary_line": runtime_market["summary_line"],
            "confirmed_style": runtime_market["confirmed_style"],
            "market_regime": runtime_market["market_regime"],
            "top_styles": [item["style_name"] for item in runtime_market["top_styles"][:3]],
            "top_meta_themes": [item["meta_theme"] for item in runtime_market["top_meta_themes"][:4]],
        },
        "raw": {
            "summary_line": raw_report["summary_line"],
            "big_style": raw_report["manual_judgement"]["big_style"],
            "direction_label": raw_report["manual_judgement"]["direction_label"],
            "top_styles": [item["style_name"] for item in raw_report["style_ranking"][:3]],
            "top_meta_themes": [item["meta_theme"] for item in raw_report["meta_theme_cli_overlay_ranking"][:4]],
        },
        "diagnosis": {
            "deprecated_app_intraday_usable": raw_report["source_validation"]["validation"]["ths_app_intraday_usable"],
            "deprecated_app_vs_cli_lag_minutes": raw_report["source_validation"]["validation"]["ths_app_vs_cli_lag_minutes"],
            "method_gap_note": (
                "如果 runtime 把 top1 元主题直接当成主导方向，而 raw judgement 仍给出多条并行主线候选，"
                "通常说明方法层还缺少主线密度、成交承载和多线并行约束。"
            ),
        },
    }

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    print(f"slot={request.analysis_snapshot_ts} round_seq={args.round_seq:02d}")
    print("runtime:", payload["runtime"]["summary_line"])
    print("raw:", payload["raw"]["summary_line"])
    print(
        "app_usable={usable} lag_min={lag}".format(
            usable=payload["diagnosis"]["deprecated_app_intraday_usable"],
            lag=payload["diagnosis"]["deprecated_app_vs_cli_lag_minutes"],
        )
    )
    print(payload["diagnosis"]["method_gap_note"])
    print("use --json for full payload")


if __name__ == "__main__":
    main()
