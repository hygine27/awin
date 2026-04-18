from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.evaluation.parity import build_parity_report_markdown, compare_v1_v2_snapshots


class EvaluationParityTestCase(unittest.TestCase):
    def test_compare_v1_v2_snapshots_reports_bucket_overlap(self) -> None:
        payload = compare_v1_v2_snapshots(
            {
                "artifact_path": "/tmp/v1.json",
                "snapshot_time": "2026-04-16T10:40:00+08:00",
                "confirmed_style": "科技成长",
                "latest_status": "stable",
                "latest_dominant_style": "科技成长",
                "market_regime": "trend_expansion",
                "top_attack_lines": ["AI算力", "光通信_CPO", "机器人"],
                "core_anchor_watchlist": [],
                "new_long_watchlist": ["300442.SZ", "601138.SH"],
                "catchup_watchlist": ["300502.SZ"],
                "short_watchlist": ["301396.SZ"],
            },
            {
                "run_id": "r03",
                "snapshot_time": "10:35:00",
                "round_seq": 3,
                "confirmed_style": "科技成长",
                "latest_status": "stable",
                "latest_dominant_style": "科技成长",
                "market_regime": "mixed_rotation",
                "top_attack_lines": ["AI算力", "光通信_CPO", "机器人"],
                "core_anchor_watchlist": ["300442.SZ"],
                "new_long_watchlist": ["601138.SH", "000938.SZ"],
                "catchup_watchlist": ["300502.SZ"],
                "short_watchlist": ["301396.SZ", "002580.SZ"],
            },
        )

        self.assertTrue(payload["style_aligned"])
        self.assertEqual(payload["top_line_overlap"], ["AI算力", "光通信_CPO", "机器人"])
        new_long = next(item for item in payload["bucket_comparisons"] if item["name"] == "new_long")
        self.assertEqual(new_long["intersection"], ["601138.SH"])
        self.assertGreater(payload["average_overlap_ratio"], 0.0)

        markdown = build_parity_report_markdown(payload)
        self.assertIn("# V1 / V2 Snapshot Compare", markdown)
        self.assertIn("个股层平均重合度", markdown)


if __name__ == "__main__":
    unittest.main()
