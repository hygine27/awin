from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.adapters import QmtBar1dRow
from awin.evaluation import compute_post_trade_outcomes


class EvaluationOutcomesTestCase(unittest.TestCase):
    def test_compute_post_trade_outcomes_aggregates_future_returns(self) -> None:
        active_symbols = [
            {
                "symbol": "300001.SZ",
                "stock_name": "A",
                "mention_count": 2,
                "latest_display_bucket": "core_anchor",
                "latest_risk_tag": None,
            },
            {
                "symbol": "300002.SZ",
                "stock_name": "B",
                "mention_count": 1,
                "latest_display_bucket": None,
                "latest_risk_tag": "warning",
            },
        ]
        bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", open_price=10.0, high_price=10.5, low_price=9.8, close_price=10.0, volume=1.0, amount=1.0, pre_close=9.8),
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-17", open_price=10.2, high_price=10.8, low_price=10.1, close_price=10.5, volume=1.0, amount=1.0, pre_close=10.0),
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-20", open_price=10.6, high_price=10.9, low_price=10.3, close_price=10.7, volume=1.0, amount=1.0, pre_close=10.5),
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-21", open_price=10.8, high_price=11.1, low_price=10.6, close_price=10.9, volume=1.0, amount=1.0, pre_close=10.7),
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-22", open_price=11.0, high_price=11.2, low_price=10.8, close_price=11.1, volume=1.0, amount=1.0, pre_close=10.9),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", open_price=20.0, high_price=20.2, low_price=19.8, close_price=20.0, volume=1.0, amount=1.0, pre_close=20.2),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-17", open_price=19.8, high_price=20.0, low_price=19.5, close_price=19.6, volume=1.0, amount=1.0, pre_close=20.0),
        ]

        payload = compute_post_trade_outcomes("2026-04-16", active_symbols, bar_rows)

        self.assertEqual(payload["symbols_with_trigger_bar"], 2)
        self.assertEqual(payload["symbols_with_next_open"], 2)
        self.assertEqual(payload["symbols_with_close_3d"], 1)
        symbol_a = next(item for item in payload["active_symbols"] if item["symbol"] == "300001.SZ")
        self.assertAlmostEqual(symbol_a["next_open_ret"], 0.02, places=6)
        self.assertAlmostEqual(symbol_a["close_ret_3d"], 0.09, places=6)

        core_summary = next(item for item in payload["cohort_summaries"] if item["cohort"] == "core_anchor")
        self.assertEqual(core_summary["sample_count"], 1)
        self.assertEqual(core_summary["trigger_count"], 1)
        self.assertAlmostEqual(core_summary["avg_close_ret_1d"], 0.05, places=6)

        risk_summary = next(item for item in payload["cohort_summaries"] if item["cohort"] == "risk")
        self.assertEqual(risk_summary["sample_count"], 1)
        self.assertAlmostEqual(risk_summary["avg_next_open_ret"], -0.01, places=6)


if __name__ == "__main__":
    unittest.main()
