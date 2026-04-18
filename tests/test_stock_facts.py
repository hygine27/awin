from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.analysis import build_stock_facts
from awin.adapters.contracts import DcfSnapshotRow, QmtBar1dRow, QmtSnapshotRow, ResearchCoverageRow, StockMasterRow


class StockFactsTestCase(unittest.TestCase):
    def test_build_stock_facts_derives_multiday_returns_from_qmt_bar_history(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="样本股", industry="软件服务", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(
                symbol="300001.SZ",
                stock_code="300001",
                trade_date="2026-04-16",
                snapshot_time="10:35:00",
                last_price=11.0,
                last_close=10.5,
                open_price=10.6,
                high_price=11.1,
                low_price=10.5,
                amount=800_000_000,
            )
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", ret_3d=0.01, ret_5d=None, ret_10d=0.02, ret_20d=None),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-09", close_price=9.0, amount=100_000_000),
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-10", close_price=9.5, amount=110_000_000),
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-11", close_price=10.0, amount=120_000_000),
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-14", close_price=10.2, amount=130_000_000),
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", close_price=10.5, amount=140_000_000),
        ]

        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, [], [])
        self.assertEqual(len(facts), 1)
        fact = facts[0]
        self.assertAlmostEqual(fact.ret_3d, 0.10, places=6)
        self.assertAlmostEqual(fact.ret_5d, 11.0 / 9.0 - 1.0, places=6)

    def test_build_stock_facts_carries_company_card_quality_fields(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="样本股", industry="软件服务", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(
                symbol="300001.SZ",
                stock_code="300001",
                trade_date="2026-04-16",
                snapshot_time="10:35:00",
                last_price=10.8,
                last_close=10.5,
                open_price=10.6,
                high_price=10.9,
                low_price=10.4,
                amount=800_000_000,
            )
        ]
        dcf_rows = [DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16")]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", close_price=10.5, amount=140_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(
                symbol="300001.SZ",
                company_card_path="card.md",
                company_card_quality_score=0.59,
                company_card_tracking_recommendation="否，先作为增强覆盖层观察",
                research_coverage_score=0.4,
            ),
        ]

        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, [], research_rows)

        self.assertEqual(len(facts), 1)
        fact = facts[0]
        self.assertAlmostEqual(fact.company_card_quality_score, 0.59, places=6)
        self.assertEqual(fact.company_card_tracking_recommendation, "否，先作为增强覆盖层观察")


if __name__ == "__main__":
    unittest.main()
