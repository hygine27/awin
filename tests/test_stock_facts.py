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
from awin.fund_flow_profile import build_fund_flow_snapshot


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

    def test_build_stock_facts_carries_style_profile_and_fund_flow_fields(self) -> None:
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
        style_profiles = [
            {
                "symbol": "300001.SZ",
                "dividend_value_score": 0.2,
                "quality_growth_score": 0.7,
                "high_beta_attack_score": 0.8,
                "dividend_style": "低股息",
                "valuation_style": "高估值",
                "growth_style": "高成长",
                "quality_style": "高质量",
                "volatility_style": "高弹性",
                "ownership_style": "民企",
                "capacity_bucket": "中小票",
                "composite_style_labels": ["科技成长", "高弹性进攻"],
            }
        ]
        fund_flow_snapshot = build_fund_flow_snapshot(
            moneyflow_ths_rows=[
                {"ts_code": "300001.SZ", "trade_date": "2026-04-15", "pct_change": 2.0, "net_amount": 20.0, "net_d5_amount": 50.0},
                {"ts_code": "300001.SZ", "trade_date": "2026-04-16", "pct_change": 3.0, "net_amount": 30.0, "net_d5_amount": 60.0},
            ],
            moneyflow_dc_rows=[
                {"ts_code": "300001.SZ", "trade_date": "2026-04-16", "net_amount_rate": 0.05, "buy_elg_amount": 12.0, "buy_lg_amount": 9.0},
            ],
            moneyflow_cnt_rows=[],
            moneyflow_ind_rows=[],
            moneyflow_mkt_rows=[],
        )

        facts = build_stock_facts(
            stock_master,
            qmt_rows,
            dcf_rows,
            [],
            [],
            [],
            style_profiles=style_profiles,
            fund_flow_snapshot=fund_flow_snapshot,
        )

        self.assertEqual(len(facts), 1)
        fact = facts[0]
        self.assertAlmostEqual(fact.quality_growth_score or 0.0, 0.7, places=6)
        self.assertAlmostEqual(fact.high_beta_attack_score or 0.0, 0.8, places=6)
        self.assertEqual(fact.dividend_style, "低股息")
        self.assertEqual(fact.valuation_style, "高估值")
        self.assertEqual(fact.growth_style, "高成长")
        self.assertEqual(fact.quality_style, "高质量")
        self.assertEqual(fact.volatility_style, "高弹性")
        self.assertEqual(fact.ownership_style, "民企")
        self.assertEqual(fact.capacity_bucket, "中小票")
        self.assertEqual(fact.composite_style_labels, ["科技成长", "高弹性进攻"])
        self.assertAlmostEqual(fact.main_net_amount_1d or 0.0, 30.0, places=6)
        self.assertAlmostEqual(fact.main_net_amount_3d_sum or 0.0, 50.0, places=6)
        self.assertAlmostEqual(fact.main_net_amount_rate_1d or 0.0, 0.05, places=6)
        self.assertEqual(fact.inflow_streak_days, 2)


if __name__ == "__main__":
    unittest.main()
