from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.fund_flow_profile import build_fund_flow_snapshot


class FundFlowProfileTestCase(unittest.TestCase):
    def test_build_fund_flow_snapshot_derives_stock_concept_industry_and_market_profiles(self) -> None:
        snapshot = build_fund_flow_snapshot(
            moneyflow_ths_rows=[
                {"ts_code": "300001.SZ", "trade_date": "2026-04-16", "pct_change": 4.0, "net_amount": -5.0, "net_d5_amount": 55.0},
                {"ts_code": "300001.SZ", "trade_date": "2026-04-14", "pct_change": 2.0, "net_amount": 20.0, "net_d5_amount": 30.0},
                {"ts_code": "300001.SZ", "trade_date": "2026-04-11", "pct_change": 1.0, "net_amount": 10.0, "net_d5_amount": 18.0},
                {"ts_code": "300001.SZ", "trade_date": "2026-04-15", "pct_change": 3.0, "net_amount": 30.0, "net_d5_amount": 45.0},
            ],
            moneyflow_dc_rows=[
                {"ts_code": "300001.SZ", "trade_date": "2026-04-16", "net_amount_rate": 0.03, "buy_elg_amount": 8.0, "buy_lg_amount": 6.0}
            ],
            moneyflow_cnt_rows=[
                {"ts_code": "885001.TI", "name": "AI算力", "trade_date": "2026-04-16", "pct_change": 2.5, "net_amount": 140.0},
                {"ts_code": "885001.TI", "name": "AI算力", "trade_date": "2026-04-15", "pct_change": 2.0, "net_amount": 100.0},
            ],
            moneyflow_ind_rows=[
                {"ts_code": "881001.TI", "industry": "软件服务", "trade_date": "2026-04-16", "pct_change": 1.8, "net_amount": 90.0},
                {"ts_code": "881001.TI", "industry": "软件服务", "trade_date": "2026-04-15", "pct_change": 1.5, "net_amount": 60.0},
            ],
            moneyflow_mkt_rows=[
                {"trade_date": "2026-04-16", "net_amount": 50.0, "net_amount_rate": 0.02, "buy_elg_amount": 25.0, "buy_lg_amount": 18.0},
                {"trade_date": "2026-04-15", "net_amount": -20.0, "net_amount_rate": -0.01, "buy_elg_amount": -10.0, "buy_lg_amount": -8.0},
            ],
        )

        self.assertEqual(len(snapshot.stock_profiles), 1)
        stock = snapshot.stock_profiles[0]
        self.assertEqual(stock.symbol, "300001.SZ")
        self.assertEqual(stock.main_net_amount_1d, -5.0)
        self.assertEqual(stock.main_net_amount_3d_sum, 45.0)
        self.assertEqual(stock.main_net_amount_5d_sum, 55.0)
        self.assertEqual(stock.main_net_amount_rate_1d, 0.03)
        self.assertEqual(stock.super_large_net_1d, 8.0)
        self.assertEqual(stock.large_order_net_1d, 6.0)
        self.assertTrue(stock.price_flow_divergence_flag)

        self.assertEqual(len(snapshot.concept_profiles), 1)
        self.assertEqual(snapshot.concept_profiles[0].concept_name, "AI算力")
        self.assertEqual(snapshot.concept_profiles[0].net_amount_1d, 140.0)

        self.assertEqual(len(snapshot.industry_profiles), 1)
        self.assertEqual(snapshot.industry_profiles[0].industry_name, "软件服务")
        self.assertEqual(snapshot.industry_profiles[0].net_amount_5d_sum, 150.0)

        self.assertIsNotNone(snapshot.market_profile)
        assert snapshot.market_profile is not None
        self.assertEqual(snapshot.market_profile.trade_date, "2026-04-16")
        self.assertEqual(snapshot.market_profile.net_amount_1d, 50.0)
        self.assertEqual(snapshot.market_profile.inflow_streak_days, 1)


if __name__ == "__main__":
    unittest.main()
