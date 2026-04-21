from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.analysis import build_stock_facts
from awin.adapters.contracts import DcfSnapshotRow, QmtBar1dRow, QmtSnapshotRow, ResearchCoverageRow, StockMasterRow, ThsConceptRow
from awin.contracts.m0 import MarketUnderstandingOutput, MetaThemeItem
from awin.risk_surveillance import compute_risk_surveillance


class RiskSurveillanceTestCase(unittest.TestCase):
    def test_compute_risk_surveillance_emits_overheat_fading_and_weakening(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300101.SZ", stock_code="300101", stock_name="过热松动股", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300102.SZ", stock_code="300102", stock_name="转弱股", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300103.SZ", stock_code="300103", stock_name="普通股", industry="半导体", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300101.SZ", stock_code="300101", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=11.1, last_close=10.0, open_price=11.4, high_price=11.6, low_price=11.0, amount=8_000_000),
            QmtSnapshotRow(symbol="300102.SZ", stock_code="300102", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=9.7, last_close=10.0, open_price=10.1, high_price=10.2, low_price=9.6, amount=6_000_000),
            QmtSnapshotRow(symbol="300103.SZ", stock_code="300103", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.0, last_close=10.0, open_price=10.0, high_price=10.1, low_price=9.9, amount=4_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300101.SZ", trade_date="2026-04-16", turnover_rate=0.09, volume_ratio=2.8, amplitude=0.16, float_mkt_cap=9_000_000_000, main_net_inflow=-200_000, super_net=-100_000, large_net=-80_000, ret_10d=0.24, ret_20d=0.36),
            DcfSnapshotRow(symbol="300102.SZ", trade_date="2026-04-16", turnover_rate=0.07, volume_ratio=1.6, amplitude=0.09, float_mkt_cap=8_000_000_000, main_net_inflow=-400_000, super_net=-150_000, large_net=-120_000, ret_10d=0.10, ret_20d=0.12),
            DcfSnapshotRow(symbol="300103.SZ", trade_date="2026-04-16", turnover_rate=0.03, volume_ratio=1.0, amplitude=0.03, float_mkt_cap=7_000_000_000, main_net_inflow=50_000, super_net=10_000, large_net=5_000, ret_10d=0.02, ret_20d=0.04),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300101.SZ", stock_code="300101", concept_name="共封装光学(CPO)", meta_theme="光通信_CPO"),
            ThsConceptRow(symbol="300102.SZ", stock_code="300102", concept_name="共封装光学(CPO)", meta_theme="光通信_CPO"),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, [], ths_rows, [ResearchCoverageRow(symbol="300101.SZ")])
        market = MarketUnderstandingOutput(top_meta_themes=[MetaThemeItem(meta_theme="光通信_CPO", rank=1)])

        output = compute_risk_surveillance(facts, market)

        self.assertTrue(output.short_watchlist)
        self.assertIn(output.short_watchlist[0].risk_tag, {"overheat_fading", "weakening", "weak"})
        self.assertTrue(any(item.risk_tag == "overheat_fading" for item in output.short_watchlist))
        self.assertTrue(any(item.risk_tag in {"weakening", "weak"} for item in output.short_watchlist))
        overheat_item = next(item for item in output.short_watchlist if item.risk_tag == "overheat_fading")
        self.assertIn("relative_to_theme", overheat_item.metadata)
        self.assertLessEqual(max(item.confidence_score for item in output.short_watchlist), 10.0)

    def test_compute_risk_surveillance_can_mark_overheat_supported(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300201.SZ", stock_code="300201", stock_name="过热承接股", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300202.SZ", stock_code="300202", stock_name="同题材普通股", industry="通信设备", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300201.SZ", stock_code="300201", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=11.6, last_close=10.0, open_price=10.7, high_price=11.7, low_price=10.6, amount=20_000_000),
            QmtSnapshotRow(symbol="300202.SZ", stock_code="300202", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.2, last_close=10.0, open_price=10.0, high_price=10.25, low_price=9.95, amount=5_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300201.SZ", trade_date="2026-04-16", turnover_rate=0.12, volume_ratio=3.5, amplitude=0.18, float_mkt_cap=10_000_000_000, main_net_inflow=300_000, super_net=160_000, large_net=110_000, ret_10d=0.32, ret_20d=0.45),
            DcfSnapshotRow(symbol="300202.SZ", trade_date="2026-04-16", turnover_rate=0.03, volume_ratio=1.0, amplitude=0.04, float_mkt_cap=8_000_000_000, main_net_inflow=20_000, super_net=5_000, large_net=4_000, ret_10d=0.03, ret_20d=0.05),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300201.SZ", stock_code="300201", concept_name="共封装光学(CPO)", meta_theme="光通信_CPO"),
            ThsConceptRow(symbol="300202.SZ", stock_code="300202", concept_name="共封装光学(CPO)", meta_theme="光通信_CPO"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300201.SZ", stock_code="300201", trade_date="2026-04-15", amount=12_000_000),
            QmtBar1dRow(symbol="300202.SZ", stock_code="300202", trade_date="2026-04-15", amount=8_000_000),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, [])
        market = MarketUnderstandingOutput(top_meta_themes=[MetaThemeItem(meta_theme="光通信_CPO", rank=1)])

        output = compute_risk_surveillance(facts, market)

        self.assertTrue(any(item.risk_tag == "overheat_supported" for item in output.short_watchlist))


if __name__ == "__main__":
    unittest.main()
