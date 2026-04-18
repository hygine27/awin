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
from awin.opportunity_discovery import PreviousBullState, compute_opportunity_discovery
from awin.opportunity_discovery.engine import _catchup_target_concepts, _concept_priority, _resolve_theme_context


class OpportunityDiscoveryTestCase(unittest.TestCase):
    def test_compute_opportunity_discovery_emits_core_long_and_catchup(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="核心一", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="核心二", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300003.SZ", stock_code="300003", stock_name="新晋一", industry="半导体", market="创业板"),
            StockMasterRow(symbol="300004.SZ", stock_code="300004", stock_name="补涨一", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300005.SZ", stock_code="300005", stock_name="补涨二", industry="软件服务", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=11.2, last_close=10.0, open_price=10.4, high_price=11.3, low_price=10.3, amount=900_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.9, last_close=10.0, open_price=10.2, high_price=11.0, low_price=10.1, amount=750_000_000),
            QmtSnapshotRow(symbol="300003.SZ", stock_code="300003", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.8, last_close=10.0, open_price=10.1, high_price=10.9, low_price=10.0, amount=680_000_000),
            QmtSnapshotRow(symbol="300004.SZ", stock_code="300004", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.35, last_close=10.0, open_price=10.0, high_price=10.4, low_price=9.9, amount=1_200_000_000),
            QmtSnapshotRow(symbol="300005.SZ", stock_code="300005", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.2, last_close=10.0, open_price=10.0, high_price=10.3, low_price=9.95, amount=490_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.08, volume_ratio=2.4, float_mkt_cap=15_000_000_000, main_net_inflow=900_000, super_net=400_000, large_net=200_000, ret_3d=0.09, ret_10d=0.16),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.06, volume_ratio=1.9, float_mkt_cap=11_000_000_000, main_net_inflow=600_000, super_net=300_000, large_net=150_000, ret_3d=0.07, ret_10d=0.12),
            DcfSnapshotRow(symbol="300003.SZ", trade_date="2026-04-16", turnover_rate=0.07, volume_ratio=2.2, float_mkt_cap=8_000_000_000, main_net_inflow=520_000, super_net=200_000, large_net=120_000, ret_3d=0.03, ret_10d=0.07),
            DcfSnapshotRow(symbol="300004.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.8, float_mkt_cap=6_000_000_000, main_net_inflow=600_000, super_net=180_000, large_net=120_000, ret_3d=0.01, ret_10d=0.03),
            DcfSnapshotRow(symbol="300005.SZ", trade_date="2026-04-16", turnover_rate=0.045, volume_ratio=1.9, float_mkt_cap=5_500_000_000, main_net_inflow=260_000, super_net=100_000, large_net=80_000, ret_3d=0.0, ret_10d=0.01),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="共封装光学(CPO)", meta_theme="光通信_CPO"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="共封装光学(CPO)", meta_theme="光通信_CPO"),
            ThsConceptRow(symbol="300003.SZ", stock_code="300003", concept_name="AIGC概念", meta_theme="AI算力"),
            ThsConceptRow(symbol="300004.SZ", stock_code="300004", concept_name="AIGC概念", meta_theme="AI算力"),
            ThsConceptRow(symbol="300005.SZ", stock_code="300005", concept_name="AIGC概念", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol=item.symbol, stock_code=item.stock_code, trade_date="2026-04-15", amount=2_000_000_000)
            for item in stock_master
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", onepage_path="onepage.md", company_card_path="card.md", research_coverage_score=0.8, research_hooks=["onepage", "龙头"]),
            ResearchCoverageRow(symbol="300004.SZ", company_card_path="card.md", recent_intel_mentions=10, research_coverage_score=0.5, research_hooks=["intel"]),
            ResearchCoverageRow(symbol="300005.SZ", onepage_path="onepage.md", research_coverage_score=0.4, research_hooks=["onepage"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[
                MetaThemeItem(meta_theme="光通信_CPO", rank=1, eq_return=0.06),
                MetaThemeItem(meta_theme="AI算力", rank=2, eq_return=0.04),
            ],
            strongest_concepts=["共封装光学(CPO)", "AIGC概念"],
            concept_overlay_score_map={"共封装光学(CPO)": 0.84, "AIGC概念": 0.76},
            concept_overlay_rank_map={"共封装光学(CPO)": 1, "AIGC概念": 2},
        )

        output = compute_opportunity_discovery(facts, market)

        self.assertTrue(output.new_long_watchlist)
        self.assertEqual(output.new_long_watchlist[0].display_bucket, "new_long")

    def test_previous_bull_state_downgrades_old_name_to_core_anchor(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="老强势", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="新强势", industry="通信设备", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.55, last_close=10.0, open_price=10.25, high_price=10.7, low_price=10.1, amount=900_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.9, last_close=10.0, open_price=10.2, high_price=11.0, low_price=10.1, amount=860_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.07, volume_ratio=2.4, float_mkt_cap=14_000_000_000, main_net_inflow=1_200_000, super_net=460_000, large_net=260_000, ret_3d=0.05, ret_10d=0.10),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.09, volume_ratio=2.6, float_mkt_cap=12_000_000_000, main_net_inflow=1_100_000, super_net=420_000, large_net=240_000, ret_3d=0.05, ret_10d=0.10),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="共封装光学(CPO)", meta_theme="光通信_CPO"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="共封装光学(CPO)", meta_theme="光通信_CPO"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol=item.symbol, stock_code=item.stock_code, trade_date="2026-04-15", amount=2_000_000_000)
            for item in stock_master
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", onepage_path="onepage-old.md", company_card_path="card-old.md", research_coverage_score=0.7, research_hooks=["anchor"]),
            ResearchCoverageRow(symbol="300002.SZ", onepage_path="onepage.md", company_card_path="card.md", research_coverage_score=0.8, research_hooks=["onepage"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="光通信_CPO", rank=1, eq_return=0.05)],
            strongest_concepts=["共封装光学(CPO)"],
            concept_overlay_score_map={"共封装光学(CPO)": 0.84},
            concept_overlay_rank_map={"共封装光学(CPO)": 1},
        )

        previous_state = {
            "300001.SZ": PreviousBullState(
                symbol="300001.SZ",
                display_bucket="new_long",
                confidence_score=9.4,
                best_meta_theme="光通信_CPO",
                best_concept="共封装光学(CPO)",
                appearances=3,
                streak=3,
                round_gap=1,
                recent_repeat=True,
                consecutive_repeat=True,
            )
        }

        output = compute_opportunity_discovery(facts, market, previous_state=previous_state)

        core_symbols = [item.symbol for item in output.core_anchor_watchlist]
        new_symbols = [item.symbol for item in output.new_long_watchlist]
        self.assertIn("300001.SZ", core_symbols)
        self.assertIn("300002.SZ", new_symbols)

    def test_high_quality_recent_repeat_can_stay_in_new_long(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="持续强势", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="次新强势", industry="软件服务", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.5, last_close=10.0, open_price=10.15, high_price=10.55, low_price=10.0, amount=2_000_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.42, last_close=10.0, open_price=10.1, high_price=10.45, low_price=10.0, amount=1_000_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.05, volume_ratio=2.0, float_mkt_cap=16_000_000_000, main_net_inflow=150_000_000, super_net=80_000_000, large_net=30_000_000, ret_3d=0.04, ret_10d=0.05),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.05, volume_ratio=1.8, float_mkt_cap=9_000_000_000, main_net_inflow=80_000_000, super_net=30_000_000, large_net=15_000_000, ret_3d=0.03, ret_10d=0.04),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI应用", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI智能体", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", amount=2_000_000_000),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-15", amount=1_200_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", onepage_path="onepage-old.md", research_coverage_score=0.6, research_hooks=["onepage"]),
            ResearchCoverageRow(symbol="300002.SZ", onepage_path="onepage.md", research_coverage_score=0.5, research_hooks=["onepage"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "AI应用"])],
            strongest_concepts=["AI智能体", "算力租赁"],
            acceleration_concepts=["AI应用"],
            concept_overlay_score_map={"AI智能体": 0.83, "AI应用": 0.78},
            concept_overlay_rank_map={"AI智能体": 1, "AI应用": 2},
            meta_theme_rank_map={"AI算力": 1},
            meta_theme_eq_return_map={"AI算力": 0.03},
        )
        previous_state = {
            "300001.SZ": PreviousBullState(
                symbol="300001.SZ",
                display_bucket="new_long",
                confidence_score=9.4,
                best_meta_theme="AI算力",
                best_concept="AI智能体",
                appearances=1,
                streak=1,
                round_gap=1,
                recent_repeat=True,
                consecutive_repeat=True,
            )
        }

        output = compute_opportunity_discovery(facts, market, previous_state=previous_state, new_long_limit=2)

        new_symbols = [item.symbol for item in output.new_long_watchlist]
        self.assertIn("300001.SZ", new_symbols)
        self.assertIn("300002.SZ", new_symbols)

    def test_new_long_keeps_trend_backbone_when_repeat_penalty_would_otherwise_push_it_out(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="主线骨干", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="补涨升级", industry="软件服务", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.45, last_close=10.0, open_price=10.1, high_price=10.5, low_price=10.0, amount=4_500_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.48, last_close=10.0, open_price=10.05, high_price=10.5, low_price=10.0, amount=10_500_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.8, float_mkt_cap=22_000_000_000, main_net_inflow=280_000_000, super_net=140_000_000, large_net=90_000_000, ret_3d=0.05, ret_5d=0.03, ret_10d=0.06),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.06, volume_ratio=2.1, float_mkt_cap=20_000_000_000, main_net_inflow=210_000_000, super_net=90_000_000, large_net=60_000_000, ret_3d=0.04, ret_5d=0.03, ret_10d=0.18),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI应用", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI应用", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", amount=3_800_000_000),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-15", amount=4_000_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", onepage_path="onepage-1.md", company_card_path="card-1.md", research_coverage_score=0.6, research_hooks=["onepage"]),
            ResearchCoverageRow(symbol="300002.SZ", onepage_path="onepage-2.md", company_card_path="card-2.md", research_coverage_score=0.6, research_hooks=["onepage"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "AI应用"])],
            strongest_concepts=["AI智能体", "算力租赁"],
            acceleration_concepts=["AI应用"],
            concept_overlay_score_map={"AI智能体": 0.83, "AI应用": 0.78},
            concept_overlay_rank_map={"AI智能体": 1, "AI应用": 2},
            meta_theme_rank_map={"AI算力": 1},
            meta_theme_eq_return_map={"AI算力": 0.03},
        )
        previous_state = {
            "300001.SZ": PreviousBullState(
                symbol="300001.SZ",
                display_bucket="new_long",
                confidence_score=9.7,
                best_meta_theme="AI算力",
                best_concept="AI智能体",
                appearances=2,
                streak=2,
                round_gap=1,
                recent_repeat=True,
                consecutive_repeat=True,
            ),
            "300002.SZ": PreviousBullState(
                symbol="300002.SZ",
                display_bucket="catchup",
                confidence_score=8.9,
                best_meta_theme="AI算力",
                best_concept="AI智能体",
                appearances=1,
                streak=1,
                round_gap=1,
                recent_repeat=True,
                consecutive_repeat=True,
            ),
        }

        output = compute_opportunity_discovery(facts, market, previous_state=previous_state, new_long_limit=1)

        self.assertEqual([item.symbol for item in output.new_long_watchlist], ["300001.SZ"])

    def test_catchup_penalizes_deep_pullback_rebound(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="深跌反弹", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="稳态补涨", industry="软件服务", market="科创板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.25, last_close=10.0, open_price=10.0, high_price=10.3, low_price=9.95, amount=3_300_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.33, last_close=10.0, open_price=10.05, high_price=10.36, low_price=10.0, amount=2_900_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.5, float_mkt_cap=28_000_000_000, main_net_inflow=97_000_000, super_net=42_000_000, large_net=23_000_000, ret_3d=0.02, ret_5d=-0.22, ret_10d=-0.21),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.6, float_mkt_cap=30_000_000_000, main_net_inflow=182_000_000, super_net=90_000_000, large_net=35_000_000, ret_3d=0.08, ret_5d=0.08, ret_10d=0.04),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI应用", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI应用", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", amount=2_600_000_000),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-15", amount=2_700_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", onepage_path="onepage-1.md", company_card_path="card-1.md", company_card_quality_score=0.7, research_coverage_score=0.5, research_hooks=["onepage"]),
            ResearchCoverageRow(symbol="300002.SZ", company_card_path="card-2.md", company_card_quality_score=0.59, company_card_tracking_recommendation="否，先作为增强覆盖层观察", research_coverage_score=0.4, research_hooks=["intel"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "算力租赁", "东数西算(算力)"])],
            strongest_concepts=["算力租赁", "AI智能体", "东数西算(算力)"],
            acceleration_concepts=["光纤概念", "液冷服务器", "共封装光学(CPO)"],
            concept_overlay_score_map={"AI智能体": 0.82, "AI应用": 0.75},
            concept_overlay_rank_map={"AI智能体": 2, "AI应用": 5},
            meta_theme_rank_map={"AI算力": 1},
            meta_theme_eq_return_map={"AI算力": 0.03},
        )
        previous_state = {
            "300001.SZ": PreviousBullState(
                symbol="300001.SZ",
                display_bucket="new_long",
                confidence_score=9.4,
                best_meta_theme="AI算力",
                best_concept="AI智能体",
                appearances=2,
                streak=2,
                round_gap=1,
                recent_repeat=True,
                consecutive_repeat=True,
            )
        }

        output = compute_opportunity_discovery(facts, market, previous_state=previous_state, catchup_limit=1)

        self.assertEqual([item.symbol for item in output.catchup_watchlist], ["300002.SZ"])

    def test_new_long_prefers_dominant_primary_concept_over_secondary_theme_sibling(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="主概念", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="次概念", industry="软件服务", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.4, last_close=10.0, open_price=10.05, high_price=10.45, low_price=10.0, amount=1_700_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.52, last_close=10.0, open_price=10.1, high_price=10.55, low_price=10.0, amount=2_100_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.05, volume_ratio=1.8, float_mkt_cap=15_000_000_000, main_net_inflow=110_000_000, super_net=50_000_000, large_net=25_000_000, ret_3d=0.04, ret_5d=0.03, ret_10d=0.07),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.05, volume_ratio=1.8, float_mkt_cap=15_000_000_000, main_net_inflow=110_000_000, super_net=50_000_000, large_net=25_000_000, ret_3d=0.04, ret_5d=0.03, ret_10d=0.07),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI应用", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="东数西算(算力)", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI应用", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", amount=1_600_000_000),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-15", amount=1_700_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", onepage_path="onepage-1.md", research_coverage_score=0.5, research_hooks=["onepage"]),
            ResearchCoverageRow(symbol="300002.SZ", onepage_path="onepage-2.md", research_coverage_score=0.5, research_hooks=["onepage"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "算力租赁", "东数西算(算力)"])],
            strongest_concepts=["算力租赁", "AI智能体", "东数西算(算力)"],
            acceleration_concepts=["AI应用"],
            concept_overlay_score_map={"AI智能体": 0.82, "东数西算(算力)": 0.82, "AI应用": 0.74},
            concept_overlay_rank_map={"AI智能体": 1, "东数西算(算力)": 2, "AI应用": 4},
            meta_theme_rank_map={"AI算力": 1},
            meta_theme_eq_return_map={"AI算力": 0.03},
        )

        output = compute_opportunity_discovery(facts, market, new_long_limit=1)

        self.assertEqual([item.symbol for item in output.new_long_watchlist], ["300001.SZ"])

    def test_new_long_does_not_overpromote_catchup_repeat_without_upgrade_shape(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="主线骨干", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="补涨升格", industry="软件服务", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.4, last_close=10.0, open_price=10.05, high_price=10.45, low_price=10.0, amount=1_600_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.47, last_close=10.0, open_price=10.1, high_price=10.5, low_price=10.0, amount=10_000_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.05, volume_ratio=1.9, float_mkt_cap=16_000_000_000, main_net_inflow=120_000_000, super_net=55_000_000, large_net=25_000_000, ret_3d=0.04, ret_5d=0.03, ret_10d=0.08),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.05, volume_ratio=2.1, float_mkt_cap=20_000_000_000, main_net_inflow=200_000_000, super_net=90_000_000, large_net=45_000_000, ret_3d=0.04, ret_5d=0.03, ret_10d=0.18),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI应用", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI应用", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", amount=1_500_000_000),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-15", amount=1_900_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", onepage_path="onepage-1.md", research_coverage_score=0.5, research_hooks=["onepage"]),
            ResearchCoverageRow(symbol="300002.SZ", onepage_path="onepage-2.md", research_coverage_score=0.5, research_hooks=["onepage"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "算力租赁", "东数西算(算力)"])],
            strongest_concepts=["算力租赁", "AI智能体", "东数西算(算力)"],
            acceleration_concepts=["AI应用"],
            concept_overlay_score_map={"AI智能体": 0.82, "AI应用": 0.75},
            concept_overlay_rank_map={"AI智能体": 1, "AI应用": 4},
            meta_theme_rank_map={"AI算力": 1},
            meta_theme_eq_return_map={"AI算力": 0.03},
        )
        previous_state = {
            "300002.SZ": PreviousBullState(
                symbol="300002.SZ",
                display_bucket="catchup",
                confidence_score=8.9,
                best_meta_theme="AI算力",
                best_concept="AI智能体",
                appearances=1,
                streak=1,
                round_gap=1,
                recent_repeat=True,
                consecutive_repeat=True,
            ),
        }

        output = compute_opportunity_discovery(facts, market, previous_state=previous_state, new_long_limit=1)

        self.assertEqual([item.symbol for item in output.new_long_watchlist], ["300001.SZ"])

    def test_catchup_does_not_duplicate_new_long_name_when_intraday_position_is_too_high(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="顺风过热", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="稳态补涨", industry="软件服务", market="科创板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.48, last_close=10.0, open_price=10.05, high_price=10.5, low_price=10.0, amount=1_700_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.33, last_close=10.0, open_price=10.05, high_price=10.36, low_price=10.0, amount=2_900_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.05, volume_ratio=1.9, float_mkt_cap=18_000_000_000, main_net_inflow=120_000_000, super_net=55_000_000, large_net=28_000_000, ret_3d=0.04, ret_5d=0.05, ret_10d=0.08),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.6, float_mkt_cap=30_000_000_000, main_net_inflow=182_000_000, super_net=90_000_000, large_net=35_000_000, ret_3d=0.08, ret_5d=0.08, ret_10d=0.04),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI应用", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI应用", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", amount=1_600_000_000),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-15", amount=2_700_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", onepage_path="onepage-1.md", research_coverage_score=0.5, research_hooks=["onepage"]),
            ResearchCoverageRow(symbol="300002.SZ", company_card_path="card-2.md", company_card_quality_score=0.59, company_card_tracking_recommendation="否，先作为增强覆盖层观察", research_coverage_score=0.4, research_hooks=["intel"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "算力租赁", "东数西算(算力)"])],
            strongest_concepts=["算力租赁", "AI智能体", "东数西算(算力)"],
            acceleration_concepts=["光纤概念", "液冷服务器", "共封装光学(CPO)"],
            concept_overlay_score_map={"AI智能体": 0.82, "AI应用": 0.75},
            concept_overlay_rank_map={"AI智能体": 2, "AI应用": 5},
            meta_theme_rank_map={"AI算力": 1},
            meta_theme_eq_return_map={"AI算力": 0.03},
        )

        output = compute_opportunity_discovery(facts, market, new_long_limit=1, catchup_limit=1)

        self.assertEqual([item.symbol for item in output.new_long_watchlist], ["300001.SZ"])
        self.assertEqual([item.symbol for item in output.catchup_watchlist], ["300002.SZ"])

    def test_catchup_penalizes_negative_main_flow(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="净流出补涨", industry="软件服务", market="主板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="净流入补涨", industry="软件服务", market="科创板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.29, last_close=10.0, open_price=10.02, high_price=10.31, low_price=10.0, amount=4_200_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.33, last_close=10.0, open_price=10.04, high_price=10.35, low_price=10.0, amount=2_800_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.6, float_mkt_cap=35_000_000_000, main_net_inflow=-3_000_000, super_net=-1_000_000, large_net=-800_000, ret_3d=0.0, ret_5d=0.01, ret_10d=0.04),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.6, float_mkt_cap=28_000_000_000, main_net_inflow=180_000_000, super_net=90_000_000, large_net=35_000_000, ret_3d=0.08, ret_5d=0.08, ret_10d=0.04),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI应用", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI应用", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", amount=4_100_000_000),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-15", amount=2_700_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", company_card_path="card-1.md", company_card_quality_score=0.7, research_coverage_score=0.4, research_hooks=["intel"]),
            ResearchCoverageRow(symbol="300002.SZ", company_card_path="card-2.md", company_card_quality_score=0.59, company_card_tracking_recommendation="否，先作为增强覆盖层观察", research_coverage_score=0.4, research_hooks=["intel"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "算力租赁", "东数西算(算力)"])],
            strongest_concepts=["算力租赁", "AI智能体", "东数西算(算力)"],
            acceleration_concepts=["光纤概念", "液冷服务器", "共封装光学(CPO)"],
            concept_overlay_score_map={"AI智能体": 0.82, "AI应用": 0.75},
            concept_overlay_rank_map={"AI智能体": 2, "AI应用": 5},
            meta_theme_rank_map={"AI算力": 1},
            meta_theme_eq_return_map={"AI算力": 0.03},
        )
        previous_state = {
            "300001.SZ": PreviousBullState(
                symbol="300001.SZ",
                display_bucket="catchup",
                confidence_score=8.8,
                best_meta_theme="AI算力",
                best_concept="AI智能体",
                appearances=1,
                streak=1,
                round_gap=1,
                recent_repeat=True,
                consecutive_repeat=True,
            )
        }

        output = compute_opportunity_discovery(facts, market, previous_state=previous_state, catchup_limit=1)

        self.assertEqual([item.symbol for item in output.catchup_watchlist], ["300002.SZ"])

    def test_catchup_penalizes_negative_ret3(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="短线未转强", industry="软件服务", market="主板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="短线转强", industry="软件服务", market="科创板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.22, last_close=10.0, open_price=10.02, high_price=10.24, low_price=10.0, amount=4_000_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.33, last_close=10.0, open_price=10.04, high_price=10.35, low_price=10.0, amount=2_800_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.6, float_mkt_cap=35_000_000_000, main_net_inflow=80_000_000, super_net=35_000_000, large_net=20_000_000, ret_3d=-0.01, ret_5d=0.04, ret_10d=0.11),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.6, float_mkt_cap=28_000_000_000, main_net_inflow=180_000_000, super_net=90_000_000, large_net=35_000_000, ret_3d=0.08, ret_5d=0.08, ret_10d=0.04),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI应用", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI应用", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", amount=4_100_000_000),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-15", amount=2_700_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", company_card_path="card-1.md", company_card_quality_score=0.7, research_coverage_score=0.4, research_hooks=["intel"]),
            ResearchCoverageRow(symbol="300002.SZ", company_card_path="card-2.md", company_card_quality_score=0.59, company_card_tracking_recommendation="否，先作为增强覆盖层观察", research_coverage_score=0.4, research_hooks=["intel"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "算力租赁", "东数西算(算力)"])],
            strongest_concepts=["算力租赁", "AI智能体", "东数西算(算力)"],
            acceleration_concepts=["光纤概念", "液冷服务器", "共封装光学(CPO)"],
            concept_overlay_score_map={"AI智能体": 0.82, "AI应用": 0.75},
            concept_overlay_rank_map={"AI智能体": 2, "AI应用": 5},
            meta_theme_rank_map={"AI算力": 1},
            meta_theme_eq_return_map={"AI算力": 0.03},
        )
        previous_state = {
            "300001.SZ": PreviousBullState(
                symbol="300001.SZ",
                display_bucket="catchup",
                confidence_score=8.8,
                best_meta_theme="AI算力",
                best_concept="AI智能体",
                appearances=1,
                streak=1,
                round_gap=1,
                recent_repeat=True,
                consecutive_repeat=True,
            )
        }

        output = compute_opportunity_discovery(facts, market, previous_state=previous_state, catchup_limit=1)

        self.assertEqual([item.symbol for item in output.catchup_watchlist], ["300002.SZ"])

    def test_catchup_can_prefer_fresh_discovery_when_intraday_turn_strength_is_clear(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="重复补涨", industry="软件服务", market="主板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="新转强补涨", industry="软件服务", market="科创板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.35, last_close=10.0, open_price=10.05, high_price=10.38, low_price=10.0, amount=3_700_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.33, last_close=10.0, open_price=10.03, high_price=10.35, low_price=10.0, amount=2_800_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.5, float_mkt_cap=30_000_000_000, main_net_inflow=120_000_000, super_net=60_000_000, large_net=25_000_000, ret_3d=0.05, ret_5d=0.06, ret_10d=0.07),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.6, float_mkt_cap=28_000_000_000, main_net_inflow=180_000_000, super_net=90_000_000, large_net=35_000_000, ret_3d=0.08, ret_5d=0.08, ret_10d=0.04),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="算力租赁", meta_theme="AI算力"),
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="东数西算(算力)", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI应用", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", amount=3_600_000_000),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-15", amount=2_700_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", company_card_path="card-1.md", company_card_quality_score=0.7, research_coverage_score=0.4, research_hooks=["intel"]),
            ResearchCoverageRow(symbol="300002.SZ", company_card_path="card-2.md", company_card_quality_score=0.59, company_card_tracking_recommendation="否，先作为增强覆盖层观察", research_coverage_score=0.4, research_hooks=["intel"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "算力租赁", "东数西算(算力)"])],
            strongest_concepts=["算力租赁", "AI智能体", "东数西算(算力)"],
            acceleration_concepts=["光纤概念", "液冷服务器", "共封装光学(CPO)"],
            concept_overlay_score_map={"AI智能体": 0.82, "AI应用": 0.75, "算力租赁": 0.84, "东数西算(算力)": 0.80},
            concept_overlay_rank_map={"算力租赁": 1, "AI智能体": 2, "东数西算(算力)": 3, "AI应用": 5},
            meta_theme_rank_map={"AI算力": 1},
            meta_theme_eq_return_map={"AI算力": 0.03},
        )
        previous_state = {
            "300001.SZ": PreviousBullState(
                symbol="300001.SZ",
                display_bucket="catchup",
                confidence_score=8.8,
                best_meta_theme="AI算力",
                best_concept="算力租赁",
                appearances=1,
                streak=1,
                round_gap=1,
                recent_repeat=True,
                consecutive_repeat=True,
            )
        }

        output = compute_opportunity_discovery(facts, market, previous_state=previous_state, catchup_limit=1)

        self.assertEqual([item.symbol for item in output.catchup_watchlist], ["300002.SZ"])

    def test_catchup_penalizes_weak_repeat_from_new_long(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="重复顺风弱延续", industry="软件服务", market="主板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="新转强补涨", industry="软件服务", market="科创板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.36, last_close=10.0, open_price=10.05, high_price=10.38, low_price=10.0, amount=2_200_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.33, last_close=10.0, open_price=10.03, high_price=10.35, low_price=10.0, amount=2_800_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.6, float_mkt_cap=25_000_000_000, main_net_inflow=60_000_000, super_net=20_000_000, large_net=10_000_000, ret_3d=0.005, ret_5d=0.07, ret_10d=0.06),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.6, float_mkt_cap=28_000_000_000, main_net_inflow=180_000_000, super_net=90_000_000, large_net=35_000_000, ret_3d=0.08, ret_5d=0.08, ret_10d=0.04),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI应用", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI应用", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-15", amount=2_100_000_000),
            QmtBar1dRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-15", amount=2_700_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", onepage_path="onepage-1.md", research_coverage_score=0.5, research_hooks=["onepage"]),
            ResearchCoverageRow(symbol="300002.SZ", company_card_path="card-2.md", company_card_quality_score=0.59, company_card_tracking_recommendation="否，先作为增强覆盖层观察", research_coverage_score=0.4, research_hooks=["intel"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "算力租赁", "东数西算(算力)"])],
            strongest_concepts=["算力租赁", "AI智能体", "东数西算(算力)"],
            acceleration_concepts=["光纤概念", "液冷服务器", "共封装光学(CPO)"],
            concept_overlay_score_map={"AI智能体": 0.82, "AI应用": 0.75},
            concept_overlay_rank_map={"AI智能体": 2, "AI应用": 5},
            meta_theme_rank_map={"AI算力": 1},
            meta_theme_eq_return_map={"AI算力": 0.03},
        )
        previous_state = {
            "300001.SZ": PreviousBullState(
                symbol="300001.SZ",
                display_bucket="new_long",
                confidence_score=9.0,
                best_meta_theme="AI算力",
                best_concept="AI智能体",
                appearances=1,
                streak=1,
                round_gap=1,
                recent_repeat=True,
                consecutive_repeat=True,
            )
        }

        output = compute_opportunity_discovery(facts, market, previous_state=previous_state, catchup_limit=1)

        self.assertEqual([item.symbol for item in output.catchup_watchlist], ["300002.SZ"])

    def test_resolve_theme_context_prefers_primary_concept_over_generic_sibling(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300364.SZ", stock_code="300364", stock_name="中文在线", industry="互联网", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(
                symbol="300364.SZ",
                stock_code="300364",
                trade_date="2026-04-16",
                snapshot_time="10:35:00",
                last_price=10.35,
                last_close=10.0,
                open_price=10.1,
                high_price=10.4,
                low_price=9.9,
                amount=1_800_000_000,
            ),
        ]
        dcf_rows = [
            DcfSnapshotRow(
                symbol="300364.SZ",
                trade_date="2026-04-16",
                turnover_rate=0.05,
                volume_ratio=2.5,
                float_mkt_cap=18_000_000_000,
                main_net_inflow=120_000_000,
                super_net=80_000_000,
                large_net=25_000_000,
                ret_3d=0.04,
                ret_10d=0.05,
            ),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300364.SZ", stock_code="300364", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300364.SZ", stock_code="300364", concept_name="AIGC概念", meta_theme="AI算力"),
            ThsConceptRow(symbol="300364.SZ", stock_code="300364", concept_name="AI应用", meta_theme="AI算力"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol="300364.SZ", stock_code="300364", trade_date="2026-04-15", amount=2_000_000_000),
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300364.SZ", onepage_path="onepage.md", research_coverage_score=0.5, research_hooks=["onepage"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "算力租赁", "AI应用"])],
            strongest_concepts=["算力租赁", "AI智能体", "液冷服务器"],
            acceleration_concepts=["AI应用", "AIGC概念"],
            concept_overlay_score_map={"AI智能体": 0.74, "AIGC概念": 0.78, "AI应用": 0.72},
            concept_overlay_rank_map={"AI智能体": 2, "AIGC概念": 4, "AI应用": 5},
            meta_theme_rank_map={"AI算力": 1},
            meta_theme_eq_return_map={"AI算力": 0.03},
        )

        context = _resolve_theme_context(facts[0], market, _concept_priority(market))

        self.assertEqual(context["best_concept"], "AI智能体")

    def test_catchup_targets_prioritize_acceleration_before_broad_hot_concepts(self) -> None:
        market = MarketUnderstandingOutput(
            top_meta_themes=[
                MetaThemeItem(meta_theme="AI算力", rank=1, strongest_concepts=["AI智能体", "算力租赁", "东数西算(算力)"]),
                MetaThemeItem(meta_theme="光通信_CPO", rank=2, strongest_concepts=["液冷服务器", "光纤概念"]),
            ],
            strongest_concepts=["东数西算(算力)", "液冷服务器", "光纤概念"],
            acceleration_concepts=["AI应用", "算力租赁", "AIGC概念"],
        )

        targets = _catchup_target_concepts(market)

        self.assertEqual(targets[:3], ["AI应用", "算力租赁", "AIGC概念"])
        self.assertIn("AI智能体", targets)

    def test_catchup_prefers_current_mainline_structure_over_unrelated_large_cap(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="主线强势", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="主线补涨", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300003.SZ", stock_code="300003", stock_name="旁支大票", industry="通信设备", market="主板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(symbol="300001.SZ", stock_code="300001", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.8, last_close=10.0, open_price=10.1, high_price=10.9, low_price=10.0, amount=1_300_000_000),
            QmtSnapshotRow(symbol="300002.SZ", stock_code="300002", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.38, last_close=10.0, open_price=10.0, high_price=10.4, low_price=9.95, amount=900_000_000),
            QmtSnapshotRow(symbol="300003.SZ", stock_code="300003", trade_date="2026-04-16", snapshot_time="10:35:00", last_price=10.28, last_close=10.0, open_price=10.0, high_price=10.3, low_price=9.92, amount=8_500_000_000),
        ]
        dcf_rows = [
            DcfSnapshotRow(symbol="300001.SZ", trade_date="2026-04-16", turnover_rate=0.06, volume_ratio=2.1, float_mkt_cap=15_000_000_000, main_net_inflow=110_000_000, super_net=45_000_000, large_net=25_000_000, ret_3d=0.04, ret_10d=0.06),
            DcfSnapshotRow(symbol="300002.SZ", trade_date="2026-04-16", turnover_rate=0.04, volume_ratio=1.9, float_mkt_cap=8_000_000_000, main_net_inflow=90_000_000, super_net=35_000_000, large_net=20_000_000, ret_3d=0.05, ret_10d=0.05),
            DcfSnapshotRow(symbol="300003.SZ", trade_date="2026-04-16", turnover_rate=0.02, volume_ratio=1.6, float_mkt_cap=90_000_000_000, main_net_inflow=260_000_000, super_net=120_000_000, large_net=70_000_000, ret_3d=0.02, ret_10d=0.03),
        ]
        ths_rows = [
            ThsConceptRow(symbol="300001.SZ", stock_code="300001", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI智能体", meta_theme="AI算力"),
            ThsConceptRow(symbol="300002.SZ", stock_code="300002", concept_name="AI应用", meta_theme="AI算力"),
            ThsConceptRow(symbol="300003.SZ", stock_code="300003", concept_name="液冷服务器", meta_theme="光通信_CPO"),
        ]
        qmt_bar_rows = [
            QmtBar1dRow(symbol=item.symbol, stock_code=item.stock_code, trade_date="2026-04-15", amount=2_000_000_000)
            for item in stock_master
        ]
        research_rows = [
            ResearchCoverageRow(symbol="300001.SZ", onepage_path="onepage-1.md", company_card_path="card-1.md", company_card_quality_score=0.45, research_coverage_score=0.75, research_hooks=["onepage"]),
            ResearchCoverageRow(symbol="300002.SZ", company_card_path="card-2.md", company_card_quality_score=0.59, company_card_tracking_recommendation="否，先作为增强覆盖层观察", research_coverage_score=0.40, research_hooks=["intel"]),
            ResearchCoverageRow(symbol="300003.SZ", company_card_path="card-3.md", company_card_quality_score=0.79, company_card_tracking_recommendation="否，先作为增强覆盖层观察", research_coverage_score=0.40, research_hooks=["intel"]),
        ]
        facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, qmt_bar_rows, ths_rows, research_rows)
        market = MarketUnderstandingOutput(
            confirmed_style="科技成长",
            top_meta_themes=[
                MetaThemeItem(meta_theme="AI算力", rank=1, eq_return=0.03, strongest_concepts=["AI智能体", "算力租赁", "东数西算(算力)"]),
                MetaThemeItem(meta_theme="光通信_CPO", rank=2, eq_return=0.02, strongest_concepts=["液冷服务器", "光纤概念"]),
            ],
            strongest_concepts=["算力租赁", "AI智能体", "东数西算(算力)", "液冷服务器"],
            acceleration_concepts=["光纤概念", "液冷服务器", "共封装光学(CPO)"],
            concept_overlay_score_map={"AI智能体": 0.82, "AI应用": 0.75, "液冷服务器": 0.84},
            concept_overlay_rank_map={"AI智能体": 2, "AI应用": 5, "液冷服务器": 1},
            meta_theme_rank_map={"AI算力": 1, "光通信_CPO": 2},
            meta_theme_eq_return_map={"AI算力": 0.03, "光通信_CPO": 0.02},
        )

        output = compute_opportunity_discovery(facts, market, new_long_limit=2, catchup_limit=2)

        new_symbols = [item.symbol for item in output.new_long_watchlist]
        catchup_symbols = [item.symbol for item in output.catchup_watchlist]
        self.assertIn("300001.SZ", new_symbols)
        self.assertIn("300002.SZ", catchup_symbols)
        self.assertNotIn("300003.SZ", catchup_symbols)


if __name__ == "__main__":
    unittest.main()
