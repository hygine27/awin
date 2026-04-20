from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.analysis import StockFact
from awin.builders.m0 import M0BuildResult, persist_m0_snapshot_bundle
from awin.contracts.m0 import (
    AlertDiffResult,
    AlertMaterial,
    AlertOutput,
    CandidateItem,
    M0SnapshotBundle,
    MarketUnderstandingOutput,
    OpportunityDiscoveryOutput,
    RiskSurveillanceOutput,
    RunContext,
)
from awin.storage.db import connect_sqlite, init_db


class PersistBundleTestCase(unittest.TestCase):
    def test_persist_preserves_higher_priority_bucket_when_symbol_is_duplicated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "awin.db"
            init_db(db_path)

            shared_long = CandidateItem(
                symbol="300017.SZ",
                stock_name="网宿科技",
                display_bucket="core_anchor",
                confidence_score=10.0,
                best_meta_theme="AI算力",
                best_concept="算力租赁",
                display_line="core",
            )
            duplicated_catchup = CandidateItem(
                symbol="300017.SZ",
                stock_name="网宿科技",
                display_bucket="catchup",
                confidence_score=8.8,
                best_meta_theme="AI算力",
                best_concept="算力租赁",
                display_line="catchup",
            )
            risk_overlay = CandidateItem(
                symbol="300017.SZ",
                stock_name="网宿科技",
                display_bucket="warning",
                confidence_score=7.1,
                best_meta_theme="AI算力",
                best_concept="算力租赁",
                risk_tag="overheat",
                display_line="risk",
                metadata={"risk_reason": "too hot"},
            )

            bundle = M0SnapshotBundle(
                run_context=RunContext(
                    run_id="2026-04-16-103500-r03",
                    trade_date="2026-04-16",
                    snapshot_time="10:35:00",
                    analysis_snapshot_ts="2026-04-16T10:35:00",
                    round_seq=3,
                ),
                market_understanding=MarketUnderstandingOutput(
                    confirmed_style="科技成长",
                    latest_status="stable",
                    latest_dominant_style="科技成长",
                    market_regime="trend_expansion",
                    summary_line="summary",
                ),
                opportunity_discovery=OpportunityDiscoveryOutput(
                    core_anchor_watchlist=[shared_long],
                    catchup_watchlist=[duplicated_catchup],
                ),
                risk_surveillance=RiskSurveillanceOutput(short_watchlist=[risk_overlay]),
                alert_output=AlertOutput(
                    material=AlertMaterial(
                        confirmed_style="科技成长",
                        latest_status="stable",
                        latest_dominant_style="科技成长",
                        market_regime="trend_expansion",
                        core_anchor_symbols=["300017.SZ"],
                        catchup_symbols=["300017.SZ"],
                        short_symbols=["300017.SZ"],
                    ),
                    diff_result=AlertDiffResult(decision="UPDATE"),
                    alert_body="body",
                ),
            )
            stock_fact = StockFact(
                symbol="300017.SZ",
                stock_code="300017",
                stock_name="网宿科技",
                exchange="SZSE",
                market="创业板",
                industry="通信设备",
                last_price=10.0,
                last_close=9.8,
                open_price=9.9,
                high_price=10.1,
                low_price=9.7,
                volume=1000.0,
                amount=5_000_000_000.0,
                bid_volume1=100.0,
                ask_volume1=80.0,
                bid_ask_imbalance=0.1,
                pct_chg_prev_close=0.02,
                open_ret=0.01,
                range_position=0.7,
                turnover_rate=0.05,
                volume_ratio=1.5,
                amplitude=0.04,
                float_mkt_cap=40_000_000_000.0,
                total_mkt_cap=45_000_000_000.0,
                avg_amount_20d=2_000_000_000.0,
                elapsed_ratio=0.4,
                money_pace_ratio=1.8,
                main_net_inflow=200_000_000.0,
                super_net=100_000_000.0,
                large_net=80_000_000.0,
                flow_ratio=0.02,
                super_flow_ratio=0.01,
                large_flow_ratio=0.008,
                ret_3d=0.05,
                ret_5d=0.06,
                ret_10d=0.08,
                ret_20d=0.10,
                dividend_value_score=0.2,
                quality_growth_score=0.7,
                high_beta_attack_score=0.8,
                low_vol_defensive_score=0.1,
                dividend_style="低股息",
                valuation_style="高估值",
                growth_style="高成长",
                quality_style="高质量",
                volatility_style="高弹性",
                ownership_style="民企",
                capacity_bucket="机构容量",
                composite_style_labels=["科技成长", "高弹性进攻"],
                main_net_amount_5d_sum=300_000_000.0,
                inflow_streak_days=3,
                outflow_streak_days=0,
                flow_acceleration_3d=50_000_000.0,
                price_flow_divergence_flag=False,
                meta_themes=["AI算力"],
                concepts=["算力租赁"],
                style_names=["科技成长"],
                best_meta_theme="AI算力",
                best_concept="算力租赁",
                research_coverage_score=0.75,
                onepage_path="onepage.md",
                company_card_path="card.md",
                recent_intel_mentions=10,
                research_hooks=["onepage"],
            )

            persist_m0_snapshot_bundle(
                db_path,
                M0BuildResult(bundle=bundle, stock_facts=[stock_fact], source_health={}),
            )

            with connect_sqlite(db_path) as connection:
                row = connection.execute(
                    """
                    SELECT display_bucket, risk_tag, is_core_anchor, is_catchup, is_short, best_meta_theme, best_concept,
                           quality_growth_score, high_beta_attack_score, dividend_style, valuation_style, growth_style,
                           quality_style, volatility_style, capacity_bucket, main_net_amount_5d_sum, inflow_streak_days
                    FROM stock_snapshot
                    WHERE run_id = ? AND symbol = ?
                    """,
                    ("2026-04-16-103500-r03", "300017.SZ"),
                ).fetchone()

            self.assertIsNotNone(row)
            self.assertEqual(row["display_bucket"], "core_anchor")
            self.assertEqual(row["risk_tag"], "overheat")
            self.assertEqual(row["is_core_anchor"], 1)
            self.assertEqual(row["is_catchup"], 1)
            self.assertEqual(row["is_short"], 1)
            self.assertEqual(row["best_meta_theme"], "AI算力")
            self.assertEqual(row["best_concept"], "算力租赁")
            self.assertEqual(row["quality_growth_score"], 0.7)
            self.assertEqual(row["high_beta_attack_score"], 0.8)
            self.assertEqual(row["dividend_style"], "低股息")
            self.assertEqual(row["valuation_style"], "高估值")
            self.assertEqual(row["growth_style"], "高成长")
            self.assertEqual(row["quality_style"], "高质量")
            self.assertEqual(row["volatility_style"], "高弹性")
            self.assertEqual(row["capacity_bucket"], "机构容量")
            self.assertEqual(row["main_net_amount_5d_sum"], 300_000_000.0)
            self.assertEqual(row["inflow_streak_days"], 3)


if __name__ == "__main__":
    unittest.main()
