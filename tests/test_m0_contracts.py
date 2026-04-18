from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.contracts.m0 import (
    AlertDiffResult,
    AlertMaterial,
    AlertOutput,
    CandidateItem,
    M0SnapshotBundle,
    MarketUnderstandingOutput,
    MetaThemeItem,
    OpportunityDiscoveryOutput,
    RiskSurveillanceOutput,
    RunContext,
    StyleRankingItem,
)


class M0ContractsTestCase(unittest.TestCase):
    def test_snapshot_bundle_to_dict_contains_nested_sections(self) -> None:
        bundle = M0SnapshotBundle(
            run_context=RunContext(
                run_id="2026-04-16-103500-r01",
                trade_date="2026-04-16",
                snapshot_time="10:35:00",
                analysis_snapshot_ts="2026-04-16T10:35:00",
                round_seq=1,
            ),
            market_understanding=MarketUnderstandingOutput(
                confirmed_style="科技成长",
                latest_status="stable",
                latest_dominant_style="科技成长",
                market_regime="trend_expansion",
                top_styles=[StyleRankingItem(style_name="科技成长", score=0.82)],
                top_meta_themes=[MetaThemeItem(meta_theme="光通信_CPO", rank=1)],
                summary_line="主风格：科技成长",
            ),
            opportunity_discovery=OpportunityDiscoveryOutput(
                new_long_watchlist=[
                    CandidateItem(
                        symbol="300570.SZ",
                        stock_name="太辰光",
                        display_bucket="new_long",
                        confidence_score=8.8,
                    )
                ]
            ),
            risk_surveillance=RiskSurveillanceOutput(),
            alert_output=AlertOutput(
                material=AlertMaterial(latest_status="stable"),
                diff_result=AlertDiffResult(decision="NO_UPDATE"),
                alert_body="NO_UPDATE",
            ),
        )

        payload = bundle.to_dict()

        self.assertEqual(payload["run_context"]["run_id"], "2026-04-16-103500-r01")
        self.assertEqual(
            payload["opportunity_discovery"]["new_long_watchlist"][0]["symbol"],
            "300570.SZ",
        )
        self.assertEqual(payload["alert_output"]["diff_result"]["decision"], "NO_UPDATE")


if __name__ == "__main__":
    unittest.main()
