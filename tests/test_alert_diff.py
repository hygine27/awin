from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.alerting.diff import build_alert_material, build_alert_output, diff_alert_material
from awin.contracts.m0 import (
    CandidateItem,
    MarketUnderstandingOutput,
    MetaThemeItem,
    OpportunityDiscoveryOutput,
    RiskSurveillanceOutput,
    RunContext,
)


class AlertDiffTestCase(unittest.TestCase):
    def _build_market(self, *, status: str = "stable") -> MarketUnderstandingOutput:
        return MarketUnderstandingOutput(
            confirmed_style="科技成长",
            latest_status=status,
            latest_dominant_style="科技成长",
            market_regime="trend_expansion",
            top_meta_themes=[
                MetaThemeItem(meta_theme="光通信_CPO", rank=1),
                MetaThemeItem(meta_theme="半导体", rank=2),
                MetaThemeItem(meta_theme="电网储能", rank=3),
            ],
            strongest_concepts=["共封装光学(CPO)", "F5G概念"],
            acceleration_concepts=["电网设备"],
            summary_line="主风格：科技成长｜状态：稳定",
            evidence_lines=["科技成长篮子领先。"],
        )

    def _build_opportunity(self) -> OpportunityDiscoveryOutput:
        return OpportunityDiscoveryOutput(
            core_anchor_watchlist=[
                CandidateItem(
                    symbol="300308.SZ",
                    stock_name="中际旭创",
                    display_bucket="core_anchor",
                    confidence_score=9.5,
                    themes=["光通信_CPO"],
                    reason="主线核心。",
                )
            ],
            new_long_watchlist=[
                CandidateItem(
                    symbol="300570.SZ",
                    stock_name="太辰光",
                    display_bucket="new_long",
                    confidence_score=8.8,
                    themes=["光通信_CPO", "F5G概念"],
                    reason="日内承接强。",
                )
            ],
            catchup_watchlist=[
                CandidateItem(
                    symbol="688313.SH",
                    stock_name="仕佳光子",
                    display_bucket="catchup",
                    confidence_score=7.6,
                    themes=["光通信_CPO"],
                    reason="潜在补涨。",
                )
            ],
        )

    def _build_risk(self) -> RiskSurveillanceOutput:
        return RiskSurveillanceOutput(
            short_watchlist=[
                CandidateItem(
                    symbol="688048.SH",
                    stock_name="长光华芯",
                    display_bucket="warning",
                    confidence_score=8.3,
                    themes=["半导体"],
                    reason="明显过热。",
                    risk_tag="overheat",
                )
            ]
        )

    def test_build_alert_material_extracts_core_fields(self) -> None:
        material = build_alert_material(
            self._build_market(),
            self._build_opportunity(),
            self._build_risk(),
        )

        self.assertEqual(material.confirmed_style, "科技成长")
        self.assertEqual(material.top_meta_themes, ["光通信_CPO", "半导体", "电网储能"])
        self.assertEqual(material.new_long_symbols, ["300570.SZ"])
        self.assertEqual(material.short_symbols, ["688048.SH"])

    def test_diff_alert_material_returns_no_update_for_same_material(self) -> None:
        material = build_alert_material(
            self._build_market(),
            self._build_opportunity(),
            self._build_risk(),
        )

        diff_result = diff_alert_material(material, material)

        self.assertEqual(diff_result.decision, "NO_UPDATE")
        self.assertEqual(diff_result.changes, [])

    def test_build_alert_output_marks_update_when_status_changes(self) -> None:
        run_context = RunContext(
            run_id="2026-04-16-103500-r01",
            trade_date="2026-04-16",
            snapshot_time="10:35:00",
            analysis_snapshot_ts="2026-04-16T10:35:00",
            round_seq=1,
        )
        previous_material = build_alert_material(
            self._build_market(status="stable"),
            self._build_opportunity(),
            self._build_risk(),
        )

        alert_output = build_alert_output(
            run_context,
            self._build_market(status="observation"),
            self._build_opportunity(),
            self._build_risk(),
            previous_material=previous_material,
        )

        self.assertEqual(alert_output.diff_result.decision, "UPDATE")
        self.assertIn("latest_status", [change.field_name for change in alert_output.diff_result.changes])
        self.assertIn("盘中风格监控有更新", alert_output.alert_body)


if __name__ == "__main__":
    unittest.main()
