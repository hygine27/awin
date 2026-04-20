from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.builders.m0 import M0BuildResult
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
)
from awin.reporting import render_intraday_summary


class ReportingSummaryTestCase(unittest.TestCase):
    def test_render_intraday_summary_outputs_v1_like_sections(self) -> None:
        bundle = M0SnapshotBundle(
            run_context=RunContext(
                run_id="2026-04-20-145000-r01",
                trade_date="2026-04-20",
                snapshot_time="14:50:00",
                analysis_snapshot_ts="2026-04-20T14:50:00",
                round_seq=1,
            ),
            market_understanding=MarketUnderstandingOutput(
                confirmed_style="科技成长",
                latest_status="stable",
                latest_dominant_style="军工大装备",
                market_regime="trend_expansion",
                top_meta_themes=[
                    MetaThemeItem(meta_theme="军工大装备", rank=1),
                    MetaThemeItem(meta_theme="商业航天低空", rank=2),
                    MetaThemeItem(meta_theme="电网设备", rank=3),
                ],
                summary_line="主风格：科技成长｜状态：稳定｜活跃方向：军工大装备 / 商业航天低空 / 电网设备｜最强主题：军工大装备 / 商业航天低空 / 电网设备",
                evidence_lines=[
                    "- 风格强弱：科技成长 当前综合分领先。",
                    "- 活跃方向：当前更接近多线并行。",
                ],
            ),
            opportunity_discovery=OpportunityDiscoveryOutput(
                core_anchor_watchlist=[
                    CandidateItem(
                        symbol="300502.SZ",
                        stock_name="新易盛",
                        display_bucket="core_anchor",
                        confidence_score=9.4,
                        themes=["军工大装备", "商业航天低空"],
                        reason="主线容量票承接稳定。",
                        metadata={
                            "display_score": 9.4,
                            "rank_score": 9.4,
                            "best_theme_rank": 1,
                            "alignment": 2.2,
                            "dual_support": 1.6,
                            "temperature": 2.0,
                            "tape": 2.1,
                            "profile": 0.8,
                            "research": 1.2,
                            "new_name_bonus": 0.6,
                            "mainline_primary_concept_bonus": 0.15,
                            "money_pace_ratio": 1.56,
                            "range_position": 0.91,
                            "volume_ratio": 2.35,
                            "amount": 1280000000,
                            "main_net_inflow": 95000000,
                        },
                    )
                ],
                new_long_watchlist=[
                    CandidateItem(
                        symbol="300170.SZ",
                        stock_name="汉得信息",
                        display_bucket="new_long",
                        confidence_score=9.0,
                        themes=["商业航天低空"],
                        reason="日内强化并获得后续确认。",
                        metadata={
                            "display_score": 9.0,
                            "rank_score": 9.0,
                            "alignment": 2.0,
                            "dual_support": 1.4,
                            "temperature": 1.8,
                            "research": 1.0,
                            "tape": 1.7,
                            "profile": 0.6,
                            "amount": 860000000,
                        },
                    )
                ],
                catchup_watchlist=[
                    CandidateItem(
                        symbol="002542.SZ",
                        stock_name="中化岩土",
                        display_bucket="catchup",
                        confidence_score=8.4,
                        themes=["电网设备"],
                        reason="同主线内相对滞涨，存在补涨观察价值。",
                        metadata={
                            "best_theme_rank": 2,
                            "catchup_score_raw": 7.36,
                            "ret_3d": 0.021,
                            "ret_10d": 0.054,
                            "money_pace_ratio": 1.28,
                            "range_position": 0.74,
                            "volume_ratio": 1.66,
                            "main_net_inflow": 23000000,
                            "amount": 2500000000,
                            "company_card_quality_score": 0.7,
                            "focus_support": 2,
                            "best_concept_focus": True,
                            "fresh_catchup_discovery_bonus": 0.0,
                            "catchup_best_concept_penalty": 0.0,
                            "catchup_pace_penalty": 0.0,
                            "deep_pullback_penalty": 0.0,
                            "duplicate_new_long_penalty": 0.0,
                            "weak_repeat_new_long_penalty": 0.0,
                            "negative_main_flow_penalty": 0.0,
                            "negative_ret3_penalty": 0.0,
                        },
                    )
                ],
            ),
            risk_surveillance=RiskSurveillanceOutput(
                short_watchlist=[
                    CandidateItem(
                        symbol="002980.SZ",
                        stock_name="华盛昌",
                        display_bucket="warning",
                        confidence_score=9.2,
                        themes=["军工大装备"],
                        reason="短期涨幅和拥挤度偏高，资金边际转弱。",
                        risk_tag="overheat",
                        metadata={
                            "relative_to_theme": 0.084,
                            "ret_10d": 0.215,
                            "ret_20d": 0.322,
                            "amplitude": 0.132,
                            "main_net_amount_1d": -3560000.0,
                            "main_net_amount_3d_sum": -8520000.0,
                            "main_net_amount_5d_sum": -128145.38,
                            "outflow_streak_days": 3,
                            "flow_acceleration_3d": -40271.85,
                            "price_flow_divergence_flag": True,
                        },
                    )
                ]
            ),
            alert_output=AlertOutput(
                material=AlertMaterial(),
                diff_result=AlertDiffResult(decision="UPDATE"),
                alert_body="body",
            ),
        )
        build_result = M0BuildResult(
            bundle=bundle,
            stock_facts=[],
            source_health={
                "qmt_ashare_snapshot_5m": {"source_status": "ready", "coverage_ratio": 0.995, "freshness_seconds": 300},
                "dcf_hq_zj_snapshot": {"source_status": "ready", "coverage_ratio": 0.992, "freshness_seconds": 300},
                "ths_cli_hot_concept": {"source_status": "ready", "freshness_seconds": 300},
            },
        )

        text = render_intraday_summary(build_result)

        self.assertIn("2026-04-20 14:50 科技成长维持领先", text)
        self.assertIn("## 结论与证据", text)
        self.assertIn("## 顺风看多观察", text)
        self.assertIn("## 潜在补涨观察", text)
        self.assertIn("## 偏空 / 过热预警", text)
        self.assertIn("## 评分说明", text)
        self.assertIn("DCF 行情/资金增强：可用", text)
        self.assertIn("本节首选：新易盛", text)
        self.assertIn("首选依据：所属主线当前排第 1", text)
        self.assertIn("模块强度", text)
        self.assertIn("主线一致性 2.20/2.50", text)
        self.assertIn("新晋加分 +0.60", text)
        self.assertIn("补涨原始分 7.36", text)
        self.assertIn("补涨拆解：盘口转强", text)
        self.assertIn("风险依据：相对主题偏离 8.4%", text)
        self.assertIn("顺风看多：汉得信息（300170.SZ）｜商业航天低空｜模块强度", text)
        self.assertIn("首选依据：补涨原始分 7.36", text)
        self.assertIn("近1日主力净额 -356.0 万", text)
        self.assertIn("近5日累计 -12.8 万", text)
        self.assertIn("已连续流出 3 天", text)
        self.assertIn("呈现价强资弱", text)
        self.assertIn("华盛昌", text)


if __name__ == "__main__":
    unittest.main()
