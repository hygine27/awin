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
    MarketEvidenceBundle,
    MarketUnderstandingOutput,
    MetaThemeItem,
    OpportunityDiscoveryOutput,
    RiskSurveillanceOutput,
    RunContext,
    ThemeEvidenceItem,
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
                summary_line="风格底色：科技成长｜风格状态：稳定｜交易主线：军工大装备 / 商业航天低空 / 电网设备｜最强主题：军工大装备 / 商业航天低空 / 电网设备",
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
                            "catchup_stage": "early",
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
                        symbol="002361.SZ",
                        stock_name="神剑股份",
                        display_bucket="warning",
                        confidence_score=9.3,
                        themes=["军工大装备", "商业航天低空"],
                        reason="短期涨幅偏高，但盘中承接仍强。",
                        risk_tag="overheat_supported",
                        metadata={
                            "relative_to_theme": 0.085,
                            "ret_10d": 0.311,
                            "ret_20d": 0.837,
                            "amplitude": 0.100,
                            "main_net_inflow": 525000000.0,
                            "super_net": 581000000.0,
                            "large_net": -56310000.0,
                            "money_pace_ratio": 1.63,
                            "range_position": 1.0,
                            "main_net_amount_5d_sum": -33717.57,
                            "outflow_streak_days": 1,
                        },
                    ),
                    CandidateItem(
                        symbol="002980.SZ",
                        stock_name="华盛昌",
                        display_bucket="warning",
                        confidence_score=9.2,
                        themes=["军工大装备"],
                        reason="短期涨幅和拥挤度偏高，资金边际转弱。",
                        risk_tag="overheat_fading",
                        metadata={
                            "relative_to_theme": 0.084,
                            "ret_10d": 0.215,
                            "ret_20d": 0.322,
                            "amplitude": 0.132,
                            "main_net_amount_1d": -45671.11,
                            "main_net_amount_3d_sum": -85200.0,
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
            market_evidence_bundle=MarketEvidenceBundle(
                theme_evidence=[
                    ThemeEvidenceItem(
                        meta_theme="军工大装备",
                        rank=1,
                        current_main_net_inflow_sum=1520000000.0,
                        current_main_flow_rate=0.064,
                        current_positive_main_flow_ratio=0.64,
                        comparison_window_label="近15分钟",
                        comparison_main_net_inflow_delta=-420000000.0,
                    ),
                    ThemeEvidenceItem(
                        meta_theme="商业航天低空",
                        rank=2,
                        current_main_net_inflow_sum=860000000.0,
                        current_main_flow_rate=0.057,
                        current_positive_main_flow_ratio=0.57,
                        comparison_window_label="近15分钟",
                        comparison_main_net_inflow_delta=180000000.0,
                    ),
                ]
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

        self.assertIn("2026-04-20 14:50 风格底色：科技成长维持领先，交易主线看军工大装备 / 商业航天低空 / 电网设备", text)
        self.assertIn("## 结论与证据", text)
        self.assertIn("### 业务判断", text)
        self.assertIn("### 数据状态", text)
        self.assertIn("## 顺风看多观察", text)
        self.assertIn("## 潜在补涨观察", text)
        self.assertIn("## 过热但承接仍强", text)
        self.assertIn("## 过热且松动", text)
        self.assertIn("## 转弱预警", text)
        self.assertNotIn("## 评分说明", text)
        self.assertNotIn("## 注解", text)
        self.assertIn("| DCF 行情/资金增强 | 可用｜覆盖率 99.2%｜新鲜度 5 分钟。 |", text)
        self.assertIn(
            "| 主线盘中资金代理（14:50） | 军工大装备，开盘以来净流入 15.20 亿，主力净流入强度 6.40%，近15分钟净流出 4.20 亿，主力流入个股占比 64.0%；商业航天低空，开盘以来净流入 8.60 亿，主力净流入强度 5.70%，近15分钟净流入 1.80 亿，主力流入个股占比 57.0%。 |",
            text,
        )
        self.assertIn("| 角色 | 股票 | 评分 | 涨跌 | 节奏 | 位置 | 主力 | 主题 |", text)
        self.assertIn("| 本节首选 | 新易盛 | 模块强度8.8/10 |", text)
        self.assertIn("- 新易盛｜军工大装备 / 商业航天低空", text)
        self.assertIn("- 首选依据：所属主线当前排第 1；模块强度 8.8/10", text)
        self.assertIn("模块强度", text)
        self.assertIn("| 项目 | 值 | 评价 |", text)
        self.assertIn("| 模块强度 | 8.8/10 | 中上 |", text)
        self.assertIn("| 主线一致性 | 2.20 | 中上 |", text)
        self.assertIn("新晋加分 +0.60", text)
        self.assertIn("| 角色 | 股票 | 评分 | 3日 / 10日 | 节奏 | 位置 | 主力 | 主题 |", text)
        self.assertIn("| 本节首选 | 中化岩土 | 补涨原始分7.36 |", text)
        self.assertIn("| 盘口转强 | 0.36 | 加分 |", text)
        self.assertIn("- 风险依据：相对主题偏离 8.4%；近10日 21.5% / 20日 32.2%；振幅 13.2%；近1日主力净额 -4.57 亿", text)
        self.assertIn("| 顺风看多 | 汉得信息 | 模块强度7.6/10 |", text)
        self.assertIn("- 首选依据：补涨原始分 7.36；所属主线当前排第 2；近3日 2.1% / 10日 5.4%", text)
        self.assertIn("当前仍属补涨早期", text)
        self.assertIn("盘中主力净流入 +5.25 亿", text)
        self.assertIn("超大单+大单合计 +5.25 亿", text)
        self.assertIn("但近5日历史资金累计 -3.37 亿", text)
        self.assertIn("近1日主力净额 -4.57 亿", text)
        self.assertIn("近5日累计 -12.81 亿", text)
        self.assertIn("已连续流出 3 天", text)
        self.assertIn("呈现价强资弱", text)
        self.assertIn("华盛昌", text)
        self.assertIn("| 角色 | 股票 | 评分 | 偏离 | 10日 / 20日 | 资金 | 主题 |", text)
        self.assertIn("当前无明确转弱预警。", text)

        noted_text = render_intraday_summary(build_result, show_notes=True)
        self.assertIn("## 注解", noted_text)
        self.assertIn("## 评分说明", noted_text)
        self.assertIn("主线盘中资金代理（14:50）", noted_text)


if __name__ == "__main__":
    unittest.main()
