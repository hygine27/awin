from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.adapters import (
    DcfHqZjSnapshotAdapter,
    QmtAshareSnapshot5mAdapter,
    QmtBar1dAdapter,
    QmtBar1dMetricsAdapter,
    ResearchCoverageAdapter,
    SnapshotRequest,
    StockMasterAdapter,
    ThsConceptAdapter,
    TsMoneyflowCntThsAdapter,
    TsMoneyflowDcAdapter,
    TsMoneyflowIndThsAdapter,
    TsMoneyflowMktDcAdapter,
    TsMoneyflowThsAdapter,
    TsStyleDailyMetricsAdapter,
)


class AdapterContractsTestCase(unittest.TestCase):
    def test_stock_master_adapter_loads_rows_from_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            master_path = Path(tmp_dir) / "a_shares_master.json"
            master_path.write_text(
                json.dumps(
                    {
                        "data": [
                            {
                                "symbol": "000001.SZ",
                                "stock_code": "000001",
                                "stock_name": "平安银行",
                                "exchange": "SZSE",
                                "market_type": "主板",
                                "industry": "银行",
                                "is_delisted": False,
                            },
                            {
                                "symbol": "000004.SZ",
                                "stock_code": "000004",
                                "stock_name": "*ST国华",
                                "exchange": "SZSE",
                                "market_type": "主板",
                                "industry": "软件服务",
                                "is_delisted": False,
                            },
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            rows = StockMasterAdapter(master_path).load_rows()

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0].symbol, "000001.SZ")
            self.assertTrue(rows[1].is_st)

    def test_ths_concept_adapter_loads_whitelisted_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            concept_map_path = Path(tmp_dir) / "concept_to_stocks.json"
            overlay_config_path = Path(tmp_dir) / "overlay.json"
            concept_map_path.write_text(
                json.dumps(
                    {
                        "data": {
                            "共封装光学(CPO)": ["300570", "688313"],
                            "无关概念": ["000001"],
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            overlay_config_path.write_text(
                json.dumps(
                    {
                        "concept_whitelist": ["共封装光学(CPO)"],
                        "meta_themes": {"光通信_CPO": ["共封装光学(CPO)"]},
                        "concept_aliases": {},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            rows = ThsConceptAdapter(concept_map_path, overlay_config_path).load_rows(
                SnapshotRequest(
                    trade_date="2026-04-16",
                    snapshot_time="10:35:00",
                    analysis_snapshot_ts="2026-04-16T10:35:00",
                )
            )

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0].concept_name, "共封装光学(CPO)")
            self.assertEqual(rows[0].meta_theme, "光通信_CPO")

    def test_research_coverage_adapter_merges_onepage_and_company_card(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            companies_dir = Path(tmp_dir) / "companies"
            onepage_dir = Path(tmp_dir) / "summaries"
            intel_dir = Path(tmp_dir) / "intel"
            companies_dir.mkdir()
            onepage_dir.mkdir()
            intel_dir.mkdir()

            (companies_dir / "300570.SZ_太辰光.md").write_text(
                """---
symbol: 300570.SZ
stock_code: 300570
theme: 光通信_CPO
chain_position: 光模块
company_role: 龙头
---

近端市场情报命中 3 条
""",
                encoding="utf-8",
            )
            (onepage_dir / "onepage-stock-300570.SZ-20260101_120000__太辰光.md").write_text(
                "# 文档摘要卡\n",
                encoding="utf-8",
            )

            rows = ResearchCoverageAdapter(companies_dir, onepage_dir, intel_dir).load_rows(
                SnapshotRequest(
                    trade_date="2026-04-16",
                    snapshot_time="10:35:00",
                    analysis_snapshot_ts="2026-04-16T10:35:00",
                )
            )

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].symbol, "300570.SZ")
            self.assertEqual(rows[0].recent_intel_mentions, 3)
            self.assertIsNotNone(rows[0].company_card_path)
            self.assertIsNotNone(rows[0].onepage_path)
            self.assertGreater(rows[0].research_coverage_score, 0.5)

    def test_qmt_and_dcf_adapters_expose_query_contracts(self) -> None:
        request = SnapshotRequest(
            trade_date="2026-04-16",
            snapshot_time="10:35:00",
            analysis_snapshot_ts="2026-04-16T10:35:00",
        )

        qmt_sql, qmt_params = QmtAshareSnapshot5mAdapter().build_query(request)
        qmt_bar_sql, qmt_bar_params = QmtBar1dAdapter().build_query(["000001.SZ"], "2026-04-16", "2026-04-30")
        dcf_batch_sql, dcf_batch_params = DcfHqZjSnapshotAdapter().build_batch_query(request)
        dcf_baseline_sql, dcf_baseline_params = DcfHqZjSnapshotAdapter().build_baseline_query(request)

        self.assertIn("stg.qmt_ashare_snapshot_5m", qmt_sql)
        self.assertIn("snapshot_time <=", qmt_sql)
        self.assertEqual(qmt_params["trade_date"], "2026-04-16")
        self.assertEqual(qmt_params["analysis_snapshot_ts"], "2026-04-16T10:35:00")
        self.assertIn("stg.qmt_bar_1d", qmt_bar_sql)
        self.assertEqual(qmt_bar_params["symbols"], ["000001.SZ"])
        self.assertEqual(qmt_bar_params["start_date"], "2026-04-16")
        self.assertEqual(qmt_bar_params["end_date"], "2026-04-30")
        self.assertIn("stg.dcf_cli_hq", dcf_batch_sql)
        self.assertIn("baseline_rows", dcf_baseline_sql)
        self.assertEqual(dcf_batch_params["trade_date"], "2026-04-16")
        self.assertEqual(dcf_baseline_params["trade_date"], "2026-04-16")

        qmt_metric_sql, qmt_metric_params = QmtBar1dMetricsAdapter().build_query(["000001.SZ"], "2026-04-16", "2026-04-30")
        self.assertIn("stg.qmt_bar_1d", qmt_metric_sql)
        self.assertIn("avg(amount)", qmt_metric_sql)
        self.assertEqual(qmt_metric_params["symbols"], ["000001.SZ"])
        self.assertEqual(qmt_metric_params["start_date"], "2026-04-16")
        self.assertEqual(qmt_metric_params["trade_date"], "2026-04-30")

    def test_ts_moneyflow_adapters_expose_query_contracts(self) -> None:
        ths_sql, ths_params = TsMoneyflowThsAdapter().build_query("2026-04-16")
        dc_sql, dc_params = TsMoneyflowDcAdapter().build_query("2026-04-16")
        cnt_sql, cnt_params = TsMoneyflowCntThsAdapter().build_query("2026-04-16")
        ind_sql, ind_params = TsMoneyflowIndThsAdapter().build_query("2026-04-16")
        mkt_sql, mkt_params = TsMoneyflowMktDcAdapter().build_query("2026-04-16")

        self.assertIn("stg.ts_moneyflow_ths", ths_sql)
        self.assertIn("trade_date <", ths_sql)
        self.assertEqual(ths_params["trade_date"], "2026-04-16")
        self.assertIn("stg.ts_moneyflow_dc", dc_sql)
        self.assertIn("latest_trade_date", dc_sql)
        self.assertEqual(dc_params["trade_date"], "2026-04-16")
        self.assertIn("stg.ts_moneyflow_cnt_ths", cnt_sql)
        self.assertEqual(cnt_params["trade_date"], "2026-04-16")
        self.assertIn("stg.ts_moneyflow_ind_ths", ind_sql)
        self.assertEqual(ind_params["trade_date"], "2026-04-16")
        self.assertIn("stg.ts_moneyflow_mkt_dc", mkt_sql)
        self.assertEqual(mkt_params["trade_date"], "2026-04-16")

    def test_ts_style_daily_metrics_adapter_exposes_query_contracts(self) -> None:
        sql, params = TsStyleDailyMetricsAdapter().build_query("2026-04-16")

        self.assertIn("stg.ts_daily", sql)
        self.assertIn("stg.ts_adj_factor", sql)
        self.assertIn("stddev_pop", sql)
        self.assertIn("max_drawdown_20d", sql)
        self.assertEqual(params["trade_date"], "2026-04-16")

    def test_db_adapters_gracefully_degrade_when_query_unavailable(self) -> None:
        request = SnapshotRequest(
            trade_date="2026-04-16",
            snapshot_time="10:35:00",
            analysis_snapshot_ts="2026-04-16T10:35:00",
        )

        with patch.object(QmtAshareSnapshot5mAdapter, "_query_rows", return_value=None):
            self.assertEqual(QmtAshareSnapshot5mAdapter().load_rows(request), [])
        with patch.object(DcfHqZjSnapshotAdapter, "_connect_with_error", return_value=(object(), None)):
            with patch.object(DcfHqZjSnapshotAdapter, "_query_rows", return_value=None):
                self.assertEqual(DcfHqZjSnapshotAdapter().load_rows(request), [])

    def test_dcf_guard_marks_stale_or_low_coverage_as_degraded(self) -> None:
        adapter = DcfHqZjSnapshotAdapter(max_freshness_minutes=20.0, min_rows_abs=5000, min_completeness_ratio=0.92)
        request = SnapshotRequest(
            trade_date="2026-04-16",
            snapshot_time="10:35:00",
            analysis_snapshot_ts="2026-04-16T10:35:00+08:00",
        )
        chosen, health = adapter.evaluate_guard(
            request,
            batch_rows=[
                {
                    "batch_ts": "2026-04-16T09:50:00+00:00",
                    "hq_rows": 5100,
                    "zj_rows": 5090,
                    "paired_rows": 4500,
                }
            ],
            baseline_rows=5200,
        )

        self.assertIsNotNone(chosen)
        self.assertEqual(health.source_status, "degraded")
        self.assertTrue(health.fallback_used)
        self.assertIn("low_coverage", health.detail)

    def test_dcf_rows_are_normalized_to_exchange_suffix_symbols(self) -> None:
        adapter = DcfHqZjSnapshotAdapter()
        request = SnapshotRequest(
            trade_date="2026-04-16",
            snapshot_time="10:35:00",
            analysis_snapshot_ts="2026-04-16T10:35:00",
        )

        with patch.object(adapter, "_connect_with_error", return_value=(object(), None)):
            with patch.object(
                adapter,
                "_query_rows",
                side_effect=[
                    [{"batch_ts": "2026-04-16T10:30:00+00:00", "hq_rows": 5200, "zj_rows": 5200, "paired_rows": 5200}],
                    [{"baseline_rows": 5200}],
                    [
                        {
                            "code": "000001",
                            "trade_date": "2026-04-16",
                            "vendor_batch_ts": "2026-04-16T10:30:00+00:00",
                            "turnover_rate": "0.03",
                            "volume_ratio": "1.2",
                            "amplitude": "0.02",
                            "float_mkt_cap": "100亿",
                            "total_mkt_cap": "120亿",
                            "ret_3d": "0.01",
                            "ret_5d": None,
                            "ret_10d": "0.03",
                            "ret_20d": "0.05",
                        }
                    ],
                    [
                        {
                            "code": "000001",
                            "main_net_inflow": "100万",
                            "super_net": "50万",
                            "large_net": "20万",
                        }
                    ],
                ],
            ):
                rows, health = adapter.load_rows_with_health(request)

        self.assertEqual(health.source_status, "ready")
        self.assertEqual(rows[0].symbol, "000001.SZ")
        self.assertIsInstance(rows[0].volume_ratio, float)
        self.assertEqual(rows[0].float_mkt_cap, 10000000000.0)
        self.assertEqual(rows[0].main_net_inflow, 1000000.0)

    def test_dcf_percentage_like_fields_are_normalized_to_ratios(self) -> None:
        adapter = DcfHqZjSnapshotAdapter()
        request = SnapshotRequest(
            trade_date="2026-04-16",
            snapshot_time="10:35:00",
            analysis_snapshot_ts="2026-04-16T10:35:00",
        )

        with patch.object(adapter, "_connect_with_error", return_value=(object(), None)):
            with patch.object(
                adapter,
                "_query_rows",
                side_effect=[
                    [{"batch_ts": "2026-04-16T10:30:00+00:00", "hq_rows": 5200, "zj_rows": 5200, "paired_rows": 5200}],
                    [{"baseline_rows": 5200}],
                    [
                        {
                            "code": "000001",
                            "trade_date": "2026-04-16",
                            "vendor_batch_ts": "2026-04-16T10:30:00+00:00",
                            "turnover_rate": "3.25",
                            "volume_ratio": "1.2",
                            "amplitude": "4.50",
                            "float_mkt_cap": "100亿",
                            "total_mkt_cap": "120亿",
                            "ret_3d": "9.08",
                            "ret_5d": None,
                            "ret_10d": "-8.15",
                            "ret_20d": "12.30",
                        }
                    ],
                    [
                        {
                            "code": "000001",
                            "main_net_inflow": "100万",
                            "super_net": "50万",
                            "large_net": "20万",
                        }
                    ],
                ],
            ):
                rows, _ = adapter.load_rows_with_health(request)

        self.assertAlmostEqual(rows[0].turnover_rate or 0.0, 0.0325, places=6)
        self.assertAlmostEqual(rows[0].amplitude or 0.0, 0.0450, places=6)
        self.assertAlmostEqual(rows[0].ret_3d or 0.0, 0.0908, places=6)
        self.assertAlmostEqual(rows[0].ret_10d or 0.0, -0.0815, places=6)
        self.assertAlmostEqual(rows[0].ret_20d or 0.0, 0.1230, places=6)


if __name__ == "__main__":
    unittest.main()
