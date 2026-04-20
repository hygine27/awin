from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.storage.db import connect_sqlite, init_db
from awin.style_profile import build_style_profiles, persist_style_profiles


class StyleProfileTestCase(unittest.TestCase):
    def test_build_style_profiles_derives_first_batch_fields(self) -> None:
        stock_basic_rows = [
            {
                "ts_code": "300001.SZ",
                "market": "创业板",
                "exchange": "SZSE",
                "industry": "软件服务",
                "act_ent_type": "民营企业",
            },
            {
                "ts_code": "600001.SH",
                "market": "主板",
                "exchange": "SSE",
                "industry": "银行",
                "act_ent_type": "中央国有企业",
            },
            {
                "ts_code": "000002.SZ",
                "market": "主板",
                "exchange": "SZSE",
                "industry": "全国地产",
                "act_ent_type": "其他",
            },
        ]
        daily_basic_rows = [
            {"ts_code": "300001.SZ", "trade_date": "20260417", "free_share": 1000.0, "circ_mv": 200000.0, "total_mv": 260000.0, "turnover_rate_f": 6.0, "pe_ttm": 45.0, "pb": 6.0, "ps_ttm": 8.0, "dv_ratio": 0.3, "dv_ttm": 0.3},
            {"ts_code": "600001.SH", "trade_date": "20260417", "free_share": 2000.0, "circ_mv": 12000000.0, "total_mv": 15000000.0, "turnover_rate_f": 0.8, "pe_ttm": 6.0, "pb": 0.8, "ps_ttm": 2.0, "dv_ratio": 4.5, "dv_ttm": 4.5},
            {"ts_code": "000002.SZ", "trade_date": "20260417", "free_share": 1500.0, "circ_mv": 1500000.0, "total_mv": 1800000.0, "turnover_rate_f": 2.0, "pe_ttm": 18.0, "pb": 1.2, "ps_ttm": 3.0, "dv_ratio": 1.2, "dv_ttm": 1.2},
        ]
        index_member_rows = [
            {
                "ts_code": "300001.SZ",
                "l1_code": "801080",
                "l1_name": "电子",
                "l2_code": "801081",
                "l2_name": "半导体",
                "l3_code": "801082",
                "l3_name": "数字芯片设计",
                "in_date": "20200101",
                "out_date": None,
            },
            {
                "ts_code": "600001.SH",
                "l1_code": "801780",
                "l1_name": "银行",
                "l2_code": "801781",
                "l2_name": "国有大型银行",
                "l3_code": "801782",
                "l3_name": "股份制银行",
                "in_date": "20200101",
                "out_date": None,
            },
        ]
        daily_rows = []
        adj_factor_rows = []
        for trade_date, growth_close, bank_close, estate_close in [
            ("20260320", 10.0, 10.0, 10.0),
            ("20260321", 10.2, 10.0, 10.1),
            ("20260324", 10.4, 10.1, 10.1),
            ("20260325", 10.5, 10.1, 10.2),
            ("20260326", 10.6, 10.1, 10.2),
            ("20260327", 10.8, 10.2, 10.3),
            ("20260330", 10.9, 10.2, 10.4),
            ("20260331", 11.0, 10.2, 10.4),
            ("20260401", 11.2, 10.2, 10.5),
            ("20260402", 11.3, 10.3, 10.5),
            ("20260403", 11.4, 10.3, 10.6),
            ("20260406", 11.6, 10.3, 10.6),
            ("20260407", 11.7, 10.3, 10.7),
            ("20260408", 11.8, 10.4, 10.7),
            ("20260409", 12.0, 10.4, 10.8),
            ("20260410", 12.1, 10.4, 10.8),
            ("20260413", 12.2, 10.5, 10.9),
            ("20260414", 12.3, 10.5, 10.9),
            ("20260415", 12.5, 10.5, 11.0),
            ("20260416", 12.6, 10.6, 11.0),
            ("20260417", 12.8, 10.6, 11.1),
        ]:
            daily_rows.extend(
                [
                    {"ts_code": "300001.SZ", "trade_date": trade_date, "close": growth_close, "amount": 250000.0},
                    {"ts_code": "600001.SH", "trade_date": trade_date, "close": bank_close, "amount": 180000.0},
                    {"ts_code": "000002.SZ", "trade_date": trade_date, "close": estate_close, "amount": 120000.0},
                ]
            )
            adj_factor_rows.extend(
                [
                    {"ts_code": "300001.SZ", "trade_date": trade_date, "adj_factor": 1.0},
                    {"ts_code": "600001.SH", "trade_date": trade_date, "adj_factor": 1.0},
                    {"ts_code": "000002.SZ", "trade_date": trade_date, "adj_factor": 1.0},
                ]
            )
        fina_indicator_rows = [
            {"ts_code": "300001.SZ", "ann_date": "20260415", "end_date": "20251231", "roe_yearly": 18.0, "roic": 15.0, "debt_to_assets": 28.0, "q_ocf_to_sales": 12.0, "tr_yoy": 42.0, "or_yoy": 35.0, "q_sales_yoy": 30.0, "netprofit_yoy": 48.0, "dt_netprofit_yoy": 45.0, "op_yoy": 40.0},
            {"ts_code": "600001.SH", "ann_date": "20260415", "end_date": "20251231", "roe_yearly": 11.0, "roic": 8.0, "debt_to_assets": 85.0, "q_ocf_to_sales": 8.0, "tr_yoy": 6.0, "or_yoy": 5.0, "q_sales_yoy": 4.0, "netprofit_yoy": 7.0, "dt_netprofit_yoy": 6.0, "op_yoy": 5.0},
            {"ts_code": "000002.SZ", "ann_date": "20260415", "end_date": "20251231", "roe_yearly": 9.0, "roic": 7.0, "debt_to_assets": 70.0, "q_ocf_to_sales": 5.0, "tr_yoy": 12.0, "or_yoy": 10.0, "q_sales_yoy": 8.0, "netprofit_yoy": 9.0, "dt_netprofit_yoy": 8.0, "op_yoy": 7.0},
        ]

        profiles = build_style_profiles(
            stock_basic_rows,
            daily_basic_rows,
            index_member_rows,
            daily_rows=daily_rows,
            adj_factor_rows=adj_factor_rows,
            fina_indicator_rows=fina_indicator_rows,
            trade_date="20260417",
        )

        self.assertEqual(len(profiles), 3)
        by_symbol = {item.symbol: item for item in profiles}

        tech = by_symbol["300001.SZ"]
        self.assertEqual(tech.market_type_label, "创业板")
        self.assertEqual(tech.exchange_label, "SZSE")
        self.assertEqual(tech.ownership_style, "民企")
        self.assertEqual(tech.sw_l1_name, "电子")
        self.assertEqual(tech.size_bucket_abs, "微盘")
        self.assertIsNotNone(tech.avg_amount_20d)
        self.assertIsNotNone(tech.growth_valuation_score)
        self.assertIsNotNone(tech.quality_growth_score)
        self.assertIsNotNone(tech.high_beta_attack_score)
        self.assertEqual(tech.valuation_style, "高估值")
        self.assertEqual(tech.growth_style, "高成长")
        self.assertEqual(tech.quality_style, "高质量")
        self.assertEqual(tech.volatility_style, "中波")
        self.assertIn("科技成长", tech.composite_style_labels)
        self.assertIn("小盘题材", tech.composite_style_labels)
        self.assertIn("高弹性进攻", tech.composite_style_labels)

        bank = by_symbol["600001.SH"]
        self.assertEqual(bank.ownership_style, "央国企")
        self.assertEqual(bank.size_bucket_abs, "超大盘")
        self.assertEqual(bank.capacity_bucket, "机构核心容量")
        self.assertIsNotNone(bank.dividend_value_score)
        self.assertIsNotNone(bank.low_vol_defensive_score)
        self.assertEqual(bank.dividend_style, "红利核心")
        self.assertEqual(bank.valuation_style, "低估值")
        self.assertEqual(bank.quality_style, "中质量")
        self.assertEqual(bank.volatility_style, "低波")
        self.assertIn("红利价值", bank.composite_style_labels)
        self.assertIn("央国企权重", bank.composite_style_labels)

        real_estate = by_symbol["000002.SZ"]
        self.assertEqual(real_estate.ownership_style, "未识别")
        self.assertEqual(real_estate.size_bucket_abs, "中盘")
        self.assertEqual(real_estate.growth_style, "中成长")
        self.assertIn("金融地产链", real_estate.composite_style_labels)

    def test_persist_style_profiles_writes_rows(self) -> None:
        profiles = build_style_profiles(
            stock_basic_rows=[
                {"ts_code": "300001.SZ", "market": "创业板", "exchange": "SZSE", "industry": "软件服务", "act_ent_type": "民营企业"}
            ],
            daily_basic_rows=[
                {"ts_code": "300001.SZ", "trade_date": "20260417", "circ_mv": 200000.0, "total_mv": 260000.0}
            ],
            index_member_rows=[],
            trade_date="20260417",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "awin.db"
            init_db(db_path)
            persist_style_profiles(db_path, profiles)

            with connect_sqlite(db_path) as connection:
                row = connection.execute(
                    """
                    SELECT trade_date, symbol, market_type_label, size_bucket_abs, high_beta_attack_score,
                           valuation_style, growth_style, quality_style, volatility_style, composite_style_labels_json
                    FROM style_profile
                    WHERE trade_date = ? AND symbol = ?
                    """,
                    ("20260417", "300001.SZ"),
                ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row["market_type_label"], "创业板")
        self.assertEqual(row["size_bucket_abs"], "微盘")
        self.assertEqual(row["high_beta_attack_score"], 1.0)
        self.assertEqual(row["valuation_style"], None)
        self.assertEqual(row["growth_style"], None)
        self.assertEqual(row["quality_style"], None)
        self.assertEqual(row["volatility_style"], None)
        self.assertEqual(json.loads(row["composite_style_labels_json"]), ["科技成长", "高弹性进攻"])

    def test_build_style_profiles_accepts_precomputed_daily_metrics(self) -> None:
        profiles = build_style_profiles(
            stock_basic_rows=[
                {"ts_code": "300001.SZ", "market": "创业板", "exchange": "SZSE", "industry": "软件服务", "act_ent_type": "民营企业"}
            ],
            daily_basic_rows=[
                {
                    "ts_code": "300001.SZ",
                    "trade_date": "20260417",
                    "free_share": 1000.0,
                    "circ_mv": 200000.0,
                    "total_mv": 260000.0,
                    "turnover_rate_f": 6.0,
                    "pe_ttm": 45.0,
                    "pb": 6.0,
                    "ps_ttm": 8.0,
                    "dv_ratio": 0.3,
                    "dv_ttm": 0.3,
                }
            ],
            index_member_rows=[],
            daily_metric_rows=[
                {
                    "ts_code": "300001.SZ",
                    "avg_amount_20d": 250000.0,
                    "ret_20d": 0.25,
                    "ret_60d": 0.40,
                    "vol_20d": 0.03,
                    "vol_60d": 0.04,
                    "max_drawdown_20d": 0.06,
                    "max_drawdown_60d": 0.10,
                }
            ],
            fina_indicator_rows=[
                {
                    "ts_code": "300001.SZ",
                    "ann_date": "20260415",
                    "end_date": "20251231",
                    "roe_yearly": 18.0,
                    "roic": 15.0,
                    "debt_to_assets": 28.0,
                    "q_ocf_to_sales": 12.0,
                    "tr_yoy": 42.0,
                    "or_yoy": 35.0,
                    "q_sales_yoy": 30.0,
                    "netprofit_yoy": 48.0,
                    "dt_netprofit_yoy": 45.0,
                    "op_yoy": 40.0,
                }
            ],
            trade_date="20260417",
        )

        self.assertEqual(len(profiles), 1)
        profile = profiles[0]
        self.assertEqual(profile.symbol, "300001.SZ")
        self.assertEqual(profile.avg_amount_20d, 250000.0)
        self.assertIsNotNone(profile.growth_valuation_score)
        self.assertIsNotNone(profile.quality_growth_score)


if __name__ == "__main__":
    unittest.main()
