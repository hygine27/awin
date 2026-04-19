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
            {"ts_code": "300001.SZ", "trade_date": "20260417", "circ_mv": 200000.0, "total_mv": 260000.0},
            {"ts_code": "600001.SH", "trade_date": "20260417", "circ_mv": 12000000.0, "total_mv": 15000000.0},
            {"ts_code": "000002.SZ", "trade_date": "20260417", "circ_mv": 1500000.0, "total_mv": 1800000.0},
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

        profiles = build_style_profiles(stock_basic_rows, daily_basic_rows, index_member_rows, trade_date="20260417")

        self.assertEqual(len(profiles), 3)
        by_symbol = {item.symbol: item for item in profiles}

        tech = by_symbol["300001.SZ"]
        self.assertEqual(tech.market_type_label, "创业板")
        self.assertEqual(tech.exchange_label, "SZSE")
        self.assertEqual(tech.ownership_style, "民企")
        self.assertEqual(tech.sw_l1_name, "电子")
        self.assertEqual(tech.size_bucket_abs, "微盘")
        self.assertIn("科技成长", tech.composite_style_labels)
        self.assertIn("小盘题材", tech.composite_style_labels)

        bank = by_symbol["600001.SH"]
        self.assertEqual(bank.ownership_style, "央国企")
        self.assertEqual(bank.size_bucket_abs, "超大盘")
        self.assertEqual(bank.capacity_bucket, "机构核心容量")
        self.assertIn("红利价值", bank.composite_style_labels)

        real_estate = by_symbol["000002.SZ"]
        self.assertEqual(real_estate.ownership_style, "未识别")
        self.assertEqual(real_estate.size_bucket_abs, "中盘")
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
                    SELECT trade_date, symbol, market_type_label, size_bucket_abs, composite_style_labels_json
                    FROM style_profile
                    WHERE trade_date = ? AND symbol = ?
                    """,
                    ("20260417", "300001.SZ"),
                ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row["market_type_label"], "创业板")
        self.assertEqual(row["size_bucket_abs"], "微盘")
        self.assertEqual(json.loads(row["composite_style_labels_json"]), ["科技成长"])


if __name__ == "__main__":
    unittest.main()
