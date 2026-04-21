from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.builders.m0 import _build_theme_evidence_items
from awin.contracts.m0 import MetaThemeItem


class ThemeEvidencePrimaryTestCase(unittest.TestCase):
    def test_build_theme_evidence_uses_primary_theme_without_double_counting(self) -> None:
        market = SimpleNamespace(
            top_meta_themes=[
                MetaThemeItem(meta_theme="锂电", rank=1, strongest_concepts=["固态电池"]),
                MetaThemeItem(meta_theme="储能", rank=2, strongest_concepts=["储能"]),
            ]
        )
        stock_facts = [
            SimpleNamespace(
                symbol="A",
                stock_name="甲",
                meta_themes=["锂电", "储能"],
                concepts=["储能"],
                best_meta_theme="锂电",
                pct_chg_prev_close=0.01,
                amount=1000.0,
                main_net_inflow=100.0,
            ),
            SimpleNamespace(
                symbol="B",
                stock_name="乙",
                meta_themes=["锂电", "储能"],
                concepts=["固态电池"],
                best_meta_theme="锂电",
                pct_chg_prev_close=0.02,
                amount=500.0,
                main_net_inflow=-50.0,
            ),
            SimpleNamespace(
                symbol="C",
                stock_name="丙",
                meta_themes=["锂电"],
                concepts=["锂电池概念"],
                best_meta_theme="锂电",
                pct_chg_prev_close=0.03,
                amount=200.0,
                main_net_inflow=20.0,
            ),
        ]

        items = _build_theme_evidence_items(
            stock_facts,
            market,
            None,
            comparison_window_label="近15分钟",
            prior_main_net_inflow_by_symbol={
                "A": 50.0,
                "B": -10.0,
                "C": 5.0,
            },
        )
        item_by_theme = {item.meta_theme: item for item in items}

        self.assertEqual(item_by_theme["锂电"].stock_count, 2)
        self.assertAlmostEqual(item_by_theme["锂电"].current_main_net_inflow_sum or 0.0, -30.0)
        self.assertAlmostEqual(item_by_theme["锂电"].current_main_flow_rate or 0.0, -30.0 / 700.0)
        self.assertAlmostEqual(item_by_theme["锂电"].current_positive_main_flow_ratio or 0.0, 0.5)
        self.assertEqual(item_by_theme["锂电"].comparison_window_label, "近15分钟")
        self.assertAlmostEqual(item_by_theme["锂电"].comparison_main_net_inflow_delta or 0.0, -25.0)

        self.assertEqual(item_by_theme["储能"].stock_count, 1)
        self.assertAlmostEqual(item_by_theme["储能"].current_main_net_inflow_sum or 0.0, 100.0)
        self.assertAlmostEqual(item_by_theme["储能"].current_main_flow_rate or 0.0, 0.1)
        self.assertAlmostEqual(item_by_theme["储能"].current_positive_main_flow_ratio or 0.0, 1.0)
        self.assertEqual(item_by_theme["储能"].comparison_window_label, "近15分钟")
        self.assertAlmostEqual(item_by_theme["储能"].comparison_main_net_inflow_delta or 0.0, 50.0)


if __name__ == "__main__":
    unittest.main()
