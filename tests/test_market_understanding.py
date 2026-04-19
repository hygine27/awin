from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.adapters.contracts import DcfSnapshotRow, QmtSnapshotRow, StockMasterRow, ThsConceptRow, ThsHotConceptRow
from awin.config import ConfigError
from awin.market_understanding import compute_market_understanding, load_style_baskets


class MarketUnderstandingTestCase(unittest.TestCase):
    def test_load_style_baskets_rejects_unknown_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "style_config.yaml"
            config_path.write_text(
                """
# comment: yaml with comments should be supported
style_baskets:
  科技成长:
    industries: [半导体]
    market_types: [科创板]
    unexpected_field: [x]
thresholds:
  min_constituents: 12
  strong_move_pct: 0.02
  near_high_threshold: 0.8
  active_pace_threshold: 1.2
  unused_threshold: 0.1
score_weights:
  eq_return: 0.4
  up_ratio: 0.2
  strong_ratio: 0.15
  near_high_ratio: 0.15
  activity_ratio: 0.1
                """.strip(),
                encoding="utf-8",
            )

            with self.assertRaises(ConfigError):
                load_style_baskets(config_path)

    def test_compute_market_understanding_produces_style_and_theme_summary(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="A", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="B", industry="半导体", market="创业板"),
            StockMasterRow(symbol="300003.SZ", stock_code="300003", stock_name="C", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300004.SZ", stock_code="300004", stock_name="D", industry="IT设备", market="创业板"),
            StockMasterRow(symbol="300005.SZ", stock_code="300005", stock_name="E", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300006.SZ", stock_code="300006", stock_name="F", industry="半导体", market="创业板"),
            StockMasterRow(symbol="300007.SZ", stock_code="300007", stock_name="G", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300008.SZ", stock_code="300008", stock_name="H", industry="IT设备", market="创业板"),
            StockMasterRow(symbol="300009.SZ", stock_code="300009", stock_name="I", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300010.SZ", stock_code="300010", stock_name="J", industry="半导体", market="创业板"),
            StockMasterRow(symbol="300011.SZ", stock_code="300011", stock_name="K", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300012.SZ", stock_code="300012", stock_name="L", industry="IT设备", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(
                symbol=item.symbol,
                stock_code=item.stock_code,
                trade_date="2026-04-16",
                snapshot_time="10:35:00",
                last_price=10.5,
                last_close=10.0,
                open_price=10.1,
                high_price=10.6,
                low_price=10.0,
                volume=100000,
                amount=1000000,
            )
            for item in stock_master
        ]
        dcf_rows = [
            DcfSnapshotRow(
                symbol=item.symbol,
                trade_date="2026-04-16",
                volume_ratio=1.5,
                turnover_rate=0.03,
            )
            for item in stock_master
        ]
        ths_rows = [
            ThsConceptRow(symbol=item.symbol, stock_code=item.stock_code, concept_name="共封装光学(CPO)", meta_theme="光通信_CPO")
            for item in stock_master[:8]
        ]
        hot_rows = [
            ThsHotConceptRow(
                source_table="stg.ths_cli_hot_concept",
                trade_date="2026-04-16",
                batch_ts="2026-04-16 10:35:00",
                concept_name="共封装光学(CPO)",
                change_pct=0.021,
                speed_1min=0.002,
                main_net_amount=1.2e8,
                limit_up_count=3,
                rising_count=20,
                falling_count=4,
            ),
            ThsHotConceptRow(
                source_table="stg.ths_cli_hot_concept",
                trade_date="2026-04-16",
                batch_ts="2026-04-16 10:30:00",
                concept_name="共封装光学(CPO)",
                change_pct=0.012,
                speed_1min=0.001,
                main_net_amount=0.9e8,
                limit_up_count=2,
                rising_count=16,
                falling_count=6,
            ),
        ]

        output = compute_market_understanding(stock_master, qmt_rows, dcf_rows, ths_rows, ths_hot_concepts=hot_rows)

        self.assertEqual(output.confirmed_style, "科技成长")
        self.assertTrue(output.top_styles)
        self.assertTrue(output.top_meta_themes)
        self.assertIn("共封装光学(CPO)", output.strongest_concepts)
        self.assertIn("共封装光学(CPO)", output.acceleration_concepts)
        self.assertIn("主风格", output.summary_line)
        self.assertTrue(output.evidence_lines)

    def test_compute_market_understanding_prefers_actionable_market_tape_regime(self) -> None:
        stock_master = [
            StockMasterRow(symbol="300001.SZ", stock_code="300001", stock_name="A", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300002.SZ", stock_code="300002", stock_name="B", industry="半导体", market="创业板"),
            StockMasterRow(symbol="300003.SZ", stock_code="300003", stock_name="C", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300004.SZ", stock_code="300004", stock_name="D", industry="IT设备", market="创业板"),
            StockMasterRow(symbol="300005.SZ", stock_code="300005", stock_name="E", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300006.SZ", stock_code="300006", stock_name="F", industry="半导体", market="创业板"),
            StockMasterRow(symbol="300007.SZ", stock_code="300007", stock_name="G", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300008.SZ", stock_code="300008", stock_name="H", industry="IT设备", market="创业板"),
            StockMasterRow(symbol="300009.SZ", stock_code="300009", stock_name="I", industry="软件服务", market="创业板"),
            StockMasterRow(symbol="300010.SZ", stock_code="300010", stock_name="J", industry="半导体", market="创业板"),
            StockMasterRow(symbol="300011.SZ", stock_code="300011", stock_name="K", industry="通信设备", market="创业板"),
            StockMasterRow(symbol="300012.SZ", stock_code="300012", stock_name="L", industry="IT设备", market="创业板"),
        ]
        qmt_rows = [
            QmtSnapshotRow(
                symbol=item.symbol,
                stock_code=item.stock_code,
                trade_date="2026-04-16",
                snapshot_time="10:35:00",
                last_price=10.2,
                last_close=10.0,
                open_price=10.1,
                high_price=10.3,
                low_price=10.0,
                volume=100000,
                amount=1000000,
            )
            for item in stock_master
        ]
        dcf_rows = [DcfSnapshotRow(symbol=item.symbol, trade_date="2026-04-16", volume_ratio=1.2, turnover_rate=0.02) for item in stock_master]
        output = compute_market_understanding(
            stock_master,
            qmt_rows,
            dcf_rows,
            [],
            market_tape={"market_regime": "trend_expansion", "regime_actionable": True},
        )
        self.assertEqual(output.market_regime, "trend_expansion")

    def test_compute_market_understanding_allows_strong_cross_section_to_override_mixed_tape(self) -> None:
        stock_master = [
            StockMasterRow(symbol=f"3000{i:02d}.SZ", stock_code=f"3000{i:02d}", stock_name=f"S{i}", industry="软件服务", market="创业板")
            for i in range(1, 13)
        ]
        qmt_rows = [
            QmtSnapshotRow(
                symbol=item.symbol,
                stock_code=item.stock_code,
                trade_date="2026-04-16",
                snapshot_time="10:35:00",
                last_price=10.5 if idx < 10 else 10.1,
                last_close=10.0,
                open_price=10.1,
                high_price=10.6,
                low_price=10.0,
                volume=100000,
                amount=1000000,
            )
            for idx, item in enumerate(stock_master)
        ]
        dcf_rows = [DcfSnapshotRow(symbol=item.symbol, trade_date="2026-04-16", volume_ratio=1.2, turnover_rate=0.02) for item in stock_master]
        output = compute_market_understanding(
            stock_master,
            qmt_rows,
            dcf_rows,
            [],
            market_tape={"market_regime": "mixed_tape", "regime_actionable": True},
        )
        self.assertEqual(output.market_regime, "trend_expansion")


if __name__ == "__main__":
    unittest.main()
