from __future__ import annotations

import tempfile
import unittest
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.evaluation import build_day_summary, build_day_summary_markdown
from awin.storage.db import connect_sqlite, init_db


class EvaluationDaySummaryTestCase(unittest.TestCase):
    def test_build_day_summary_aggregates_symbol_mentions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "awin.db"
            init_db(db_path)
            with connect_sqlite(db_path) as connection:
                connection.execute(
                    """
                    INSERT INTO monitor_run (
                        run_id, trade_date, snapshot_time, analysis_snapshot_ts, round_seq,
                        market_regime, style_state, stock_count, confirmed_style, latest_status, alert_decision
                    ) VALUES
                    ('r1','2026-04-16','09:35:00','2026-04-16T09:35:00',1,'mixed','stable',2,'科技成长','stable','UPDATE'),
                    ('r2','2026-04-16','10:00:00','2026-04-16T10:00:00',2,'mixed','stable',2,'科技成长','stable','UPDATE')
                    """
                )
                connection.execute(
                    """
                    INSERT INTO stock_snapshot (
                        run_id, trade_date, snapshot_time, analysis_snapshot_ts, symbol, stock_name,
                        signal_state, display_bucket, risk_tag, confidence_score, is_watchlist, is_warning
                    ) VALUES
                    ('r1','2026-04-16','09:35:00','2026-04-16T09:35:00','300001.SZ','A','bull','new_long',NULL,8.0,1,0),
                    ('r2','2026-04-16','10:00:00','2026-04-16T10:00:00','300001.SZ','A','warning','warning','warning',9.0,0,1)
                    """
                )
                connection.commit()

            payload = build_day_summary(db_path, "2026-04-16")
            self.assertEqual(payload["run_count"], 2)
            self.assertEqual(payload["active_symbols"][0]["symbol"], "300001.SZ")
            self.assertEqual(payload["active_symbols"][0]["mention_count"], 2)
            self.assertIn("new_long", payload["active_symbols"][0]["seen_buckets"])
            self.assertIn("risk", payload["active_symbols"][0]["seen_buckets"])

            markdown = build_day_summary_markdown(db_path, "2026-04-16")
            self.assertIn("# A视界 Evaluation | 2026-04-16", markdown)
            self.assertIn("## 活跃标的一览", markdown)
            self.assertIn("A(300001.SZ)", markdown)


if __name__ == "__main__":
    unittest.main()
