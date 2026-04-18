from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.builders.m0 import load_previous_bull_state_history
from awin.storage.db import connect_sqlite, init_db


class StateHistoryTestCase(unittest.TestCase):
    def test_load_previous_bull_state_history_tracks_appearances_and_streak(self) -> None:
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
                    ('r2','2026-04-16','09:40:00','2026-04-16T09:40:00',2,'mixed','stable',2,'科技成长','stable','UPDATE'),
                    ('r3','2026-04-16','09:45:00','2026-04-16T09:45:00',3,'mixed','stable',2,'科技成长','stable','UPDATE'),
                    ('r4','2026-04-16','09:50:00','2026-04-16T09:50:00',4,'mixed','stable',2,'科技成长','stable','UPDATE')
                    """
                )
                connection.execute(
                    """
                    INSERT INTO stock_snapshot (
                        run_id, trade_date, snapshot_time, analysis_snapshot_ts, symbol, stock_name,
                        signal_state, display_bucket, confidence_score, best_meta_theme, best_concept, is_watchlist, is_warning
                    ) VALUES
                    ('r1','2026-04-16','09:35:00','2026-04-16T09:35:00','300001.SZ','A','bull','new_long',8.6,'光通信_CPO','CPO',1,0),
                    ('r2','2026-04-16','09:40:00','2026-04-16T09:40:00','300001.SZ','A','bull','new_long',8.8,'光通信_CPO','CPO',1,0),
                    ('r3','2026-04-16','09:45:00','2026-04-16T09:45:00','300001.SZ','A','bull','core_anchor',9.1,'光通信_CPO','CPO',1,0),
                    ('r1','2026-04-16','09:35:00','2026-04-16T09:35:00','300002.SZ','B','bull','new_long',8.2,'AI算力','AIGC概念',1,0),
                    ('r3','2026-04-16','09:45:00','2026-04-16T09:45:00','300002.SZ','B','bull','new_long',8.4,'AI算力','AIGC概念',1,0)
                    """
                )
                connection.commit()

            payload = load_previous_bull_state_history(
                db_path,
                "r4-current",
                "2026-04-16T09:50:00",
                trade_date="2026-04-16",
                current_round_seq=4,
            )

            self.assertEqual(payload["300001.SZ"].appearances, 3)
            self.assertEqual(payload["300001.SZ"].streak, 3)
            self.assertTrue(payload["300001.SZ"].recent_repeat)
            self.assertTrue(payload["300001.SZ"].consecutive_repeat)
            self.assertEqual(payload["300001.SZ"].display_bucket, "core_anchor")

            self.assertEqual(payload["300002.SZ"].appearances, 2)
            self.assertEqual(payload["300002.SZ"].streak, 1)
            self.assertTrue(payload["300002.SZ"].recent_repeat)
            self.assertTrue(payload["300002.SZ"].consecutive_repeat)


if __name__ == "__main__":
    unittest.main()
