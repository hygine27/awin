from __future__ import annotations

import tempfile
import unittest
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.replay import build_day_replay, build_day_replay_markdown
from awin.storage.db import connect_sqlite, init_db


class ReplayDayTestCase(unittest.TestCase):
    def test_build_day_replay_tracks_added_removed_symbols(self) -> None:
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
                    ('r2','2026-04-16','09:40:00','2026-04-16T09:40:00',2,'mixed','stable',2,'科技成长','stable','UPDATE')
                    """
                )
                connection.execute(
                    """
                    INSERT INTO stock_snapshot (
                        run_id, trade_date, snapshot_time, analysis_snapshot_ts, symbol, stock_name,
                        signal_state, display_bucket, confidence_score, is_watchlist, is_warning
                    ) VALUES
                    ('r1','2026-04-16','09:35:00','2026-04-16T09:35:00','300001.SZ','A','bull','new_long',8.0,1,0),
                    ('r2','2026-04-16','09:40:00','2026-04-16T09:40:00','300002.SZ','B','bull','new_long',8.5,1,0)
                    """
                )
                connection.commit()

            payload = build_day_replay(db_path, "2026-04-16")
            self.assertEqual(payload["run_count"], 2)
            self.assertEqual(payload["timeline"][0]["added_new_long"], ["300001.SZ"])
            self.assertEqual(payload["timeline"][1]["added_new_long"], ["300002.SZ"])
            self.assertEqual(payload["timeline"][1]["removed_new_long"], ["300001.SZ"])

            markdown = build_day_replay_markdown(db_path, "2026-04-16")
            self.assertIn("# A视界 Replay | 2026-04-16", markdown)
            self.assertIn("## 轮次回放", markdown)
            self.assertIn("+看多1", markdown)


if __name__ == "__main__":
    unittest.main()
