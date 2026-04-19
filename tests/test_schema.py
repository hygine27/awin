from __future__ import annotations

import tempfile
import unittest
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.builders.run_once import RunOnceArgs, write_monitor_run
from awin.storage.db import connect_sqlite, init_db


class SchemaTestCase(unittest.TestCase):
    def test_init_db_creates_core_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "awin.db"
            init_db(db_path)

            with connect_sqlite(db_path) as connection:
                table_names = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }

            self.assertIn("monitor_run", table_names)
            self.assertIn("stock_snapshot", table_names)
            self.assertIn("alert_log", table_names)
            self.assertIn("style_profile", table_names)

    def test_write_monitor_run_inserts_one_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "awin.db"
            args = RunOnceArgs(
                trade_date="2026-04-16",
                snapshot_time="10:35:00",
                round_seq=3,
                db_path=db_path,
                dry_run=False,
            )

            run_id = write_monitor_run(args)

            with connect_sqlite(db_path) as connection:
                row = connection.execute(
                    "SELECT run_id, source_status FROM monitor_run WHERE run_id = ?",
                    (run_id,),
                ).fetchone()

            self.assertIsNotNone(row)
            self.assertEqual(row["source_status"], "SCAFFOLD_ONLY")

    def test_init_db_upgrades_legacy_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "awin.db"
            with connect_sqlite(db_path) as connection:
                connection.execute(
                    """
                    CREATE TABLE monitor_run (
                        run_id TEXT PRIMARY KEY
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE stock_snapshot (
                        run_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        PRIMARY KEY (run_id, symbol)
                    )
                    """
                )
                connection.commit()

            init_db(db_path)

            with connect_sqlite(db_path) as connection:
                monitor_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(monitor_run)").fetchall()
                }
                snapshot_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(stock_snapshot)").fetchall()
                }
                style_profile_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(style_profile)").fetchall()
                }

            self.assertIn("trade_date", monitor_columns)
            self.assertIn("coverage_ratio", monitor_columns)
            self.assertIn("analysis_snapshot_ts", snapshot_columns)
            self.assertIn("best_meta_theme", snapshot_columns)
            self.assertIn("is_core_anchor", snapshot_columns)
            self.assertIn("is_new_long", snapshot_columns)
            self.assertIn("is_catchup", snapshot_columns)
            self.assertIn("is_short", snapshot_columns)
            self.assertIn("market_type_label", style_profile_columns)
            self.assertIn("ownership_style", style_profile_columns)
            self.assertIn("size_bucket_pct", style_profile_columns)
            self.assertIn("composite_style_labels_json", style_profile_columns)


if __name__ == "__main__":
    unittest.main()
