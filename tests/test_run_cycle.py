from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from scripts import run_cycle


class _FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 4, 21, 9, 17, 42, tzinfo=tz)


class RunCycleResolveSlotTestCase(unittest.TestCase):
    def test_returns_explicit_slot_without_latest_lookup(self) -> None:
        args = run_cycle.RunCycleArgs(
            trade_date="2026-04-20",
            snapshot_time="14:50",
            floor_minutes=5,
            db_path=Path("/tmp/awin.db"),
            dry_run=True,
        )

        with patch.object(run_cycle, "_lookup_latest_qmt_slot") as latest_lookup:
            trade_date, snapshot_time = run_cycle._resolve_slot(args)

        self.assertEqual(("2026-04-20", "14:50:00"), (trade_date, snapshot_time))
        latest_lookup.assert_not_called()

    def test_uses_latest_slot_for_explicit_trade_date_without_snapshot_time(self) -> None:
        args = run_cycle.RunCycleArgs(
            trade_date="2026-04-20",
            snapshot_time=None,
            floor_minutes=5,
            db_path=Path("/tmp/awin.db"),
            dry_run=True,
        )

        with patch.object(run_cycle, "_lookup_latest_qmt_slot", return_value=("2026-04-20", "14:50:00")) as latest_lookup:
            trade_date, snapshot_time = run_cycle._resolve_slot(args)

        self.assertEqual(("2026-04-20", "14:50:00"), (trade_date, snapshot_time))
        latest_lookup.assert_called_once_with("2026-04-20")

    def test_uses_latest_slot_when_trade_date_and_snapshot_time_are_omitted(self) -> None:
        args = run_cycle.RunCycleArgs(
            trade_date=None,
            snapshot_time=None,
            floor_minutes=5,
            db_path=Path("/tmp/awin.db"),
            dry_run=True,
        )

        with patch.object(run_cycle, "_lookup_latest_qmt_slot", return_value=("2026-04-20", "14:50:00")) as latest_lookup:
            trade_date, snapshot_time = run_cycle._resolve_slot(args)

        self.assertEqual(("2026-04-20", "14:50:00"), (trade_date, snapshot_time))
        latest_lookup.assert_called_once_with(None)

    def test_falls_back_to_current_floored_clock_when_latest_slot_is_unavailable(self) -> None:
        args = run_cycle.RunCycleArgs(
            trade_date=None,
            snapshot_time=None,
            floor_minutes=5,
            db_path=Path("/tmp/awin.db"),
            dry_run=True,
        )

        with patch.object(run_cycle, "_lookup_latest_qmt_slot", return_value=None), patch.object(run_cycle, "datetime", _FixedDatetime):
            with self.assertRaisesRegex(ValueError, "source database"):
                run_cycle._resolve_slot(args)

    def test_raises_when_trade_date_is_explicit_but_no_latest_snapshot_exists(self) -> None:
        args = run_cycle.RunCycleArgs(
            trade_date="2026-04-20",
            snapshot_time=None,
            floor_minutes=5,
            db_path=Path("/tmp/awin.db"),
            dry_run=True,
        )

        with patch.object(run_cycle, "_lookup_latest_qmt_slot", return_value=None):
            with self.assertRaisesRegex(ValueError, "2026-04-20"):
                run_cycle._resolve_slot(args)


if __name__ == "__main__":
    unittest.main()
