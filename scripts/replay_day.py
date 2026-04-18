from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.config import get_app_config
from awin.replay import build_day_replay_json, build_day_replay_markdown


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay awin runs for one trade date.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--db-path", type=Path, default=get_app_config().sqlite_path)
    parser.add_argument("--format", choices=["json", "markdown"], default="json")
    args = parser.parse_args()
    if args.format == "markdown":
        print(build_day_replay_markdown(args.db_path, args.trade_date))
        return
    print(build_day_replay_json(args.db_path, args.trade_date))


if __name__ == "__main__":
    main()
