from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.config import ENV_PATH, get_app_config


class ConfigRuntimeTestCase(unittest.TestCase):
    def test_env_file_exists(self) -> None:
        self.assertTrue(ENV_PATH.exists())

    def test_strict_config_loads_required_keys(self) -> None:
        config = get_app_config()
        self.assertTrue(str(config.sqlite_path))
        self.assertTrue(str(config.stock_master_path))
        self.assertTrue(str(config.style_profile_config_path))
        self.assertTrue(str(config.opportunity_config_path))
        self.assertTrue(str(config.ths_market_overview_path))
        self.assertTrue(config.qt_db.host)
        self.assertTrue(config.fin_db.host)


if __name__ == "__main__":
    unittest.main()
