from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.adapters import DcfHqZjSnapshotAdapter, QmtAshareSnapshot5mAdapter, ResearchCoverageAdapter, StockMasterAdapter, ThsConceptAdapter, ThsMarketOverviewAdapter
from awin.config import ConfigError, ENV_PATH, get_app_config


def main() -> None:
    payload: dict = {
        "env_path": str(ENV_PATH),
        "env_exists": ENV_PATH.exists(),
    }

    try:
        config = get_app_config()
        payload["config_status"] = "ready"
        payload["config"] = {
            "sqlite_path": str(config.sqlite_path),
            "stock_master_path": str(config.stock_master_path),
            "company_cards_dir": str(config.company_cards_dir),
            "onepage_dir": str(config.onepage_dir),
            "market_intel_dir": str(config.market_intel_dir),
            "ths_overlay_config_path": str(config.ths_overlay_config_path),
            "style_config_path": str(config.style_config_path),
            "ths_concept_map_path": str(config.ths_concept_map_path),
            "ths_market_overview_path": str(config.ths_market_overview_path),
            "qt_db": {
                "host": config.qt_db.host,
                "port": config.qt_db.port,
                "dbname": config.qt_db.dbname,
                "user": config.qt_db.user,
            },
            "fin_db": {
                "host": config.fin_db.host,
                "port": config.fin_db.port,
                "dbname": config.fin_db.dbname,
                "user": config.fin_db.user,
            },
        }
    except ConfigError as exc:
        payload["config_status"] = "error"
        payload["config_error"] = str(exc)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        raise SystemExit(1)

    try:
        import psycopg  # type: ignore

        payload["psycopg_status"] = "ready"
        payload["psycopg_version"] = getattr(psycopg, "__version__", "unknown")
    except Exception as exc:  # pragma: no cover
        payload["psycopg_status"] = "missing"
        payload["psycopg_error"] = repr(exc)

    payload["source_health"] = {
        "stock_master": StockMasterAdapter().health().to_dict(),
        "ths_concepts": ThsConceptAdapter().health().to_dict(),
        "research": ResearchCoverageAdapter().health().to_dict(),
        "qmt_ashare_snapshot_5m": QmtAshareSnapshot5mAdapter().health().to_dict(),
        "dcf_hq_zj_snapshot": DcfHqZjSnapshotAdapter().health().to_dict(),
        "ths_market_overview": ThsMarketOverviewAdapter().health().to_dict(),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
