from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = PROJECT_ROOT / ".env"


class ConfigError(RuntimeError):
    """Raised when required awin configuration is missing or invalid."""


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    sqlite_path: Path
    stock_master_path: Path
    company_cards_dir: Path
    onepage_dir: Path
    market_intel_dir: Path
    ths_overlay_config_path: Path
    style_config_path: Path
    ths_concept_map_path: Path
    ths_market_overview_path: Path
    dcf_max_freshness_minutes: float
    dcf_min_rows_abs: int
    dcf_min_completeness_ratio: float
    qt_db: DbConfig
    fin_db: DbConfig


def _parse_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        raise ConfigError(f"missing .env file: {path}")

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        values[key] = value
    return values


def _require(env: dict[str, str], key: str) -> str:
    value = env.get(key, "").strip()
    if not value:
        raise ConfigError(f"missing required config key in .env: {key}")
    return value


def _resolve_path(raw: str) -> Path:
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def _build_db_config(env: dict[str, str], prefix: str) -> DbConfig:
    return DbConfig(
        host=_require(env, f"{prefix}_HOST"),
        port=int(_require(env, f"{prefix}_PORT")),
        dbname=_require(env, f"{prefix}_NAME"),
        user=_require(env, f"{prefix}_USER"),
        password=_require(env, f"{prefix}_PASSWORD"),
    )


@lru_cache(maxsize=1)
def get_app_config() -> AppConfig:
    env = _parse_env_file(ENV_PATH)
    return AppConfig(
        project_root=PROJECT_ROOT,
        sqlite_path=_resolve_path(_require(env, "AWIN_SQLITE_PATH")),
        stock_master_path=_resolve_path(_require(env, "AWIN_STOCK_MASTER_PATH")),
        company_cards_dir=_resolve_path(_require(env, "AWIN_COMPANY_CARDS_DIR")),
        onepage_dir=_resolve_path(_require(env, "AWIN_ONEPAGE_DIR")),
        market_intel_dir=_resolve_path(_require(env, "AWIN_MARKET_INTEL_DIR")),
        ths_overlay_config_path=_resolve_path(_require(env, "AWIN_THS_OVERLAY_CONFIG_PATH")),
        style_config_path=_resolve_path(_require(env, "AWIN_STYLE_CONFIG_PATH")),
        ths_concept_map_path=_resolve_path(_require(env, "AWIN_THS_CONCEPT_MAP_PATH")),
        ths_market_overview_path=_resolve_path(_require(env, "AWIN_THS_MARKET_OVERVIEW_PATH")),
        dcf_max_freshness_minutes=float(_require(env, "AWIN_DCF_MAX_FRESHNESS_MINUTES")),
        dcf_min_rows_abs=int(_require(env, "AWIN_DCF_MIN_ROWS_ABS")),
        dcf_min_completeness_ratio=float(_require(env, "AWIN_DCF_MIN_COMPLETENESS_RATIO")),
        qt_db=_build_db_config(env, "QT_DB"),
        fin_db=_build_db_config(env, "FIN_DB"),
    )
