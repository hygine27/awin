from __future__ import annotations

from pathlib import Path
from typing import Any

from awin.config import ConfigError, get_app_config
from awin.utils.structured_config import load_structured_config


def load_risk_rules(config_path: Path | None = None) -> dict[str, Any]:
    config = get_app_config()
    path = config_path or config.risk_config_path
    payload = load_structured_config(path, label="risk config")
    if not isinstance(payload, dict):
        raise ConfigError(f"invalid risk config at {path}: root must be an object")
    for section in ("theme_priority", "thresholds", "weights", "quota", "overheat_rules"):
        if not isinstance(payload.get(section), dict):
            raise ConfigError(f"invalid risk config at {path}: {section} must be an object")
    return payload
