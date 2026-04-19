from __future__ import annotations

from pathlib import Path
from typing import Any

from awin.config import ConfigError, get_app_config
from awin.utils.structured_config import load_structured_config


def load_opportunity_rules(config_path: Path | None = None) -> dict[str, Any]:
    config = get_app_config()
    path = config_path or config.opportunity_config_path
    payload = load_structured_config(path, label="opportunity config")
    if not isinstance(payload, dict):
        raise ConfigError(f"invalid opportunity config at {path}: root must be an object")

    for section in (
        "long_score_caps",
        "meta_to_style_hint",
        "concept_priority",
        "theme_context_rules",
        "novelty_rules",
        "long_score_rules",
        "repeat_rules",
        "bucket_rules",
        "catchup_rules",
    ):
        if not isinstance(payload.get(section), dict):
            raise ConfigError(f"invalid opportunity config at {path}: {section} must be an object")
    return payload
