from __future__ import annotations

from pathlib import Path
from typing import Any

from awin.config import ConfigError, get_app_config
from awin.utils.structured_config import load_structured_config


def _validate_rules_list(
    payload: Any,
    *,
    path: Path,
    section_name: str,
    required_keys: set[str],
) -> list[dict[str, Any]]:
    if not isinstance(payload, list) or not payload:
        raise ConfigError(f"invalid style profile config at {path}: {section_name} must be a non-empty list")
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise ConfigError(f"invalid style profile config at {path}: {section_name}[{idx}] must be an object")
        missing = required_keys - set(item)
        if missing:
            raise ConfigError(
                f"invalid style profile config at {path}: {section_name}[{idx}] missing keys: {', '.join(sorted(missing))}"
            )
        out.append(item)
    return out


def load_style_profile_rules(config_path: Path | None = None) -> dict[str, Any]:
    config = get_app_config()
    path = config_path or config.style_profile_config_path
    payload = load_structured_config(path, label="style profile config")

    if not isinstance(payload, dict):
        raise ConfigError(f"invalid style profile config at {path}: root must be an object")

    ownership = payload.get("ownership")
    if not isinstance(ownership, dict):
        raise ConfigError(f"invalid style profile config at {path}: ownership must be an object")
    default_label = str(ownership.get("default_label") or "").strip()
    if not default_label:
        raise ConfigError(f"invalid style profile config at {path}: ownership.default_label is required")
    ownership_rules = _validate_rules_list(
        ownership.get("rules"),
        path=path,
        section_name="ownership.rules",
        required_keys={"label", "match_any"},
    )

    for idx, item in enumerate(ownership_rules, start=1):
        if not isinstance(item.get("match_any"), list) or not item.get("match_any"):
            raise ConfigError(
                f"invalid style profile config at {path}: ownership.rules[{idx}].match_any must be a non-empty list"
            )

    size_bucket_abs_rules = _validate_rules_list(
        payload.get("size_bucket_abs_rules"),
        path=path,
        section_name="size_bucket_abs_rules",
        required_keys={"label", "min_float_mv"},
    )
    capacity_bucket_rules = _validate_rules_list(
        payload.get("capacity_bucket_rules"),
        path=path,
        section_name="capacity_bucket_rules",
        required_keys={"label", "min_float_mv"},
    )
    size_bucket_pct_rules = _validate_rules_list(
        payload.get("size_bucket_pct_rules"),
        path=path,
        section_name="size_bucket_pct_rules",
        required_keys={"label", "min_rank_pct"},
    )
    composite_label_rules = _validate_rules_list(
        payload.get("composite_label_rules"),
        path=path,
        section_name="composite_label_rules",
        required_keys={"label"},
    )

    return {
        "ownership": {
            "default_label": default_label,
            "rules": ownership_rules,
        },
        "size_bucket_abs_rules": size_bucket_abs_rules,
        "capacity_bucket_rules": capacity_bucket_rules,
        "size_bucket_pct_rules": size_bucket_pct_rules,
        "composite_label_rules": composite_label_rules,
    }
