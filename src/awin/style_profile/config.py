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
    history_windows = payload.get("history_windows")
    if not isinstance(history_windows, dict) or not history_windows:
        raise ConfigError(f"invalid style profile config at {path}: history_windows must be a non-empty object")
    score_weights = payload.get("score_weights")
    if not isinstance(score_weights, dict) or not score_weights:
        raise ConfigError(f"invalid style profile config at {path}: score_weights must be a non-empty object")
    business_label_score_weights = payload.get("business_label_score_weights")
    if not isinstance(business_label_score_weights, dict) or not business_label_score_weights:
        raise ConfigError(
            f"invalid style profile config at {path}: business_label_score_weights must be a non-empty object"
        )
    business_label_rules = _validate_rules_list(
        payload.get("business_label_rules"),
        path=path,
        section_name="business_label_rules",
        required_keys={"field", "source_score", "bands"},
    )

    normalized_history_windows: dict[str, int] = {}
    for key, value in history_windows.items():
        try:
            normalized_value = int(value)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"invalid style profile config at {path}: history_windows.{key} must be an integer") from exc
        if normalized_value <= 0:
            raise ConfigError(f"invalid style profile config at {path}: history_windows.{key} must be positive")
        normalized_history_windows[str(key)] = normalized_value

    normalized_score_weights: dict[str, dict[str, float]] = {}
    for score_name, score_rule in score_weights.items():
        if not isinstance(score_rule, dict) or not score_rule:
            raise ConfigError(f"invalid style profile config at {path}: score_weights.{score_name} must be a non-empty object")
        normalized_score_weights[str(score_name)] = {}
        for factor_name, weight in score_rule.items():
            try:
                normalized_score_weights[str(score_name)][str(factor_name)] = float(weight)
            except (TypeError, ValueError) as exc:
                raise ConfigError(
                    f"invalid style profile config at {path}: score_weights.{score_name}.{factor_name} must be numeric"
                ) from exc

    normalized_business_label_score_weights: dict[str, dict[str, float]] = {}
    for score_name, score_rule in business_label_score_weights.items():
        if not isinstance(score_rule, dict) or not score_rule:
            raise ConfigError(
                f"invalid style profile config at {path}: business_label_score_weights.{score_name} must be a non-empty object"
            )
        normalized_business_label_score_weights[str(score_name)] = {}
        for factor_name, weight in score_rule.items():
            try:
                normalized_business_label_score_weights[str(score_name)][str(factor_name)] = float(weight)
            except (TypeError, ValueError) as exc:
                raise ConfigError(
                    f"invalid style profile config at {path}: business_label_score_weights.{score_name}.{factor_name} must be numeric"
                ) from exc

    normalized_business_label_rules: list[dict[str, Any]] = []
    for idx, rule in enumerate(business_label_rules, start=1):
        field_name = str(rule.get("field") or "").strip()
        source_score = str(rule.get("source_score") or "").strip()
        bands = rule.get("bands")
        if not field_name:
            raise ConfigError(f"invalid style profile config at {path}: business_label_rules[{idx}].field is required")
        if not source_score:
            raise ConfigError(
                f"invalid style profile config at {path}: business_label_rules[{idx}].source_score is required"
            )
        if not isinstance(bands, list) or not bands:
            raise ConfigError(
                f"invalid style profile config at {path}: business_label_rules[{idx}].bands must be a non-empty list"
            )
        normalized_bands: list[dict[str, Any]] = []
        for band_idx, band in enumerate(bands, start=1):
            if not isinstance(band, dict):
                raise ConfigError(
                    f"invalid style profile config at {path}: business_label_rules[{idx}].bands[{band_idx}] must be an object"
                )
            label = str(band.get("label") or "").strip()
            if not label:
                raise ConfigError(
                    f"invalid style profile config at {path}: business_label_rules[{idx}].bands[{band_idx}].label is required"
                )
            try:
                min_score = float(band.get("min_score"))
            except (TypeError, ValueError) as exc:
                raise ConfigError(
                    f"invalid style profile config at {path}: business_label_rules[{idx}].bands[{band_idx}].min_score must be numeric"
                ) from exc
            normalized_bands.append({"label": label, "min_score": min_score})
        normalized_business_label_rules.append(
            {
                "field": field_name,
                "source_score": source_score,
                "bands": normalized_bands,
            }
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
        "history_windows": normalized_history_windows,
        "score_weights": normalized_score_weights,
        "business_label_score_weights": normalized_business_label_score_weights,
        "business_label_rules": normalized_business_label_rules,
    }
