from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from awin.config import ConfigError


def load_structured_config(path: Path, *, label: str) -> Any:
    suffix = path.suffix.lower()
    text = path.read_text(encoding="utf-8")
    try:
        if suffix == ".json":
            return json.loads(text)
        if suffix in {".yaml", ".yml"}:
            return yaml.safe_load(text)
    except Exception as exc:  # pragma: no cover - exercised by callers
        raise ConfigError(f"invalid {label} at {path}: {exc}") from exc
    raise ConfigError(f"invalid {label} at {path}: unsupported file extension {path.suffix}")
