from __future__ import annotations

from pathlib import Path


def _parse_scalar(value: str):
    text = str(value).strip()
    if text == "":
        return ""
    lower = text.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if lower in {"null", "none"}:
        return None
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
    return text


def parse_markdown_frontmatter(path: Path) -> dict:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8", errors="ignore")
    if not text.startswith("---"):
        return {}
    end = text.find("\n---", 3)
    if end < 0:
        return {}
    block = text[3:end].strip("\n")
    meta: dict = {}
    current_section: str | None = None
    for raw_line in block.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue
        if line.startswith("  ") and current_section:
            child = line.strip()
            if ":" not in child:
                continue
            key, value = child.split(":", 1)
            meta.setdefault(current_section, {})[key.strip()] = _parse_scalar(value)
            continue
        if ":" not in line:
            current_section = None
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if value == "":
            meta[key] = {}
            current_section = key
        else:
            meta[key] = _parse_scalar(value)
            current_section = None
    return meta
