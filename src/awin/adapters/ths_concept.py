"""ths_concept interface.

Reads the THS concept-to-stock mapping and the local overlay config so each
stock can be mapped into monitored concepts and meta-themes.
"""

from __future__ import annotations

from pathlib import Path

from awin.adapters.base import FileBackedAdapter, SnapshotRequest
from awin.adapters.contracts import SourceHealth, ThsConceptRow
from awin.config import get_app_config
from awin.utils.structured_config import load_structured_config
from awin.utils.symbols import infer_symbol_from_stock_code, normalize_stock_code


class ThsConceptAdapter(FileBackedAdapter):
    """Load THS concept membership and normalize it into monitored themes."""

    source_name = "ths_concepts"

    def __init__(
        self,
        concept_map_path: Path | None = None,
        overlay_config_path: Path | None = None,
    ) -> None:
        config = get_app_config()
        super().__init__(concept_map_path or config.ths_concept_map_path)
        self.overlay_config_path = overlay_config_path or config.ths_overlay_config_path

    def health(self) -> SourceHealth:
        missing = []
        if not self.root.exists():
            missing.append(str(self.root))
        if not self.overlay_config_path.exists():
            missing.append(str(self.overlay_config_path))
        if missing:
            return SourceHealth(
                source_name=self.source_name,
                source_status="missing",
                detail="missing file: " + ", ".join(missing),
            )
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def load_rows(self, request: SnapshotRequest) -> list[ThsConceptRow]:
        _ = request
        if not self.root.exists() or not self.overlay_config_path.exists():
            return []

        concept_map_payload = load_structured_config(self.root, label="ths concept map")
        overlay_payload = load_structured_config(self.overlay_config_path, label="overlay config")
        concept_whitelist = set(overlay_payload.get("concept_whitelist", []))
        aliases = overlay_payload.get("concept_aliases", {})
        alias_to_canonical = {}
        for canonical, alias_list in aliases.items():
            alias_to_canonical[canonical] = canonical
            for alias in alias_list:
                alias_to_canonical[str(alias)] = canonical

        concept_to_meta: dict[str, str] = {}
        for meta_theme, concepts in overlay_payload.get("meta_themes", {}).items():
            for concept in concepts:
                canonical = alias_to_canonical.get(str(concept), str(concept))
                concept_to_meta[canonical] = str(meta_theme)

        rows: list[ThsConceptRow] = []
        for raw_concept, codes in concept_map_payload.get("data", {}).items():
            canonical_concept = alias_to_canonical.get(str(raw_concept), str(raw_concept))
            if concept_whitelist and canonical_concept not in concept_whitelist:
                continue
            meta_theme = concept_to_meta.get(canonical_concept)
            for code in codes:
                stock_code = normalize_stock_code(code)
                rows.append(
                    ThsConceptRow(
                        symbol=infer_symbol_from_stock_code(stock_code),
                        stock_code=stock_code,
                        concept_name=canonical_concept,
                        meta_theme=meta_theme,
                    )
                )
        return rows
