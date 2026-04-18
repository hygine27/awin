from __future__ import annotations

import re
from pathlib import Path

from awin.adapters.base import FileBackedAdapter, SnapshotRequest
from awin.adapters.contracts import ResearchCoverageRow, SourceHealth
from awin.config import get_app_config
from awin.utils.markdown import parse_markdown_frontmatter
from awin.utils.symbols import normalize_stock_code


def _label_company_card_level(raw: str | None) -> float:
    mapping = {
        "high": 1.0,
        "medium": 0.65,
        "low": 0.25,
        "高": 1.0,
        "中": 0.65,
        "低": 0.25,
    }
    return mapping.get(str(raw or "").strip().lower(), 0.0)


def _company_card_quality_score(meta: dict) -> float:
    coverage = meta.get("source_coverage") if isinstance(meta.get("source_coverage"), dict) else {}
    source_points = 0.0
    source_points += 0.25 if coverage.get("onepage") else 0.0
    source_points += 0.20 if coverage.get("summary") else 0.0
    source_points += 0.20 if coverage.get("intel") else 0.0
    sufficiency = _label_company_card_level(meta.get("information_sufficiency"))
    confidence = _label_company_card_level(meta.get("confidence"))
    if confidence <= 0.0:
        confidence_assessment = meta.get("confidence_assessment")
        if isinstance(confidence_assessment, dict):
            confidence = _label_company_card_level(confidence_assessment.get("overall"))
    tracking_text = str(meta.get("tracking_pool_recommendation") or "").strip()
    tracking_bonus = 0.20 if tracking_text and "否" not in tracking_text else 0.0
    return round(min(1.0, 0.35 * sufficiency + 0.25 * confidence + source_points + tracking_bonus), 3)


class ResearchCoverageAdapter(FileBackedAdapter):
    source_name = "research_coverage"

    def __init__(
        self,
        companies_root: Path | None = None,
        onepage_root: Path | None = None,
        intel_root: Path | None = None,
    ) -> None:
        config = get_app_config()
        super().__init__(companies_root or config.company_cards_dir)
        self.onepage_root = onepage_root or config.onepage_dir
        self.intel_root = intel_root or config.market_intel_dir

    def health(self) -> SourceHealth:
        existing = [path for path in [self.root, self.onepage_root, self.intel_root] if path.exists()]
        if existing:
            return SourceHealth(source_name=self.source_name, source_status="ready")
        return SourceHealth(source_name=self.source_name, source_status="missing", detail="no research directories found")

    def load_rows(self, request: SnapshotRequest) -> list[ResearchCoverageRow]:
        _ = request
        rows_by_symbol: dict[str, ResearchCoverageRow] = {}

        for path in sorted(self.onepage_root.glob("onepage-stock-*.md")) if self.onepage_root.exists() else []:
            match = re.search(r"onepage-stock-(\d{6}\.[A-Z]{2})-", path.name)
            if not match:
                continue
            symbol = match.group(1)
            rows_by_symbol.setdefault(symbol, ResearchCoverageRow(symbol=symbol))
            rows_by_symbol[symbol].onepage_path = str(path)
            rows_by_symbol[symbol].research_coverage_score += 0.35
            rows_by_symbol[symbol].research_hooks.append("onepage")

        for path in sorted(self.root.glob("*.md")) if self.root.exists() else []:
            meta = parse_markdown_frontmatter(path)
            symbol = str(meta.get("symbol") or path.name.split("_")[0]).strip()
            if not symbol:
                continue
            rows_by_symbol.setdefault(symbol, ResearchCoverageRow(symbol=symbol))
            row = rows_by_symbol[symbol]
            row.company_card_path = str(path)
            row.research_coverage_score += 0.40
            row.company_card_information_sufficiency = str(meta.get("information_sufficiency") or "").strip() or None
            row.company_card_confidence = str(meta.get("confidence") or "").strip() or None
            row.company_card_tracking_recommendation = str(meta.get("tracking_pool_recommendation") or "").strip() or None
            row.company_card_quality_score = max(row.company_card_quality_score, _company_card_quality_score(meta))
            hooks = [
                str(meta.get("theme") or "").strip(),
                str(meta.get("chain_position") or "").strip(),
                str(meta.get("company_role") or "").strip(),
            ]
            for hook in hooks:
                if hook and hook not in row.research_hooks:
                    row.research_hooks.append(hook)

            text = path.read_text(encoding="utf-8", errors="ignore")
            mention_match = re.search(r"近端市场情报命中\s*(\d+)\s*条", text)
            if mention_match:
                row.recent_intel_mentions = max(row.recent_intel_mentions, int(mention_match.group(1)))
            if row.recent_intel_mentions > 0 and "intel" not in row.research_hooks:
                row.research_hooks.append("intel")

        for row in rows_by_symbol.values():
            row.research_coverage_score = round(min(1.0, row.research_coverage_score), 3)
            row.research_hooks = [hook for hook in row.research_hooks if hook]

        return sorted(rows_by_symbol.values(), key=lambda item: item.symbol)
