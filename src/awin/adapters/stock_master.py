"""stock_master interface.

Reads the local A-share master universe file and exposes the static stock pool
used by downstream snapshot, style and watchlist calculations.
"""

from __future__ import annotations

import json
from pathlib import Path

from awin.adapters.base import FileBackedAdapter
from awin.adapters.contracts import SourceHealth, StockMasterRow
from awin.config import get_app_config
from awin.utils.symbols import normalize_stock_code


class StockMasterAdapter(FileBackedAdapter):
    """Load the local stock master file as the static cross-market universe."""

    source_name = "stock_master"

    def __init__(self, root: Path | None = None) -> None:
        config = get_app_config()
        super().__init__(root or config.stock_master_path)

    def health(self) -> SourceHealth:
        if self.root.exists():
            return SourceHealth(source_name=self.source_name, source_status="ready")
        return SourceHealth(source_name=self.source_name, source_status="missing", detail=f"missing file: {self.root}")

    def load_rows(self) -> list[StockMasterRow]:
        if not self.root.exists():
            return []
        payload = json.loads(self.root.read_text(encoding="utf-8"))
        rows: list[StockMasterRow] = []
        for item in payload.get("data", []):
            stock_name = str(item.get("stock_name") or "").strip()
            rows.append(
                StockMasterRow(
                    symbol=str(item.get("symbol") or "").strip(),
                    stock_code=normalize_stock_code(item.get("stock_code") or item.get("symbol")),
                    stock_name=stock_name,
                    exchange=str(item.get("exchange") or "").strip() or None,
                    market=str(item.get("market_type") or "").strip() or None,
                    industry=str(item.get("industry") or "").strip() or None,
                    is_st="ST" in stock_name.upper(),
                    is_listed=not bool(item.get("is_delisted")),
                )
            )
        return rows
