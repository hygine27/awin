from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

from awin.adapters.base import DbBackedAdapter, SnapshotRequest
from awin.adapters.contracts import SourceHealth, ThsHotConceptRow
from awin.config import get_app_config


def _canonical_mapping(overlay_config_path: Path) -> tuple[set[str], dict[str, str]]:
    payload = json.loads(overlay_config_path.read_text(encoding="utf-8"))
    whitelist = {str(item) for item in payload.get("concept_whitelist", []) if str(item).strip()}
    aliases = payload.get("concept_aliases", {})
    alias_to_canonical: dict[str, str] = {}
    for canonical, alias_list in aliases.items():
        canonical_name = str(canonical)
        alias_to_canonical[canonical_name] = canonical_name
        for alias in alias_list:
            alias_to_canonical[str(alias)] = canonical_name
    return whitelist, alias_to_canonical


def _canonicalize(name: str | None, *, whitelist: set[str], alias_to_canonical: dict[str, str]) -> str | None:
    text = str(name or "").strip()
    if not text:
        return None
    canonical = alias_to_canonical.get(text, text)
    if whitelist and canonical not in whitelist:
        return None
    return canonical


def _to_float(value) -> float | None:
    if value in {None, ""}:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    sign = -1.0 if text.startswith("-") else 1.0
    text = text.lstrip("+-")
    multiplier = 1.0
    if text.endswith("%"):
        multiplier = 0.01
        text = text[:-1]
    elif text.endswith("万亿"):
        multiplier = 1e12
        text = text[:-2]
    elif text.endswith("亿"):
        multiplier = 1e8
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 1e4
        text = text[:-1]
    try:
        return sign * float(text) * multiplier
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int | None:
    if value in {None, ""}:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"-?\d+", text)
    if match is None:
        return None
    try:
        return int(match.group(0))
    except (TypeError, ValueError):
        return None


class ThsCliHotConceptAdapter(DbBackedAdapter):
    source_name = "ths_cli_hot_concept"

    def __init__(self, overlay_config_path: Path | None = None) -> None:
        config = get_app_config()
        super().__init__(db_config=config.fin_db, dsn_label="fin")
        self.overlay_config_path = overlay_config_path or config.ths_overlay_config_path

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=error)
        if not self.overlay_config_path.exists():
            return SourceHealth(source_name=self.source_name, source_status="missing", detail=f"missing file: {self.overlay_config_path}")
        return SourceHealth(source_name=self.source_name, source_status="ready")

    def build_query(self, request: SnapshotRequest) -> tuple[str, dict[str, str]]:
        cutoff = datetime.fromisoformat(request.analysis_snapshot_ts.replace("Z", "+00:00"))
        cutoff_naive = cutoff.replace(tzinfo=None)
        sql = """
        with candidate_batches as (
          select distinct batch_ts
          from stg.ths_cli_hot_concept
          where trade_date = %(trade_date)s::date
            and batch_ts <= %(cutoff_naive)s::timestamp
        ), chosen as (
          select batch_ts
          from candidate_batches
          order by batch_ts desc
          limit 2
        ), fallback as (
          select distinct batch_ts
          from stg.ths_cli_hot_concept
          where trade_date = %(trade_date)s::date
          order by batch_ts desc
          limit 2
        )
        select
          trade_date::text as trade_date,
          batch_ts::text as batch_ts,
          concept_name,
          change_pct,
          speed_1min,
          main_net_amount,
          limit_up_count,
          rising_count,
          falling_count,
          leading_stock
        from stg.ths_cli_hot_concept
        where batch_ts in (
          select batch_ts from chosen
          union
          select batch_ts from fallback
        )
        """
        return sql, {
            "trade_date": request.trade_date,
            "cutoff_naive": cutoff_naive.isoformat(sep=" "),
        }

    def load_rows(self, request: SnapshotRequest) -> list[ThsHotConceptRow]:
        if not self.overlay_config_path.exists():
            return []
        sql, params = self.build_query(request)
        result = self._query_rows(sql, params)
        if result is None:
            return []

        whitelist, alias_to_canonical = _canonical_mapping(self.overlay_config_path)
        rows: list[ThsHotConceptRow] = []
        for payload in result:
            canonical_name = _canonicalize(payload.get("concept_name"), whitelist=whitelist, alias_to_canonical=alias_to_canonical)
            if canonical_name is None:
                continue
            rows.append(
                ThsHotConceptRow(
                    source_table="stg.ths_cli_hot_concept",
                    trade_date=payload.get("trade_date"),
                    batch_ts=str(payload["batch_ts"]),
                    concept_name=canonical_name,
                    limit_up_count=_to_int(payload.get("limit_up_count")),
                    rising_count=_to_int(payload.get("rising_count")),
                    falling_count=_to_int(payload.get("falling_count")),
                    leading_stock=str(payload.get("leading_stock") or "").strip() or None,
                    change_pct=_to_float(payload.get("change_pct")),
                    speed_1min=_to_float(payload.get("speed_1min")),
                    main_net_amount=_to_float(payload.get("main_net_amount")),
                )
            )
        return rows
