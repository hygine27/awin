"""ths_app_hot_concept interface.

Reads the THS app hot-concept snapshot table and adds the app-side concept heat
ranking overlay used for deprecated debug / preopen-attention scenarios.

Status:
- deprecated
- not used by the production runtime pipeline
- retained only for manual validation, source comparison, and preopen observation
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from awin.adapters.base import DbBackedAdapter, SnapshotRequest
from awin.adapters.contracts import SourceHealth, ThsHotConceptRow
from awin.config import get_app_config
from awin.utils.structured_config import load_structured_config


def _canonical_mapping(overlay_config_path: Path) -> tuple[set[str], dict[str, str]]:
    payload = load_structured_config(overlay_config_path, label="overlay config")
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
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class ThsAppHotConceptAdapter(DbBackedAdapter):
    """Load the latest THS app hot-concept batch before the cutoff.

    Deprecated:
    This adapter is intentionally kept out of the production scoring flow because
    the source does not behave as a reliable intraday feed in current validation.
    """

    source_name = "ths_app_hot_concept"
    lifecycle_status = "deprecated"

    def __init__(self, overlay_config_path: Path | None = None) -> None:
        config = get_app_config()
        super().__init__(db_config=config.fin_db, dsn_label="fin")
        self.overlay_config_path = overlay_config_path or config.ths_overlay_config_path

    def health(self) -> SourceHealth:
        _, error = self._connect_with_error()
        if error is not None:
            return SourceHealth(
                source_name=self.source_name,
                source_status="missing",
                detail=f"deprecated source; {error}",
            )
        if not self.overlay_config_path.exists():
            return SourceHealth(
                source_name=self.source_name,
                source_status="missing",
                detail=f"deprecated source; missing file: {self.overlay_config_path}",
            )
        return SourceHealth(
            source_name=self.source_name,
            source_status="ready",
            detail="deprecated source; retained for debug / preopen_attention only",
        )

    def build_query(self, request: SnapshotRequest) -> tuple[str, dict[str, str]]:
        cutoff = datetime.fromisoformat(request.analysis_snapshot_ts.replace("Z", "+00:00"))
        cutoff_naive = cutoff.replace(tzinfo=None)
        sql = """
        with best_batch as (
          select max(created_at) as batch_ts
          from stg.ths_app_hot_concept_trade
          where created_at::date = %(trade_date)s::date
            and created_at <= %(cutoff_naive)s::timestamp
        ), chosen as (
          select coalesce(
            (select batch_ts from best_batch),
            (select max(created_at) from stg.ths_app_hot_concept_trade where created_at::date = %(trade_date)s::date),
            (select max(created_at) from stg.ths_app_hot_concept_trade)
          ) as batch_ts
        )
        select
          created_at::date::text as trade_date,
          created_at::text as batch_ts,
          plate_name as concept_name,
          rank as concept_rank,
          hot_score as concept_hot_score,
          hot_rank_chg as concept_rank_change,
          limit_up_tag
        from stg.ths_app_hot_concept_trade
        where created_at = (select batch_ts from chosen)
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
                    source_table="stg.ths_app_hot_concept_trade",
                    trade_date=payload.get("trade_date"),
                    batch_ts=str(payload["batch_ts"]),
                    concept_name=canonical_name,
                    concept_rank=_to_int(payload.get("concept_rank")),
                    concept_hot_score=_to_float(payload.get("concept_hot_score")),
                    concept_rank_change=_to_int(payload.get("concept_rank_change")),
                    limit_up_tag=str(payload.get("limit_up_tag") or "").strip() or None,
                )
            )
        return rows
