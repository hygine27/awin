from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


SourceStatus = Literal["ready", "degraded", "missing", "fallback"]


@dataclass(slots=True)
class SerializableDataclass:
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SourceHealth(SerializableDataclass):
    source_name: str
    source_status: SourceStatus
    freshness_seconds: int | None = None
    coverage_ratio: float | None = None
    fallback_used: bool = False
    detail: str = ""


@dataclass(slots=True)
class StockMasterRow(SerializableDataclass):
    symbol: str
    stock_code: str
    stock_name: str | None = None
    exchange: str | None = None
    market: str | None = None
    industry: str | None = None
    is_st: bool = False
    is_listed: bool = True


@dataclass(slots=True)
class QmtSnapshotRow(SerializableDataclass):
    symbol: str
    stock_code: str
    trade_date: str
    snapshot_time: str
    last_price: float | None = None
    last_close: float | None = None
    open_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    volume: float | None = None
    amount: float | None = None
    bid_price1: float | None = None
    ask_price1: float | None = None
    bid_volume1: float | None = None
    ask_volume1: float | None = None


@dataclass(slots=True)
class QmtBar1dRow(SerializableDataclass):
    symbol: str
    stock_code: str
    trade_date: str
    open_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    close_price: float | None = None
    volume: float | None = None
    amount: float | None = None
    pre_close: float | None = None
    source_health: SourceHealth | None = None


@dataclass(slots=True)
class DcfSnapshotRow(SerializableDataclass):
    symbol: str
    trade_date: str
    vendor_batch_ts: str | None = None
    turnover_rate: float | None = None
    volume_ratio: float | None = None
    amplitude: float | None = None
    float_mkt_cap: float | None = None
    total_mkt_cap: float | None = None
    ret_3d: float | None = None
    ret_5d: float | None = None
    ret_10d: float | None = None
    ret_20d: float | None = None
    main_net_inflow: float | None = None
    super_net: float | None = None
    large_net: float | None = None
    source_health: SourceHealth | None = None


@dataclass(slots=True)
class ThsConceptRow(SerializableDataclass):
    symbol: str
    stock_code: str
    concept_name: str
    meta_theme: str | None = None
    concept_rank: int | None = None
    concept_hot_score: float | None = None
    concept_rank_change: int | None = None
    concept_limit_up_count: int | None = None


@dataclass(slots=True)
class ThsHotConceptRow(SerializableDataclass):
    source_table: str
    trade_date: str | None
    batch_ts: str
    concept_name: str
    concept_rank: int | None = None
    concept_hot_score: float | None = None
    concept_rank_change: int | None = None
    limit_up_count: int | None = None
    limit_up_tag: str | None = None
    rising_count: int | None = None
    falling_count: int | None = None
    leading_stock: str | None = None
    change_pct: float | None = None
    speed_1min: float | None = None
    main_net_amount: float | None = None
    board_score_hint: float | None = None


@dataclass(slots=True)
class ResearchCoverageRow(SerializableDataclass):
    symbol: str
    onepage_path: str | None = None
    company_card_path: str | None = None
    recent_intel_mentions: int = 0
    research_coverage_score: float = 0.0
    company_card_information_sufficiency: str | None = None
    company_card_confidence: str | None = None
    company_card_tracking_recommendation: str | None = None
    company_card_quality_score: float = 0.0
    research_hooks: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SnapshotSourceBundle(SerializableDataclass):
    stock_master: list[StockMasterRow] = field(default_factory=list)
    qmt_snapshot: list[QmtSnapshotRow] = field(default_factory=list)
    dcf_snapshot: list[DcfSnapshotRow] = field(default_factory=list)
    ths_concepts: list[ThsConceptRow] = field(default_factory=list)
    ths_hot_concepts: list[ThsHotConceptRow] = field(default_factory=list)
    research_coverage: list[ResearchCoverageRow] = field(default_factory=list)
    source_health: list[SourceHealth] = field(default_factory=list)
