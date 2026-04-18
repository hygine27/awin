from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


StyleStatus = Literal["stable", "observation", "confirmation", "backswitch"]
AlertDecision = Literal["UPDATE", "NO_UPDATE"]
DisplayBucket = Literal["core_anchor", "new_long", "catchup", "warning"]
RiskTag = Literal["overheat", "weak", "warning"]


@dataclass(slots=True)
class SerializableDataclass:
    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RunContext(SerializableDataclass):
    run_id: str
    trade_date: str
    snapshot_time: str
    analysis_snapshot_ts: str
    round_seq: int


@dataclass(slots=True)
class StyleRankingItem(SerializableDataclass):
    style_name: str
    score: float | None = None
    eq_return: float | None = None
    up_ratio: float | None = None
    strong_ratio: float | None = None
    near_high_ratio: float | None = None
    activity_ratio: float | None = None
    spread_to_leader: float | None = None


@dataclass(slots=True)
class MetaThemeItem(SerializableDataclass):
    meta_theme: str
    score: float | None = None
    eq_return: float | None = None
    rank: int | None = None
    strongest_concepts: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MarketUnderstandingOutput(SerializableDataclass):
    confirmed_style: str | None = None
    latest_status: StyleStatus = "stable"
    latest_dominant_style: str | None = None
    market_regime: str | None = None
    top_styles: list[StyleRankingItem] = field(default_factory=list)
    top_meta_themes: list[MetaThemeItem] = field(default_factory=list)
    strongest_concepts: list[str] = field(default_factory=list)
    acceleration_concepts: list[str] = field(default_factory=list)
    concept_overlay_score_map: dict[str, float] = field(default_factory=dict)
    concept_overlay_rank_map: dict[str, int] = field(default_factory=dict)
    meta_theme_rank_map: dict[str, int] = field(default_factory=dict)
    meta_theme_eq_return_map: dict[str, float] = field(default_factory=dict)
    summary_line: str = ""
    evidence_lines: list[str] = field(default_factory=list)


@dataclass(slots=True)
class CandidateItem(SerializableDataclass):
    symbol: str
    stock_name: str
    display_bucket: DisplayBucket
    confidence_score: float
    themes: list[str] = field(default_factory=list)
    reason: str = ""
    display_line: str = ""
    best_meta_theme: str | None = None
    best_concept: str | None = None
    risk_tag: RiskTag | None = None
    research_hooks: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class OpportunityDiscoveryOutput(SerializableDataclass):
    core_anchor_watchlist: list[CandidateItem] = field(default_factory=list)
    new_long_watchlist: list[CandidateItem] = field(default_factory=list)
    catchup_watchlist: list[CandidateItem] = field(default_factory=list)


@dataclass(slots=True)
class RiskSurveillanceOutput(SerializableDataclass):
    short_watchlist: list[CandidateItem] = field(default_factory=list)


@dataclass(slots=True)
class AlertMaterial(SerializableDataclass):
    confirmed_style: str | None = None
    latest_status: str | None = None
    latest_dominant_style: str | None = None
    market_regime: str | None = None
    top_meta_themes: list[str] = field(default_factory=list)
    core_anchor_symbols: list[str] = field(default_factory=list)
    new_long_symbols: list[str] = field(default_factory=list)
    short_symbols: list[str] = field(default_factory=list)
    catchup_symbols: list[str] = field(default_factory=list)


@dataclass(slots=True)
class AlertChange(SerializableDataclass):
    field_name: str
    previous: Any
    current: Any
    change_type: str


@dataclass(slots=True)
class AlertDiffResult(SerializableDataclass):
    decision: AlertDecision
    changes: list[AlertChange] = field(default_factory=list)


@dataclass(slots=True)
class AlertOutput(SerializableDataclass):
    material: AlertMaterial
    diff_result: AlertDiffResult
    alert_body: str


@dataclass(slots=True)
class M0SnapshotBundle(SerializableDataclass):
    run_context: RunContext
    market_understanding: MarketUnderstandingOutput
    opportunity_discovery: OpportunityDiscoveryOutput
    risk_surveillance: RiskSurveillanceOutput
    alert_output: AlertOutput
