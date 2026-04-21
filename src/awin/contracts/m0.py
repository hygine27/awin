from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


StyleStatus = Literal["stable", "observation", "confirmation", "backswitch"]
AlertDecision = Literal["UPDATE", "NO_UPDATE"]
DisplayBucket = Literal["core_anchor", "new_long", "catchup", "warning"]
RiskTag = Literal["overheat", "overheat_supported", "overheat_fading", "weak", "warning", "weakening"]


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
class MarketFundEvidence(SerializableDataclass):
    scope: str
    asof: str | None = None
    net_amount: float | None = None
    net_amount_rate: float | None = None
    super_large_net: float | None = None
    large_order_net: float | None = None
    middle_order_net: float | None = None
    small_order_net: float | None = None
    positive_stock_ratio: float | None = None
    positive_main_flow_ratio: float | None = None
    inflow_streak_days: int = 0
    outflow_streak_days: int = 0


@dataclass(slots=True)
class ThemeEvidenceItem(SerializableDataclass):
    meta_theme: str
    rank: int | None = None
    stock_count: int = 0
    avg_pct_chg_prev_close: float | None = None
    positive_stock_ratio: float | None = None
    current_main_net_inflow_sum: float | None = None
    current_main_flow_rate: float | None = None
    current_positive_main_flow_ratio: float | None = None
    comparison_window_label: str | None = None
    comparison_main_net_inflow_delta: float | None = None
    strongest_concepts: list[str] = field(default_factory=list)
    strongest_concept_flow_1d_map: dict[str, float] = field(default_factory=dict)
    leader_stocks: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MarketEvidenceBundle(SerializableDataclass):
    confirmed_style: str | None = None
    latest_status: StyleStatus = "stable"
    latest_dominant_style: str | None = None
    market_regime: str | None = None
    summary_line: str = ""
    evidence_lines: list[str] = field(default_factory=list)
    top_styles: list[StyleRankingItem] = field(default_factory=list)
    top_meta_themes: list[MetaThemeItem] = field(default_factory=list)
    strongest_concepts: list[str] = field(default_factory=list)
    acceleration_concepts: list[str] = field(default_factory=list)
    t1_market_fund: MarketFundEvidence | None = None
    intraday_market_fund: MarketFundEvidence | None = None
    theme_evidence: list[ThemeEvidenceItem] = field(default_factory=list)
    source_health: dict[str, dict[str, Any]] = field(default_factory=dict)


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
class StockEvidenceItem(SerializableDataclass):
    symbol: str
    stock_name: str
    role: str
    display_bucket: DisplayBucket
    confidence_score: float
    best_meta_theme: str | None = None
    best_concept: str | None = None
    theme_rank: int | None = None
    concept_overlay_rank: int | None = None
    risk_tag: RiskTag | None = None
    reason: str = ""
    themes: list[str] = field(default_factory=list)
    style_names: list[str] = field(default_factory=list)
    composite_style_labels: list[str] = field(default_factory=list)
    pct_chg_prev_close: float | None = None
    open_ret: float | None = None
    range_position: float | None = None
    amount: float | None = None
    money_pace_ratio: float | None = None
    volume_ratio: float | None = None
    turnover_rate: float | None = None
    amplitude: float | None = None
    main_net_inflow: float | None = None
    super_net: float | None = None
    large_net: float | None = None
    ret_3d: float | None = None
    ret_10d: float | None = None
    ret_20d: float | None = None
    main_net_amount_1d: float | None = None
    main_net_amount_5d_sum: float | None = None
    outflow_streak_days: int = 0
    price_flow_divergence_flag: bool = False
    research_coverage_score: float = 0.0
    research_hooks: list[str] = field(default_factory=list)
    candidate_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class StockEvidenceBundle(SerializableDataclass):
    focus_stocks: list[StockEvidenceItem] = field(default_factory=list)


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
    market_evidence_bundle: MarketEvidenceBundle = field(default_factory=MarketEvidenceBundle)
    stock_evidence_bundle: StockEvidenceBundle = field(default_factory=StockEvidenceBundle)
