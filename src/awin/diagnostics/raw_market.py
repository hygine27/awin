from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any, Mapping

from awin.adapters import (
    DcfHqZjSnapshotAdapter,
    QmtAshareSnapshot5mAdapter,
    SnapshotRequest,
    StockMasterAdapter,
    ThsCliHotConceptAdapter,
    ThsConceptAdapter,
    ThsMarketOverviewAdapter,
    TsDailyBasicAdapter,
    TsFinaIndicatorAdapter,
    TsIndexMemberAllAdapter,
    TsStockBasicAdapter,
    TsStyleDailyMetricsAdapter,
)
from awin.diagnostics.intraday_sources import collect_intraday_source_state
from awin.market_understanding.engine import load_overlay_config, load_style_baskets, load_style_score_weights
from awin.style_matching import style_rule_matches
from awin.style_profile import build_style_profiles


def _safe_div(numerator: float | int, denominator: float | int) -> float | None:
    if denominator in {0, 0.0}:
        return None
    return float(numerator) / float(denominator)


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _range_position(last_price: float | None, high_price: float | None, low_price: float | None) -> float | None:
    if last_price is None or high_price is None or low_price is None:
        return None
    spread = float(high_price) - float(low_price)
    if spread <= 0:
        return None
    return max(0.0, min(1.0, (float(last_price) - float(low_price)) / spread))


def _pct_chg_prev_close(last_price: float | None, last_close: float | None) -> float | None:
    if last_price is None or last_close in {None, 0, 0.0}:
        return None
    return float(last_price) / float(last_close) - 1.0


def _score_rows(rows: list[dict[str, Any]], *, score_field: str, weights: Mapping[str, float]) -> list[dict[str, Any]]:
    if not rows:
        return []
    metrics = ["eq_return", "up_ratio", "strong_ratio", "near_high_ratio", "activity_ratio"]
    for metric in metrics:
        valid_values = sorted(row[metric] for row in rows if row[metric] is not None)
        rank_map = {value: idx / len(valid_values) for idx, value in enumerate(valid_values, start=1)} if valid_values else {}
        for row in rows:
            row[f"{metric}_rank"] = rank_map.get(row[metric], 0.0) if row[metric] is not None else 0.0
    for row in rows:
        row[score_field] = round(sum(row[f"{metric}_rank"] * float(weights.get(metric, 0.0)) for metric in metrics), 4)
    rows.sort(key=lambda item: (item[score_field], item.get("eq_return") or -999), reverse=True)
    return rows


def _build_percentile_map(rows: list[dict[str, Any]], field: str, *, reverse: bool = False) -> dict[str, float]:
    valid = [(str(row["concept_name"]), row.get(field)) for row in rows if row.get(field) is not None]
    if not valid:
        return {}
    valid.sort(key=lambda item: item[1], reverse=reverse)
    total = len(valid)
    return {concept_name: idx / total for idx, (concept_name, _) in enumerate(valid, start=1)}


def _style_profile_field(profile: Mapping[str, Any] | None, field_name: str) -> str | None:
    if profile is None:
        return None
    value = profile.get(field_name)
    text = str(value or "").strip()
    return text or None


def _market_type_summary(stock_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in stock_rows:
        if row.get("pct_chg_prev_close") is None:
            continue
        buckets[str(row.get("market") or "未识别")].append(row)
    out: list[dict[str, Any]] = []
    for market_type, members in buckets.items():
        pct_values = [item["pct_chg_prev_close"] for item in members if item["pct_chg_prev_close"] is not None]
        out.append(
            {
                "market_type": market_type,
                "member_count": len(members),
                "eq_return": _average(pct_values),
                "up_ratio": _safe_div(sum((item["pct_chg_prev_close"] or 0.0) > 0 for item in members), len(members)),
            }
        )
    out.sort(key=lambda item: (item["eq_return"] or -999, item["up_ratio"] or -999), reverse=True)
    return out


def _describe_big_style(style_rows: list[dict[str, Any]], market_type_rows: list[dict[str, Any]]) -> str:
    if not style_rows:
        return "暂无"
    leader = style_rows[0]
    runner_up = style_rows[1] if len(style_rows) >= 2 else None
    best_market_type = market_type_rows[0]["market_type"] if market_type_rows else None
    leader_name = str(leader["style_name"])

    if (
        leader_name == "红利价值"
        and runner_up is not None
        and str(runner_up["style_name"]) == "科技成长"
        and float(leader["raw_score"] - runner_up["raw_score"]) <= 0.15
        and best_market_type in {"创业板", "科创板"}
    ):
        return "科技成长偏强（混合轮动）"

    if runner_up is not None and float(leader["raw_score"] - runner_up["raw_score"]) <= 0.08:
        return f"混合轮动（{leader_name} / {runner_up['style_name']})"
    return leader_name


def _describe_direction(meta_rows: list[dict[str, Any]]) -> tuple[str, list[str]]:
    if not meta_rows:
        return "暂无", []
    top_names = [str(row["meta_theme"]) for row in meta_rows[:4]]
    leader_score = float(meta_rows[0]["score"])
    third_score = float(meta_rows[2]["score"]) if len(meta_rows) >= 3 else None
    fourth_score = float(meta_rows[3]["score"]) if len(meta_rows) >= 4 else None
    if third_score is not None and third_score >= leader_score - 0.08:
        return "混合轮动", top_names
    if fourth_score is not None and fourth_score >= leader_score - 0.14:
        return "混合轮动", top_names
    return str(meta_rows[0]["meta_theme"]), top_names


@dataclass(slots=True)
class RawMarketReport:
    request: dict[str, str]
    market_environment: dict[str, Any]
    source_validation: dict[str, Any]
    summary_line: str
    style_ranking: list[dict[str, Any]]
    market_type_ranking: list[dict[str, Any]]
    concept_stock_only_ranking: list[dict[str, Any]]
    concept_cli_overlay_ranking: list[dict[str, Any]]
    meta_theme_stock_only_ranking: list[dict[str, Any]]
    meta_theme_cli_overlay_ranking: list[dict[str, Any]]
    manual_judgement: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_raw_market_report(request: SnapshotRequest) -> RawMarketReport:
    style_baskets, thresholds = load_style_baskets()
    style_score_weights = load_style_score_weights()
    overlay_config = load_overlay_config()
    overlay_thresholds = overlay_config.get("thresholds", {})
    min_constituents = int(thresholds.get("min_constituents", 12))
    strong_move_pct = float(thresholds.get("strong_move_pct", 0.02))
    near_high_threshold = float(thresholds.get("near_high_threshold", 0.8))
    active_pace_threshold = float(thresholds.get("active_pace_threshold", 1.2))
    min_concept_members = int(overlay_thresholds.get("min_members", 8))
    cli_weight = 0.22

    source_state = collect_intraday_source_state(request)

    stock_master_adapter = StockMasterAdapter()
    ths_adapter = ThsConceptAdapter()
    qmt_adapter = QmtAshareSnapshot5mAdapter()
    dcf_adapter = DcfHqZjSnapshotAdapter()
    cli_adapter = ThsCliHotConceptAdapter()
    market_overview_adapter = ThsMarketOverviewAdapter()
    ts_stock_basic_adapter = TsStockBasicAdapter()
    ts_daily_basic_adapter = TsDailyBasicAdapter()
    ts_index_member_all_adapter = TsIndexMemberAllAdapter()
    ts_style_daily_metrics_adapter = TsStyleDailyMetricsAdapter()
    ts_fina_indicator_adapter = TsFinaIndicatorAdapter()

    stock_master = stock_master_adapter.load_rows()
    ths_rows = ths_adapter.load_rows(request)
    qmt_rows = qmt_adapter.load_rows(request)
    dcf_rows, _ = dcf_adapter.load_rows_with_health(request)
    cli_rows = cli_adapter.load_rows(request)
    market_tape = market_overview_adapter.load_market_tape()
    ts_stock_basic_rows, _ = ts_stock_basic_adapter.load_rows_with_health()
    ts_daily_basic_rows, _ = ts_daily_basic_adapter.load_rows_with_health(request.trade_date)
    ts_index_member_all_rows, _ = ts_index_member_all_adapter.load_rows_with_health(request.trade_date)
    ts_style_daily_metric_rows, _ = ts_style_daily_metrics_adapter.load_rows_with_health(request.trade_date)
    ts_fina_indicator_rows, _ = ts_fina_indicator_adapter.load_rows_with_health(request.trade_date)

    style_profiles = build_style_profiles(
        ts_stock_basic_rows,
        ts_daily_basic_rows,
        ts_index_member_all_rows,
        daily_metric_rows=ts_style_daily_metric_rows,
        fina_indicator_rows=ts_fina_indicator_rows,
        trade_date=request.trade_date.replace("-", ""),
    )
    style_profile_by_symbol = {item.symbol: item.to_dict() for item in style_profiles}
    master_by_symbol = {item.symbol: item for item in stock_master if item.is_listed and not item.is_st}
    dcf_by_symbol = {item.symbol: item for item in dcf_rows}

    concept_members_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    concept_meta_by_name: dict[str, str] = {}
    for row in ths_rows:
        if row.meta_theme:
            concept_meta_by_name[row.concept_name] = row.meta_theme
        concept_members_by_symbol[row.symbol].append(
            {
                "concept_name": row.concept_name,
                "meta_theme": row.meta_theme,
            }
        )

    stock_rows: list[dict[str, Any]] = []
    style_members: dict[str, list[dict[str, Any]]] = defaultdict(list)
    concept_members: dict[str, list[dict[str, Any]]] = defaultdict(list)
    meta_members: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for qmt in qmt_rows:
        master = master_by_symbol.get(qmt.symbol)
        if master is None:
            continue
        dcf = dcf_by_symbol.get(qmt.symbol)
        style_profile = style_profile_by_symbol.get(qmt.symbol)
        mapped_concepts = concept_members_by_symbol.get(qmt.symbol, [])
        concept_names = sorted({item["concept_name"] for item in mapped_concepts if item["concept_name"]})
        meta_themes = sorted({item["meta_theme"] for item in mapped_concepts if item["meta_theme"]})
        row = {
            "symbol": qmt.symbol,
            "stock_name": master.stock_name,
            "market": master.market,
            "industry": master.industry,
            "amount": float(qmt.amount) if qmt.amount is not None else None,
            "pct_chg_prev_close": _pct_chg_prev_close(qmt.last_price, qmt.last_close),
            "range_position": _range_position(qmt.last_price, qmt.high_price, qmt.low_price),
            "volume_ratio": dcf.volume_ratio if dcf else None,
            "concepts": concept_names,
            "meta_themes": meta_themes,
        }
        stock_rows.append(row)

        for style_name, style_rule in style_baskets.items():
            if style_rule_matches(
                style_rule,
                industry=master.industry,
                market_type=master.market,
                ownership_style=_style_profile_field(style_profile, "ownership_style"),
                size_bucket_abs=_style_profile_field(style_profile, "size_bucket_abs"),
                size_bucket_pct=_style_profile_field(style_profile, "size_bucket_pct"),
                capacity_bucket=_style_profile_field(style_profile, "capacity_bucket"),
                composite_labels=list(style_profile.get("composite_style_labels") or []) if style_profile else [],
            ):
                style_members[style_name].append(row)
        for concept_name in concept_names:
            concept_members[concept_name].append(row)
        for meta_theme in meta_themes:
            meta_members[meta_theme].append(row)

    style_rows: list[dict[str, Any]] = []
    for style_name, members in style_members.items():
        if len(members) < min_constituents:
            continue
        pct_values = [item["pct_chg_prev_close"] for item in members if item["pct_chg_prev_close"] is not None]
        style_rows.append(
            {
                "style_name": style_name,
                "member_count": len(members),
                "eq_return": _average(pct_values),
                "up_ratio": _safe_div(sum((item["pct_chg_prev_close"] or 0.0) > 0 for item in members), len(members)),
                "strong_ratio": _safe_div(sum((item["pct_chg_prev_close"] or 0.0) >= strong_move_pct for item in members), len(members)),
                "near_high_ratio": _safe_div(sum((item["range_position"] or 0.0) >= near_high_threshold for item in members), len(members)),
                "activity_ratio": _safe_div(sum((item["volume_ratio"] or 0.0) >= active_pace_threshold for item in members), len(members)),
            }
        )
    style_rows = _score_rows(style_rows, score_field="raw_score", weights=style_score_weights)

    concept_rows: list[dict[str, Any]] = []
    for concept_name, members in concept_members.items():
        if len(members) < min_concept_members:
            continue
        pct_values = [item["pct_chg_prev_close"] for item in members if item["pct_chg_prev_close"] is not None]
        concept_rows.append(
            {
                "concept_name": concept_name,
                "meta_theme": concept_meta_by_name.get(concept_name),
                "member_count": len(members),
                "eq_return": _average(pct_values),
                "up_ratio": _safe_div(sum((item["pct_chg_prev_close"] or 0.0) > 0 for item in members), len(members)),
                "strong_ratio": _safe_div(sum((item["pct_chg_prev_close"] or 0.0) >= strong_move_pct for item in members), len(members)),
                "near_high_ratio": _safe_div(sum((item["range_position"] or 0.0) >= near_high_threshold for item in members), len(members)),
                "activity_ratio": _safe_div(sum((item["volume_ratio"] or 0.0) >= active_pace_threshold for item in members), len(members)),
                "amount_sum": sum(item.get("amount") or 0.0 for item in members),
            }
        )
    concept_rows = _score_rows(concept_rows, score_field="stock_score", weights=style_score_weights)

    latest_cli_batch = max((row.batch_ts for row in cli_rows), default=None)
    cli_latest_rows = [row for row in cli_rows if row.batch_ts == latest_cli_batch] if latest_cli_batch else []
    cli_score_rows = [
        {
            "concept_name": row.concept_name,
            "change_pct": row.change_pct,
            "speed_1min": row.speed_1min,
            "main_net_amount": row.main_net_amount,
            "limit_up_count": row.limit_up_count,
            "rise_spread": ((row.rising_count or 0) - (row.falling_count or 0)) if row.rising_count is not None or row.falling_count is not None else None,
        }
        for row in cli_latest_rows
    ]
    cli_change_pct = _build_percentile_map(cli_score_rows, "change_pct")
    cli_speed_pct = _build_percentile_map(cli_score_rows, "speed_1min")
    cli_flow_pct = _build_percentile_map(cli_score_rows, "main_net_amount")
    cli_limit_pct = _build_percentile_map(cli_score_rows, "limit_up_count")
    cli_rise_spread_pct = _build_percentile_map(cli_score_rows, "rise_spread")

    cli_score_map: dict[str, float] = {}
    for row in cli_score_rows:
        concept_name = str(row["concept_name"])
        cli_score_map[concept_name] = round(
            0.40 * cli_change_pct.get(concept_name, 0.0)
            + 0.20 * cli_speed_pct.get(concept_name, 0.0)
            + 0.20 * cli_flow_pct.get(concept_name, 0.0)
            + 0.10 * cli_limit_pct.get(concept_name, 0.0)
            + 0.10 * cli_rise_spread_pct.get(concept_name, 0.0),
            4,
        )

    for row in concept_rows:
        cli_score = cli_score_map.get(str(row["concept_name"]))
        row["cli_score"] = cli_score
        row["cli_overlay_score"] = round((1.0 - cli_weight) * row["stock_score"] + cli_weight * float(cli_score or 0.0), 4)
    concept_stock_only_rows = sorted(concept_rows, key=lambda item: (item["stock_score"], item["eq_return"] or -999), reverse=True)
    concept_cli_overlay_rows = sorted(concept_rows, key=lambda item: (item["cli_overlay_score"], item["eq_return"] or -999), reverse=True)

    overlay_payload = load_overlay_config()
    concept_to_meta = {
        str(concept): str(meta_theme)
        for meta_theme, concepts in overlay_payload.get("meta_themes", {}).items()
        for concept in concepts
    }
    for row in concept_stock_only_rows:
        if not row.get("meta_theme"):
            row["meta_theme"] = concept_to_meta.get(str(row["concept_name"]))
    for row in concept_cli_overlay_rows:
        if not row.get("meta_theme"):
            row["meta_theme"] = concept_to_meta.get(str(row["concept_name"]))

    def build_meta_rows(score_field: str) -> list[dict[str, Any]]:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in concept_rows:
            meta_theme = str(row.get("meta_theme") or "").strip()
            if not meta_theme:
                continue
            groups[meta_theme].append(row)
        meta_rows: list[dict[str, Any]] = []
        for meta_theme, members in groups.items():
            top_members = sorted(members, key=lambda item: (item[score_field], item["eq_return"] or -999), reverse=True)[:3]
            unique_symbols = {item["symbol"] for item in stock_rows if meta_theme in item["meta_themes"]}
            amount_sum = sum((item.get("amount") or 0.0) for item in stock_rows if meta_theme in item["meta_themes"])
            meta_rows.append(
                {
                    "meta_theme": meta_theme,
                    "score": round(_average([float(item[score_field]) for item in top_members]) or 0.0, 4),
                    "stock_count": len(unique_symbols),
                    "amount_sum": amount_sum,
                    "top_concepts": [str(item["concept_name"]) for item in top_members],
                }
            )
        meta_rows.sort(key=lambda item: (item["score"], item["amount_sum"]), reverse=True)
        return meta_rows

    meta_stock_only_rows = build_meta_rows("stock_score")
    meta_cli_overlay_rows = build_meta_rows("cli_overlay_score")
    market_type_rows = _market_type_summary(stock_rows)

    big_style_label = _describe_big_style(style_rows, market_type_rows)
    direction_label, direction_candidates = _describe_direction(meta_cli_overlay_rows)
    regime_label = str((market_tape.get("market_regime_label") or "未知环境")).strip() or "未知环境"
    summary_line = (
        f"市场环境：{regime_label}｜大风格：{big_style_label}｜活跃方向：{direction_label}"
        f"｜主线候选：{' / '.join(direction_candidates[:4]) if direction_candidates else '暂无'}"
    )

    return RawMarketReport(
        request={
            "trade_date": request.trade_date,
            "snapshot_time": request.snapshot_time,
            "analysis_snapshot_ts": request.analysis_snapshot_ts,
        },
        market_environment=market_tape,
        source_validation=source_state.to_dict(),
        summary_line=summary_line,
        style_ranking=style_rows[:8],
        market_type_ranking=market_type_rows[:6],
        concept_stock_only_ranking=concept_stock_only_rows[:12],
        concept_cli_overlay_ranking=concept_cli_overlay_rows[:12],
        meta_theme_stock_only_ranking=meta_stock_only_rows[:10],
        meta_theme_cli_overlay_ranking=meta_cli_overlay_rows[:10],
        manual_judgement={
            "big_style": big_style_label,
            "direction_label": direction_label,
            "direction_candidates": direction_candidates[:4],
            "core_observation": (
                "更接近混合轮动盘面，不能把 top1 元主题直接等同于单一主攻线；"
                "需要同时看主线密度、成交承载和相邻主题共振。"
            ),
        },
    )
