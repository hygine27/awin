from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from awin.adapters.contracts import DcfSnapshotRow, QmtSnapshotRow, StockMasterRow, ThsConceptRow, ThsHotConceptRow
from awin.config import ConfigError, get_app_config
from awin.contracts.m0 import MarketUnderstandingOutput, MetaThemeItem, StyleRankingItem
from awin.utils.structured_config import load_structured_config
from awin.utils.symbols import normalize_stock_code


SUPPORTED_STYLE_BASKET_KEYS = {"industries", "market_types"}
SUPPORTED_STYLE_THRESHOLD_KEYS = {
    "min_constituents",
    "strong_move_pct",
    "near_high_threshold",
    "active_pace_threshold",
}
SUPPORTED_STYLE_SCORE_WEIGHT_KEYS = {
    "eq_return",
    "up_ratio",
    "strong_ratio",
    "near_high_ratio",
    "activity_ratio",
}


def _validate_string_list(
    values: object,
    *,
    path: Path,
    section_name: str,
    field_name: str,
) -> list[str]:
    if values is None:
        return []
    if not isinstance(values, list):
        raise ConfigError(f"invalid style config at {path}: {section_name}.{field_name} must be a list")
    normalized = [str(item).strip() for item in values if str(item).strip()]
    return normalized


def _validate_style_config(payload: dict, path: Path) -> tuple[dict[str, dict[str, list[str]]], dict[str, float], dict[str, float]]:
    raw_style_baskets = payload.get("style_baskets")
    raw_thresholds = payload.get("thresholds")
    raw_score_weights = payload.get("score_weights")
    if not isinstance(raw_style_baskets, dict) or not raw_style_baskets:
        raise ConfigError(f"invalid style config at {path}: style_baskets must be a non-empty object")
    if not isinstance(raw_thresholds, dict):
        raise ConfigError(f"invalid style config at {path}: thresholds must be an object")
    if not isinstance(raw_score_weights, dict):
        raise ConfigError(f"invalid style config at {path}: score_weights must be an object")

    unknown_threshold_keys = sorted(set(raw_thresholds) - SUPPORTED_STYLE_THRESHOLD_KEYS)
    if unknown_threshold_keys:
        raise ConfigError(
            f"invalid style config at {path}: unsupported threshold keys: {', '.join(unknown_threshold_keys)}"
        )
    missing_threshold_keys = sorted(SUPPORTED_STYLE_THRESHOLD_KEYS - set(raw_thresholds))
    if missing_threshold_keys:
        raise ConfigError(
            f"invalid style config at {path}: missing threshold keys: {', '.join(missing_threshold_keys)}"
        )
    unknown_score_weight_keys = sorted(set(raw_score_weights) - SUPPORTED_STYLE_SCORE_WEIGHT_KEYS)
    if unknown_score_weight_keys:
        raise ConfigError(
            f"invalid style config at {path}: unsupported score weight keys: {', '.join(unknown_score_weight_keys)}"
        )
    missing_score_weight_keys = sorted(SUPPORTED_STYLE_SCORE_WEIGHT_KEYS - set(raw_score_weights))
    if missing_score_weight_keys:
        raise ConfigError(
            f"invalid style config at {path}: missing score weight keys: {', '.join(missing_score_weight_keys)}"
        )

    style_baskets: dict[str, dict[str, list[str]]] = {}
    for style_name, raw_rule in raw_style_baskets.items():
        if not isinstance(raw_rule, dict):
            raise ConfigError(f"invalid style config at {path}: style_baskets.{style_name} must be an object")
        unknown_rule_keys = sorted(set(raw_rule) - SUPPORTED_STYLE_BASKET_KEYS)
        if unknown_rule_keys:
            raise ConfigError(
                f"invalid style config at {path}: unsupported keys in style_baskets.{style_name}: {', '.join(unknown_rule_keys)}"
            )
        industries = _validate_string_list(
            raw_rule.get("industries"),
            path=path,
            section_name=f"style_baskets.{style_name}",
            field_name="industries",
        )
        market_types = _validate_string_list(
            raw_rule.get("market_types"),
            path=path,
            section_name=f"style_baskets.{style_name}",
            field_name="market_types",
        )
        if not industries and not market_types:
            raise ConfigError(
                f"invalid style config at {path}: style_baskets.{style_name} must define industries or market_types"
            )
        style_baskets[str(style_name)] = {
            "industries": industries,
            "market_types": market_types,
        }

    thresholds = {key: float(raw_thresholds[key]) for key in SUPPORTED_STYLE_THRESHOLD_KEYS}
    thresholds["min_constituents"] = int(raw_thresholds["min_constituents"])
    score_weights = {key: float(raw_score_weights[key]) for key in SUPPORTED_STYLE_SCORE_WEIGHT_KEYS}
    return style_baskets, thresholds, score_weights


def load_style_baskets(config_path: Path | None = None) -> tuple[dict[str, dict], dict[str, float]]:
    config = get_app_config()
    path = config_path or config.style_config_path
    payload = load_structured_config(path, label="style config")
    style_baskets, thresholds, _ = _validate_style_config(payload, path)
    return style_baskets, thresholds


def load_style_score_weights(config_path: Path | None = None) -> dict[str, float]:
    config = get_app_config()
    path = config_path or config.style_config_path
    payload = load_structured_config(path, label="style config")
    _, _, score_weights = _validate_style_config(payload, path)
    return score_weights


def load_overlay_config(config_path: Path | None = None) -> dict:
    config = get_app_config()
    path = config_path or config.ths_overlay_config_path
    return load_structured_config(path, label="overlay config")


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
    spread = high_price - low_price
    if spread <= 0:
        return None
    value = (last_price - low_price) / spread
    return max(0.0, min(1.0, value))


def _pct_chg_prev_close(last_price: float | None, last_close: float | None) -> float | None:
    if last_price is None or last_close in {None, 0, 0.0}:
        return None
    return float(last_price) / float(last_close) - 1.0


def _style_matches(style_rule: dict, master: StockMasterRow) -> bool:
    industries = set(style_rule.get("industries", []))
    market_types = set(style_rule.get("market_types", []))
    return (
        (master.industry in industries if industries else False)
        or (master.market in market_types if market_types else False)
    )


def _score_buckets(
    rows: list[dict],
    *,
    score_field: str = "composite_score",
    score_weights: dict[str, float],
) -> list[dict]:
    if not rows:
        return []
    metrics = ["eq_return", "up_ratio", "strong_ratio", "near_high_ratio", "activity_ratio"]
    for metric in metrics:
        valid_values = [row[metric] for row in rows if row[metric] is not None]
        ordered = sorted(valid_values)
        rank_map: dict[float, float] = {}
        for idx, value in enumerate(ordered, start=1):
            rank_map[value] = idx / len(ordered) if ordered else 0.0
        for row in rows:
            row[f"{metric}_rank"] = rank_map.get(row[metric], 0.0) if row[metric] is not None else 0.0
    for row in rows:
        row[score_field] = round(
            sum(row[f"{metric}_rank"] * float(score_weights.get(metric, 0.0)) for metric in metrics),
            4,
        )
    rows.sort(key=lambda item: (item[score_field], item.get("eq_return") or -999), reverse=True)
    top_score = rows[0][score_field] if rows else None
    for row in rows:
        row["spread_to_leader"] = round((top_score or 0.0) - row[score_field], 4) if top_score is not None else None
    return rows


def _build_alias_to_canonical(overlay_payload: dict) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    whitelist = overlay_payload.get("concept_whitelist", [])
    for concept in whitelist:
        alias_map[str(concept)] = str(concept)
    for canonical, aliases in overlay_payload.get("concept_aliases", {}).items():
        canonical_name = str(canonical)
        alias_map[canonical_name] = canonical_name
        for alias in aliases:
            alias_map[str(alias)] = canonical_name
    return alias_map


def _build_percentile_map(rows: list[dict], field: str, *, reverse: bool = False) -> dict[str, float]:
    valid = [(str(row["concept_name"]), row.get(field)) for row in rows if row.get(field) is not None]
    if not valid:
        return {}
    valid.sort(key=lambda item: item[1], reverse=reverse)
    total = len(valid)
    out: dict[str, float] = {}
    for idx, (concept_name, _) in enumerate(valid, start=1):
        out[concept_name] = idx / total
    return out


def _latest_rows_by_source(
    hot_concepts: list[ThsHotConceptRow],
    source_table: str,
) -> tuple[dict[str, ThsHotConceptRow], dict[str, ThsHotConceptRow]]:
    source_rows = [row for row in hot_concepts if row.source_table == source_table]
    if not source_rows:
        return {}, {}
    batch_values = sorted({row.batch_ts for row in source_rows})
    latest_batch = batch_values[-1]
    previous_batch = batch_values[-2] if len(batch_values) >= 2 else None
    latest = {row.concept_name: row for row in source_rows if row.batch_ts == latest_batch}
    previous = {row.concept_name: row for row in source_rows if previous_batch and row.batch_ts == previous_batch}
    return latest, previous


def _derive_board_score_maps(
    hot_concepts: list[ThsHotConceptRow],
) -> tuple[dict[str, float], dict[str, dict[str, float | int | str | None]], list[str]]:
    app_latest, _ = _latest_rows_by_source(hot_concepts, "stg.ths_app_hot_concept_trade")
    cli_latest, cli_previous = _latest_rows_by_source(hot_concepts, "stg.ths_cli_hot_concept")

    app_rows = [
        {
            "concept_name": row.concept_name,
            "concept_hot_score": row.concept_hot_score,
            "concept_rank": row.concept_rank,
        }
        for row in app_latest.values()
    ]
    app_hot_pct = _build_percentile_map(app_rows, "concept_hot_score")
    app_rank_pct = _build_percentile_map(app_rows, "concept_rank", reverse=True)

    cli_rows = []
    for row in cli_latest.values():
        cli_rows.append(
            {
                "concept_name": row.concept_name,
                "change_pct": row.change_pct,
                "speed_1min": row.speed_1min,
                "main_net_amount": row.main_net_amount,
                "limit_up_count": row.limit_up_count,
                "rise_spread": ((row.rising_count or 0) - (row.falling_count or 0)) if row.rising_count is not None or row.falling_count is not None else None,
            }
        )
    cli_change_pct = _build_percentile_map(cli_rows, "change_pct")
    cli_speed_pct = _build_percentile_map(cli_rows, "speed_1min")
    cli_flow_pct = _build_percentile_map(cli_rows, "main_net_amount")
    cli_limit_pct = _build_percentile_map(cli_rows, "limit_up_count")
    cli_rise_spread_pct = _build_percentile_map(cli_rows, "rise_spread")

    board_score_map: dict[str, float] = {}
    board_detail_map: dict[str, dict[str, float | int | str | None]] = {}
    concept_names = sorted(set(app_latest) | set(cli_latest))
    for concept_name in concept_names:
        score_parts: list[float] = []
        if concept_name in app_latest:
            score_parts.append(0.6 * app_hot_pct.get(concept_name, 0.0) + 0.4 * app_rank_pct.get(concept_name, 0.0))
        if concept_name in cli_latest:
            cli_score = (
                0.35 * cli_change_pct.get(concept_name, 0.0)
                + 0.20 * cli_speed_pct.get(concept_name, 0.0)
                + 0.20 * cli_flow_pct.get(concept_name, 0.0)
                + 0.15 * cli_limit_pct.get(concept_name, 0.0)
                + 0.10 * cli_rise_spread_pct.get(concept_name, 0.0)
            )
            score_parts.append(cli_score)
        if score_parts:
            board_score_map[concept_name] = round(sum(score_parts) / len(score_parts), 4)
        latest_cli = cli_latest.get(concept_name)
        prev_cli = cli_previous.get(concept_name)
        board_detail_map[concept_name] = {
            "concept_rank": app_latest.get(concept_name).concept_rank if concept_name in app_latest else None,
            "concept_hot_score": app_latest.get(concept_name).concept_hot_score if concept_name in app_latest else None,
            "concept_rank_change": app_latest.get(concept_name).concept_rank_change if concept_name in app_latest else None,
            "limit_up_count": latest_cli.limit_up_count if latest_cli else (app_latest.get(concept_name).limit_up_count if concept_name in app_latest else None),
            "leading_stock": latest_cli.leading_stock if latest_cli else None,
            "change_pct": latest_cli.change_pct if latest_cli else None,
            "speed_1min": latest_cli.speed_1min if latest_cli else None,
            "main_net_amount": latest_cli.main_net_amount if latest_cli else None,
            "change_pct_delta": (
                (latest_cli.change_pct or 0.0) - (prev_cli.change_pct or 0.0)
                if latest_cli is not None and prev_cli is not None
                else latest_cli.change_pct if latest_cli is not None
                else None
            ),
            "rise_spread_delta": (
                ((latest_cli.rising_count or 0) - (latest_cli.falling_count or 0))
                - ((prev_cli.rising_count or 0) - (prev_cli.falling_count or 0))
                if latest_cli is not None and prev_cli is not None
                else ((latest_cli.rising_count or 0) - (latest_cli.falling_count or 0)) if latest_cli is not None
                else None
            ),
            "limit_up_delta": (
                (latest_cli.limit_up_count or 0) - (prev_cli.limit_up_count or 0)
                if latest_cli is not None and prev_cli is not None
                else latest_cli.limit_up_count if latest_cli is not None
                else None
            ),
        }
    return board_score_map, board_detail_map, concept_names


def compute_market_understanding(
    stock_master: list[StockMasterRow],
    qmt_snapshot: list[QmtSnapshotRow],
    dcf_snapshot: list[DcfSnapshotRow],
    ths_concepts: list[ThsConceptRow],
    *,
    ths_hot_concepts: list[ThsHotConceptRow] | None = None,
    market_tape: dict | None = None,
    style_baskets_config_path: Path | None = None,
    overlay_config_path: Path | None = None,
) -> MarketUnderstandingOutput:
    style_baskets, thresholds = load_style_baskets(style_baskets_config_path)
    style_score_weights = load_style_score_weights(style_baskets_config_path)
    overlay_payload = load_overlay_config(overlay_config_path)
    overlay_thresholds = overlay_payload.get("thresholds", {})
    concept_to_meta = {
        str(concept): str(meta_theme)
        for meta_theme, concepts in overlay_payload.get("meta_themes", {}).items()
        for concept in concepts
    }
    concept_downweight = {
        str(name): float(payload.get("weight", 1.0))
        for name, payload in overlay_payload.get("concept_downweight", {}).items()
    }
    alias_to_canonical = _build_alias_to_canonical(overlay_payload)

    master_by_code = {
        normalize_stock_code(item.stock_code or item.symbol): item
        for item in stock_master
        if item.is_listed and not item.is_st
    }
    dcf_by_symbol = {item.symbol: item for item in dcf_snapshot}

    concept_map: dict[str, list[ThsConceptRow]] = defaultdict(list)
    for row in ths_concepts:
        concept_map[row.symbol].append(row)

    stock_rows: list[dict] = []
    style_members: dict[str, list[dict]] = defaultdict(list)
    concept_members: dict[str, list[dict]] = defaultdict(list)

    strong_move_pct = float(thresholds.get("strong_move_pct", 0.02))
    near_high_threshold = float(thresholds.get("near_high_threshold", 0.8))
    active_pace_threshold = float(thresholds.get("active_pace_threshold", 1.2))
    min_constituents = int(thresholds.get("min_constituents", 12))
    min_concept_members = int(overlay_thresholds.get("min_members", 8))
    stock_score_weight = float(overlay_thresholds.get("stock_score_weight", 0.78))
    board_score_weight = float(overlay_thresholds.get("board_score_weight", 0.22))
    meta_theme_top_concepts = int(overlay_thresholds.get("meta_theme_top_concepts", 3))

    for qmt in qmt_snapshot:
        stock_code = normalize_stock_code(qmt.stock_code or qmt.symbol)
        master = master_by_code.get(stock_code)
        if master is None:
            continue

        pct = _pct_chg_prev_close(qmt.last_price, qmt.last_close)
        range_position = _range_position(qmt.last_price, qmt.high_price, qmt.low_price)
        dcf = dcf_by_symbol.get(qmt.symbol)
        volume_ratio = dcf.volume_ratio if dcf else None

        row = {
            "symbol": qmt.symbol,
            "stock_code": stock_code,
            "stock_name": master.stock_name or qmt.symbol,
            "industry": master.industry,
            "market": master.market,
            "pct_chg_prev_close": pct,
            "range_position": range_position,
            "volume_ratio": volume_ratio,
            "turnover_rate": dcf.turnover_rate if dcf else None,
            "meta_themes": sorted({item.meta_theme for item in concept_map.get(qmt.symbol, []) if item.meta_theme}),
            "concepts": sorted({alias_to_canonical.get(item.concept_name, item.concept_name) for item in concept_map.get(qmt.symbol, []) if item.concept_name}),
        }
        stock_rows.append(row)

        for style_name, style_rule in style_baskets.items():
            if _style_matches(style_rule, master):
                style_members[style_name].append(row)
        for concept_name in row["concepts"]:
            concept_members[concept_name].append(row)

    if not stock_rows:
        return MarketUnderstandingOutput(summary_line="主风格：暂无｜状态：稳定｜主导方向：暂无")

    style_rows: list[dict] = []
    for style_name, members in style_members.items():
        if len(members) < min_constituents:
            continue
        pct_values = [item["pct_chg_prev_close"] for item in members if item["pct_chg_prev_close"] is not None]
        near_high_count = sum((item["range_position"] or 0.0) >= near_high_threshold for item in members)
        active_count = sum((item["volume_ratio"] or 0.0) >= active_pace_threshold for item in members)
        style_rows.append(
            {
                "style_name": style_name,
                "member_count": len(members),
                "eq_return": _average(pct_values),
                "up_ratio": _safe_div(sum((item["pct_chg_prev_close"] or 0.0) > 0 for item in members), len(members)),
                "strong_ratio": _safe_div(sum((item["pct_chg_prev_close"] or 0.0) >= strong_move_pct for item in members), len(members)),
                "near_high_ratio": _safe_div(near_high_count, len(members)),
                "activity_ratio": _safe_div(active_count, len(members)),
            }
        )

    style_rows = _score_buckets(style_rows, score_weights=style_score_weights)
    top_styles = [
        StyleRankingItem(
            style_name=row["style_name"],
            score=row["composite_score"],
            eq_return=row["eq_return"],
            up_ratio=row["up_ratio"],
            strong_ratio=row["strong_ratio"],
            near_high_ratio=row["near_high_ratio"],
            activity_ratio=row["activity_ratio"],
            spread_to_leader=row["spread_to_leader"],
        )
        for row in style_rows[:5]
    ]

    concept_rows: list[dict] = []
    for concept_name, members in concept_members.items():
        if len(members) < min_concept_members:
            continue
        pct_values = [item["pct_chg_prev_close"] for item in members if item["pct_chg_prev_close"] is not None]
        near_high_count = sum((item["range_position"] or 0.0) >= near_high_threshold for item in members)
        active_count = sum((item["volume_ratio"] or 0.0) >= active_pace_threshold for item in members)
        concept_rows.append(
            {
                "concept_name": concept_name,
                "meta_theme": concept_to_meta.get(concept_name),
                "member_count": len(members),
                "eq_return": _average(pct_values),
                "up_ratio": _safe_div(sum((item["pct_chg_prev_close"] or 0.0) > 0 for item in members), len(members)),
                "strong_ratio": _safe_div(sum((item["pct_chg_prev_close"] or 0.0) >= strong_move_pct for item in members), len(members)),
                "near_high_ratio": _safe_div(near_high_count, len(members)),
                "activity_ratio": _safe_div(active_count, len(members)),
            }
        )
    concept_rows = _score_buckets(concept_rows, score_field="stock_composite_score", score_weights=style_score_weights)

    board_score_map, board_detail_map, _ = _derive_board_score_maps(ths_hot_concepts or [])
    for idx, row in enumerate(concept_rows, start=1):
        row["stock_composite_rank"] = idx
        board_score = board_score_map.get(row["concept_name"], row["stock_composite_score"])
        row["board_score"] = board_score
        row["concept_weight"] = concept_downweight.get(row["concept_name"], 1.0)
        row["overlay_score_raw"] = stock_score_weight * row["stock_composite_score"] + board_score_weight * board_score
        row["overlay_score"] = round(row["overlay_score_raw"] * row["concept_weight"], 4)
        row.update(board_detail_map.get(row["concept_name"], {}))
    concept_rows.sort(key=lambda item: (item["overlay_score"], item["stock_composite_score"], item.get("eq_return") or -999), reverse=True)
    for idx, row in enumerate(concept_rows, start=1):
        row["overlay_rank"] = idx

    acceleration_candidates: list[dict] = []
    latest_cli, previous_cli = _latest_rows_by_source(ths_hot_concepts or [], "stg.ths_cli_hot_concept")
    for row in concept_rows:
        current = latest_cli.get(row["concept_name"])
        previous = previous_cli.get(row["concept_name"])
        if current is None:
            continue
        change_pct_delta = ((current.change_pct or 0.0) - (previous.change_pct or 0.0)) if previous is not None else (current.change_pct or 0.0)
        rise_spread_delta = (
            ((current.rising_count or 0) - (current.falling_count or 0)) - ((previous.rising_count or 0) - (previous.falling_count or 0))
            if previous is not None
            else ((current.rising_count or 0) - (current.falling_count or 0))
        )
        limit_up_delta = ((current.limit_up_count or 0) - (previous.limit_up_count or 0)) if previous is not None else (current.limit_up_count or 0)
        acceleration_candidates.append(
            {
                "concept_name": row["concept_name"],
                "change_pct_delta": change_pct_delta,
                "speed_1min": current.speed_1min,
                "main_net_amount": current.main_net_amount,
                "rise_spread_delta": rise_spread_delta,
                "limit_up_delta": limit_up_delta,
                "overlay_rank": row["overlay_rank"],
                "eq_return": row["eq_return"],
            }
        )
    if acceleration_candidates:
        for field in ["change_pct_delta", "speed_1min", "main_net_amount", "rise_spread_delta", "limit_up_delta"]:
            rank_map = _build_percentile_map(acceleration_candidates, field)
            for row in acceleration_candidates:
                row[f"{field}_rank"] = rank_map.get(row["concept_name"], 0.0)
        for row in acceleration_candidates:
            row["acceleration_score"] = round(
                0.35 * row["change_pct_delta_rank"]
                + 0.25 * row["speed_1min_rank"]
                + 0.15 * row["main_net_amount_rank"]
                + 0.15 * row["rise_spread_delta_rank"]
                + 0.10 * row["limit_up_delta_rank"],
                4,
            )
        acceleration_candidates = [
            row
            for row in acceleration_candidates
            if (row.get("eq_return") or 0.0) > 0 or int(row.get("overlay_rank") or 999) <= 10
        ]
        acceleration_candidates.sort(
            key=lambda item: (
                item["acceleration_score"],
                item["change_pct_delta"],
                item["speed_1min"] or -999,
                -(item["overlay_rank"] or 999),
            ),
            reverse=True,
        )

    meta_rows: list[dict] = []
    grouped_concepts: dict[str, list[dict]] = defaultdict(list)
    for row in concept_rows:
        meta_theme = row.get("meta_theme")
        if meta_theme:
            grouped_concepts[str(meta_theme)].append(row)
    for meta_theme, members in grouped_concepts.items():
        top_members = sorted(members, key=lambda item: (item["overlay_score"], item["eq_return"] or -999), reverse=True)[:meta_theme_top_concepts]
        meta_rows.append(
            {
                "meta_theme": meta_theme,
                "score": round(_average([item["overlay_score"] for item in top_members]) or 0.0, 4),
                "eq_return": _average([item["eq_return"] for item in top_members if item["eq_return"] is not None]),
                "strongest_concepts": [item["concept_name"] for item in top_members[:3]],
            }
        )
    meta_rows.sort(key=lambda item: (item["score"], item["eq_return"] or -999), reverse=True)
    top_meta_themes = [
        MetaThemeItem(
            meta_theme=row["meta_theme"],
            score=row["score"],
            eq_return=row["eq_return"],
            rank=idx + 1,
            strongest_concepts=row["strongest_concepts"],
        )
        for idx, row in enumerate(meta_rows[:5])
    ]

    market_up_ratio = _safe_div(sum((row["pct_chg_prev_close"] or 0.0) > 0 for row in stock_rows), len(stock_rows)) or 0.0
    market_strong_ratio = _safe_div(sum((row["pct_chg_prev_close"] or 0.0) >= 0.04 for row in stock_rows), len(stock_rows)) or 0.0
    tape_regime = None
    if market_tape and market_tape.get("regime_actionable") and market_tape.get("market_regime"):
        tape_regime = str(market_tape["market_regime"])

    cross_section_regime = None
    if market_up_ratio >= 0.60 and market_strong_ratio >= 0.12:
        cross_section_regime = "trend_expansion"
    elif market_up_ratio >= 0.48:
        cross_section_regime = "mixed_rotation"
    else:
        cross_section_regime = "weak_market_relative_strength"

    if tape_regime and tape_regime in {"mixed_tape", "mixed_rotation"} and market_up_ratio >= 0.72 and market_strong_ratio >= 0.09:
        market_regime = "trend_expansion"
    elif tape_regime:
        market_regime = tape_regime
    elif market_up_ratio >= 0.60 and market_strong_ratio >= 0.12:
        market_regime = "trend_expansion"
    elif market_up_ratio >= 0.48:
        market_regime = "mixed_rotation"
    else:
        market_regime = "weak_market_relative_strength"

    confirmed_style = top_styles[0].style_name if top_styles else None
    strongest_concepts = [item["concept_name"] for item in concept_rows[:5]]
    acceleration_concepts = [item["concept_name"] for item in acceleration_candidates[:3]]
    top_meta_theme_names = [item.meta_theme for item in top_meta_themes[:3]]

    summary_line = (
        f"主风格：{confirmed_style or '暂无'}｜状态：稳定｜主导方向：{top_meta_theme_names[0] if top_meta_theme_names else (confirmed_style or '暂无')}"
        f"｜最强主题：{' / '.join(top_meta_theme_names) if top_meta_theme_names else '暂无'}"
    )

    evidence_lines = []
    if top_styles:
        leader = top_styles[0]
        evidence_lines.append(
            f"- 风格强弱：{leader.style_name} 当前综合分领先，等权收益 {((leader.eq_return or 0.0) * 100):.2f}% ，上涨占比 {((leader.up_ratio or 0.0) * 100):.1f}% 。"
        )
    if top_meta_themes:
        theme = top_meta_themes[0]
        evidence_lines.append(
            f"- 主线解释：{theme.meta_theme} 当前最强，细分概念看 {' / '.join(theme.strongest_concepts) if theme.strongest_concepts else '暂无'}。"
        )
    if acceleration_concepts:
        evidence_lines.append(f"- 加速方向：{' / '.join(acceleration_concepts)} 当前抬升最快。")
    if tape_regime:
        evidence_lines.append(
            f"- 市场环境：{market_regime}（来自 THS market tape），全市场上涨占比约 {(market_up_ratio * 100):.1f}% ，强势股占比约 {(market_strong_ratio * 100):.1f}% 。"
        )
    else:
        evidence_lines.append(
            f"- 市场环境：{market_regime}，全市场上涨占比约 {(market_up_ratio * 100):.1f}% ，强势股占比约 {(market_strong_ratio * 100):.1f}% 。"
        )

    return MarketUnderstandingOutput(
        confirmed_style=confirmed_style,
        latest_status="stable",
        latest_dominant_style=confirmed_style,
        market_regime=market_regime,
        top_styles=top_styles,
        top_meta_themes=top_meta_themes,
        strongest_concepts=strongest_concepts,
        acceleration_concepts=acceleration_concepts,
        concept_overlay_score_map={str(item["concept_name"]): float(item["overlay_score"]) for item in concept_rows[:50]},
        concept_overlay_rank_map={str(item["concept_name"]): int(item["overlay_rank"]) for item in concept_rows[:50]},
        meta_theme_rank_map={item.meta_theme: int(item.rank or 0) for item in top_meta_themes if item.meta_theme},
        meta_theme_eq_return_map={item.meta_theme: float(item.eq_return or 0.0) for item in top_meta_themes if item.meta_theme},
        summary_line=summary_line,
        evidence_lines=evidence_lines,
    )
