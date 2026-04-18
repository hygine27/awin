from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


def _parse_clock(value: str) -> datetime:
    text = value.strip()
    fmt = "%H:%M:%S" if len(text.split(":")) == 3 else "%H:%M"
    return datetime.strptime(text, fmt)


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _ensure_text_list(values) -> list[str]:
    out: list[str] = []
    for value in values or []:
        if isinstance(value, dict):
            symbol = str(value.get("symbol") or "").strip()
            if symbol:
                out.append(symbol)
            continue
        text = str(value or "").strip()
        if text:
            out.append(text)
    return out


def _row_flag(row: sqlite3.Row, flag_name: str) -> bool:
    keys = set(row.keys())
    if flag_name not in keys:
        return False
    return bool(row[flag_name])


@dataclass(frozen=True)
class BucketComparison:
    name: str
    v1_count: int
    v2_count: int
    intersection_count: int
    overlap_ratio: float
    intersection: list[str]
    only_v1: list[str]
    only_v2: list[str]


def _compare_bucket(name: str, v1_symbols: list[str], v2_symbols: list[str]) -> BucketComparison:
    v1_set = set(v1_symbols)
    v2_set = set(v2_symbols)
    intersection = sorted(v1_set & v2_set)
    denominator = max(len(v1_set), len(v2_set), 1)
    return BucketComparison(
        name=name,
        v1_count=len(v1_set),
        v2_count=len(v2_set),
        intersection_count=len(intersection),
        overlap_ratio=len(intersection) / denominator,
        intersection=intersection,
        only_v1=sorted(v1_set - v2_set),
        only_v2=sorted(v2_set - v1_set),
    )


def locate_v1_snapshot(v1_root: Path, trade_date: str, snapshot_time: str) -> Path:
    target = _parse_clock(snapshot_time)
    base_dir = v1_root / "runtime/state/durable/intraday-style-monitor" / trade_date
    candidates = sorted(base_dir.glob("*__style-monitor-v1_2.json"))
    if not candidates:
        raise FileNotFoundError(f"no V1 snapshots found under {base_dir}")

    def _distance_minutes(path: Path) -> tuple[float, str]:
        stem = path.name.split("__", 1)[0]
        clock = f"{stem[:2]}:{stem[2:]}"
        delta = abs((_parse_clock(clock) - target).total_seconds())
        return delta, stem

    return min(candidates, key=_distance_minutes)


def load_v1_snapshot(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    top_meta_themes = []
    meta_theme_summary = payload.get("meta_theme_summary") or {}
    for item in meta_theme_summary.get("top_meta_themes") or []:
        meta_theme = str(item.get("meta_theme") or "").strip()
        if meta_theme:
            top_meta_themes.append(meta_theme)
    if not top_meta_themes:
        concept_heat_summary = payload.get("concept_heat_summary") or {}
        for item in (concept_heat_summary.get("acceleration_concepts") or [])[:3]:
            concept_name = str(item.get("concept_name") or "").strip()
            if concept_name:
                top_meta_themes.append(concept_name)
        if not top_meta_themes:
            for item in (concept_heat_summary.get("top_concepts") or [])[:3]:
                concept_name = str(item.get("concept_name") or "").strip()
                if concept_name:
                    top_meta_themes.append(concept_name)

    return {
        "artifact_path": str(path),
        "snapshot_time": str(payload.get("latest_snapshot_time") or ""),
        "confirmed_style": str((payload.get("style_state") or {}).get("confirmed_style") or ""),
        "latest_status": str((payload.get("style_state") or {}).get("latest_status") or ""),
        "latest_dominant_style": str((payload.get("style_state") or {}).get("latest_dominant_style") or ""),
        "market_regime": str((payload.get("market_tape") or {}).get("market_regime") or ""),
        "top_attack_lines": top_meta_themes[:3],
        "core_anchor_watchlist": _ensure_text_list(payload.get("core_anchor_watchlist")),
        "new_long_watchlist": _ensure_text_list(payload.get("new_long_watchlist")),
        "catchup_watchlist": _ensure_text_list(payload.get("catchup_watchlist")),
        "short_watchlist": _ensure_text_list(payload.get("short_watchlist")),
    }


def locate_v2_run(db_path: Path, trade_date: str, snapshot_time: str) -> str:
    target = _parse_clock(snapshot_time)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT run_id, snapshot_time, round_seq, stock_count, alert_decision
            FROM monitor_run
            WHERE trade_date = ?
            ORDER BY snapshot_time ASC, round_seq DESC
            """,
            (trade_date,),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        raise FileNotFoundError(f"no V2 runs found for trade_date={trade_date} in {db_path}")

    def _distance(row) -> tuple[float, int, int]:
        delta = abs((_parse_clock(str(row["snapshot_time"])) - target).total_seconds())
        stock_penalty = 0 if int(row["stock_count"] or 0) > 0 else 1
        round_seq = -int(row["round_seq"] or 0)
        return delta, stock_penalty, round_seq

    return str(min(rows, key=_distance)["run_id"])


def load_v2_snapshot(db_path: Path, run_id: str) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        run = conn.execute(
            """
            SELECT run_id, trade_date, snapshot_time, round_seq, confirmed_style,
                   latest_status, latest_dominant_style, market_regime, top_attack_lines
            FROM monitor_run
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if run is None:
            raise FileNotFoundError(f"run_id not found in SQLite: {run_id}")
        rows = conn.execute(
            """
            SELECT symbol, display_bucket, risk_tag, is_core_anchor, is_new_long, is_catchup, is_short
            FROM stock_snapshot
            WHERE run_id = ?
              AND (is_watchlist = 1 OR is_warning = 1)
            ORDER BY confidence_score DESC, symbol ASC
            """,
            (run_id,),
        ).fetchall()
    finally:
        conn.close()

    return {
        "run_id": str(run["run_id"]),
        "snapshot_time": str(run["snapshot_time"]),
        "round_seq": int(run["round_seq"] or 0),
        "confirmed_style": str(run["confirmed_style"] or ""),
        "latest_status": str(run["latest_status"] or ""),
        "latest_dominant_style": str(run["latest_dominant_style"] or ""),
        "market_regime": str(run["market_regime"] or ""),
        "top_attack_lines": [item.strip() for item in str(run["top_attack_lines"] or "").split("/") if item.strip()],
        "core_anchor_watchlist": [
            str(row["symbol"])
            for row in rows
            if _row_flag(row, "is_core_anchor") or row["display_bucket"] == "core_anchor"
        ],
        "new_long_watchlist": [
            str(row["symbol"])
            for row in rows
            if _row_flag(row, "is_new_long") or row["display_bucket"] == "new_long"
        ],
        "catchup_watchlist": [
            str(row["symbol"])
            for row in rows
            if _row_flag(row, "is_catchup") or row["display_bucket"] == "catchup"
        ],
        "short_watchlist": [
            str(row["symbol"])
            for row in rows
            if _row_flag(row, "is_short") or row["risk_tag"]
        ],
    }


def compare_v1_v2_snapshots(v1_snapshot: dict, v2_snapshot: dict) -> dict:
    buckets = [
        _compare_bucket("core_anchor", v1_snapshot["core_anchor_watchlist"], v2_snapshot["core_anchor_watchlist"]),
        _compare_bucket("new_long", v1_snapshot["new_long_watchlist"], v2_snapshot["new_long_watchlist"]),
        _compare_bucket("catchup", v1_snapshot["catchup_watchlist"], v2_snapshot["catchup_watchlist"]),
        _compare_bucket("short", v1_snapshot["short_watchlist"], v2_snapshot["short_watchlist"]),
    ]
    avg_overlap = sum(item.overlap_ratio for item in buckets) / len(buckets) if buckets else 0.0
    style_aligned = (
        v1_snapshot.get("confirmed_style") == v2_snapshot.get("confirmed_style")
        and v1_snapshot.get("latest_status") == v2_snapshot.get("latest_status")
    )
    top_line_overlap = sorted(set(v1_snapshot.get("top_attack_lines", [])) & set(v2_snapshot.get("top_attack_lines", [])))
    return {
        "v1_snapshot": v1_snapshot,
        "v2_snapshot": v2_snapshot,
        "style_aligned": style_aligned,
        "top_line_overlap": top_line_overlap,
        "bucket_comparisons": [item.__dict__ for item in buckets],
        "average_overlap_ratio": avg_overlap,
    }


def build_parity_report_markdown(payload: dict) -> str:
    v1 = payload["v1_snapshot"]
    v2 = payload["v2_snapshot"]
    lines: list[str] = []
    lines.append("# V1 / V2 Snapshot Compare")
    lines.append("")
    lines.append("## Matched Artifacts")
    lines.append(f"- V1: `{v1.get('artifact_path')}`")
    lines.append(f"- V1 snapshot time: `{v1.get('snapshot_time')}`")
    lines.append(f"- V2 run: `{v2.get('run_id')}`")
    lines.append(f"- V2 snapshot time: `{v2.get('snapshot_time')}` r{int(v2.get('round_seq') or 0):02d}")
    lines.append("")
    lines.append("## Market Layer")
    lines.append(
        f"- 风格状态：V1 `{v1.get('confirmed_style')}/{v1.get('latest_status')}` vs "
        f"V2 `{v2.get('confirmed_style')}/{v2.get('latest_status')}`"
    )
    lines.append(
        f"- 市场环境：V1 `{v1.get('market_regime') or '-'}` vs V2 `{v2.get('market_regime') or '-'}`"
    )
    lines.append(
        f"- 主攻线交集：{', '.join(payload.get('top_line_overlap') or []) if payload.get('top_line_overlap') else '无'}"
    )
    lines.append(
        f"- 个股层平均重合度：{_fmt_pct(float(payload.get('average_overlap_ratio') or 0.0))}"
    )
    lines.append("")
    lines.append("## Watchlist Layer")
    lines.append("| Bucket | V1 | V2 | Intersection | Overlap |")
    lines.append("|---|---|---|---|---|")
    for item in payload.get("bucket_comparisons", []):
        lines.append(
            f"| {item['name']} | {item['v1_count']} | {item['v2_count']} | {item['intersection_count']} | {_fmt_pct(float(item['overlap_ratio']))} |"
        )
    lines.append("")
    for item in payload.get("bucket_comparisons", []):
        lines.append(f"## {item['name']}")
        lines.append(f"- 交集：{', '.join(item['intersection']) if item['intersection'] else '无'}")
        lines.append(f"- 仅 V1：{', '.join(item['only_v1'][:12]) if item['only_v1'] else '无'}")
        lines.append(f"- 仅 V2：{', '.join(item['only_v2'][:12]) if item['only_v2'] else '无'}")
        lines.append("")
    lines.append("## Diagnosis")
    if not payload.get("style_aligned"):
        lines.append("- 市场层还未完全对齐，先不要解读个股差异。")
    elif float(payload.get("average_overlap_ratio") or 0.0) < 0.20:
        lines.append("- 市场层大方向接近，但个股层语义和筛选机制明显分叉，当前 V2 不能视为 V1 parity。")
    else:
        lines.append("- 已出现一定重合，下一步应聚焦差异最大的 bucket 继续收敛。")
    return "\n".join(lines)
