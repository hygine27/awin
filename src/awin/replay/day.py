from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from awin.storage.db import connect_sqlite, init_db


WATCHLIST_BUCKETS = {"core_anchor", "new_long", "catchup"}


@dataclass(slots=True)
class ReplayRunItem:
    run_id: str
    trade_date: str
    snapshot_time: str
    round_seq: int | None
    market_regime: str | None
    confirmed_style: str | None
    latest_status: str | None
    top_attack_lines: str | None
    alert_decision: str | None
    stock_count: int
    core_count: int
    new_long_count: int
    catchup_count: int
    risk_count: int
    added_core: list[str]
    added_new_long: list[str]
    added_catchup: list[str]
    added_risk: list[str]
    removed_core: list[str]
    removed_new_long: list[str]
    removed_catchup: list[str]
    removed_risk: list[str]

    def to_dict(self) -> dict:
        return asdict(self)


def _bucket_symbols(connection, run_id: str) -> dict[str, list[str]]:
    rows = connection.execute(
        """
        SELECT symbol, display_bucket, risk_tag
        FROM stock_snapshot
        WHERE run_id = ?
        ORDER BY confidence_score DESC, symbol ASC
        """,
        (run_id,),
    ).fetchall()

    buckets = {
        "core_anchor": [],
        "new_long": [],
        "catchup": [],
        "risk": [],
    }
    for row in rows:
        display_bucket = row["display_bucket"]
        if display_bucket in WATCHLIST_BUCKETS:
            buckets[display_bucket].append(row["symbol"])
        if row["risk_tag"]:
            buckets["risk"].append(row["symbol"])
    return buckets


def build_day_replay(db_path: Path, trade_date: str) -> dict:
    init_db(db_path)
    with connect_sqlite(db_path) as connection:
        run_rows = connection.execute(
            """
            SELECT run_id, trade_date, snapshot_time, round_seq, market_regime, confirmed_style,
                   latest_status, top_attack_lines, alert_decision, stock_count
            FROM monitor_run
            WHERE trade_date = ?
            ORDER BY snapshot_time ASC, round_seq ASC
            """,
            (trade_date,),
        ).fetchall()

        timeline: list[ReplayRunItem] = []
        previous_buckets = {
            "core_anchor": [],
            "new_long": [],
            "catchup": [],
            "risk": [],
        }

        for row in run_rows:
            current_buckets = _bucket_symbols(connection, row["run_id"])
            item = ReplayRunItem(
                run_id=row["run_id"],
                trade_date=row["trade_date"],
                snapshot_time=row["snapshot_time"],
                round_seq=row["round_seq"],
                market_regime=row["market_regime"],
                confirmed_style=row["confirmed_style"],
                latest_status=row["latest_status"],
                top_attack_lines=row["top_attack_lines"],
                alert_decision=row["alert_decision"],
                stock_count=int(row["stock_count"] or 0),
                core_count=len(current_buckets["core_anchor"]),
                new_long_count=len(current_buckets["new_long"]),
                catchup_count=len(current_buckets["catchup"]),
                risk_count=len(current_buckets["risk"]),
                added_core=sorted(set(current_buckets["core_anchor"]) - set(previous_buckets["core_anchor"])),
                added_new_long=sorted(set(current_buckets["new_long"]) - set(previous_buckets["new_long"])),
                added_catchup=sorted(set(current_buckets["catchup"]) - set(previous_buckets["catchup"])),
                added_risk=sorted(set(current_buckets["risk"]) - set(previous_buckets["risk"])),
                removed_core=sorted(set(previous_buckets["core_anchor"]) - set(current_buckets["core_anchor"])),
                removed_new_long=sorted(set(previous_buckets["new_long"]) - set(current_buckets["new_long"])),
                removed_catchup=sorted(set(previous_buckets["catchup"]) - set(current_buckets["catchup"])),
                removed_risk=sorted(set(previous_buckets["risk"]) - set(current_buckets["risk"])),
            )
            timeline.append(item)
            previous_buckets = current_buckets

    return {
        "trade_date": trade_date,
        "run_count": len(timeline),
        "effective_run_count": len([item for item in timeline if item.stock_count > 0 or item.alert_decision]),
        "timeline": [item.to_dict() for item in timeline],
    }


def build_day_replay_json(db_path: Path, trade_date: str) -> str:
    return json.dumps(build_day_replay(db_path, trade_date), ensure_ascii=False, indent=2)


def build_day_replay_markdown(db_path: Path, trade_date: str) -> str:
    payload = build_day_replay(db_path, trade_date)
    timeline = payload["timeline"]

    lines: list[str] = []
    lines.append(f"# A视界 Replay | {trade_date}")
    lines.append("")

    if not timeline:
        lines.append("当日暂无运行记录。")
        return "\n".join(lines)

    effective_timeline = [item for item in timeline if item["stock_count"] > 0 or item["alert_decision"]]
    latest = effective_timeline[-1] if effective_timeline else timeline[-1]
    lines.append("## 日内结论")
    lines.append(
        "- 最新状态："
        f"{latest.get('market_regime') or '未知'} / "
        f"{latest.get('confirmed_style') or '未知'} / "
        f"{latest.get('latest_status') or '未知'}"
    )
    lines.append(
        "- 最新清单："
        f"核心锚定 {latest['core_count']} 只，"
        f"新晋看多 {latest['new_long_count']} 只，"
        f"潜在补涨 {latest['catchup_count']} 只，"
        f"风险预警 {latest['risk_count']} 只"
    )
    lines.append(f"- 最新主攻线：{latest.get('top_attack_lines') or '暂无'}")
    lines.append(
        f"- 日内轮次：总计 {payload['run_count']} 轮，"
        f"有效分析 {payload.get('effective_run_count', len(effective_timeline))} 轮"
    )
    lines.append("")

    lines.append("## 轮次回放")
    lines.append("| 时间 | 风格/环境 | 主攻线 | 清单变化 | 提醒 |")
    lines.append("|---|---|---|---|---|")
    for item in effective_timeline or timeline:
        changes: list[str] = []
        if item["added_core"]:
            changes.append(f"+核心{len(item['added_core'])}")
        if item["added_new_long"]:
            changes.append(f"+看多{len(item['added_new_long'])}")
        if item["added_catchup"]:
            changes.append(f"+补涨{len(item['added_catchup'])}")
        if item["added_risk"]:
            changes.append(f"+风险{len(item['added_risk'])}")
        if item["removed_core"]:
            changes.append(f"-核心{len(item['removed_core'])}")
        if item["removed_new_long"]:
            changes.append(f"-看多{len(item['removed_new_long'])}")
        if item["removed_catchup"]:
            changes.append(f"-补涨{len(item['removed_catchup'])}")
        if item["removed_risk"]:
            changes.append(f"-风险{len(item['removed_risk'])}")
        round_text = f"r{int(item['round_seq']):02d}" if item.get("round_seq") is not None else "-"
        lines.append(
            "| {time} | {regime} / {style} / {status} | {attack} | {changes} | {decision} |".format(
                time=f"{item['snapshot_time']} {round_text}",
                regime=item.get("market_regime") or "-",
                style=item.get("confirmed_style") or "-",
                status=item.get("latest_status") or "-",
                attack=item.get("top_attack_lines") or "-",
                changes="，".join(changes) if changes else "无显著变化",
                decision=item.get("alert_decision") or "-",
            )
        )
    lines.append("")

    lines.append("## 最新新增")
    latest_additions = [
        ("核心锚定", latest["added_core"]),
        ("新晋看多", latest["added_new_long"]),
        ("潜在补涨", latest["added_catchup"]),
        ("风险预警", latest["added_risk"]),
    ]
    for label, symbols in latest_additions:
        display = "、".join(symbols[:8]) if symbols else "无"
        suffix = " ..." if len(symbols) > 8 else ""
        lines.append(f"- {label}：{display}{suffix}")

    return "\n".join(lines)
