from __future__ import annotations

import json
from pathlib import Path

from awin.evaluation.outcomes import load_post_trade_outcomes
from awin.storage.db import connect_sqlite, init_db


def build_day_summary(db_path: Path, trade_date: str, *, include_outcomes: bool = False) -> dict:
    init_db(db_path)
    with connect_sqlite(db_path) as connection:
        run_rows = connection.execute(
            """
            SELECT run_id, snapshot_time, round_seq, confirmed_style, latest_status, market_regime, stock_count, alert_decision
            FROM monitor_run
            WHERE trade_date = ?
            ORDER BY snapshot_time ASC, round_seq ASC
            """,
            (trade_date,),
        ).fetchall()
        symbol_rows = connection.execute(
            """
            SELECT
                ss.symbol,
                ss.stock_name,
                MIN(ss.snapshot_time) AS first_seen_time,
                MAX(ss.snapshot_time) AS last_seen_time,
                MIN(mr.round_seq) AS first_seen_round,
                MAX(mr.round_seq) AS last_seen_round,
                COUNT(*) AS mention_count,
                MAX(confidence_score) AS max_confidence_score,
                MAX(CASE WHEN display_bucket = 'core_anchor' THEN 1 ELSE 0 END) AS seen_core,
                MAX(CASE WHEN display_bucket = 'new_long' THEN 1 ELSE 0 END) AS seen_new_long,
                MAX(CASE WHEN display_bucket = 'catchup' THEN 1 ELSE 0 END) AS seen_catchup,
                MAX(CASE WHEN risk_tag IS NOT NULL THEN 1 ELSE 0 END) AS seen_risk
            FROM stock_snapshot ss
            JOIN monitor_run mr
              ON mr.run_id = ss.run_id
            WHERE ss.trade_date = ?
              AND (is_watchlist = 1 OR is_warning = 1)
            GROUP BY ss.symbol, ss.stock_name
            ORDER BY mention_count DESC, max_confidence_score DESC, symbol ASC
            """,
            (trade_date,),
        ).fetchall()

        latest_rows = connection.execute(
            """
            SELECT symbol, display_bucket, risk_tag, confidence_score
            FROM stock_snapshot
            WHERE run_id = (
                SELECT run_id
                FROM monitor_run
                WHERE trade_date = ?
                ORDER BY snapshot_time DESC, round_seq DESC
                LIMIT 1
            )
            """,
            (trade_date,),
        ).fetchall()

    latest_by_symbol = {
        row["symbol"]: {
            "latest_display_bucket": row["display_bucket"],
            "latest_risk_tag": row["risk_tag"],
            "latest_confidence_score": row["confidence_score"],
        }
        for row in latest_rows
    }

    symbols = []
    for row in symbol_rows:
        symbol = row["symbol"]
        latest = latest_by_symbol.get(symbol, {})
        seen_buckets = []
        if row["seen_core"]:
            seen_buckets.append("core_anchor")
        if row["seen_new_long"]:
            seen_buckets.append("new_long")
        if row["seen_catchup"]:
            seen_buckets.append("catchup")
        if row["seen_risk"]:
            seen_buckets.append("risk")
        symbols.append(
            {
                "symbol": symbol,
                "stock_name": row["stock_name"],
                "first_seen_time": row["first_seen_time"],
                "last_seen_time": row["last_seen_time"],
                "first_seen_round": row["first_seen_round"],
                "last_seen_round": row["last_seen_round"],
                "mention_count": int(row["mention_count"] or 0),
                "max_confidence_score": row["max_confidence_score"],
                "seen_buckets": seen_buckets,
                **latest,
            }
        )

    run_timeline = [
        {
            "run_id": row["run_id"],
            "snapshot_time": row["snapshot_time"],
            "round_seq": row["round_seq"],
            "confirmed_style": row["confirmed_style"],
            "latest_status": row["latest_status"],
            "market_regime": row["market_regime"],
            "stock_count": row["stock_count"],
            "alert_decision": row["alert_decision"],
        }
        for row in run_rows
    ]

    payload = {
        "trade_date": trade_date,
        "run_count": len(run_timeline),
        "effective_run_count": len([item for item in run_timeline if item["stock_count"] or item["alert_decision"]]),
        "run_timeline": run_timeline,
        "active_symbols": symbols,
    }
    if include_outcomes and symbols:
        outcomes = load_post_trade_outcomes(trade_date, symbols)
        payload["post_trade_outcomes"] = outcomes
        payload["active_symbols"] = outcomes["active_symbols"]
    return payload


def build_day_summary_json(db_path: Path, trade_date: str, *, include_outcomes: bool = False) -> str:
    return json.dumps(build_day_summary(db_path, trade_date, include_outcomes=include_outcomes), ensure_ascii=False, indent=2)


def build_day_summary_markdown(db_path: Path, trade_date: str, *, include_outcomes: bool = False) -> str:
    payload = build_day_summary(db_path, trade_date, include_outcomes=include_outcomes)
    lines: list[str] = []
    lines.append(f"# A视界 Evaluation | {trade_date}")
    lines.append("")

    run_timeline = payload["run_timeline"]
    active_symbols = payload["active_symbols"]

    if not run_timeline:
        lines.append("当日暂无可评估轮次。")
        return "\n".join(lines)

    effective_runs = [item for item in run_timeline if item["stock_count"] or item["alert_decision"]]
    latest_run = effective_runs[-1] if effective_runs else run_timeline[-1]
    lines.append("## 日内概览")
    lines.append(
        "- 收盘前最新框架："
        f"{latest_run.get('market_regime') or '未知'} / "
        f"{latest_run.get('confirmed_style') or '未知'} / "
        f"{latest_run.get('latest_status') or '未知'}"
    )
    lines.append(
        f"- 当日共运行 {payload['run_count']} 轮，"
        f"其中有效分析 {payload.get('effective_run_count', len(effective_runs))} 轮。"
    )
    lines.append(f"- 进入过观察或预警名单的股票共 {len(active_symbols)} 只。")

    persistent = [item for item in active_symbols if item["mention_count"] >= 2]
    current_bulls = [
        item
        for item in active_symbols
        if item.get("latest_display_bucket") in {"core_anchor", "new_long", "catchup"}
    ]
    current_risks = [item for item in active_symbols if item.get("latest_risk_tag")]

    lines.append(
        f"- 持续被系统确认的标的 {len(persistent)} 只，"
        f"其中最新仍在多头清单 {len(current_bulls)} 只，"
        f"最新处于风险清单 {len(current_risks)} 只。"
    )
    lines.append("")

    lines.append("## 重点结论")
    if persistent:
        top_persistent = "、".join(
            f"{item['stock_name']}({item['symbol']})"
            for item in persistent[:5]
        )
        lines.append(f"- 持续性最强：{top_persistent}")
    else:
        lines.append("- 持续性最强：暂无跨轮次确认标的")

    if current_bulls:
        top_bulls = "、".join(
            f"{item['stock_name']}({item['symbol']},{item.get('latest_display_bucket')})"
            for item in current_bulls[:5]
        )
        lines.append(f"- 最新可继续跟踪：{top_bulls}")
    else:
        lines.append("- 最新可继续跟踪：暂无")

    if current_risks:
        top_risks = "、".join(
            f"{item['stock_name']}({item['symbol']},{item.get('latest_risk_tag')})"
            for item in current_risks[:5]
        )
        lines.append(f"- 最新风险焦点：{top_risks}")
    else:
        lines.append("- 最新风险焦点：暂无")
    lines.append("")

    post_trade_outcomes = payload.get("post_trade_outcomes")
    if post_trade_outcomes is not None:
        lines.append("## 后验收益")
        lines.append(
            "- 数据状态："
            f"{post_trade_outcomes.get('source_status') or 'unknown'}"
            f" / {post_trade_outcomes.get('detail') or '-'}"
        )
        lines.append(
            "- 触发日覆盖："
            f"{post_trade_outcomes.get('symbols_with_trigger_bar', 0)} / "
            f"{post_trade_outcomes.get('symbol_count', 0)} 只股票有触发日收盘基准。"
        )
        lines.append(
            "- 后续窗口覆盖："
            f"next_open {post_trade_outcomes.get('symbols_with_next_open', 0)}，"
            f"close+1d {post_trade_outcomes.get('symbols_with_close_1d', 0)}，"
            f"close+3d {post_trade_outcomes.get('symbols_with_close_3d', 0)}，"
            f"close+5d {post_trade_outcomes.get('symbols_with_close_5d', 0)}。"
        )
        lines.append("| Cohort | Sample | Trigger | N.Open N | C+1 N | C+3 N | C+5 N | Next Open | Close+1D | Close+3D | Close+5D |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
        for item in post_trade_outcomes.get("cohort_summaries", []):
            def _fmt(value):
                if value is None:
                    return "-"
                return f"{float(value) * 100:.2f}%"

            lines.append(
                "| {cohort} | {sample} | {trigger} | {next_open_n} | {close_1d_n} | {close_3d_n} | {close_5d_n} | {next_open} | {close_1d} | {close_3d} | {close_5d} |".format(
                    cohort=item["cohort"],
                    sample=item["sample_count"],
                    trigger=item["trigger_count"],
                    next_open_n=item["next_open_count"],
                    close_1d_n=item["close_1d_count"],
                    close_3d_n=item["close_3d_count"],
                    close_5d_n=item["close_5d_count"],
                    next_open=_fmt(item["avg_next_open_ret"]),
                    close_1d=_fmt(item["avg_close_ret_1d"]),
                    close_3d=_fmt(item["avg_close_ret_3d"]),
                    close_5d=_fmt(item["avg_close_ret_5d"]),
                )
            )
        lines.append("")

    lines.append("## 活跃标的一览")
    lines.append("| 股票 | 首次出现 | 最后出现 | 提及轮次 | 历史角色 | 最新状态 | 最高分 |")
    lines.append("|---|---|---|---|---|---|---|")
    for item in active_symbols[:20]:
        latest_state = item.get("latest_display_bucket") or item.get("latest_risk_tag") or "已退出"
        seen_buckets = " / ".join(item["seen_buckets"]) if item["seen_buckets"] else "-"
        score = item["max_confidence_score"]
        score_text = f"{float(score):.1f}" if score is not None else "-"
        first_round = item.get("first_seen_round")
        last_round = item.get("last_seen_round")
        first_seen = item["first_seen_time"]
        last_seen = item["last_seen_time"]
        if first_round is not None:
            first_seen = f"{first_seen}(r{int(first_round):02d})"
        if last_round is not None:
            last_seen = f"{last_seen}(r{int(last_round):02d})"
        lines.append(
            "| {name}({symbol}) | {first} | {last} | {mention} | {seen} | {latest} | {score} |".format(
                name=item["stock_name"],
                symbol=item["symbol"],
                first=first_seen,
                last=last_seen,
                mention=item["mention_count"],
                seen=seen_buckets,
                latest=latest_state,
                score=score_text,
            )
        )

    return "\n".join(lines)
