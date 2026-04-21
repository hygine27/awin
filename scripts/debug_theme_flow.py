from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.adapters import SnapshotRequest
from awin.adapters.ts_moneyflow_cnt_ths import TsMoneyflowCntThsAdapter
from awin.builders.m0 import _build_primary_theme_groups, build_m0_snapshot_bundle
from awin.config import get_app_config
from awin.utils.structured_config import load_structured_config


def _fmt_pct(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.{digits}f}%"


def _fmt_amt_yi(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value / 100000000:.2f}亿"


def _fmt_amt_wan(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value / 10000:.2f}万"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect constituent-level flow for one meta theme.")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--snapshot-time", required=True)
    parser.add_argument("--theme", required=True)
    parser.add_argument("--limit", type=int, default=9999)
    parser.add_argument("--sort", choices=("desc", "asc"), default="desc")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    snapshot_time = args.snapshot_time if len(args.snapshot_time) == 8 else f"{args.snapshot_time}:00"
    request = SnapshotRequest(
        trade_date=args.trade_date,
        snapshot_time=snapshot_time,
        analysis_snapshot_ts=f"{args.trade_date}T{snapshot_time}",
    )
    result = build_m0_snapshot_bundle(request, round_seq=1)
    concept_board_rows = TsMoneyflowCntThsAdapter().load_rows(args.trade_date)

    theme = str(args.theme)
    overlay_payload = load_structured_config(get_app_config().ths_overlay_config_path, label="ths overlay config")
    theme_config_concepts = [str(item) for item in overlay_payload.get("meta_themes", {}).get(theme, [])]
    facts = [item for item in result.stock_facts if theme in item.meta_themes]
    primary_theme_groups = _build_primary_theme_groups(result.stock_facts, result.bundle.market_understanding)
    primary_facts = list(primary_theme_groups.get(theme, []))
    facts.sort(
        key=lambda item: (
            float(item.main_net_inflow) if item.main_net_inflow is not None else -10**30,
            float(item.amount) if item.amount is not None else 0.0,
        ),
        reverse=args.sort == "desc",
    )

    flow_covered = [item for item in facts if item.main_net_inflow is not None]
    flow_strength_covered = [item for item in flow_covered if item.amount not in {None, 0, 0.0}]
    total_main_net = sum(float(item.main_net_inflow or 0.0) for item in flow_covered)
    total_amount = sum(float(item.amount or 0.0) for item in flow_strength_covered)
    positive_flow_count = sum(1 for item in flow_covered if float(item.main_net_inflow or 0.0) > 0)
    flow_ratio = (total_main_net / total_amount) if total_amount else None
    primary_flow_covered = [item for item in primary_facts if item.main_net_inflow is not None]
    primary_flow_strength_covered = [item for item in primary_flow_covered if item.amount not in {None, 0, 0.0}]
    primary_total_main_net = sum(float(item.main_net_inflow or 0.0) for item in primary_flow_covered)
    primary_total_amount = sum(float(item.amount or 0.0) for item in primary_flow_strength_covered)
    primary_positive_flow_count = sum(1 for item in primary_flow_covered if float(item.main_net_inflow or 0.0) > 0)
    primary_flow_ratio = (primary_total_main_net / primary_total_amount) if primary_total_amount else None

    top_theme = next((item for item in result.bundle.market_understanding.top_meta_themes if item.meta_theme == theme), None)
    strongest_concepts = list(top_theme.strongest_concepts) if top_theme else []

    print(f"theme={theme} trade_date={args.trade_date} snapshot_time={snapshot_time}")
    print(f"member_count={len(facts)} flow_covered={len(flow_covered)}")
    print(f"theme_config_concepts={' / '.join(theme_config_concepts) if theme_config_concepts else 'n/a'}")
    print(f"strongest_concepts={' / '.join(strongest_concepts) if strongest_concepts else 'n/a'}")
    print(f"old_sum_main_net_inflow={_fmt_amt_yi(total_main_net)}")
    print(f"old_positive_main_flow_ratio={_fmt_pct((positive_flow_count / len(flow_covered)) if flow_covered else None, 1)}")
    print(f"new_main_flow_rate={_fmt_pct(flow_ratio, 2)}")
    print(f"amount_sum={_fmt_amt_yi(total_amount)}")
    print(f"primary_member_count={len(primary_facts)}")
    print(f"primary_sum_main_net_inflow={_fmt_amt_yi(primary_total_main_net)}")
    print(f"primary_positive_main_flow_ratio={_fmt_pct((primary_positive_flow_count / len(primary_flow_covered)) if primary_flow_covered else None, 1)}")
    print(f"primary_main_flow_rate={_fmt_pct(primary_flow_ratio, 2)}")
    print(f"primary_amount_sum={_fmt_amt_yi(primary_total_amount)}")
    print(f"single_theme_members={sum(1 for item in facts if len(item.meta_themes) == 1)}")
    print(f"multi_theme_members={sum(1 for item in facts if len(item.meta_themes) > 1)}")
    print()

    latest_board_by_concept: dict[str, dict[str, object]] = {}
    for row in concept_board_rows:
        concept_name = str(row.get("name") or "").strip()
        if concept_name not in theme_config_concepts:
            continue
        current = latest_board_by_concept.get(concept_name)
        row_trade_date = str(row.get("trade_date") or "")
        if current is None or row_trade_date >= str(current.get("trade_date") or ""):
            latest_board_by_concept[concept_name] = row
    print("== theme_concept_board_t1 ==")
    print("concept\ttrade_date\tcompany_num\tnet_buy_amount\tnet_sell_amount\tnet_amount")
    for concept in theme_config_concepts:
        row = latest_board_by_concept.get(concept)
        if row is None:
            print(f"{concept}\tn/a\tn/a\tn/a\tn/a\tn/a")
            continue
        print(
            f"{concept}\t{row.get('trade_date')}\t{row.get('company_num')}\t"
            f"{_fmt_amt_yi(float(row.get('net_buy_amount') or 0.0) * 100000000)}\t"
            f"{_fmt_amt_yi(float(row.get('net_sell_amount') or 0.0) * 100000000)}\t"
            f"{_fmt_amt_yi(float(row.get('net_amount') or 0.0) * 100000000)}"
        )
    print()

    concept_rows: list[tuple[str, int, float, float, float | None]] = []
    for concept in theme_config_concepts:
        members = [item for item in facts if concept in item.concepts]
        concept_flow_covered = [item for item in members if item.main_net_inflow is not None]
        concept_amount_covered = [item for item in concept_flow_covered if item.amount not in {None, 0, 0.0}]
        concept_net = sum(float(item.main_net_inflow or 0.0) for item in concept_flow_covered)
        concept_amt = sum(float(item.amount or 0.0) for item in concept_amount_covered)
        concept_rate = (concept_net / concept_amt) if concept_amt else None
        concept_rows.append((concept, len(members), concept_net, concept_amt, concept_rate))
    concept_rows.sort(key=lambda item: (item[2], item[1]), reverse=True)

    print("== theme_concept_breakdown ==")
    print("concept\tmembers\tmain_net_inflow\tamount\tflow_rate")
    for concept, count, concept_net, concept_amt, concept_rate in concept_rows:
        print(f"{concept}\t{count}\t{_fmt_amt_yi(concept_net)}\t{_fmt_amt_yi(concept_amt)}\t{_fmt_pct(concept_rate, 2)}")
    print()

    overlap_rows: list[tuple[str, int]] = []
    overlap_theme_counts: dict[str, int] = {}
    for item in facts:
        for other_theme in item.meta_themes:
            if other_theme == theme:
                continue
            overlap_theme_counts[other_theme] = overlap_theme_counts.get(other_theme, 0) + 1
    overlap_rows = sorted(overlap_theme_counts.items(), key=lambda pair: (pair[1], pair[0]), reverse=True)
    print("== overlap_meta_themes ==")
    print("meta_theme\tmember_count")
    for other_theme, count in overlap_rows[:20]:
        print(f"{other_theme}\t{count}")
    print()

    print("== constituents ==")
    print("symbol\tstock_name\tmain_net_inflow\tamount\tflow_ratio\tmeta_themes\tconcepts")
    for item in facts[: args.limit]:
        flow_ratio_item = None
        if item.main_net_inflow is not None and item.amount not in {None, 0, 0.0}:
            flow_ratio_item = float(item.main_net_inflow) / float(item.amount)
        print(
            "\t".join(
                [
                    item.symbol,
                    item.stock_name,
                    _fmt_amt_yi(item.main_net_inflow),
                    _fmt_amt_yi(item.amount),
                    _fmt_pct(flow_ratio_item, 2),
                    ",".join(item.meta_themes),
                    ",".join(item.concepts),
                ]
            )
        )


if __name__ == "__main__":
    main()
