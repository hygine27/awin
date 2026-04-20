from __future__ import annotations

import argparse
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.adapters import (
    DcfHqZjSnapshotAdapter,
    QmtAshareSnapshot5mAdapter,
    QmtBar1dMetricsAdapter,
    ResearchCoverageAdapter,
    SnapshotRequest,
    StockMasterAdapter,
    ThsAppHotConceptAdapter,
    ThsCliHotConceptAdapter,
    ThsConceptAdapter,
    ThsMarketOverviewAdapter,
    TsDailyBasicAdapter,
    TsFinaIndicatorAdapter,
    TsIndexMemberAllAdapter,
    TsMoneyflowCntThsAdapter,
    TsMoneyflowDcAdapter,
    TsMoneyflowIndThsAdapter,
    TsMoneyflowMktDcAdapter,
    TsMoneyflowThsAdapter,
    TsStockBasicAdapter,
    TsStyleDailyMetricsAdapter,
)
from awin.analysis import build_stock_facts
from awin.fund_flow_profile import build_fund_flow_snapshot
from awin.market_understanding import compute_market_understanding
from awin.opportunity_discovery import compute_opportunity_discovery
from awin.risk_surveillance import compute_risk_surveillance
from awin.style_profile import build_style_profiles


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile AWIN M0 source and stage timings.")
    now = datetime.now()
    parser.add_argument("--trade-date", default=now.strftime("%Y-%m-%d"))
    parser.add_argument("--snapshot-time", default=now.strftime("%H:%M:%S"))
    parser.add_argument("--analysis-snapshot-ts", default=now.strftime("%Y-%m-%dT%H:%M:%S"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    request = SnapshotRequest(
        trade_date=args.trade_date,
        snapshot_time=args.snapshot_time,
        analysis_snapshot_ts=args.analysis_snapshot_ts,
    )
    trade_day = date.fromisoformat(request.trade_date)
    timings: list[dict[str, object]] = []
    stage_timings: list[dict[str, object]] = []

    def run_serial_stage(stage_name: str, steps: dict[str, Callable[[], object]]) -> dict[str, object]:
        started = time.time()
        results: dict[str, object] = {}
        for step_name, fn in steps.items():
            _, result, elapsed, size = _timed_step(step_name, fn)
            results[step_name] = result
            timings.append(
                {
                    "stage": stage_name,
                    "step": step_name,
                    "elapsed_sec": elapsed,
                    "size": size,
                }
            )
        wall_elapsed = round(time.time() - started, 2)
        stage_timings.append(
            {
                "stage": stage_name,
                "elapsed_sec": wall_elapsed,
                "step_count": len(steps),
            }
        )
        return results

    def run_parallel_stage(
        stage_name: str,
        steps: dict[str, Callable[[], object]],
        *,
        max_workers: int = 4,
    ) -> dict[str, object]:
        started = time.time()
        worker_count = max(1, min(len(steps), max_workers))
        completed_results: dict[str, object] = {}
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(_timed_step, step_name, fn): step_name
                for step_name, fn in steps.items()
            }
            for future in as_completed(future_map):
                step_name, result, elapsed, size = future.result()
                completed_results[step_name] = result
                timings.append(
                    {
                        "stage": stage_name,
                        "step": step_name,
                        "elapsed_sec": elapsed,
                        "size": size,
                    }
                )
        wall_elapsed = round(time.time() - started, 2)
        stage_timings.append(
            {
                "stage": stage_name,
                "elapsed_sec": wall_elapsed,
                "step_count": len(steps),
            }
        )
        return {name: completed_results[name] for name in steps}

    def _timed_step(step_name: str, fn: Callable[[], object]) -> tuple[str, object, float, int | None]:
        started = time.time()
        result = fn()
        elapsed = round(time.time() - started, 2)
        size = len(result) if hasattr(result, "__len__") else None
        return step_name, result, elapsed, size

    source_results = run_serial_stage(
        "source_load",
        {
            "stock_master": lambda: StockMasterAdapter().load_rows(),
            "ths_concepts": lambda: ThsConceptAdapter().load_rows(request),
            "ths_app_hot_concept": lambda: ThsAppHotConceptAdapter().load_rows(request),
            "ths_cli_hot_concept": lambda: ThsCliHotConceptAdapter().load_rows(request),
            "research": lambda: ResearchCoverageAdapter().load_rows(request),
            "qmt_ashare_snapshot_5m": lambda: QmtAshareSnapshot5mAdapter().load_rows(request),
            "dcf_hq_zj_snapshot": lambda: DcfHqZjSnapshotAdapter().load_rows_with_health(request)[0],
            "ths_market_overview": lambda: ThsMarketOverviewAdapter().load_market_tape(),
            "ts_stock_basic": lambda: TsStockBasicAdapter().load_rows_with_health()[0],
            "ts_daily_basic": lambda: TsDailyBasicAdapter().load_rows_with_health(request.trade_date)[0],
            "ts_index_member_all": lambda: TsIndexMemberAllAdapter().load_rows_with_health(request.trade_date)[0],
            "ts_style_daily_metrics": lambda: TsStyleDailyMetricsAdapter().load_rows_with_health(request.trade_date)[0],
            "ts_fina_indicator": lambda: TsFinaIndicatorAdapter().load_rows_with_health(request.trade_date)[0],
            "ts_moneyflow_ths": lambda: TsMoneyflowThsAdapter().load_rows_with_health(request.trade_date)[0],
            "ts_moneyflow_dc": lambda: TsMoneyflowDcAdapter().load_rows_with_health(request.trade_date)[0],
            "ts_moneyflow_cnt_ths": lambda: TsMoneyflowCntThsAdapter().load_rows_with_health(request.trade_date)[0],
            "ts_moneyflow_ind_ths": lambda: TsMoneyflowIndThsAdapter().load_rows_with_health(request.trade_date)[0],
            "ts_moneyflow_mkt_dc": lambda: TsMoneyflowMktDcAdapter().load_rows_with_health(request.trade_date)[0],
        },
    )

    stock_master = source_results["stock_master"]
    ths_concepts = source_results["ths_concepts"]
    ths_app_hot = source_results["ths_app_hot_concept"]
    ths_cli_hot = source_results["ths_cli_hot_concept"]
    research = source_results["research"]
    qmt_rows = source_results["qmt_ashare_snapshot_5m"]
    dcf_rows = source_results["dcf_hq_zj_snapshot"]
    market_tape = source_results["ths_market_overview"]
    ts_stock_basic_rows = source_results["ts_stock_basic"]
    ts_daily_basic_rows = source_results["ts_daily_basic"]
    ts_index_member_all_rows = source_results["ts_index_member_all"]
    ts_style_daily_metric_rows = source_results["ts_style_daily_metrics"]
    ts_fina_indicator_rows = source_results["ts_fina_indicator"]
    ts_moneyflow_ths_rows = source_results["ts_moneyflow_ths"]
    ts_moneyflow_dc_rows = source_results["ts_moneyflow_dc"]
    ts_moneyflow_cnt_rows = source_results["ts_moneyflow_cnt_ths"]
    ts_moneyflow_ind_rows = source_results["ts_moneyflow_ind_ths"]
    ts_moneyflow_mkt_rows = source_results["ts_moneyflow_mkt_dc"]

    derived_results = run_parallel_stage(
        "derived_build",
        {
            "qmt_bar_1d_metrics": lambda: QmtBar1dMetricsAdapter().load_rows_with_health(
                [item.symbol for item in qmt_rows],
                start_date=(trade_day - timedelta(days=45)).isoformat(),
                trade_date=request.trade_date,
            )[0],
            "build_style_profiles": lambda: build_style_profiles(
                ts_stock_basic_rows,
                ts_daily_basic_rows,
                ts_index_member_all_rows,
                daily_metric_rows=ts_style_daily_metric_rows,
                fina_indicator_rows=ts_fina_indicator_rows,
                trade_date=request.trade_date.replace("-", ""),
            ),
            "build_fund_flow_snapshot": lambda: build_fund_flow_snapshot(
                ts_moneyflow_ths_rows,
                ts_moneyflow_dc_rows,
                ts_moneyflow_cnt_rows,
                ts_moneyflow_ind_rows,
                ts_moneyflow_mkt_rows,
            ),
        },
    )

    qmt_bar_metrics = derived_results["qmt_bar_1d_metrics"]
    style_profiles = derived_results["build_style_profiles"]
    fund_flow_snapshot = derived_results["build_fund_flow_snapshot"]
    style_profile_rows = [item.to_dict() for item in style_profiles]
    ths_hot_concepts = ths_app_hot + ths_cli_hot

    analysis_results = run_parallel_stage(
        "analysis_build",
        {
            "compute_market_understanding": lambda: compute_market_understanding(
                stock_master,
                qmt_rows,
                dcf_rows,
                ths_concepts,
                ths_hot_concepts=ths_hot_concepts,
                market_tape=market_tape,
                style_profiles=style_profile_rows,
                fund_flow_snapshot=fund_flow_snapshot,
            ),
            "build_stock_facts": lambda: build_stock_facts(
                stock_master,
                qmt_rows,
                dcf_rows,
                [],
                ths_concepts,
                research,
                qmt_bar_metrics=qmt_bar_metrics,
                style_profiles=style_profile_rows,
                fund_flow_snapshot=fund_flow_snapshot,
            ),
        },
    )

    market_understanding = analysis_results["compute_market_understanding"]
    stock_facts = analysis_results["build_stock_facts"]

    decision_results = run_parallel_stage(
        "decision_build",
        {
            "compute_opportunity_discovery": lambda: compute_opportunity_discovery(stock_facts, market_understanding),
            "compute_risk_surveillance": lambda: compute_risk_surveillance(stock_facts, market_understanding),
        },
        max_workers=2,
    )

    opportunity = decision_results["compute_opportunity_discovery"]
    risk = decision_results["compute_risk_surveillance"]

    total_elapsed = round(sum(float(item["elapsed_sec"]) for item in stage_timings), 2)
    payload = {
        "request": {
            "trade_date": request.trade_date,
            "snapshot_time": request.snapshot_time,
            "analysis_snapshot_ts": request.analysis_snapshot_ts,
        },
        "stage_timings": stage_timings,
        "timings": timings,
        "total_elapsed_sec": total_elapsed,
        "summary": {
            "confirmed_style": market_understanding.confirmed_style,
            "latest_status": market_understanding.latest_status,
            "latest_dominant_style": market_understanding.latest_dominant_style,
            "market_regime": market_understanding.market_regime,
            "core_anchor": len(opportunity.core_anchor_watchlist),
            "new_long": len(opportunity.new_long_watchlist),
            "catchup": len(opportunity.catchup_watchlist),
            "risk": len(risk.short_watchlist),
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
