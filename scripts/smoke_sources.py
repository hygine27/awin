from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from awin.adapters import (
    DcfSnapshotAdapter,
    QmtSnapshotAdapter,
    ResearchCoverageAdapter,
    SnapshotRequest,
    StockMasterAdapter,
    ThsConceptAdapter,
    ThsMarketOverviewAdapter,
)
from awin.analysis import build_stock_facts
from awin.market_understanding import compute_market_understanding
from awin.opportunity_discovery import compute_opportunity_discovery
from awin.risk_surveillance import compute_risk_surveillance


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke check awin source adapters and market_understanding.")
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

    stock_master_adapter = StockMasterAdapter()
    ths_adapter = ThsConceptAdapter()
    research_adapter = ResearchCoverageAdapter()
    qmt_adapter = QmtSnapshotAdapter()
    dcf_adapter = DcfSnapshotAdapter()
    market_overview_adapter = ThsMarketOverviewAdapter()

    stock_master = stock_master_adapter.load_rows()
    ths_concepts = ths_adapter.load_rows(request)
    research = research_adapter.load_rows(request)
    qmt_rows = qmt_adapter.load_rows(request)
    dcf_rows, dcf_health = dcf_adapter.load_rows_with_health(request)
    market_tape = market_overview_adapter.load_market_tape()

    market_output = compute_market_understanding(stock_master, qmt_rows, dcf_rows, ths_concepts, market_tape=market_tape)
    stock_facts = build_stock_facts(stock_master, qmt_rows, dcf_rows, [], ths_concepts, research)
    opportunity_output = compute_opportunity_discovery(stock_facts, market_output)
    risk_output = compute_risk_surveillance(stock_facts, market_output)

    payload = {
        "request": {
            "trade_date": request.trade_date,
            "snapshot_time": request.snapshot_time,
            "analysis_snapshot_ts": request.analysis_snapshot_ts,
        },
        "source_health": {
            "stock_master": stock_master_adapter.health().to_dict(),
            "ths_concepts": ths_adapter.health().to_dict(),
            "research": research_adapter.health().to_dict(),
            "qmt": qmt_adapter.health().to_dict(),
            "dcf": dcf_health.to_dict(),
            "ths_market_overview": market_overview_adapter.health().to_dict(),
        },
        "source_counts": {
            "stock_master": len(stock_master),
            "ths_concepts": len(ths_concepts),
            "research": len(research),
            "qmt_rows": len(qmt_rows),
            "dcf_rows": len(dcf_rows),
            "stock_facts": len(stock_facts),
        },
        "market_understanding": market_output.to_dict(),
        "opportunity_discovery": opportunity_output.to_dict(),
        "risk_surveillance": risk_output.to_dict(),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
