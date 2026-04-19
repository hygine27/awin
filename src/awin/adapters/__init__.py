"""Read-only source adapters for awin."""

from awin.adapters.base import SnapshotRequest
from awin.adapters.contracts import (
    DcfSnapshotRow,
    QmtBar1dRow,
    QmtSnapshotRow,
    ResearchCoverageRow,
    SnapshotSourceBundle,
    SourceHealth,
    StockMasterRow,
    ThsConceptRow,
    ThsHotConceptRow,
)
from awin.adapters.dcf_hq_zj_snapshot import DcfHqZjSnapshotAdapter
from awin.adapters.qmt_ashare_snapshot_5m import QmtAshareSnapshot5mAdapter
from awin.adapters.qmt_bar_1d import QmtBar1dAdapter
from awin.adapters.research_coverage import ResearchCoverageAdapter
from awin.adapters.stock_master import StockMasterAdapter
from awin.adapters.ths_app_hot_concept import ThsAppHotConceptAdapter
from awin.adapters.ths_cli_hot_concept import ThsCliHotConceptAdapter
from awin.adapters.ths_concept import ThsConceptAdapter
from awin.adapters.ths_market_overview import ThsMarketOverviewAdapter, derive_market_tape

__all__ = [
    "DcfHqZjSnapshotAdapter",
    "DcfSnapshotRow",
    "QmtAshareSnapshot5mAdapter",
    "QmtBar1dAdapter",
    "QmtBar1dRow",
    "QmtSnapshotRow",
    "ResearchCoverageAdapter",
    "ResearchCoverageRow",
    "SnapshotRequest",
    "SnapshotSourceBundle",
    "SourceHealth",
    "StockMasterAdapter",
    "StockMasterRow",
    "ThsConceptAdapter",
    "ThsConceptRow",
    "ThsAppHotConceptAdapter",
    "ThsCliHotConceptAdapter",
    "ThsHotConceptRow",
    "ThsMarketOverviewAdapter",
    "derive_market_tape",
]
