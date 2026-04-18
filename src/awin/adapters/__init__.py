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
from awin.adapters.dcf import DcfSnapshotAdapter
from awin.adapters.master import StockMasterAdapter
from awin.adapters.qmt_bar_1d import QmtBar1dAdapter
from awin.adapters.qmt import QmtSnapshotAdapter
from awin.adapters.research import ResearchCoverageAdapter
from awin.adapters.ths import ThsConceptAdapter
from awin.adapters.ths_hot_app import ThsAppHotConceptAdapter
from awin.adapters.ths_hot_cli import ThsCliHotConceptAdapter
from awin.adapters.ths_market_overview import ThsMarketOverviewAdapter, derive_market_tape

__all__ = [
    "DcfSnapshotAdapter",
    "DcfSnapshotRow",
    "QmtBar1dAdapter",
    "QmtBar1dRow",
    "QmtSnapshotAdapter",
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
