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
from awin.adapters.qmt_bar_1d_metrics import QmtBar1dMetricsAdapter
from awin.adapters.research_coverage import ResearchCoverageAdapter
from awin.adapters.stock_master import StockMasterAdapter
from awin.adapters.ths_app_hot_concept import ThsAppHotConceptAdapter
from awin.adapters.ths_cli_hot_concept import ThsCliHotConceptAdapter
from awin.adapters.ths_concept import ThsConceptAdapter
from awin.adapters.ths_market_overview import ThsMarketOverviewAdapter, derive_market_tape
from awin.adapters.ts_adj_factor import TsAdjFactorAdapter
from awin.adapters.ts_daily import TsDailyAdapter
from awin.adapters.ts_daily_basic import TsDailyBasicAdapter
from awin.adapters.ts_fina_indicator import TsFinaIndicatorAdapter
from awin.adapters.ts_index_member_all import TsIndexMemberAllAdapter
from awin.adapters.ts_moneyflow_cnt_ths import TsMoneyflowCntThsAdapter
from awin.adapters.ts_moneyflow_dc import TsMoneyflowDcAdapter
from awin.adapters.ts_moneyflow_ind_ths import TsMoneyflowIndThsAdapter
from awin.adapters.ts_moneyflow_mkt_dc import TsMoneyflowMktDcAdapter
from awin.adapters.ts_moneyflow_ths import TsMoneyflowThsAdapter
from awin.adapters.ts_stock_basic import TsStockBasicAdapter
from awin.adapters.ts_style_daily_metrics import TsStyleDailyMetricsAdapter

__all__ = [
    "DcfHqZjSnapshotAdapter",
    "DcfSnapshotRow",
    "QmtAshareSnapshot5mAdapter",
    "QmtBar1dAdapter",
    "QmtBar1dMetricsAdapter",
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
    "TsAdjFactorAdapter",
    "TsDailyAdapter",
    "TsDailyBasicAdapter",
    "TsFinaIndicatorAdapter",
    "TsIndexMemberAllAdapter",
    "TsMoneyflowCntThsAdapter",
    "TsMoneyflowDcAdapter",
    "TsMoneyflowIndThsAdapter",
    "TsMoneyflowMktDcAdapter",
    "TsMoneyflowThsAdapter",
    "TsStockBasicAdapter",
    "TsStyleDailyMetricsAdapter",
    "derive_market_tape",
]
