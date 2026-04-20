from __future__ import annotations

import json
from pathlib import Path

from awin.storage.db import connect_sqlite, init_db
from awin.style_profile.engine import StyleProfile


def persist_style_profiles(db_path: Path, profiles: list[StyleProfile]) -> None:
    init_db(db_path)
    if not profiles:
        return

    trade_dates = sorted({profile.trade_date for profile in profiles if profile.trade_date})
    with connect_sqlite(db_path) as connection:
        for trade_date in trade_dates:
            connection.execute("DELETE FROM style_profile WHERE trade_date = ?", (trade_date,))

        for profile in profiles:
            connection.execute(
                """
                INSERT INTO style_profile (
                    trade_date,
                    symbol,
                    market_type_label,
                    exchange_label,
                    ownership_style,
                    legacy_industry_label,
                    sw_l1_code,
                    sw_l1_name,
                    sw_l2_code,
                    sw_l2_name,
                    sw_l3_code,
                    sw_l3_name,
                    free_float_share,
                    float_mv,
                    total_mv,
                    avg_amount_20d,
                    size_bucket_pct,
                    size_bucket_abs,
                    capacity_bucket,
                    dividend_value_score,
                    growth_valuation_score,
                    quality_growth_score,
                    sales_growth_score,
                    profit_growth_score,
                    low_vol_defensive_score,
                    high_beta_attack_score,
                    dividend_style,
                    valuation_style,
                    growth_style,
                    quality_style,
                    volatility_style,
                    composite_style_labels_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    profile.trade_date,
                    profile.symbol,
                    profile.market_type_label,
                    profile.exchange_label,
                    profile.ownership_style,
                    profile.legacy_industry_label,
                    profile.sw_l1_code,
                    profile.sw_l1_name,
                    profile.sw_l2_code,
                    profile.sw_l2_name,
                    profile.sw_l3_code,
                    profile.sw_l3_name,
                    profile.free_float_share,
                    profile.float_mv,
                    profile.total_mv,
                    profile.avg_amount_20d,
                    profile.size_bucket_pct,
                    profile.size_bucket_abs,
                    profile.capacity_bucket,
                    profile.dividend_value_score,
                    profile.growth_valuation_score,
                    profile.quality_growth_score,
                    profile.sales_growth_score,
                    profile.profit_growth_score,
                    profile.low_vol_defensive_score,
                    profile.high_beta_attack_score,
                    profile.dividend_style,
                    profile.valuation_style,
                    profile.growth_style,
                    profile.quality_style,
                    profile.volatility_style,
                    json.dumps(profile.composite_style_labels, ensure_ascii=False),
                ),
            )
        connection.commit()
