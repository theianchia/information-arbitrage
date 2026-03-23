import polars as pl

from config.constants import MARKET_DATA_DATABASE, STOCK_OHLCV_TABLE, LOOKBACK_DAYS
from typing import Dict, Any
import datetime as dt


def ohlcv_to_polars_df(symbol: str, data: Dict[str, Any]) -> pl.DataFrame:
    rows = []
    cutoff_date = dt.date.today() - dt.timedelta(days=LOOKBACK_DAYS)

    for date_str, values in data.items():
        date_obj = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        if date_obj < cutoff_date:
            continue

        rows.append(
            {
                "symbol": symbol,
                "date": date_obj,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"]),
            }
        )

    if not rows:
        return pl.DataFrame([])

    df = pl.DataFrame(rows)
    df = df.with_columns(
        [
            pl.col("date").cast(pl.Date),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.UInt64),
        ]
    )
    return df


def insert_ohlcv_into_clickhouse(client, df: pl.DataFrame):
    if df.is_empty():
        return
    cols = list(df.columns)
    rows = df.select(cols).rows()
    client.insert(
        f"{MARKET_DATA_DATABASE}.{STOCK_OHLCV_TABLE}",
        rows,
        column_names=cols,
    )
