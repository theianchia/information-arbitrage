import polars as pl
from typing import List, Dict, Any
import datetime as dt

from config.constants import MARKET_DATA_DATABASE, TECH_NEWS_TABLE


def news_to_polars_df(feed: List[Dict[str, Any]]) -> pl.DataFrame:
    import uuid

    rows = []

    for item in feed:
        time_published_str = item.get("time_published")
        if time_published_str:
            dt_obj = dt.datetime.strptime(time_published_str, "%Y%m%dT%H%M%S")
        else:
            dt_obj = None

        tickers = [
            ts.get("ticker")
            for ts in item.get("ticker_sentiment", [])
            if ts.get("ticker")
        ]

        # Use URL + time_published as a stable key
        base = f"{item.get('url', '')}|{time_published_str or ''}"
        stable_id = str(uuid.uuid5(uuid.NAMESPACE_URL, base))

        rows.append(
            {
                "id": stable_id,
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "time_published": dt_obj,
                "source": item.get("source", ""),
                "tickers": tickers,
            }
        )

    df = pl.DataFrame(rows)
    return df


def insert_news_into_clickhouse(client, df: pl.DataFrame):
    if df.is_empty():
        return
    cols = list(df.columns)
    rows = df.select(cols).rows()
    client.insert(
        f"{MARKET_DATA_DATABASE}.{TECH_NEWS_TABLE}",
        rows,
        column_names=cols,
    )
