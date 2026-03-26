import polars as pl
from typing import List, Dict, Any
import datetime as dt
import uuid

from config.constants import MARKET_DATA_DATABASE, TICKER_SENTIMENT_TABLE


def ticker_sentiment_to_polars_df(feed: List[Dict[str, Any]]) -> pl.DataFrame:
    rows = []

    for item in feed:
        time_published_str = item.get("time_published")
        if time_published_str:
            dt_obj = dt.datetime.strptime(time_published_str, "%Y%m%dT%H%M%S")
        else:
            dt_obj = None

        ticker_sentiment = item.get("ticker_sentiment", [])
        for ts in ticker_sentiment:
            ticker = ts.get("ticker")
            if not ticker:
                continue

            # Use URL + ticker + time_published as a stable key.
            base = f"{item.get('url', '')}|{ticker}|{time_published_str or ''}"
            stable_id = str(uuid.uuid5(uuid.NAMESPACE_URL, base))

            rows.append(
                {
                    "id": stable_id,
                    "symbol": ticker,
                    "time_published": dt_obj,
                    "source": item.get("source", ""),
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "relevance_score": float(ts.get("relevance_score", 0)),
                    "ticker_sentiment_score": float(
                        ts.get("ticker_sentiment_score", 0)
                    ),
                    "ticker_sentiment_label": ts.get("ticker_sentiment_label", ""),
                    "overall_sentiment_score": float(
                        item.get("overall_sentiment_score", 0)
                    ),
                    "overall_sentiment_label": item.get("overall_sentiment_label", ""),
                }
            )

    df = pl.DataFrame(rows)
    return df


def insert_ticker_sentiment_into_clickhouse(client, df: pl.DataFrame):
    if df.is_empty():
        return
    cols = list(df.columns)
    rows = df.select(cols).rows()
    client.insert(
        f"{MARKET_DATA_DATABASE}.{TICKER_SENTIMENT_TABLE}",
        rows,
        column_names=cols,
    )
