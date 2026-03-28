import polars as pl

from clients.alpha_vantage import (
    fetch_latest_news_sentiment_for_ticker,
    fetch_ohlcv_for_symbol,
)
from clients.clickhouse import get_clickhouse_client, init_clickhouse
from services.ingestion.sentiment_ingestion import (
    ticker_sentiment_to_polars_df,
    insert_ticker_sentiment_into_clickhouse,
)
from services.ingestion.ohlcv_ingestion import (
    ohlcv_to_polars_df,
    insert_ohlcv_into_clickhouse,
)


def seed_sentiment_and_ohlcv(ticker: str):
    print("Alpha Vantage | Fetching latest tech news from Alpha Vantage")
    feed = fetch_latest_news_sentiment_for_ticker(ticker)
    sentiment_df = ticker_sentiment_to_polars_df(feed)
    print(f"Alpha Vantage | Got {sentiment_df.height} ticker sentiment rows")

    tickers = (
        sorted(sentiment_df["symbol"].unique().to_list())
        if not sentiment_df.is_empty()
        else []
    )
    print(f"Alpha Vantage | Tickers extracted from news: {tickers}")

    print("ClickHouse | Connecting to ClickHouse")
    client = get_clickhouse_client()
    init_clickhouse(client)

    print("ClickHouse | Inserting ticker sentiment into ClickHouse")
    insert_ticker_sentiment_into_clickhouse(client, sentiment_df)

    all_ohlcv = []
    for ticker in tickers:
        print(f"Alpha Vantage | Fetching OHLCV for {ticker}")
        ticker_data = fetch_ohlcv_for_symbol(ticker)
        if not ticker_data:
            print(f"Alpha Vantage | No OHLCV data for {ticker}")
            continue
        ticker_ohlcv_df = ohlcv_to_polars_df(ticker, ticker_data)
        all_ohlcv.append(ticker_ohlcv_df)

    if all_ohlcv:
        ohlcv_df = pl.concat(all_ohlcv, how="vertical")
        print(f"ClickHouse | Inserting {ohlcv_df.height} OHLCV rows into ClickHouse")
        insert_ohlcv_into_clickhouse(client, ohlcv_df)
    else:
        print("Alpha Vantage | No OHLCV data collected; nothing to insert")

    print("ETL | Done")
