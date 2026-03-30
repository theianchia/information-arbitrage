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
    sentiment_df = ticker_sentiment_to_polars_df(feed, ticker)

    print("ClickHouse | Connecting to ClickHouse")
    client = get_clickhouse_client()
    init_clickhouse(client)

    print(
        f"ClickHouse | Inserting {sentiment_df.height} {ticker} sentiment rows into ClickHouse"
    )
    insert_ticker_sentiment_into_clickhouse(client, sentiment_df)

    ticker_data = fetch_ohlcv_for_symbol(ticker)
    if not ticker_data:
        print(f"Alpha Vantage | No OHLCV data for {ticker}")
        return
    ohlcv_df = ohlcv_to_polars_df(ticker, ticker_data)
    print(
        f"ClickHouse | Inserting {ohlcv_df.height} {ticker} OHLCV rows into ClickHouse"
    )
    insert_ohlcv_into_clickhouse(client, ohlcv_df)

    print("ETL | Done")
