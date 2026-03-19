import polars as pl

from clients.alpha_vantage import fetch_latest_tech_news, get_unique_tickers_from_news, fetch_ohlcv_for_symbol
from clients.clickhouse import get_clickhouse_client, init_clickhouse
from services.news_ingestion import news_to_polars_df, insert_news_into_clickhouse
from services.ohlcv_ingestion import ohlcv_to_polars_df, insert_ohlcv_into_clickhouse

def seed_news_and_ohlcv():
    print("Alpha Vantage | Fetching latest tech news from Alpha Vantage")
    feed = fetch_latest_tech_news()
    news_df = news_to_polars_df(feed)
    print(f"Alpha Vantage | Got {news_df.height} news items")

    tickers = get_unique_tickers_from_news(feed)
    print(f"Alpha Vantage | Tickers extracted from news: {tickers}")

    print("ClickHouse | Connecting to ClickHouse")
    client = get_clickhouse_client()
    init_clickhouse(client)

    print("ClickHouse | Inserting news into ClickHouse")
    insert_news_into_clickhouse(client, news_df)

    all_ohlcv = []
    for ticker in tickers:
        print(f"Alpha Vantage | Fetching OHLCV for {ticker}")
        ticker_data = fetch_ohlcv_for_symbol(ticker)
        if not ticker_data:
            print(f"Alpha Vantage | No OHLCV data for {ticker}")
            continue
        ticker_ohlcv_df = ohlcv_to_polars_df(ticker_data)
        all_ohlcv.append(ticker_ohlcv_df)

    if all_ohlcv:
        ohlcv_df = pl.concat(all_ohlcv, how="vertical")
        print(f"ClickHouse | Inserting {ohlcv_df.height} OHLCV rows into ClickHouse")
        insert_ohlcv_into_clickhouse(client, ohlcv_df)
    else:
        print("Alpha Vantage | No OHLCV data collected; nothing to insert")

    print("ETL | Done")
