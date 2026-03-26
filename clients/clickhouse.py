import clickhouse_connect

from config.constants import (
    MARKET_DATA_DATABASE,
    TICKER_SENTIMENT_TABLE,
    TICKER_OHLCV_TABLE,
)
from config.settings import SETTINGS


def get_clickhouse_client(database: str = "default"):
    return clickhouse_connect.get_client(
        host=SETTINGS.clickhouse_host,
        port=SETTINGS.clickhouse_port,
        username=SETTINGS.clickhouse_username,
        password=SETTINGS.clickhouse_password,
        database=database,
    )


def init_clickhouse(client):
    client.command(f"CREATE DATABASE IF NOT EXISTS {MARKET_DATA_DATABASE}")

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {MARKET_DATA_DATABASE}.{TICKER_SENTIMENT_TABLE} (
            id String,
            symbol String,
            time_published DateTime,
            source String,
            title String,
            url String,
            relevance_score Float64,
            ticker_sentiment_score Float64,
            ticker_sentiment_label String,
            overall_sentiment_score Float64,
            overall_sentiment_label String
        )
        ENGINE = MergeTree
        ORDER BY (symbol, time_published, id)
        """
    )

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {MARKET_DATA_DATABASE}.{TICKER_OHLCV_TABLE} (
            symbol String,
            date Date,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume UInt64
        )
        ENGINE = MergeTree
        ORDER BY (symbol, date)
        """
    )
