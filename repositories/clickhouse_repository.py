from typing import Any

from clients.clickhouse import get_clickhouse_client, init_clickhouse
from config.constants import (
    MARKET_DATA_DATABASE,
    TICKER_OHLCV_TABLE,
    TICKER_SENTIMENT_TABLE,
)


def query_rows(sql: str) -> list[dict[str, Any]]:
    client = get_clickhouse_client()
    init_clickhouse(client)
    result = client.query(sql)
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def fetch_ticker_sentiment_in_window(
    ticker: str, lookback_days: int, max_rows: int = 2000
) -> list[dict[str, Any]]:
    safe_ticker = ticker.replace("'", "''").upper()
    safe_days = max(1, min(int(lookback_days), 365))
    safe_max = max(1, min(int(max_rows), 5000))
    sql = f"""
    SELECT
        id,
        symbol,
        title,
        summary,
        url,
        time_published,
        source,
        relevance_score,
        ticker_sentiment_score,
        ticker_sentiment_label,
        overall_sentiment_score,
        overall_sentiment_label
    FROM {MARKET_DATA_DATABASE}.{TICKER_SENTIMENT_TABLE}
    WHERE symbol = '{safe_ticker}'
        AND time_published >= now() - INTERVAL {safe_days} DAY
    ORDER BY time_published DESC
    LIMIT {safe_max}
    """
    return query_rows(sql)


def fetch_ticker_ohlcv_in_window(
    ticker: str, lookback_days: int
) -> list[dict[str, Any]]:
    safe_ticker = ticker.replace("'", "''").upper()
    safe_days = max(1, min(int(lookback_days), 365))
    sql = f"""
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume
    FROM {MARKET_DATA_DATABASE}.{TICKER_OHLCV_TABLE}
    WHERE symbol = '{safe_ticker}'
        AND date >= today() - INTERVAL {safe_days} DAY
    ORDER BY date ASC
    """
    return query_rows(sql)
