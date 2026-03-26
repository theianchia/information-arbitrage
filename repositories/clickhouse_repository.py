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


def fetch_latest_ticker_sentiment(limit: int) -> list[dict[str, Any]]:
    sql = f"""
    SELECT
        id,
        symbol,
        title,
        url,
        time_published,
        source,
        relevance_score,
        ticker_sentiment_score,
        ticker_sentiment_label,
        overall_sentiment_score,
        overall_sentiment_label
    FROM {MARKET_DATA_DATABASE}.{TICKER_SENTIMENT_TABLE}
    ORDER BY time_published DESC
    LIMIT {limit}
    """
    return query_rows(sql)


def fetch_relevant_stock_data(price_lookback_days: int) -> list[dict[str, Any]]:
    sql = f"""
    WITH relevant_tickers AS (
        SELECT DISTINCT symbol
        FROM {MARKET_DATA_DATABASE}.{TICKER_SENTIMENT_TABLE}
        WHERE time_published >= now() - INTERVAL 7 DAY
    )
    SELECT
        o.symbol,
        o.date,
        o.open,
        o.high,
        o.low,
        o.close,
        o.volume
    FROM {MARKET_DATA_DATABASE}.{TICKER_OHLCV_TABLE} AS o
    INNER JOIN relevant_tickers r ON o.symbol = r.symbol
    WHERE o.date >= today() - INTERVAL {price_lookback_days} DAY
    ORDER BY o.symbol, o.date DESC
    """
    return query_rows(sql)


def fetch_news_stock_analytics(
    news_lookback_days: int, price_lookback_days: int, top_n: int
) -> list[dict[str, Any]]:
    sql = f"""
    WITH news_ticker AS (
        SELECT
            symbol,
            count() AS news_count,
            max(time_published) AS latest_news_time,
            round(avg(ticker_sentiment_score), 4) AS avg_ticker_sentiment_score
        FROM {MARKET_DATA_DATABASE}.{TICKER_SENTIMENT_TABLE}
        WHERE time_published >= now() - INTERVAL {news_lookback_days} DAY
        GROUP BY symbol
    ),
    price_agg AS (
        SELECT
            symbol,
            min(date) AS first_date,
            max(date) AS last_date,
            argMin(close, date) AS first_close,
            argMax(close, date) AS last_close,
            avg(close) AS avg_close,
            min(low) AS min_low,
            max(high) AS max_high,
            sum(volume) AS total_volume
        FROM {MARKET_DATA_DATABASE}.{TICKER_OHLCV_TABLE}
        WHERE date >= today() - INTERVAL {price_lookback_days} DAY
        GROUP BY symbol
    )
    SELECT
        n.symbol,
        n.news_count,
        n.latest_news_time,
        n.avg_ticker_sentiment_score,
        p.first_date,
        p.last_date,
        p.first_close,
        p.last_close,
        round(p.avg_close, 4) AS avg_close,
        p.min_low,
        p.max_high,
        p.total_volume,
        round(((p.last_close - p.first_close) / nullIf(p.first_close, 0)) * 100, 4) AS pct_change
    FROM news_ticker n
    INNER JOIN price_agg p ON n.symbol = p.symbol
    ORDER BY n.news_count DESC, pct_change DESC
    LIMIT {top_n}
    """
    return query_rows(sql)
