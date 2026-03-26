from typing import Any

from repositories.clickhouse_repository import fetch_latest_ticker_sentiment, fetch_relevant_stock_data


def get_latest_ticker_sentiment(limit: int = 10) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 100))
    return fetch_latest_ticker_sentiment(safe_limit)


def get_relevant_stock_data(price_lookback_days: int = 30) -> list[dict[str, Any]]:
    safe_days = max(1, min(price_lookback_days, 365))
    return fetch_relevant_stock_data(safe_days)
