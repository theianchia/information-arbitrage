from typing import Any

from repositories.clickhouse_repository import fetch_news_stock_analytics


def get_news_stock_analytics(
    news_lookback_days: int = 7,
    price_lookback_days: int = 30,
    top_n: int = 20,
) -> list[dict[str, Any]]:
    safe_news_days = max(1, min(news_lookback_days, 90))
    safe_price_days = max(1, min(price_lookback_days, 365))
    safe_top_n = max(1, min(top_n, 100))
    return fetch_news_stock_analytics(safe_news_days, safe_price_days, safe_top_n)
