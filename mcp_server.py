from typing import Any

from mcp.server.fastmcp import FastMCP

from services.analytics_service import get_news_stock_analytics as run_news_stock_analytics
from services.etl import seed_news_and_ohlcv
from services.query_service import (
    get_latest_news as run_latest_news_query,
    get_relevant_stock_data as run_relevant_stock_query,
)


mcp = FastMCP("market-intel")


@mcp.tool()
def refresh_market_data() -> dict[str, str]:
    """
    Pull latest tech news and related OHLCV into ClickHouse.
    """
    seed_news_and_ohlcv()
    return {"status": "ok", "message": "Market data refreshed successfully."}


@mcp.tool()
def get_latest_news(limit: int = 10) -> list[dict[str, Any]]:
    """
    Retrieve latest news rows from ClickHouse.
    """
    return run_latest_news_query(limit)


@mcp.tool()
def get_relevant_stock_data(price_lookback_days: int = 30) -> list[dict[str, Any]]:
    """
    Retrieve OHLCV rows for symbols appearing in recent tech news.
    """
    return run_relevant_stock_query(price_lookback_days)


@mcp.tool()
def get_news_stock_analytics(
    news_lookback_days: int = 7,
    price_lookback_days: int = 30,
    top_n: int = 20,
) -> list[dict[str, Any]]:
    """
    Aggregate news and stock metrics per ticker.
    """
    return run_news_stock_analytics(news_lookback_days, price_lookback_days, top_n)


if __name__ == "__main__":
    mcp.run()
