from typing import Any

from mcp.server.fastmcp import FastMCP

from services.application.analytics_service import (
    get_news_stock_analytics as run_news_stock_analytics,
)
from services.application.etl import seed_sentiment_and_ohlcv
from services.application.query_service import (
    get_latest_ticker_sentiment as run_latest_ticker_sentiment_query,
    get_relevant_stock_data as run_relevant_stock_query,
)


mcp = FastMCP("market-intel")


@mcp.tool()
def refresh_market_data(ticker: str = "AAPL") -> dict[str, str]:
    """
    Pull latest ticker sentiment and related OHLCV into ClickHouse.

    Pass `ticker` (e.g. MSFT, NVDA) to refresh data for that symbol; other tickers
    mentioned in the same news feed are still ingested for OHLCV follow-up.
    """
    seed_sentiment_and_ohlcv(ticker.strip().upper())
    return {"status": "ok", "message": "Market data refreshed successfully."}


@mcp.tool()
def get_latest_ticker_sentiment(
    ticker: str,
    limit: int = 20,
    semantic_similarity_threshold: float = 0.8,
) -> list[dict[str, Any]]:
    """
    Retrieve latest ticker sentiment from ClickHouse and semantically deduplicate similar news.
    """
    return run_latest_ticker_sentiment_query(
        ticker=ticker,
        limit=limit,
        semantic_similarity_threshold=semantic_similarity_threshold,
    )


@mcp.tool()
def get_relevant_stock_data(price_lookback_days: int = 30) -> list[dict[str, Any]]:
    """
    Retrieve OHLCV rows for symbols appearing in recent sentiment data.
    """
    return run_relevant_stock_query(price_lookback_days)


@mcp.tool()
def get_news_stock_analytics(
    news_lookback_days: int = 7,
    price_lookback_days: int = 30,
    top_n: int = 20,
) -> list[dict[str, Any]]:
    """
    Aggregate sentiment and stock metrics per ticker.
    """
    return run_news_stock_analytics(news_lookback_days, price_lookback_days, top_n)


if __name__ == "__main__":
    mcp.run()
