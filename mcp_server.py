from typing import Any

from mcp.server.fastmcp import FastMCP

from services.application.analytics import get_ticker_sentiment_price_analytics as run_ticker_sentiment_price_analytics
from services.application.etl import seed_sentiment_and_ohlcv


mcp = FastMCP("info-arb")


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
def get_ticker_sentiment_price_analytics(
    ticker: str,
    sentiment_lookback_days: int = 90,
    price_lookback_days: int = 90,
    semantic_similarity_threshold: float = 0.8,
    sentiment_roll_short: int = 5,
    sentiment_roll_long: int = 20,
) -> dict[str, Any]:
    """
    For one ticker: load sentiment + OHLCV from ClickHouse, deduplicate sentiments by embedding
    cosine similarity, then return rolling sentiment stats (vs typical) and price indicators
    (period VWAP, rolling VWAP, SMAs, RSI, ATR, Bollinger when SMA-20 exists).
    """
    return run_ticker_sentiment_price_analytics(
        ticker=ticker,
        sentiment_lookback_days=sentiment_lookback_days,
        price_lookback_days=price_lookback_days,
        semantic_similarity_threshold=semantic_similarity_threshold,
        sentiment_roll_short=sentiment_roll_short,
        sentiment_roll_long=sentiment_roll_long,
    )


if __name__ == "__main__":
    mcp.run()
