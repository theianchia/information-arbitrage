import requests
from typing import Any, Dict, List


from config.settings import SETTINGS
from config.constants import TICKER_SENTIMENT_LIMIT

ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"


def fetch_latest_news_sentiment_for_ticker(
    ticker: str, limit: int = TICKER_SENTIMENT_LIMIT
) -> List[Dict[str, Any]]:
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": ticker,
        "sort": "LATEST",
        "apikey": SETTINGS.alpha_vantage_api_key,
        "limit": limit,
    }
    resp = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    feed = data.get("feed", [])
    return feed


def fetch_ohlcv_for_symbol(symbol: str) -> Dict[str, Any]:
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": SETTINGS.alpha_vantage_api_key,
    }
    resp = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    ts_key = "Time Series (Daily)"
    ts = data.get(ts_key, {})
    if not ts:
        print(f"Alpha Vantage | {data}")
    return ts
