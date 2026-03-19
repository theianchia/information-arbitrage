import requests
import datetime as dt
from typing import Any, Dict, List

import polars as pl

from config.settings import SETTINGS
from config.constants import NEWS_LIMIT

ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

def fetch_latest_tech_news() -> List[Dict[str, Any]]:
    params = {
        "function": "NEWS_SENTIMENT",
        "topics": "technology",
        "sort": "LATEST",
        "time_from": "",  # let API decide; we just limit by `NEWS_LIMIT`
        "apikey": SETTINGS.alpha_vantage_api_key,
    }
    resp = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    feed = data.get("feed", [])
    return feed[:NEWS_LIMIT]


def get_unique_tickers_from_news(feed: List[Dict[str, Any]]) -> List[str]:
    tickers = set()
    for item in feed:
        for ts in item.get("ticker_sentiment", []):
            ticker = ts.get("ticker")
            if ticker:
                tickers.add(ticker)
    return sorted(tickers)


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
    return ts
