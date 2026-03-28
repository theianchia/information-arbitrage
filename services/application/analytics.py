from __future__ import annotations

from typing import Any

import polars as pl

from repositories.clickhouse_repository import (
    fetch_ticker_ohlcv_in_window,
    fetch_ticker_sentiment_in_window,
)
from utils.finance.indicators import (
    enrich_ohlcv_dataframe,
    period_price_return_pct,
    period_vwap_from_frame,
)
from utils.serialization.json_values import json_safe
from utils.sentiment.deduplicate import deduplicate_sentiment_rows


def process_deduped_sentiment_analytics(
    deduped: list[dict[str, Any]],
    *,
    symbol: str,
    lookback_days: int,
    raw_row_count: int,
    sentiment_roll_short: int,
    sentiment_roll_long: int,
    sentiment_vs_mean_epsilon: float,
) -> dict[str, Any]:
    """Build sentiment summary from already-deduplicated rows (time-ordered processing)."""
    block: dict[str, Any] = {
        "symbol": symbol,
        "lookback_days": lookback_days,
        "raw_row_count": raw_row_count,
        "deduped_count": len(deduped),
    }
    if not deduped:
        block["latest_vs_deduped_mean"] = None
        return block

    rows = []
    for r in deduped:
        rows.append(
            {
                "time_published": r.get("time_published"),
                "ticker_sentiment_score": float(r.get("ticker_sentiment_score", 0)),
                "overall_sentiment_score": float(r.get("overall_sentiment_score", 0)),
                "relevance_score": float(r.get("relevance_score", 0)),
                "ticker_sentiment_label": r.get("ticker_sentiment_label", ""),
                "title": r.get("title", ""),
            }
        )
    s_df = pl.DataFrame(rows).sort("time_published")
    score = s_df["ticker_sentiment_score"]
    mean_score = float(score.mean()) if len(score) else 0.0
    latest_score = float(score[-1]) if len(score) else 0.0

    w_short = max(1, min(sentiment_roll_short, 100))
    w_long = max(1, min(sentiment_roll_long, 200))
    roll_long = s_df.with_columns(
        [
            pl.col("ticker_sentiment_score")
            .rolling_mean(window_size=w_short, min_samples=1)
            .alias("roll_short"),
            pl.col("ticker_sentiment_score")
            .rolling_mean(window_size=w_long, min_samples=1)
            .alias("roll_long"),
        ]
    )
    last_short = float(roll_long["roll_short"][-1]) if len(roll_long) else None
    last_long = float(roll_long["roll_long"][-1]) if len(roll_long) else None

    bias = "inline"
    if latest_score > mean_score + sentiment_vs_mean_epsilon:
        bias = "more_bullish_than_typical"
    elif latest_score < mean_score - sentiment_vs_mean_epsilon:
        bias = "more_bearish_than_typical"

    block.update(
        {
            "latest_ticker_sentiment_score": latest_score,
            "mean_ticker_sentiment_score_deduped": round(mean_score, 6),
            f"rolling_avg_ticker_sentiment_{w_short}": last_short,
            f"rolling_avg_ticker_sentiment_{w_long}": last_long,
            "latest_vs_deduped_mean": bias,
            "deduped_series_tail": [
                {
                    "time_published": json_safe(x["time_published"]),
                    "ticker_sentiment_score": x["ticker_sentiment_score"],
                    "title": (x["title"] or "")[:120],
                }
                for x in roll_long.tail(min(15, len(roll_long))).to_dicts()
            ],
        }
    )
    return block


def process_ohlcv_rows_for_price_analytics(
    ohlcv_rows: list[dict[str, Any]],
    *,
    symbol: str,
    lookback_days: int,
    price_sma_windows: tuple[int, ...] = (5, 10, 20),
    rsi_period: int = 14,
    rolling_vwap_window: int = 10,
    bollinger_period: int = 20,
    atr_period: int = 14,
) -> dict[str, Any]:
    """Build price / technical summary from raw OHLCV rows from the repository."""
    block: dict[str, Any] = {
        "symbol": symbol,
        "lookback_days": lookback_days,
        "bar_count": len(ohlcv_rows),
    }
    if not ohlcv_rows:
        block["note"] = "no OHLCV rows for this window"
        return block

    p_df = enrich_ohlcv_dataframe(
        pl.DataFrame(ohlcv_rows),
        sma_windows=price_sma_windows,
        rsi_period=rsi_period,
        rolling_vwap_window=rolling_vwap_window,
        bollinger_period=bollinger_period,
        atr_period=atr_period,
    )
    period_vwap = period_vwap_from_frame(p_df)
    rvw = max(2, min(rolling_vwap_window, 120))
    last = p_df.tail(1).to_dicts()[0]

    block.update(
        {
            "latest_date": json_safe(last.get("date")),
            "latest_close": float(last.get("close", 0)),
            "period_vwap": round(period_vwap, 6),
            f"rolling_vwap_{rvw}_latest": json_safe(last.get(f"rolling_vwap_{rvw}")),
            f"rsi_{rsi_period}_latest": json_safe(last.get(f"rsi_{rsi_period}")),
            f"atr_{atr_period}_latest": json_safe(last.get(f"atr_{atr_period}")),
        }
    )

    bb_u = f"bb_upper_{bollinger_period}"
    bb_l = f"bb_lower_{bollinger_period}"
    sma_mid = f"sma_{bollinger_period}"
    if bb_u in p_df.columns:
        block[f"bollinger_{bollinger_period}_upper_latest"] = json_safe(last.get(bb_u))
        block[f"bollinger_{bollinger_period}_lower_latest"] = json_safe(last.get(bb_l))
        block[f"bollinger_{bollinger_period}_mid_latest"] = json_safe(last.get(sma_mid))

    for w in price_sma_windows:
        ww = max(1, min(w, 200))
        block[f"sma_{ww}_latest"] = json_safe(last.get(f"sma_{ww}"))

    block["period_high"] = round(float(p_df["high"].max()), 6)
    block["period_low"] = round(float(p_df["low"].min()), 6)
    block["period_return_pct"] = round(period_price_return_pct(p_df), 4)

    return block


def get_ticker_sentiment_price_analytics(
    ticker: str,
    sentiment_lookback_days: int = 90,
    price_lookback_days: int = 90,
    semantic_similarity_threshold: float = 0.8,
    sentiment_roll_short: int = 5,
    sentiment_roll_long: int = 20,
    price_sma_windows: tuple[int, ...] = (5, 10, 20),
    rsi_period: int = 14,
    rolling_vwap_window: int = 10,
    sentiment_vs_mean_epsilon: float = 0.02,
) -> dict[str, Any]:
    """
    Load sentiment + OHLCV from ClickHouse for one ticker, deduplicate sentiments, then
    aggregate rolling sentiment stats and price indicators.
    """
    symbol = ticker.strip().upper()
    if not symbol:
        return {"symbol": "", "error": "empty ticker"}

    s_days = max(1, min(sentiment_lookback_days, 365))
    p_days = max(1, min(price_lookback_days, 365))
    thr = max(0.0, min(semantic_similarity_threshold, 1.0))
    roll_s = max(1, min(sentiment_roll_short, 100))
    roll_l = max(1, min(sentiment_roll_long, 200))

    raw_sentiment = fetch_ticker_sentiment_in_window(symbol, s_days)
    deduped = deduplicate_sentiment_rows(raw_sentiment, thr)

    sentiment_block = process_deduped_sentiment_analytics(
        deduped,
        symbol=symbol,
        lookback_days=s_days,
        raw_row_count=len(raw_sentiment),
        sentiment_roll_short=roll_s,
        sentiment_roll_long=roll_l,
        sentiment_vs_mean_epsilon=sentiment_vs_mean_epsilon,
    )

    ohlcv_rows = fetch_ticker_ohlcv_in_window(symbol, p_days)
    price_block = process_ohlcv_rows_for_price_analytics(
        ohlcv_rows,
        symbol=symbol,
        lookback_days=p_days,
        price_sma_windows=price_sma_windows,
        rsi_period=rsi_period,
        rolling_vwap_window=rolling_vwap_window,
    )

    return {"symbol": symbol, "sentiment": sentiment_block, "price": price_block}
