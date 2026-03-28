"""
Technical-analysis helpers on Polars OHLCV frames.
Expects columns: date, open, high, low, close, volume (and optionally symbol).
"""

from __future__ import annotations

import polars as pl


def rsi_expr(close: pl.Expr, period: int = 14) -> pl.Expr:
    delta = close.diff()
    gain = delta.clip(lower_bound=0.0)
    loss = (-delta).clip(lower_bound=0.0)
    avg_gain = gain.ewm_mean(span=period, adjust=False)
    avg_loss = loss.ewm_mean(span=period, adjust=False)
    rs = avg_gain / avg_loss.clip(lower_bound=1e-10)
    return 100.0 - (100.0 / (1.0 + rs))


def period_vwap_from_frame(df: pl.DataFrame) -> float:
    """Session / window VWAP: sum(typical * volume) / sum(volume)."""
    if df.is_empty():
        return 0.0
    total_vol = float(df["volume"].sum())
    if total_vol <= 0:
        return float(df["close"].tail(1).item())
    return float(df["tp_vol"].sum()) / total_vol


def enrich_ohlcv_dataframe(
    df: pl.DataFrame,
    *,
    sma_windows: tuple[int, ...] = (5, 10, 20),
    rsi_period: int = 14,
    rolling_vwap_window: int = 10,
    bollinger_period: int = 20,
    atr_period: int = 14,
) -> pl.DataFrame:
    """
    Sort by date, add typical_price, tp_vol, SMAs, rolling VWAP, RSI, ATR, optional Bollinger.
    """
    out = df.sort("date")
    out = out.with_columns(
        ((pl.col("high") + pl.col("low") + pl.col("close")) / 3.0).alias(
            "typical_price"
        )
    )
    out = out.with_columns(
        (pl.col("typical_price") * pl.col("volume").cast(pl.Float64)).alias("tp_vol")
    )

    for w in sma_windows:
        ww = max(1, min(w, 200))
        out = out.with_columns(
            pl.col("close")
            .rolling_mean(window_size=ww, min_samples=1)
            .alias(f"sma_{ww}")
        )

    rvw = max(2, min(rolling_vwap_window, 120))
    vol_f = pl.col("volume").cast(pl.Float64)
    out = out.with_columns(
        (
            pl.col("tp_vol").rolling_sum(window_size=rvw, min_samples=1)
            / vol_f.rolling_sum(window_size=rvw, min_samples=1).clip(lower_bound=1e-10)
        ).alias(f"rolling_vwap_{rvw}")
    )
    out = out.with_columns(
        rsi_expr(pl.col("close"), rsi_period).alias(f"rsi_{rsi_period}")
    )

    out = out.with_columns(
        pl.col("close")
        .rolling_std(window_size=bollinger_period, min_samples=1)
        .alias(f"close_std_{bollinger_period}")
    )
    out = out.with_columns(
        (
            pl.max_horizontal(
                pl.col("high") - pl.col("low"),
                (pl.col("high") - pl.col("close").shift(1)).abs(),
                (pl.col("low") - pl.col("close").shift(1)).abs(),
            )
        ).alias("_true_range")
    )
    out = out.with_columns(
        pl.col("_true_range")
        .rolling_mean(window_size=atr_period, min_samples=1)
        .alias(f"atr_{atr_period}")
    )

    sma_bb = f"sma_{bollinger_period}"
    std_bb = f"close_std_{bollinger_period}"
    if sma_bb in out.columns and std_bb in out.columns:
        out = out.with_columns(
            (pl.col(sma_bb) + 2 * pl.col(std_bb)).alias(f"bb_upper_{bollinger_period}"),
            (pl.col(sma_bb) - 2 * pl.col(std_bb)).alias(f"bb_lower_{bollinger_period}"),
        )

    return out


def period_price_return_pct(df: pl.DataFrame) -> float:
    if df.is_empty():
        return 0.0
    first = float(df["close"][0])
    last = float(df["close"][-1])
    return ((last - first) / max(first, 1e-10)) * 100.0
