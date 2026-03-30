# Leading Indicators Information Arbitrage

Identifying divergences between qualitative sentiment and price data and evaluating the strength of qualitative data as leading indicators.

## MCP server

Run the MCP server over stdio:

```bash
python mcp_server.py
```

The server name is **`info-arb`**.

### Tools

| Tool | Purpose |
|------|--------|
| **`refresh_market_data`** | Fetches ticker-scoped news sentiment from Alpha Vantage and writes ticker sentiment rows plus related OHLCV into ClickHouse. |
| **`get_ticker_sentiment_price_analytics`** | Reads sentiment and OHLCV for a single symbol from ClickHouse, deduplicates similar items via embedding cosine similarity, and returns combined analytics (rolling sentiment vs typical, VWAP, SMAs, RSI, ATR, Bollinger, etc.). |

**`refresh_market_data`**

- **`ticker`** (optional, default `AAPL`): symbol used for the Alpha Vantage `NEWS_SENTIMENT` request; co-mentioned tickers can still be ingested for OHLCV follow-up.

**`get_ticker_sentiment_price_analytics`**

- **`ticker`** (required): symbol to analyze.
- **`sentiment_lookback_days`** (default `90`, max `365`): window for sentiment rows.
- **`price_lookback_days`** (default `90`, max `365`): window for daily OHLCV.
- **`semantic_similarity_threshold`** (default `0.8`): cosine similarity threshold for deduplicating near-duplicate news.
- **`sentiment_roll_short`** / **`sentiment_roll_long`** (defaults `5` / `20`): rolling mean windows on deduplicated ticker sentiment scores.

Call **`refresh_market_data`** (for the symbols you care about) before expecting analytics to have data in ClickHouse.

---

## Claude API client

Runs an Anthropic Messages request with **the same logical tools** as the MCP server, implemented in-process (no MCP transport). Requires **`ANTHROPIC_API_KEY`** in the environment or a `.env` file.

```bash
python claude_client.py
```

Optional flags:

```bash
python claude_client.py --model claude-sonnet-4-5 --prompt "Your custom prompt"
```

### Tools exposed to Claude

- **`refresh_market_data`**: optional `ticker` (defaults to `AAPL` if omitted).
- **`get_ticker_sentiment_price_analytics`**: required `ticker`; optional `sentiment_lookback_days`, `price_lookback_days`, `semantic_similarity_threshold`, `sentiment_roll_short`, `sentiment_roll_long` (same defaults and bounds as the MCP tool).

Tool results are JSON-serialized back to the model for the next turn.
