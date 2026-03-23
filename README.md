# Leading Indicators Information Arbitrage

Identifying divergences between qualitative sentiment and price data and evaluating the strength of qualitative data as leading indicators.

## MCP server

Run the MCP server over stdio:

```bash
python mcp_server.py
```

Available tools:

- `refresh_market_data`: pull latest tech news + relevant OHLCV into ClickHouse
- `get_latest_news`: return latest news rows from ClickHouse
- `get_relevant_stock_data`: return recent OHLCV rows for symbols in recent news
- `get_news_stock_analytics`: aggregated per-ticker news + price analytics
