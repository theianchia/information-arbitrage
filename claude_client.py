import argparse
import json
import os
from typing import Any

from anthropic import Anthropic
from dotenv import load_dotenv

from services.application.etl import seed_sentiment_and_ohlcv


load_dotenv()


SYSTEM_PROMPT = """
You are a market research analyst. Use tools to collect current news and stock data.
Assess whether recent tech news intensity appears to be a leading indicator for price moves.
Always support conclusions with tool outputs and call out sample-size limitations.
""".strip()


TOOLS: list[dict[str, Any]] = [
    {
        "name": "refresh_market_data",
        "description": "Pull latest ticker sentiment and related OHLCV data into ClickHouse.",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": [],
        },
    },
]


def run_tool(name: str, tool_input: dict[str, Any]) -> Any:
    if name == "refresh_market_data":
        seed_sentiment_and_ohlcv(tool_input.get("ticker", "AAPL"))
        return {"status": "ok", "message": "Market data refreshed successfully."}
    raise ValueError(f"Unknown tool: {name}")


def ask_claude_with_tools(user_prompt: str, model: str, max_rounds: int = 8) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing ANTHROPIC_API_KEY in environment or .env")

    client = Anthropic(api_key=api_key)
    messages: list[dict[str, Any]] = [{"role": "user", "content": user_prompt}]

    for _ in range(max_rounds):
        response = client.messages.create(
            model=model,
            max_tokens=1800,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        assistant_content: list[dict[str, Any]] = []
        tool_results: list[dict[str, Any]] = []

        for block in response.content:
            block_type = getattr(block, "type", None)
            if block_type == "text":
                assistant_content.append({"type": "text", "text": block.text})
            elif block_type == "tool_use":
                assistant_content.append(
                    {
                        "type": "tool_use",
                        "id": block.id,
                        "name": block.name,
                        "input": block.input,
                    }
                )
                result = run_tool(block.name, block.input)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result, default=str),
                    }
                )

        messages.append({"role": "assistant", "content": assistant_content})

        if not tool_results:
            text_blocks = [b for b in assistant_content if b["type"] == "text"]
            return "\n".join(b["text"] for b in text_blocks).strip()

        messages.append({"role": "user", "content": tool_results})

    raise RuntimeError(
        "Claude tool loop exceeded max rounds; increase max_rounds if needed."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Claude API with local market tools."
    )
    parser.add_argument(
        "--model",
        default="claude-sonnet-4-5",
        help="Anthropic model name to use.",
    )
    parser.add_argument(
        "--prompt",
        default=(
            "Refresh market data, then analyze whether recent tech news intensity is a "
            "leading indicator for 1d/3d/5d price moves. Return ranking, confidence, and caveats."
        ),
        help="Prompt for Claude.",
    )
    args = parser.parse_args()

    result = ask_claude_with_tools(args.prompt, model=args.model)
    print(result)


if __name__ == "__main__":
    main()
