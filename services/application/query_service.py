from typing import Any

from repositories.clickhouse_repository import (
    fetch_latest_ticker_sentiment,
    fetch_relevant_stock_data,
)
from utils.text_similarity import cosine_similarity, embed_text


def get_latest_ticker_sentiment(
    ticker: str, limit: int = 20, semantic_similarity_threshold: float = 0.8
) -> list[dict[str, Any]]:
    clean_ticker = ticker.strip().upper()
    if not clean_ticker:
        return []

    safe_limit = max(1, min(limit, 100))
    safe_similarity = max(0.0, min(semantic_similarity_threshold, 1.0))

    # Overfetch from storage to improve dedupe quality, then trim.
    fetch_limit = min(500, safe_limit * 5)
    candidates = fetch_latest_ticker_sentiment(ticker=clean_ticker, limit=fetch_limit)

    # Greedy semantic grouping with embeddings over title + summary.
    clusters: list[dict[str, Any]] = []
    for candidate in candidates:
        candidate_text = f"{candidate['title']} {candidate['summary']}".strip()
        candidate_embedding = embed_text(candidate_text)
        matched_idx = -1
        for idx, cluster in enumerate(clusters):
            if (
                cosine_similarity(candidate_embedding, cluster["embedding"])
                >= safe_similarity
            ):
                matched_idx = idx
                break

        if matched_idx == -1:
            clusters.append({"embedding": candidate_embedding, "best": candidate})
            continue

        if candidate["relevance_score"] > clusters[matched_idx]["best"][
            "relevance_score"
        ] or (
            candidate["relevance_score"]
            == clusters[matched_idx]["best"]["relevance_score"]
            and str(candidate["time_published"])
            > str(clusters[matched_idx]["best"]["time_published"])
        ):
            clusters[matched_idx]["best"] = candidate
            clusters[matched_idx]["embedding"] = candidate_embedding

    deduped = [cluster["best"] for cluster in clusters]
    deduped.sort(
        key=lambda x: (str(x["time_published"]), x["relevance_score"]),
        reverse=True,
    )
    return deduped[:safe_limit]


def get_relevant_stock_data(price_lookback_days: int = 30) -> list[dict[str, Any]]:
    safe_days = max(1, min(price_lookback_days, 365))
    return fetch_relevant_stock_data(safe_days)
