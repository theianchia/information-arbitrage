"""Cluster near-duplicate news sentiment rows by semantic similarity."""

from __future__ import annotations

from typing import Any

from utils.text.embeddings import cosine_similarity, embed_text


def deduplicate_sentiment_rows(
    candidates: list[dict[str, Any]],
    semantic_similarity_threshold: float,
) -> list[dict[str, Any]]:
    """
    Greedy clustering by embedding cosine similarity on title + summary.
    Keeps the row with highest relevance_score per cluster (tie-break: newer time_published).
    """
    if not candidates:
        return []

    safe_similarity = max(0.0, min(semantic_similarity_threshold, 1.0))
    clusters: list[dict[str, Any]] = []
    for candidate in candidates:
        title = candidate.get("title") or ""
        summary = candidate.get("summary") or ""
        candidate_text = f"{title} {summary}".strip()
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

        best = clusters[matched_idx]["best"]
        rel = float(candidate.get("relevance_score", 0))
        best_rel = float(best.get("relevance_score", 0))
        t_c = str(candidate.get("time_published", ""))
        t_b = str(best.get("time_published", ""))
        if rel > best_rel or (rel == best_rel and t_c > t_b):
            clusters[matched_idx]["best"] = candidate
            clusters[matched_idx]["embedding"] = candidate_embedding

    deduped = [c["best"] for c in clusters]
    deduped.sort(
        key=lambda x: (
            str(x.get("time_published", "")),
            float(x.get("relevance_score", 0)),
        ),
        reverse=True,
    )
    return deduped
