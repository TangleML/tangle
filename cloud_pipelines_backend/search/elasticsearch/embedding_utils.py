"""Shared utility for generating text embedding vectors via the AI proxy API."""

import requests


def embed_texts(
    *,
    texts: list[str],
    embedding_model: str,
    endpoint: str,
    api_token: str,
) -> list[list[float]]:
    """Call the embedding API and return vectors for each input text."""
    response = requests.post(
        url=endpoint,
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        },
        json={"model": embedding_model, "input": texts},
    )
    response.raise_for_status()
    embeddings: list[list[float] | None] = [None] * len(texts)
    for item in response.json()["data"]:
        embeddings[item["index"]] = item["embedding"]
    return embeddings  # type: ignore[return-value]
