# recommender/recommend.py

import os
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv

load_dotenv()

# -------------------------
# DATABASE (Neon)
# -------------------------
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)


# -------------------------
# LOAD EMBEDDINGS
# -------------------------
def load_embeddings():
    query = text("""
        SELECT m.imdb_id, m.title, e.embedding
        FROM movies m
        JOIN movie_embeddings e
        ON m.imdb_id = e.imdb_id
    """)

    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    if not rows:
        return [], [], np.empty((0, 0), dtype=np.float32)

    titles = []
    imdb_ids = []
    embeddings = []

    for imdb_id, title, emb in rows:
        imdb_ids.append(imdb_id)
        titles.append(title)
        embeddings.append(emb)  # JSONB → already list

    embeddings = np.asarray(embeddings, dtype=np.float32)

    if embeddings.ndim != 2:
        raise ValueError("Embeddings are malformed")

    return titles, imdb_ids, embeddings


# -------------------------
# RECOMMENDER
# -------------------------
def recommend(title: str, top_k: int = 5):
    titles, imdb_ids, embeddings = load_embeddings()

    if embeddings.size == 0:
        raise ValueError("No embeddings found in database")

    # normalize titles once
    title_map = {t.lower(): i for i, t in enumerate(titles)}
    key = title.lower()

    if key not in title_map:
        raise ValueError(f"Movie '{title}' not found in database")

    idx = title_map[key]
    query_emb = embeddings[idx].reshape(1, -1)

    similarities = cosine_similarity(query_emb, embeddings)[0]
    ranked_indices = np.argsort(similarities)[::-1]

    recommendations = []
    for i in ranked_indices:
        if i == idx:
            continue

        recommendations.append({
            "title": titles[i],
            "score": float(similarities[i]),
        })

        if len(recommendations) >= top_k:
            break

    return recommendations
