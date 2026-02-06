from prefect import task, flow
import pandas as pd
from sqlalchemy import create_engine, text
from sentence_transformers import SentenceTransformer
import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# -------------------------
# ENV + LOGGING
# -------------------------
os.environ["TOKENIZERS_PARALLELISM"] = "false"
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)


# -------------------------
# LOAD ONLY NEW MOVIES
# -------------------------
@task(log_prints=True)
def load_new_movies_only() -> pd.DataFrame:
    query = """
    SELECT *
    FROM movies
    WHERE imdb_id NOT IN (
        SELECT imdb_id FROM movie_embeddings
    )
    """

    df = pd.read_sql(query, engine)
    print(f"Found {len(df)} new movies to embed")
    return df


# -------------------------
# HELPERS
# -------------------------
def normalize_text(x: str) -> str:
    if not isinstance(x, str):
        return ""
    return x.lower().replace(",", " ").strip()


# -------------------------
# PREPROCESS
# -------------------------
@task(log_prints=True)
def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # numeric cleanup
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["imdb_votes"] = pd.to_numeric(df["imdb_votes"], errors="coerce")
    df["box_office"] = pd.to_numeric(df["box_office"], errors="coerce")

    # text cleanup
    text_cols = ["genre", "director", "actors", "country", "language"]
    for col in text_cols:
        df[col] = df[col].fillna("").apply(normalize_text)

    # combined semantic text
    df["content"] = (
        df["genre"] + " " +
        df["director"] + " " +
        df["actors"] + " " +
        df["language"] + " " +
        df["country"]
    ).str.strip()

    # drop rows with no usable content
    df = df[df["content"] != ""]

    print(f"{len(df)} movies left after preprocessing")
    return df


# -------------------------
# EMBEDDINGS
# -------------------------
@task(log_prints=True)
def build_embeddings(texts: list[str]):
    model = SentenceTransformer("all-MiniLM-L6-v2")

    embeddings = model.encode(
        texts,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    return embeddings


# -------------------------
# STORE (Postgres / Neon)
# -------------------------
@task(log_prints=True)
def save_embeddings_to_db(imdb_ids: list[str], embeddings):
    insert_query = text("""
        INSERT INTO movie_embeddings (imdb_id, embedding)
        VALUES (:imdb_id, :embedding)
        ON CONFLICT (imdb_id)
        DO UPDATE SET embedding = EXCLUDED.embedding
    """)

    with engine.begin() as conn:
        for imdb_id, emb in zip(imdb_ids, embeddings):
            conn.execute(
                insert_query,
                {
                    "imdb_id": imdb_id,
                    "embedding": json.dumps(emb.tolist()),
                },
            )

    print(f"Saved {len(imdb_ids)} embeddings")


# -------------------------
# FLOW
# -------------------------
@flow(name="movie_embedding_pipeline_incremental")
def embedding_pipeline() -> int:
    df_new = load_new_movies_only()

    if df_new.empty:
        print("No new movies found. Skipping embedding step.")
        return 0

    df_clean = preprocess_data(df_new)

    if df_clean.empty:
        print("No valid content to embed after preprocessing.")
        return 0

    embeddings = build_embeddings(df_clean["content"].tolist())

    save_embeddings_to_db(
        imdb_ids=df_clean["imdb_id"].tolist(),
        embeddings=embeddings,
    )

    print("Incremental embeddings completed successfully")
    return len(df_clean)


if __name__ == "__main__":
    embedding_pipeline()
