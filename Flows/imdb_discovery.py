from prefect import flow, task
import httpx
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime

load_dotenv()

# =========================
# Environment
# =========================
API_KEY = os.getenv("API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

KEYWORDS = ["thriller", "crime", "drama", "action", "mystery"]
CURRENT_YEAR = datetime.now().year


# =========================
# Tasks
# =========================

@task(log_prints=True)
def fetch_existing_and_pending_ids() -> set[str]:
    query = text("""
        SELECT imdb_id FROM movies
        UNION
        SELECT imdb_id FROM pending_movies
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        ids = {row[0] for row in result}

    print(f"Loaded {len(ids)} existing + pending IMDb IDs")
    return ids


@task(log_prints=True)
def discover_new_imdb_ids(limit: int = 25) -> list[str]:
    ids: list[str] = []

    search_url = f"http://www.omdbapi.com/?s=movie&type=movie&apikey={API_KEY}"
    response = httpx.get(search_url, timeout=10)
    response.raise_for_status()

    data = response.json()
    if data.get("Response") == "False":
        print("OMDb search returned no results")
        return []

    for movie in data.get("Search", [])[:limit]:
        imdb_id = movie.get("imdbID")

        detail_url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={API_KEY}"
        detail = httpx.get(detail_url, timeout=10).json()

        raw_rating = detail.get("imdbRating")
        rating = float(raw_rating) if raw_rating not in (None, "N/A") else 0.0

        # optional quality filter
        if rating >= 7.0:
            ids.append(imdb_id)

    print(f"Discovered {len(ids)} new IMDb IDs after filtering")
    return ids


@task(log_prints=True)
def insert_into_pending_movies(imdb_ids: list[str]):
    if not imdb_ids:
        print("No new IMDb IDs to insert")
        return

    insert_query = text("""
        INSERT INTO pending_movies (imdb_id)
        VALUES (:imdb_id)
        ON CONFLICT (imdb_id) DO NOTHING
    """)

    with engine.begin() as conn:
        for imdb_id in imdb_ids:
            conn.execute(insert_query, {"imdb_id": imdb_id})

    print(f"Inserted {len(imdb_ids)} IMDb IDs into pending_movies")


# =========================
# Flow
# =========================

@flow(name="IMDb Discovery Pipeline")
def imdb_discovery_pipeline():
    discovered_ids = discover_new_imdb_ids(limit=25)
    insert_into_pending_movies(discovered_ids)


if __name__ == "__main__":
    imdb_discovery_pipeline()
