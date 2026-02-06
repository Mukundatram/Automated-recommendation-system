from prefect import flow, task
import httpx
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

load_dotenv()

# =========================
# Database (Neon PostgreSQL)
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

API_KEY = os.getenv("API_KEY")


# =========================
# Tasks
# =========================

@task(log_prints=True)
def get_existing_imdb_ids() -> set[str]:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT imdb_id FROM movies"))
        existing_ids = {row[0] for row in result}

    print(f"Found {len(existing_ids)} movies already in DB")
    return existing_ids


@task(log_prints=True)
def load_pending_imdb_ids() -> list[str]:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT imdb_id FROM pending_movies"))
        ids = [row[0] for row in result]

    print(f"Loaded {len(ids)} pending IMDb IDs")
    return ids


@task(log_prints=True, retries=2, retry_delay_seconds=2)
def fetch_movie(imdb_id: str) -> dict:
    url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={API_KEY}"
    response = httpx.get(url, timeout=10)
    response.raise_for_status()

    movie = response.json()
    if movie.get("Response") == "False":
        raise ValueError(f"Failed to fetch movie {imdb_id}")

    print(f"Fetched: {movie['Title']}")
    return movie


def parse_int(value):
    if not value or value in ("N/A", ""):
        return None
    return int(value.replace(",", "").replace("$", ""))


def parse_float(value):
    if not value or value in ("N/A", ""):
        return None
    return float(value)


@task(log_prints=True)
def clean_movie(movie: dict) -> dict:
    return {
        "imdb_id": movie.get("imdbID"),
        "title": movie.get("Title"),
        "year": parse_int(movie.get("Year")),
        "genre": movie.get("Genre"),
        "director": movie.get("Director"),
        "actors": movie.get("Actors"),
        "imdb_rating": parse_float(movie.get("imdbRating")),
        "imdb_votes": parse_int(movie.get("imdbVotes")),
        "box_office": parse_int(movie.get("BoxOffice")),
        "country": movie.get("Country"),
        "language": movie.get("Language"),
    }


@task(log_prints=True)
def save_movies_to_db(movies: list[dict]):
    if not movies:
        print("No new movies to save")
        return

    insert_query = text("""
        INSERT INTO movies (
            imdb_id, title, year, genre, director, actors,
            imdb_rating, imdb_votes, box_office, country, language
        )
        VALUES (
            :imdb_id, :title, :year, :genre, :director, :actors,
            :imdb_rating, :imdb_votes, :box_office, :country, :language
        )
        ON CONFLICT (imdb_id) DO NOTHING
    """)

    with engine.begin() as conn:
        for movie in movies:
            conn.execute(insert_query, movie)

    print(f"Saved {len(movies)} new movies to DB")


@task(log_prints=True)
def cleanup_already_ingested():
    with engine.begin() as conn:
        result = conn.execute(text("""
            DELETE FROM pending_movies pm
            USING movies m
            WHERE pm.imdb_id = m.imdb_id
        """))

    print(f"Removed {result.rowcount} already-ingested movies from pending queue")


# =========================
# Flow
# =========================

@flow(name="IMDb Incremental Ingestion Pipeline")
def imdb_batch_pipeline() -> list[str]:
    cleanup_already_ingested()

    imdb_ids = load_pending_imdb_ids()
    if not imdb_ids:
        print("No pending IMDb IDs found")
        return []

    existing_ids = get_existing_imdb_ids()
    new_ids = [i for i in imdb_ids if i not in existing_ids]

    if not new_ids:
        print("No new IMDb IDs to ingest")
        return []

    movies = fetch_movie.map(new_ids)
    cleaned_movies = clean_movie.map(movies)

    save_movies_to_db(cleaned_movies)

    return new_ids  # important for flow chaining


if __name__ == "__main__":
    imdb_batch_pipeline()
