from prefect import flow
from data_ingestion import imdb_batch_pipeline
from data_preprocessing import embedding_pipeline
from imdb_discovery import imdb_discovery_pipeline

@flow(name="movie_pipeline_orchestrator")
def movie_pipeline_orchestrator():
    """
    Full movie pipeline orchestration:
    1. Discover new IMDb IDs
    2. Ingest new movies incrementally
    3. Embed only newly ingested movies
    """

    print("Starting IMDb discovery...")
    imdb_discovery_pipeline()
    
    print("Starting movie ingestion pipeline...")
    new_ids = imdb_batch_pipeline()

    if not new_ids:
        print("No new movies ingested. Skipping embedding.")
        return

    print(f"{len(new_ids)} new movies ingested. Starting embedding...")
    embedding_pipeline()

    print("Movie pipeline completed successfully")


if __name__ == "__main__":
    movie_pipeline_orchestrator()
