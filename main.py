import datetime
from fastapi import FastAPI, HTTPException
from recommender.recommend import recommend

app = FastAPI()


@app.get("/recommend")
def get_recommendations(title: str, top_k: int = 5):
    try:
        recommendations = recommend(title, top_k)
        return {
            "title": title,
            "top_k": top_k,
            "recommendations": recommendations
        }

    except ValueError as e:
        # expected errors (movie not found, no embeddings, etc.)
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

    except Exception as e:
        # unexpected errors
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )


@app.get("/")
def home():
    return {"message": "Welcome to the Movie Recommender API!"}


@app.get("/health")
def health_check():
    return {
        "status": "ok",
        "message": "The Movie Recommender API is running smoothly.",
        "timestamp": datetime.datetime.now().isoformat()
    }
