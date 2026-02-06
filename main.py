from fastapi import FastAPI
from recommender.recommend import recommend

app = FastAPI()

@app.get("/recommend")
def get_recommendations(title: str, top_k: int = 5):
    try:
        return {"recommendations": recommend(title, top_k)}
    except ValueError as e:
        return {"error": str(e)}
