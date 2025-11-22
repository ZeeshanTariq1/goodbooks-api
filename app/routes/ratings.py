from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from typing import List

router = APIRouter()

class RatingIn(BaseModel):
    user_id: int = Field(..., description="User ID")
    book_id: int = Field(..., description="Book ID") 
    rating: int = Field(..., ge=1, le=5, description="Rating from 1 to 5")


def require_api_key(request: Request):
    api_key = request.headers.get("x-api-key")
    if api_key != router.api_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )
    return True

@router.get("/books/{book_id}/ratings/summary")
async def get_ratings_summary(book_id: int):
    
    pipeline = [
        {"$match": {"book_id": book_id}},
        {"$group": {
            "_id": "$book_id",
            "average_rating": {"$avg": "$rating"},
            "ratings_count": {"$sum": 1},
            "histogram": {
                "$push": "$rating"
            }
        }}
    ]
    
    result = list(router.db.ratings.aggregate(pipeline))
    if not result:
        raise HTTPException(status_code=404, detail="No ratings found for this book")
    
    
    ratings_data = result[0]
    histogram = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    for rating in ratings_data["histogram"]:
        histogram[rating] += 1
    
    return {
        "book_id": book_id,
        "average_rating": round(ratings_data["average_rating"], 2),
        "ratings_count": ratings_data["ratings_count"],
        "histogram": histogram
    }

@router.post("/ratings")
async def create_rating(
    rating: RatingIn,
    authenticated: bool = Depends(require_api_key)
):
    
    result = router.db.ratings.update_one(
        {"user_id": rating.user_id, "book_id": rating.book_id},
        {"$set": rating.dict()},
        upsert=True
    )
    
    if result.upserted_id:
        return {"message": "Rating created", "rating_id": str(result.upserted_id)}
    else:
        return {"message": "Rating updated"}

def set_db(db):
    router.db = db

def set_api_key(api_key):
    router.api_key = api_key