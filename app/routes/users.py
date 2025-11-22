from fastapi import APIRouter, Query, HTTPException
from typing import List
import math

router = APIRouter()

@router.get("/users/{user_id}/to-read")
async def get_user_to_read_list(
    user_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$lookup": {
            "from": "books",
            "localField": "book_id", 
            "foreignField": "book_id",
            "as": "book_details"
        }},
        {"$unwind": "$book_details"},
        {"$project": {
            "book_id": 1,
            "title": "$book_details.title",
            "authors": "$book_details.authors",
            "average_rating": "$book_details.average_rating",
            "image_url": "$book_details.image_url"
        }},
        {"$skip": (page - 1) * page_size},
        {"$limit": page_size}
    ]
    
    to_read_books = list(router.db.to_read.aggregate(pipeline))
    
    
    for book in to_read_books:
        book["_id"] = str(book["_id"])
    
    total = router.db.to_read.count_documents({"user_id": user_id})
    
    return {
        "user_id": user_id,
        "items": to_read_books,
        "page": page,
        "page_size": page_size,
        "total": total
    }

def set_db(db):
    router.db = db