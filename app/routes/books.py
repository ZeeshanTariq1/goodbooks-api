from fastapi import APIRouter, Query, HTTPException
from pymongo import ASCENDING, DESCENDING
from typing import Optional
import math

router = APIRouter()

@router.get("/books")
async def list_books(
    q: Optional[str] = Query(None, description="Search in title and authors"),
    min_avg: Optional[float] = Query(None, ge=0, le=5, description="Minimum average rating"),
    year_from: Optional[int] = Query(None, description="Publication year from"),
    year_to: Optional[int] = Query(None, description="Publication year to"),
    sort: str = Query("avg", description="Sort field", regex="^(avg|ratings_count|year|title)$"),
    order: str = Query("desc", description="Sort order", regex="^(asc|desc)$"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Page size")
):
    filter_query = {}
    
    if q:
        filter_query["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"authors": {"$regex": q, "$options": "i"}}
        ]
    
    if min_avg is not None:
        filter_query["average_rating"] = {"$gte": min_avg}
    
    year_filter = {}
    if year_from is not None:
        year_filter["$gte"] = year_from
    if year_to is not None:
        year_filter["$lte"] = year_to
    if year_filter:
        filter_query["original_publication_year"] = year_filter
    
    
    sort_map = {
        "avg": "average_rating",
        "ratings_count": "ratings_count", 
        "year": "original_publication_year",
        "title": "title"
    }
    
    sort_direction = DESCENDING if order == "desc" else ASCENDING
    
    
    total = router.db.books.count_documents(filter_query)
    
    
    skip = (page - 1) * page_size
    books_cursor = router.db.books.find(filter_query).sort(sort_map[sort], sort_direction).skip(skip).limit(page_size)
    
    items = []
    for book in books_cursor:
        book["_id"] = str(book["_id"])
        items.append(book)
    
    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "total": total
    }

@router.get("/books/{book_id}")
async def get_book(book_id: int):
    book = router.db.books.find_one({"book_id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    book["_id"] = str(book["_id"])
    return book


def set_db(db):
    router.db = db