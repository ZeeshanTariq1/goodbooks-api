from fastapi import APIRouter, Query
from typing import Optional

router = APIRouter()

@router.get("/tags")
async def list_tags(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
):
    
    pipeline = [
        {
            "$lookup": {
                "from": "book_tags",
                "localField": "tag_id",
                "foreignField": "tag_id",
                "as": "book_usage"
            }
        },
        {
            "$project": {
                "tag_id": 1,
                "tag_name": 1,
                "book_count": {"$size": "$book_usage"}
            }
        },
        {"$sort": {"book_count": -1}},
        {"$skip": (page - 1) * page_size},
        {"$limit": page_size}
    ]
    
    tags = list(router.db.tags.aggregate(pipeline))
    
    
    for tag in tags:
        tag["_id"] = str(tag["_id"])
    
    total = router.db.tags.count_documents({})
    
    return {
        "items": tags,
        "page": page,
        "page_size": page_size,
        "total": total
    }

def set_db(db):
    router.db = db