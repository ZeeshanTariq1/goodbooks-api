from fastapi import FastAPI, Query, HTTPException, Depends, Request, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pymongo import MongoClient
from typing import Optional, List, Dict, Any
import os
import time
import logging
from contextlib import asynccontextmanager


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/goodbooks")
DB_NAME = os.getenv("DB_NAME", "goodbooks")
API_KEY = os.getenv("API_KEY", "dev-key")

@asynccontextmanager
async def lifespan(app: FastAPI):
    
    try:
        app.mongodb_client = MongoClient(MONGO_URI)
        app.db = app.mongodb_client[DB_NAME]
        
        app.db.command('ping')
        logger.info("Connected to MongoDB successfully")
        
        
        from app.routes import books, ratings, tags, users
        books.set_db(app.db)
        ratings.set_db(app.db)
        tags.set_db(app.db)
        users.set_db(app.db)
        logger.info("All routers database configured")
        
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise e
    
    yield
    
    
    try:
        app.mongodb_client.close()
        logger.info("MongoDB connection closed")
    except Exception as e:
        logger.error(f"Error closing MongoDB connection: {e}")

app = FastAPI(
    title="GoodBooks API",
    description="A MongoDB-backed REST API for book data",
    version="1.0.0",
    lifespan=lifespan
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RatingIn(BaseModel):
    user_id: int = Field(..., description="User ID")
    book_id: int = Field(..., description="Book ID") 
    rating: int = Field(..., ge=1, le=5, description="Rating from 1 to 5")


def require_api_key(request: Request):
    api_key = request.headers.get("x-api-key")
    if api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000
    
    logger.info({
        "route": request.url.path,
        "method": request.method,
        "query_params": dict(request.query_params),
        "status_code": response.status_code,
        "latency_ms": round(process_time, 2),
        "client_ip": request.client.host
    })
    
    return response


@app.get("/healthz")
async def health_check():
    try:
        
        app.db.command('ping')
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")

#
@app.get("/")
async def root():
    return {"message": "GoodBooks API", "version": "1.0.0"}


from app.routes import books, ratings, tags, users
app.include_router(books.router, prefix="/api/v1", tags=["books"])
app.include_router(ratings.router, prefix="/api/v1", tags=["ratings"])
app.include_router(tags.router, prefix="/api/v1", tags=["tags"])
app.include_router(users.router, prefix="/api/v1", tags=["users"])