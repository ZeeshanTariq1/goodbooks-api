import pandas as pd
import pymongo
import os
from typing import List, Dict, Any
import argparse

class DataIngestor:
    def __init__(self, mongo_uri: str, db_name: str):
        if not mongo_uri.endswith(f'/{db_name}'):
            mongo_uri = f"{mongo_uri}/{db_name}"
        self.client = pymongo.MongoClient(mongo_uri)
        self.db = self.client[db_name]
        
    def create_indexes(self):
        """Create required indexes for optimal performance"""
        self.db.books.create_index([("title", pymongo.TEXT), ("authors", pymongo.TEXT)])
        self.db.books.create_index([("average_rating", pymongo.DESCENDING)])
        self.db.books.create_index([("book_id", pymongo.ASCENDING)], unique=True)
        
    
        self.db.ratings.create_index([("book_id", pymongo.ASCENDING)])
        self.db.ratings.create_index([("user_id", pymongo.ASCENDING), ("book_id", pymongo.ASCENDING)], unique=True)
        
        
        self.db.tags.create_index([("tag_id", pymongo.ASCENDING)], unique=True)
        self.db.tags.create_index([("tag_name", pymongo.ASCENDING)])
        
        
        self.db.book_tags.create_index([("tag_id", pymongo.ASCENDING)])
        self.db.book_tags.create_index([("goodreads_book_id", pymongo.ASCENDING)])
        
        
        self.db.to_read.create_index([("user_id", pymongo.ASCENDING), ("book_id", pymongo.ASCENDING)], unique=True)
        
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame by filling NaN values and ensuring proper types"""
        
        df = df.fillna({
            'original_publication_year': 0,
            'average_rating': 0.0,
            'ratings_count': 0,
            'count': 0
        })
        
        
        int_columns = ['book_id', 'goodreads_book_id', 'work_id', 'books_count', 
                      'original_publication_year', 'ratings_count', 'work_ratings_count',
                      'work_text_reviews_count', 'user_id', 'tag_id', 'count']
        
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        return df
    
    def ingest_collection(self, collection_name: str, csv_url: str, id_field: str = None):
        """Ingest data from CSV URL into MongoDB collection"""
        print(f"Downloading {collection_name} data from {csv_url}")
        
        try:
            df = pd.read_csv(csv_url)
            df = self.clean_dataframe(df)
            
            
            records = df.to_dict('records')
            
            
            collection = self.db[collection_name]
            if id_field:
                for record in records:
                    filter_query = {id_field: record[id_field]}
                    collection.update_one(filter_query, {'$set': record}, upsert=True)
            else:
                collection.insert_many(records, ordered=False)
                
            print(f"Successfully ingested {len(records)} records into {collection_name}")
            
        except Exception as e:
            print(f"Error ingesting {collection_name}: {str(e)}")
    
    def ingest_all_data(self, use_samples: bool = True):
        """Ingest all datasets"""
        base_url = "https://raw.githubusercontent.com/zygmuntz/goodbooks-10k/master/"
        folder = "samples/" if use_samples else ""
        
        datasets = {
            "books": f"{base_url}{folder}books.csv",
            "ratings": f"{base_url}{folder}ratings.csv", 
            "tags": f"{base_url}{folder}tags.csv",
            "book_tags": f"{base_url}{folder}book_tags.csv",
            "to_read": f"{base_url}{folder}to_read.csv"
        }
        
        id_fields = {
            "books": "book_id",
            "ratings": None,
            "tags": "tag_id", 
            "book_tags": None,
            "to_read": None
        }
        
        
        self.create_indexes()
        
        
        for collection_name, csv_url in datasets.items():
            self.ingest_collection(collection_name, csv_url, id_fields.get(collection_name))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest GoodBooks data into MongoDB')
    
    parser.add_argument('--mongo-uri', default='mongodb://mongo:27017/goodbooks', help='MongoDB connection URI')
    parser.add_argument('--db-name', default='goodbooks', help='Database name')
    parser.add_argument('--full-data', action='store_true', help='Use full dataset instead of samples')
    
    args = parser.parse_args()
    
    ingestor = DataIngestor(args.mongo_uri, args.db_name)
    ingestor.ingest_all_data(use_samples=not args.full_data)