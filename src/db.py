from __future__ import annotations
from functools import lru_cache
from pymongo import MongoClient
from pymongo.database import Database

@lru_cache()
def get_db(mongo_uri: str, db_name: str) -> Database:
    client = MongoClient(mongo_uri)
    return client[db_name]