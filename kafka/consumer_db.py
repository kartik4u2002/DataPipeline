# consumerdb.py
import os
import logging
from dotenv import load_dotenv
from pymongo import MongoClient, errors as pymongo_errors

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "news_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "articles")

logger = logging.getLogger("consumerdb")


def create_mongo_client():
    """Create and return a connected MongoClient (raises on failure)."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # ping to ensure connection early
    client.admin.command("ping")
    logger.info("Connected to MongoDB at %s", MONGO_URI)
    return client


class MongoStore:
    def __init__(self, client: MongoClient = None):
        if client is None:
            client = create_mongo_client()
        self.client = client
        self.db = self.client[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]

    def insert(self, document: dict):
        """Insert a single document. Returns inserted_id on success."""
        try:
            res = self.collection.insert_one(document)
            logger.info("Inserted document with _id=%s", res.inserted_id)
            return res.inserted_id
        except pymongo_errors.DuplicateKeyError:
            logger.warning("DuplicateKeyError on insert; document not inserted.")
        except Exception as e:
            logger.exception("Failed to insert document into MongoDB: %s", e)
            raise

    def close(self):
        try:
            self.client.close()
        except Exception:
            pass
