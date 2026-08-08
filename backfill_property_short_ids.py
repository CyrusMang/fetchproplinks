import json
import os
import random
import sys
from typing import Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")

SHORT_ID_LENGTH = 8
SHORT_ID_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz"
MAX_ATTEMPTS = 20


def random_short_id(length: int = SHORT_ID_LENGTH) -> str:
    return "".join(random.choice(SHORT_ID_ALPHABET) for _ in range(length))


def generate_unique_short_id(collection: Collection, max_attempts: int = MAX_ATTEMPTS) -> str:
    for _ in range(max_attempts):
        candidate = random_short_id()
        exists = collection.find_one({"short_id": candidate}, {"_id": 1}) is not None
        if not exists:
            return candidate
    raise RuntimeError("Failed to generate unique short_id after maximum attempts")


def get_mongo_client() -> MongoClient:
    if not MONGODB_CONNECTION_STRING:
        raise RuntimeError("MONGODB_CONNECTION_STRING is not set")
    return MongoClient(MONGODB_CONNECTION_STRING)


def run() -> int:
    client: Optional[MongoClient] = None
    try:
        client = get_mongo_client()
        db_name = 'prop_main'
        db = client[db_name]
        properties = db["props"]

        properties.create_index(
            [("short_id", 1)],
            name="short_id_unique_sparse",
            unique=True,
            sparse=True,
        )

        docs = list(
            properties.find(
                {
                    "$or": [
                        {"short_id": {"$exists": False}},
                        {"short_id": ""},
                    ]
                },
                {"_id": 1, "id": 1, "short_id": 1},
            )
        )

        updated_count = 0

        for doc in docs:
            for _ in range(MAX_ATTEMPTS):
                short_id = generate_unique_short_id(properties)
                try:
                    result = properties.update_one(
                        {
                            "_id": doc["_id"],
                            "$or": [
                                {"short_id": {"$exists": False}},
                                {"short_id": ""},
                            ],
                        },
                        {"$set": {"short_id": short_id}},
                    )
                    if result.modified_count == 1:
                        updated_count += 1
                    break
                except DuplicateKeyError:
                    # Race-safe retry if another writer claimed the same short_id.
                    continue
            else:
                raise RuntimeError(f"Could not assign unique short_id for document _id={doc['_id']}")

        print(
            json.dumps(
                {
                    "scanned": len(docs),
                    "updated": updated_count,
                    "index": "short_id_unique_sparse",
                },
                indent=2,
            )
        )

        return 0
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    raise SystemExit(run())
