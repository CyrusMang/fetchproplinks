import os
import uuid
import json
from pymongo import MongoClient
from dotenv import load_dotenv
import requests

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")

batch_size = 10

def check_batch(collection, filter, skip=0, limit=batch_size):
    properties = collection.find(filter).sort("updated_at", 1).skip(skip).limit(limit)
    if properties.count() == 0:
        print("No properties found for review.")
        return False
    for prop in properties:
        response = requests.get(prop['source_url'])
        if response.status_code != 200:
            break
        collection.update_one(
            { 'source_id': prop['source_id'] },
            { '$set': { 'status': "archived" } }
        )
    return True

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']

    f = {
        'type': "apartment",
        'status': { "$ne": "archived" },
    }
    skip = 0
    while True:
        if not check_batch(collection, f, skip=skip):
            break
        skip += batch_size
    print("Review completed.")
    client.close()