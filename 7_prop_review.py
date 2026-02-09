import os
import time
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
    count = 0
    for prop in properties:
        count += 1
        response = requests.get(prop['source_url'])
        if response.status_code != 200:
            collection.update_one(
                { 'source_id': prop['source_id'] },
                { '$set': { 'status': "archived" } }
            )
            print(f"Archived place {prop['source_id']} due to inaccessible URL. Status code: {response.status_code}")
        else:
            print(f"Place {prop['source_id']} is still accessible.")
    if count < limit:
        return False
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
        time.sleep(2)  # Wait for page load
    print("Review completed.")
    client.close()

if __name__ == '__main__':
   main()