from datetime import datetime
import time
import os
import uuid
import undetected_chromedriver as uc
from pymongo import MongoClient
from dotenv import load_dotenv
from reviewers import n28hse, house730

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")

batch_size = 1000

def check_batch(db, skip=0, limit=batch_size):
    properties = db['props'].find({
        'id': { "$exists": False },
    }).skip(skip).limit(limit)
    count = 0
    for prop in properties:
        new_id = str(uuid.uuid4())
        db['props'].update_one({'_id': prop['_id']}, {'$set': {'id': new_id}})
        db['prop_photos'].update_many({'prop_source_id': prop['source_id']}, {'$set': {'prop_id': new_id}})
        print(f"Updated prop {prop['source_id']} with new id {new_id}")
        count += 1
    if count < limit:
        return False
    return True

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']

    skip = 0
    while True:
        if not check_batch(db, skip=skip):
            break
        skip += batch_size
    print("Assign completed.")
    client.close()

if __name__ == '__main__':
   main()