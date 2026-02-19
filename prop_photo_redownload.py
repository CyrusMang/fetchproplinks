from datetime import datetime
import cloudscraper
import time
import os
import uuid
import undetected_chromedriver as uc
import mimetypes
from pymongo import MongoClient
from dotenv import load_dotenv
from utils.azure_blob import upload

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")

scraper = cloudscraper.create_scraper()

batch_size = 1000

def check_batch(db, skip=0, limit=batch_size):
    photos = db['prop_photos'].find({
        'blob_url': { "$exists": True },
        'redownloaded_at': { "$exists": False },
    }).skip(skip).limit(limit)
    count = 0
    for photo in photos:
        try:
            response = scraper.get(photo['photo_url'], stream=True)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', 'image/jpeg')
            ext = mimetypes.guess_extension(content_type.split(';')[0].strip())
            name = f"{photo['photo_id']}.{ext}"
            blob_info = upload('props', name, response.content, response.headers.get('content-type'))
            prop = db['prop_photos'].update_one({'photo_id': photo['photo_id']}, {
                '$set': {
                    'blob_url': blob_info.get('blob_url'), 
                    'redownloaded_at': datetime.utcnow().timestamp()
                }
            })
            print(f"✓ Redownloaded photo {photo['photo_id']} for prop {photo['prop_source_id']}")
        except Exception as e:
            print(f"Error downloading photo: {photo['photo_url']} : {e}")
            prop = db['prop_photos'].update_one({'photo_id': photo['photo_id']}, {'$unset': {'blob_url': None}})
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