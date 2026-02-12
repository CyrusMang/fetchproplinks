import os
import json
import requests
import cloudscraper
from openai import AzureOpenAI
from pymongo import MongoClient
from dotenv import load_dotenv
from utils.azure_blob import upload

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

scraper = cloudscraper.create_scraper()

def process_photo_analysis_result(prop, collection):
    new_analysed_photos = []
    analysed_photos = prop.get('analysed_photos', [])
    for photo_analysis in analysed_photos:
        photo_url = photo_analysis.get('original_url')
        if not photo_url:
            continue
        
        blob_url = photo_analysis.get('blob_url')
        if blob_url:
            new_analysed_photos.append(photo_analysis)
            continue
        
        # Select high-quality indoor photos without policy violations or people
        if (not photo_analysis['is_violating_policy'] and 
            not photo_analysis['is_human_in_photo'] and
            photo_analysis['quality_score'] > 40):
            try:
                response = scraper.get(photo_analysis['original_url'], stream=True)
                response.raise_for_status()

                name = photo_analysis['original_url'].split('/')[-1].split('?')[0]
                blob_info = upload('props', name, response.content, response.headers.get('content-type'))
                photo_analysis['blob_url'] = blob_info.get('blob_url')
            except Exception as e:
                print(f"Error downloading photo: {photo_analysis['original_url']} : {e}")
        
        new_analysed_photos.append(photo_analysis)
    
    new_analysed_photos.sort(key=lambda x: x['quality_score'], reverse=True)

    # Update MongoDB
    update_data = {
        'status': 'photo_analysed',
        'analysed_photos': new_analysed_photos,
    }
    
    collection.update_one(
        {'source_id': prop['source_id']},
        {'$set': update_data}
    )
    
    print(f"✓ Updated {prop['source_id']}: {len(new_analysed_photos)} analyzed")
    return True

def main():
    # user input source id
    id = input("Enter source id: ")

    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    db = mongo_client['prop_main']
    collection = db['props']
    
    prop = collection.find_one({'source_id': id})

    if not prop:
        print(f"Property with source_id {id} not found.")
        return

    process_photo_analysis_result(prop, collection)

if __name__ == '__main__':
    main()
