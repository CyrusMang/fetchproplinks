from datetime import datetime
import os
import uuid
import json
import cloudscraper
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

dir = os.path.dirname(os.path.abspath(__file__))
artifacts = os.path.join(dir, ARTIFACTS_FOLDER)
folder = os.path.join(artifacts, 'photo_analysis')

# Create necessary folders
os.makedirs(os.path.join(folder, 'batch_files'), exist_ok=True)
os.makedirs(os.path.join(folder, 'upload_batches'), exist_ok=True)
os.makedirs(os.path.join(folder, 'results'), exist_ok=True)

batch_size = 100
max_photos_per_property = 3

scraper = cloudscraper.create_scraper()

def gen_batch_code():
    return str(uuid.uuid4())

def create_photo_analysis_prompt(photo_url):
    """Create a compact prompt for GPT-4o mini photo analysis."""
    system_content = """Analyze one property photo and return only valid JSON.

Return these fields:
- image_description: short but specific description
- is_photo_of_property: true if the image is part of the listing
- is_indoor: true if taken indoors
- is_human_in_photo: true if people are visible
- is_violating_policy: true if inappropriate content is present
- quality_score: 0-100 score for clarity and property appeal
- room_type: one of living_room, bedroom, kitchen, bathroom, exterior, view, other"""
    
    # Build content array with text and images using low detail mode
    user_content = [
        {
            "type": "text",
            "text": f"Analyze this property photo and provide detailed information."
        },
        {
            "type": "image_url",
            "image_url": {
                "url": photo_url,
                "detail": "low"
            }
        }
    ]

    return [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_content}
    ]

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']
    photo_collection = db['prop_photos']

    props = list(collection.find({ 'status': 'data_extracted' }).sort("created_at", -1).limit(batch_size))

    if len(props) == 0:
        print("No properties found for photo analysis.")
        client.close()
        return

    batch_code = gen_batch_code()
    batch_file_path = os.path.join(folder, 'batch_files', f"batch-{batch_code}.jsonl")

    processed_count = 0

    estate_building_place_map = {}

    def target_photo_count(candidate_count):
        if candidate_count <= 1:
            return candidate_count
        if candidate_count <= 3:
            return 2
        return 1

    with open(batch_file_path, 'w', encoding='utf-8') as batch_file:
        for prop in props:
            links = prop.get('image_links')
            if not isinstance(links, list):
                links = []

            extracted_data = prop.get('v1_extracted_data')
            if not isinstance(extracted_data, dict):
                extracted_data = {}

            photo_urls = extracted_data.get('photo_urls')
            if not isinstance(photo_urls, list):
                photo_urls = []

            for l in photo_urls:
                if l not in links:
                    links.append(l)
            links = list(dict.fromkeys(links))
            photo_limit = target_photo_count(len(links))
            prop_photo_count = 0
            for link in links[:photo_limit]:
                existing_photo = photo_collection.find_one({ 'prop_source_id': prop.get('source_id'), 'photo_url': link })
                if existing_photo:
                    print(f"Photo already exists in collection, skipping: {link}")
                    continue
                try:
                    response = scraper.get(link, stream=True)
                    response.raise_for_status()
                except Exception as e:
                    print(f"Error accessing photo: {link} : {e}")
                    continue

                messages = create_photo_analysis_prompt(link)
                photo_id = str(uuid.uuid4())
                row = {
                    "custom_id": f"photo-{photo_id}",
                    "method": "POST",
                    "url": "/chat/completions",
                    "body": {
                        "model": "gpt-4o-mini-batch",
                        "messages": messages,
                        "max_tokens": 300,
                        "temperature": 0.3,
                        "response_format": { "type": "json_object" }
                    }
                }
                
                batch_file.write(f"{json.dumps(row)}\n")

                photo_collection.insert_one({
                    'photo_id': photo_id,
                    'prop_type': prop.get('type'),
                    'prop_id': prop.get('id'),
                    'prop_source_id': prop.get('source_id'),
                    'prop_source_channel': prop.get('source_channel'),
                    'prop_estate_or_building_name': extracted_data.get('estate_or_building_name'),
                    'prop_estate_or_building_id': prop.get('estate_or_building_id'),
                    'prop_estate_or_building_regions': prop.get('estate_building_regions', []),
                    'prop_rent_price': extracted_data.get('rent_price'),
                    'prop_sell_price': extracted_data.get('sell_price'),
                    'prop_bedrooms': extracted_data.get('number_of_bedrooms'),
                    'prop_district': extracted_data.get('district'),
                    'keywords': extracted_data.get('features', []),
                    'photo_url': link,
                    'photo_analysis_batch_code': batch_code,
                    'status': 'batch_created',
                    'created_at': datetime.now().timestamp(),
                })
                processed_count += 1
                prop_photo_count += 1
                print(f"Processed photo for property {prop.get('source_id')} ({photo_id}): {link}")

            if prop_photo_count > 0:
                collection.update_one(
                    { 'source_id': prop.get('source_id') },
                    { '$set': { 'status': 'photo_analysing' } }
                )
            else:
                collection.update_one(
                    { 'source_id': prop.get('source_id') },
                    { '$set': { 'status': 'photo_analysed' } }
                )
                print(f"No photos to batch for {prop.get('source_id')}, marked as photo_analysed.")

    if processed_count == 0:
        if os.path.exists(batch_file_path):
            os.remove(batch_file_path)
        print("No new photos to analyze. Empty batch file removed.")
        client.close()
        return
    
    print(f"\nBatch file created: {batch_file_path}")
    print(f"Processed {processed_count} photos for analysis.")
    print(f"Batch code: {batch_code}")
    print(f"\nNext steps:")
    print(f"1. Run: python 21_photo_analysis_batch_upload.py")
    print(f"2. Run: python 22_photo_analysis_batch_track.py")
    print(f"3. Run: python 23_photo_analysis_batch_update.py")
    
    client.close()

if __name__ == '__main__':
    main()