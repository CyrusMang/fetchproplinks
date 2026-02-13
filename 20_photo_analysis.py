from datetime import datetime
import os
import uuid
import json
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

batch_size = 50

def gen_batch_code():
    return str(uuid.uuid4())

def create_photo_analysis_prompt(photo_url):
    """Create prompt for GPT-4o mini to analyze property photos with low detail mode."""
    system_content = """You are a property photo analysis expert. Analyze the provided property photo and return a JSON object containing details.

For each photo, extract:
- image_description: Detailed description of what's shown in the photo
- is_photo_of_property: Boolean indicating if the photo is relevant to the property (not a random image or unrelated content)
- is_indoor: Boolean indicating if the photo is taken indoors
- is_human_in_photo: Boolean indicating if there are people visible
- is_violating_policy: Boolean indicating if image contains inappropriate content (adult content, nudity, violence)
- detected_objects: Array of specific objects/furniture found in the image
- quality_score: Number 0-100 indicating photo quality (clarity, lighting, composition)
- room_type: String identifying the room type (e.g., "living_room", "bedroom", "kitchen", "bathroom", "exterior", "view")

Return ONLY valid JSON in this format"""
    
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

    props = collection.find({ 'status': 'data_extracted' }).limit(batch_size)

    batch_code = gen_batch_code()
    batch_file_path = os.path.join(folder, 'batch_files', f"batch-{batch_code}.jsonl")

    processed_count = 0

    with open(batch_file_path, 'w', encoding='utf-8') as batch_file:
        for prop in props:
            links = prop.get('image_links', [])
            photo_urls = prop.get('v1_extracted_data', {}).get('photo_urls', [])
            for link in photo_urls:
                if link not in links:
                    links.append(link)
            for link in links:
                messages = create_photo_analysis_prompt(link)
                photo_id = str(uuid.uuid4())
                row = {
                    "custom_id": f"photo-{photo_id}",
                    "method": "POST",
                    "url": "/chat/completions",
                    "body": {
                        "model": "gpt-4o-mini-batch",
                        "messages": messages,
                        "max_tokens": 4000,
                        "temperature": 0.3,
                        "response_format": { "type": "json_object" }
                    }
                }
                
                batch_file.write(f"{json.dumps(row)}\n")

                extracted_data = prop.get('v1_extracted_data', {})
                
                existing_photo = photo_collection.find_one({ 'photo_url': link })
                if existing_photo:
                    print(f"Photo already exists in collection, skipping: {link}")
                    continue

                photo_collection.insert_one({
                    'photo_id': photo_id,
                    'prop_type': prop.get('type'),
                    'prop_source_id': prop.get('source_id'),
                    'prop_source_channel': prop.get('source_channel'),
                    'prop_estate_or_building_name': extracted_data.get('estate_or_building_name'),
                    'prop_rent_price': extracted_data.get('rent_price'),
                    'prop_sell_price': extracted_data.get('sell_price'),
                    'prop_bedrooms': extracted_data.get('number_of_bedrooms'),
                    'prop_district': extracted_data.get('district'),
                    'prop_summary': extracted_data.get('summary'),
                    'keywords': extracted_data.get('features', []),
                    'photo_url': link,
                    'photo_analysis_batch_code': batch_code,
                    'status': 'batch_created',
                    'created_at': datetime.now().timestamp(),
                })
                processed_count += 1
                print(f"Processed photo for property {prop.get('source_id')} ({photo_id}): {link}")

            collection.update_one(
                { 'source_id': prop.get('source_id') },
                { '$set': { 'status': 'photo_analysing' } }
            )
    
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