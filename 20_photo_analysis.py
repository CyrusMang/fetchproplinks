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

def create_photo_analysis_prompt(photo_urls):
    """Create prompt for GPT-4o mini to analyze property photos with low detail mode."""
    system_content = """You are a property photo analysis expert. Analyze the provided property photos and return a JSON object with a "photos" array containing details for each photo in the same order.

For each photo, extract:
- image_description: Detailed description of what's shown in the photo
- is_indoor: Boolean indicating if the photo is taken indoors
- is_human_in_photo: Boolean indicating if there are people visible
- is_violating_policy: Boolean indicating if image contains inappropriate content (adult content, nudity, violence)
- detected_objects: Array of specific objects/furniture found in the image
- quality_score: Number 0-100 indicating photo quality (clarity, lighting, composition)
- room_type: String identifying the room type (e.g., "living_room", "bedroom", "kitchen", "bathroom", "exterior", "view")

Return ONLY valid JSON in this format: {"photos": [{...}, {...}, ...]}"""

    # Build content array with text and images using low detail mode
    user_content = [
        {
            "type": "text",
            "text": f"Analyze these {len(photo_urls)} property photos in order and provide detailed information for each. Return JSON with a 'photos' array."
        }
    ]
    
    # Add each image with low detail mode (85 tokens each)
    for url in photo_urls:
        user_content.append({
            "type": "image_url",
            "image_url": {
                "url": url,
                "detail": "low"
            }
        })

    return [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_content}
    ]

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']

    # Find properties ready for photo analysis
    f = {
        'status': "data_extracted",
        'v1_extracted_data.photo_urls': { '$exists': True },
        'photo_analysis_batch_code': { '$exists': False },
    }

    count = collection.count_documents(f)

    if count == 0:
        print("No properties found for photo analysis.")
        client.close()
        return

    print(f"Found {count} properties for photo analysis.")
    
    properties = collection.find(f).sort("updated_at", 1).limit(batch_size)

    batch_code = gen_batch_code()
    batch_file_path = os.path.join(folder, 'batch_files', f"batch-{batch_code}.jsonl")
    
    processed_count = 0
    
    with open(batch_file_path, 'w', encoding='utf-8') as batch_file:
        for prop in properties:
            photo_urls = prop.get('v1_extracted_data', {}).get('photo_urls', [])
            
            # Also include any existing image_links
            existing_links = prop.get('image_links', [])
            for link in existing_links:
                if link not in photo_urls:
                    photo_urls.append(link)
            
            if not photo_urls or len(photo_urls) == 0:
                print(f"Skipping {prop['source_id']} - no photos found.")
                continue
            
            # Limit to first 20 photos to avoid token limits
            photo_urls = photo_urls[:20]
            
            messages = create_photo_analysis_prompt(photo_urls)
            
            row = {
                "custom_id": f"photo-{prop['source_id']}",
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
            
            # Mark property with batch code
            collection.update_one(
                { 'source_id': prop['source_id'] },
                { 
                    '$set': { 
                        'photo_analysis_batch_code': batch_code,
                        'photo_analysis_status': 'batch_created'
                    } 
                }
            )
            
            processed_count += 1
            print(f"Added {prop['source_id']} to batch (photos: {len(photo_urls)})")
    
    print(f"\nBatch file created: {batch_file_path}")
    print(f"Processed {processed_count} properties")
    print(f"Batch code: {batch_code}")
    print(f"\nNext steps:")
    print(f"1. Run: python 21_photo_analysis_batch_upload.py")
    print(f"2. Run: python 22_photo_analysis_batch_track.py")
    print(f"3. Run: python 23_photo_analysis_batch_update.py")
    
    client.close()

if __name__ == '__main__':
    main()