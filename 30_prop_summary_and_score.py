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
folder = os.path.join(artifacts, 'prop_summary')

# Create necessary folders
os.makedirs(os.path.join(folder, 'batch_files'), exist_ok=True)
os.makedirs(os.path.join(folder, 'upload_batches'), exist_ok=True)
os.makedirs(os.path.join(folder, 'results'), exist_ok=True)

batch_size = 50

def gen_batch_code():
    return str(uuid.uuid4())

def create_summary_prompt(prop_data):
    """Create prompt for GPT-4o nano to summarize property and evaluate quality."""
    
    system_content = """You are a property data quality analyst. Analyze the property information and photos, then provide a comprehensive summary and quality score.

Evaluate:
1. **Data Completeness** (0-30 points): Are key fields filled? (price, size, bedrooms, location, contacts)
2. **Data Quality** (0-30 points): Is data accurate, consistent, and detailed?
3. **Photo Quality** (0-40 points): Number of quality photos, variety of rooms, clarity

Return JSON:
{
    "summary": "2-3 sentence property summary highlighting key features",
    "data_completeness_score": 0-30,
    "data_quality_score": 0-30,
    "photo_quality_score": 0-40,
    "overall_score": 0-100
}"""

    # Prepare property data summary
    extracted = prop_data.get('v1_extracted_data', {})
    
    property_info = {
        "title": extracted.get('title'),
        "description": extracted.get('description'),
        "district": extracted.get('district'),
        "estate_name": extracted.get('estate_or_building_name'),
        "floor": extracted.get('floor'),
        "rent_price": extracted.get('rent_price'),
        "sell_price": extracted.get('sell_price'),
        "net_size_sqft": extracted.get('net_size_sqft'),
        "gross_size_sqft": extracted.get('gross_size_sqft'),
        "bedrooms": extracted.get('number_of_bedrooms'),
        "bathrooms": extracted.get('number_of_bathrooms'),
        "building_age": extracted.get('building_age'),
        "features": extracted.get('features', []),
        "nearby_places": extracted.get('nearby_places', []),
        "transportation": extracted.get('transportation_options', []),
        "contacts": extracted.get('contacts', []),
        "posted_date": extracted.get('posted_date'),
        "updated_date": extracted.get('post_updated_date'),
        "analysed_photos": [{
            "image_description": p.get('description'),
            "is_indoor": p.get('is_indoor'),
            "is_human_in_photo": p.get('is_human_in_photo'),
            "is_violating_policy": p.get('is_violating_policy'),
            "detected_objects": p.get('detected_objects', []),
            "quality_score": p.get('quality_score'),
            "room_type": p.get('room_type')
        } for p in prop_data.get('analysed_photos', []) if p.get('blob_url')]
    }
    
    user_content = f"""Analyze this property listing and provide quality assessment:

PROPERTY DATA:
{json.dumps(property_info, indent=2, ensure_ascii=False)}

Provide comprehensive summary and scores."""

    return [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_content}
    ]

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']

    # Find properties ready for summarization
    f = {
        'status': 'photo_analysed',
        'v1_extracted_data': {'$exists': True},
        'summary_batch_code': {'$exists': False}
    }

    count = collection.count_documents(f)

    if count == 0:
        print("No properties found for summarization.")
        client.close()
        return

    print(f"Found {count} properties for summarization.")
    
    properties = collection.find(f).sort("updated_at", 1).limit(batch_size)

    batch_code = gen_batch_code()
    batch_file_path = os.path.join(folder, 'batch_files', f"batch-{batch_code}.jsonl")
    
    processed_count = 0
    
    with open(batch_file_path, 'w', encoding='utf-8') as batch_file:
        for prop in properties:
            # Get photo analysis data
            
            messages = create_summary_prompt(prop)
            
            row = {
                "custom_id": f"summary-{prop['source_id']}",
                "method": "POST",
                "url": "/chat/completions",
                "body": {
                    "model": "gpt-4o-mini",
                    "messages": messages,
                    "max_tokens": 1000,
                    "temperature": 0.3,
                    "response_format": {"type": "json_object"}
                }
            }
            
            batch_file.write(f"{json.dumps(row)}\n")
            
            # Mark property with batch code
            collection.update_one(
                {'source_id': prop['source_id']},
                {
                    '$set': {
                        'summary_batch_code': batch_code,
                        'summary_status': 'batch_created'
                    }
                }
            )
            
            processed_count += 1
            print(f"Added {prop['source_id']} to batch")
    
    print(f"\nBatch file created: {batch_file_path}")
    print(f"Processed {processed_count} properties")
    print(f"Batch code: {batch_code}")
    print(f"\nNext steps:")
    print(f"1. Run: python 31_prop_summary_batch_upload.py")
    print(f"2. Run: python 32_prop_summary_batch_track.py")
    print(f"3. Run: python 33_prop_summary_batch_update.py")
    
    client.close()

if __name__ == '__main__':
    main()
