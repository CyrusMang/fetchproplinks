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
folder = os.path.join(artifacts, 'data_refine')

# Create necessary folders
os.makedirs(os.path.join(folder, 'batch_files'), exist_ok=True)
os.makedirs(os.path.join(folder, 'upload_batches'), exist_ok=True)
os.makedirs(os.path.join(folder, 'results'), exist_ok=True)

batch_size = 50

def gen_batch_code():
    return str(uuid.uuid4())

def get_districts(db):
    """Retrieve all districts from the districts collection."""
    districts_collection = db['districts']
    districts = list(districts_collection.find({}))
    
    district_list = []
    for d in districts:
        district_list.append({
            'code': d.get('code'),
            'CNAME': d.get('CNAME'),
            'CNAME_S': d.get('CNAME_S'),
            'ENAME': d.get('ENAME')
        })
    return district_list

def create_refine_prompt(property_data, extracted_data, districts):
    """Create prompt for GPT to refine property data."""
    
    districts_json = json.dumps(districts, ensure_ascii=False, indent=2)
    extracted_json = json.dumps(extracted_data, ensure_ascii=False, indent=2)
    
    system_content = f"""You are a property data refinement expert. Your task is to:
1. Map the district name to the correct district_id from the provided districts list
2. Translate title, summary, features text to different languages
3. Refine and standardize the data

Available districts:
{districts_json}

Current property data:
{extracted_json}

Instructions:
- Match the 'district' field to one of the districts using CNAME, CNAME_S, or ENAME
- Set 'district_id' to the matched district's code
- Translate 'title' to English as 'title_en' (if not already in English)
- Translate 'description' to English as 'description_en' (if not already in English)
- Translate 'estate_or_building_name' to English as 'estate_or_building_name_en' (if applicable)
- Translate all items in 'features' array to English as 'features_en'
- Translate 'nearby_places' to English as 'nearby_places_en'
- Translate 'transportation_options' to English as 'transportation_options_en'
- Keep all original Chinese fields intact
- Standardize and clean up any inconsistent formatting

Return a JSON object with the following structure:
{{
    "district_code": string|null,
    "district_matched": string|null,
    "title_en": "string",
    "description_en": "string",
    "estate_or_building_name_en": "string",
    "features_en": ["string", ...],
    "nearby_places_en": ["string", ...],
    "transportation_options_en": ["string", ...],
    "refinement_notes": "any notes about the refinement process"
}}
"""
    
    return [{
        "role": "system",
        "content": system_content
    }]

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']
    
    # Get all districts
    districts = get_districts(db)
    print(f"Loaded {len(districts)} districts from database.")
    
    # Find properties with evaluation score > 60 and status "evaluated"
    filter_query = {
        'status': 'evaluated',
        'evaluation.overall_score': { '$gt': 60 },
        'v1_extracted_data': { '$exists': True },
        'v1_data_refining_code': { '$exists': False }
    }
    
    count = collection.count_documents(filter_query)
    
    if count == 0:
        print("No properties found for data refinement (score > 60).")
        client.close()
        return
    
    print(f"Found {count} properties with score > 60 to refine.")
    
    properties = collection.find(filter_query).sort("evaluation.overall_score", -1).limit(batch_size)
    
    batch_code = gen_batch_code()
    batch_file_path = os.path.join(folder, 'batch_files', f"batch-{batch_code}.jsonl")
    
    processed_count = 0
    
    with open(batch_file_path, 'w', encoding='utf-8') as batch_file:
        for prop in properties:
            extracted_data = prop.get('v1_extracted_data', {})
            
            if not extracted_data:
                print(f"No extracted data found for property {prop['source_id']}.")
                continue
            
            prompt = create_refine_prompt(prop, extracted_data, districts)
            
            row = {
                "custom_id": f"refine-{prop['source_id']}",
                "method": "POST",
                "url": "/chat/completions",
                "body": {
                    "model": "gpt-4o-mini-batch",
                    "messages": prompt,
                    "max_tokens": 4000,
                    "temperature": 0.3,
                    "response_format": { "type": "json_object" }
                }
            }
            
            batch_file.write(f"{json.dumps(row, ensure_ascii=False)}\n")
            
            # Mark property as being refined
            collection.update_one(
                { 'source_id': prop['source_id'] },
                { '$set': { 'v1_data_refining_code': batch_code } }
            )
            
            processed_count += 1
            score = prop.get('evaluation', {}).get('overall_score', 0)
            grade = prop.get('evaluation', {}).get('grade', 'N/A')
            print(f"Added to batch: {prop['source_id']} (Score: {score:.1f}, Grade: {grade})")
    
    print(f"\n{'='*60}")
    print(f"Batch file created: {batch_file_path}")
    print(f"Processed {processed_count} properties for refinement.")
    print(f"Batch code: {batch_code}")
    print(f"\nNext steps:")
    print(f"1. Run: python 32_data_refine_batch_upload.py")
    print(f"2. Run: python 33_data_refine_batch_track.py")
    print(f"3. Run: python 34_data_refine_batch_update.py")
    
    client.close()

if __name__ == '__main__':
    main()
