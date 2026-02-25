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
folder = os.path.join(artifacts, 'extract_data')

# Create necessary folders
os.makedirs(os.path.join(folder, 'batch_files'), exist_ok=True)
os.makedirs(os.path.join(folder, 'upload_batches'), exist_ok=True)
os.makedirs(os.path.join(folder, 'results'), exist_ok=True)
os.makedirs(os.path.join(folder, 'data'), exist_ok=True)
os.makedirs(os.path.join(folder, 'backup'), exist_ok=True)

batch_size = 50

def system_prompt(body):
    return f"""
Answer the questions based on following context only.

Context: 
HTML body of a property details page.
{body}

Questions: 
Extract the useful information and summarize about the property into the following JSON format:
{{
    "title": "string",
    "description": "string",
    "estate_or_building_name": "string"|null (need to be specified if it's an estate or building, otherwise it should be null),
    "district": "string",
    "floor": "string",
    "features": [ "string", ... ],
    "photo_urls": [ "string", ... ],
    "rent_price": number|null,
    "sell_price": number|null,
    "net_size_sqft": number|null,
    "gross_size_sqft": number|null,
    "number_of_bedrooms": number|null,
    "number_of_bathrooms": number|null,
    "building_age": number|null,
    "nearby_places": [ "string", ... ],
    "transportation_options": [ "string", ... ],
    "contacts": [{{
        "name": "string",
        "phone": "string",
        "whatsapp": "string",
        "is_agent": boolean,
        "license_no": "string",
    }}, ... ],
    "additional_notes": "string",
    "additional_information_in_json": {{ "key": "value", ...}},
    "information_updated_date": "string",
    "posted_date": "string",
    "post_updated_date": "string",
    "summary": "2-3 sentence summary of the property highlighting key features"
}}
"""

def gen_batch_code():
    return str(uuid.uuid4())

def create_prompt(body):
    return [{
        "role": "system", 
        "content": system_prompt(body)
    }]

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']

    f = {
        'status': "pending_extraction",
        'type': "apartment",
        'v1_data_extracting_code': { '$exists': False },
    }

    count = collection.count_documents(f)

    if count == 0:
        print("No properties found for extraction.")
        client.close()
        return

    properties = collection.find(f).sort("updated_at", -1).limit(batch_size)

    batch_code = gen_batch_code()
    batch_file_path = os.path.join(folder, 'batch_files', f"batch-{batch_code}.jsonl")
    with open(batch_file_path, 'w', encoding='utf-8') as batch_file:
        for property in properties:
            body = property.get('source_html_content', None)
            if not body:
                print(f"No html body found for property {property['source_id']}.")
                continue
            prompt = create_prompt(body)
            row = {
                "custom_id": f"task-{property['source_id']}",
                "method": "POST",
                "url": "/chat/completions",
                "body": {
                    "model": "gpt-4.1-nano",
                    "messages": prompt,
                    "response_format": { "type": "json_object" }
                }
            }
            batch_file.write(f"{json.dumps(row, ensure_ascii=False)}\n")
            collection.update_one(
                { 'source_id': property['source_id'] },
                { '$set': { 'v1_data_extracting_code': batch_code } }
            )
    print(f"Batch file created: {batch_file_path}")
    client.close()

if __name__ == '__main__':
    main()