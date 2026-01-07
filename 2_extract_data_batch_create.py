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

batch_size = 10

def system_prompt(body):
    return f"""
Answer the questions based on following context only.

Context: 
HTML body of a property details page.
{body}

Questions: 
Extract the useful information and summarize about the property into the following JSON format:
{{
    "estate_or_building_name": "string",
    "features": [ "string", ... ],
    "rent_price": "string",
    "sell_price": "string",
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
    "summary": "string"
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

    properties = collection.find(f).limit(batch_size)

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