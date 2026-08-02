import os
import uuid
import json
from pymongo import MongoClient
from dotenv import load_dotenv
from bs4 import BeautifulSoup

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

batch_size = 100
HTML_MAX_CHARS = int(os.getenv("EXTRACT_HTML_MAX_CHARS", "12000"))


def trim_html_for_llm(body, max_chars=HTML_MAX_CHARS):
    if not body:
        return body

    soup = BeautifulSoup(body, "lxml")
    for tag_name in ["script", "style", "noscript", "svg", "iframe", "aside", "footer", "header", "nav", "form"]:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    for selector in ["main", "article", '[role="main"]', '#pc-services-detail', '.content_body', 'body']:
        node = soup.select_one(selector)
        if node:
            trimmed = str(node)
            return trimmed[:max_chars]

    return str(soup)[:max_chars]

def system_prompt(body):
    return f"""
Extract structured property data from the HTML below.
Use only evidence in the HTML. If unsure, use null or an empty array.

HTML:
{body}
Return only valid JSON in this format:
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
    "maid_rooms": number|null,
    "storerooms": number|null,
    "has_balcony": boolean|null,
    "has_terrace": boolean|null,
    "kitchen_type": "open"|"closed"|null,
    "building_age": number|null,
    "is_village_house": boolean|null,
    "allow_pets": boolean|null,
    "is_direct_owner_listing": boolean|null,
    "accept_short_term_rental": boolean|null,
    "with_car_park": boolean|null,
    "nearby_places": [ "string", ... ],
    "transportation_options": [ "string", ... ],
    "additional_notes": "string",
    "information_updated_date": "string",
    "posted_date": "string",
    "post_updated_date": "string"
}}
"""

def gen_batch_code():
    return str(uuid.uuid4())

def create_prompt(body):
    return [{
        "role": "system", 
        "content": system_prompt(trim_html_for_llm(body))
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

    properties = collection.find(f).sort("created_at", -1).limit(batch_size)

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
                    "max_tokens": 1200,
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