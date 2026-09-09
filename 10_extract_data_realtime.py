import datetime
import json
import os
import re
import uuid

from bs4 import BeautifulSoup, Comment
from dotenv import load_dotenv
from openai import OpenAI
from pymongo import MongoClient, ReturnDocument


load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "prop_main")
QWEN_API_KEY = os.getenv("QWEN_API_KEY") or os.getenv("DASHSCOPE_API_KEY")
QWEN_API_URL = os.getenv("QWEN_API_URL") or os.getenv("DASHSCOPE_BASE_URL")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen3.7-flash")

HTML_MAX_CHARS = int(os.getenv("EXTRACT_HTML_MAX_CHARS", "30000"))
PROCESS_LIMIT = int(os.getenv("EXTRACT_DATA_REALTIME_LIMIT", "0"))
CLAIM_TIMEOUT_SECONDS = int(os.getenv("EXTRACT_DATA_CLAIM_TIMEOUT_SECONDS", "7200"))

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def trim_html_for_llm(body):
    if not body:
        return body

    soup = BeautifulSoup(body, "lxml")
    for tag_name in [
        "script", "style", "noscript", "svg", "iframe", "aside",
        "footer", "header", "nav", "form", "button", "input", "select", "textarea"
    ]:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    for span in soup.find_all('span'):
        span.unwrap()

    for tag in soup.find_all(True):
        if tag.has_attr('class'):
            del tag['class']
        if tag.has_attr('style'):
            del tag["style"]
        if tag.has_attr('widthobj'):
            del tag["widthobj"]
        if tag.has_attr('href'):
            del tag["href"]
        if tag.has_attr('langcode'):
            del tag["langcode"]
        if tag.has_attr('lang'):
            del tag["lang"]

    for selector in ["#detail-ref", "#pc-services-detail", ".content_body", "main", "article", '[role="main"]', "body"]:
        node = soup.select_one(selector)
        if node:
            return re.sub(r'[\s]{2,}', ' ', str(node)).strip().replace('\n', ' ').replace('\r', '')[:HTML_MAX_CHARS]

    return re.sub(r'[\s]{2,}', ' ', str(soup)).strip().replace('\n', ' ').replace('\r', '')[:HTML_MAX_CHARS]


def build_system_prompt():
    return """Extract structured property data from the supplied HTML.
Use only evidence in the HTML. If unsure, use null or an empty array.
Return only valid JSON. Do not wrap the JSON in markdown fences.

The JSON must use this schema:
{
    "estate_or_building_name": "string"|null,
    "estate_phase": "string"|null,
    "block": "string"|null,
    "district": "string",
    "floor": "string",
    "features": ["string", ...],
    "photo_urls": ["string", ...],
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
    "is_tenement_building": boolean|null,
    "is_village_house": boolean|null,
    "allow_pets": boolean|null,
    "is_direct_owner_listing": boolean|null,
    "accept_short_term_rental": boolean|null,
    "with_car_park": boolean|null,
    "nearby_places": ["string", ...],
    "transportation_options": ["string", ...],
    "additional_notes": "string",
    "information_updated_date": "string",
    "posted_date": "string",
    "post_updated_date": "string"
}"""


def create_client():
    if QWEN_API_KEY and QWEN_API_URL:
        return OpenAI(api_key=QWEN_API_KEY, base_url=QWEN_API_URL), QWEN_MODEL

    raise RuntimeError(
        "Missing QWEN_API_KEY/DASHSCOPE_API_KEY and DASHSCOPE_BASE_URL, "
        "or the existing Azure OpenAI configuration."
    )


def parse_json_response(content):
    cleaned = content.strip()
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.IGNORECASE)
    return json.loads(cleaned)


def extract_property(client, model, body):
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": build_system_prompt()},
            {"role": "user", "content": trim_html_for_llm(body)},
        ],
        max_tokens=1200,
        response_format={"type": "json_object"},
    )
    return parse_json_response(response.choices[0].message.content)


def claim_property(collection, worker_id):
    stale_before = datetime.datetime.now(datetime.timezone.utc).timestamp() - CLAIM_TIMEOUT_SECONDS
    return collection.find_one_and_update(
        {
            "type": "apartment",
            "post_type": "rent",
            "v1_data_extracting_code": {"$exists": False},
            "$or": [
                {"status": "pending_extraction"},
                {
                    "status": "extracting_realtime",
                    "v1_realtime_extracting_at": {"$lt": stale_before},
                },
            ],
        },
        {
            "$set": {
                "status": "extracting_realtime",
                "v1_realtime_extracting_by": worker_id,
                "v1_realtime_extracting_at": datetime.datetime.now(datetime.timezone.utc).timestamp(),
            }
        },
        sort=[("created_at", -1)],
        return_document=ReturnDocument.AFTER,
    )


def main():
    if not MONGODB_CONNECTION_STRING:
        raise RuntimeError("Missing MONGODB_CONNECTION_STRING in environment.")

    llm_client, model = create_client()
    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    collection = mongo_client[MONGODB_DATABASE]["props"]
    worker_id = str(uuid.uuid4())

    processed_count = 0
    failed_count = 0
    print(f"Starting real-time extraction with {model}.")

    try:
        while PROCESS_LIMIT <= 0 or processed_count + failed_count < PROCESS_LIMIT:
            property_data = claim_property(collection, worker_id)
            if not property_data:
                print("No unclaimed properties found.")
                break

            source_id = property_data.get("source_id", "unknown")
            body = property_data.get("source_html_content")
            if not body:
                print(f"Skipping {source_id}: no HTML body.")
                collection.update_one(
                    {"source_id": source_id, "v1_realtime_extracting_by": worker_id},
                    {
                        "$set": {
                            "status": "extraction_failed",
                            "v1_extract_data_error": "No HTML body found.",
                        },
                        "$unset": {
                            "v1_realtime_extracting_by": "",
                            "v1_realtime_extracting_at": "",
                        },
                    },
                )
                failed_count += 1
                continue

            try:
                extracted_data = extract_property(llm_client, model, body)
                collection.update_one(
                    {"source_id": source_id, "v1_realtime_extracting_by": worker_id},
                    {
                        "$set": {
                            "v1_extracted_data": extracted_data,
                            "status": "data_extracted",
                            "source_html_content": None,
                            "reextract_needed": False,
                            "reextract_reason": "",
                        },
                        "$unset": {
                            "v1_extract_data_error": "",
                            "v1_realtime_extracting_by": "",
                            "v1_realtime_extracting_at": "",
                        },
                    },
                )
                processed_count += 1
                print(f"Updated {source_id} ({processed_count} successful).")
            except Exception as error:
                failed_count += 1
                collection.update_one(
                    {"source_id": source_id, "v1_realtime_extracting_by": worker_id},
                    {
                        "$set": {
                            "status": "extraction_failed",
                            "v1_extract_data_error": str(error),
                        },
                        "$unset": {
                            "v1_realtime_extracting_by": "",
                            "v1_realtime_extracting_at": "",
                        },
                    },
                )
                print(f"Error processing {source_id}: {error}")
    except Exception as error:
        print(f"Error processing: {error}")
    finally:
        mongo_client.close()

    print(f"Finished: {processed_count} successful, {failed_count} failed.")


if __name__ == "__main__":
    main()