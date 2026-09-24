import datetime
import base64
import json
import os
import re
import uuid

import cloudscraper
from dotenv import load_dotenv
from openai import OpenAI
from pymongo import MongoClient, ReturnDocument


load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "prop_main")
QWEN_API_KEY = os.getenv("QWEN_API_KEY") or os.getenv("DASHSCOPE_API_KEY")
QWEN_API_URL = os.getenv("QWEN_API_URL") or os.getenv("DASHSCOPE_BASE_URL")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen3.7-flash")

PROCESS_LIMIT = int(os.getenv("PHOTO_ANALYSIS_REALTIME_LIMIT", "0"))
MAX_PHOTOS_PER_PROPERTY = int(os.getenv("PHOTO_ANALYSIS_MAX_PHOTOS_PER_PROPERTY", "3"))
CLAIM_TIMEOUT_SECONDS = int(os.getenv("PHOTO_ANALYSIS_CLAIM_TIMEOUT_SECONDS", "7200"))

scraper = cloudscraper.create_scraper()


def create_client():
    if QWEN_API_KEY and QWEN_API_URL:
        return OpenAI(api_key=QWEN_API_KEY, base_url=QWEN_API_URL), QWEN_MODEL
    raise RuntimeError(
        "Missing QWEN_API_KEY/DASHSCOPE_API_KEY and DASHSCOPE_BASE_URL."
    )


def create_photo_analysis_messages(image_data_url):
    system_content = """Analyze one property photo and return only valid JSON.
The top-level JSON value must be one object, never an array.

Return these fields:
- image_description: short but specific description
- is_photo_of_property: true if the image is part of the listing
- is_indoor: true if taken indoors
- is_human_in_photo: true if people are visible
- is_violating_policy: true if inappropriate content is present
- have_watermark: true if a watermark is present
- quality_score: 0-100 score for clarity and property appeal
- room_type: one of living_room, bedroom, kitchen, bathroom, exterior, view, other"""
    return [
        {"role": "system", "content": system_content},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Analyze this property photo."},
                {
                    "type": "image_url",
                    "image_url": {"url": image_data_url, "detail": "low"},
                },
            ],
        },
    ]


def parse_json_response(content):
    if isinstance(content, str):
        cleaned = content.strip()
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.IGNORECASE)
        result = json.loads(cleaned)
    else:
        result = content

    analysis_fields = {
        "image_description", "is_photo_of_property", "is_indoor",
        "is_human_in_photo", "is_violating_policy", "have_watermark",
        "quality_score", "room_type",
    }

    def find_analysis_object(value):
        if isinstance(value, dict):
            if analysis_fields.intersection(value):
                return value
            for nested_value in value.values():
                match = find_analysis_object(nested_value)
                if match:
                    return match
        elif isinstance(value, list):
            for item in value:
                match = find_analysis_object(item)
                if match:
                    return match
        elif isinstance(value, str):
            try:
                return find_analysis_object(json.loads(value))
            except (json.JSONDecodeError, TypeError):
                return None
        return None

    result = find_analysis_object(result)
    if not result:
        print(content)
        raise ValueError(
            "Expected a photo-analysis object in the model response"
        )
    return result


def analyze_photo(client, model, image_data_url):
    response = client.chat.completions.create(
        model=model,
        messages=create_photo_analysis_messages(image_data_url),
        max_tokens=300,
        temperature=0.3,
        response_format={"type": "json_object"},
    )
    return parse_json_response(response.choices[0].message.content)


def download_as_data_url(photo_url):
    response = scraper.get(photo_url, timeout=30)
    response.raise_for_status()
    content_type = response.headers.get("content-type", "image/jpeg").split(";", 1)[0].strip()
    if not content_type.startswith("image/"):
        raise ValueError(f"Photo URL returned non-image content type: {content_type}")
    encoded_image = base64.b64encode(response.content).decode("ascii")
    return f"data:{content_type};base64,{encoded_image}"


def claim_property(collection, worker_id):
    stale_before = datetime.datetime.now(datetime.timezone.utc).timestamp() - CLAIM_TIMEOUT_SECONDS
    return collection.find_one_and_update(
        {
            "type": "apartment",
            "$or": [
                {"status": "data_extracted"},
                {
                    "status": "photo_analysing",
                    "photo_realtime_extracting_by": {"$exists": False},
                },
                {
                    "status": "photo_analysing",
                    "photo_realtime_extracting_at": {"$lt": stale_before},
                },
            ],
        },
        {
            "$set": {
                "status": "photo_analysing",
                "photo_realtime_extracting_by": worker_id,
                "photo_realtime_extracting_at": datetime.datetime.now(datetime.timezone.utc).timestamp(),
            }
        },
        sort=[("created_at", -1)],
        return_document=ReturnDocument.AFTER,
    )


def get_photo_urls(property_data):
    links = property_data.get("image_links")
    if not isinstance(links, list):
        links = []

    extracted_data = property_data.get("v1_extracted_data")
    if not isinstance(extracted_data, dict):
        extracted_data = {}
    photo_urls = extracted_data.get("photo_urls")
    if isinstance(photo_urls, list):
        links.extend(photo_urls)

    return list(dict.fromkeys(link for link in links if isinstance(link, str) and link))


def create_photo_document(property_data, extracted_data, photo_id, photo_url, worker_id):
    return {
        "photo_id": photo_id,
        "prop_type": property_data.get("type"),
        "prop_id": property_data.get("id"),
        "prop_source_id": property_data.get("source_id"),
        "prop_source_channel": property_data.get("source_channel"),
        "prop_estate_or_building_name": extracted_data.get("estate_or_building_name"),
        "prop_estate_or_building_id": property_data.get("estate_or_building_id"),
        "prop_estate_or_building_regions": property_data.get("estate_building_regions", []),
        "prop_rent_price": extracted_data.get("rent_price"),
        "prop_sell_price": extracted_data.get("sell_price"),
        "prop_bedrooms": extracted_data.get("number_of_bedrooms"),
        "prop_district": extracted_data.get("district"),
        "keywords": extracted_data.get("features", []),
        "photo_url": photo_url,
        "photo_analysis_worker_id": worker_id,
        "status": "photo_analysis_processing",
        "created_at": datetime.datetime.now(datetime.timezone.utc).timestamp(),
    }


def has_complete_analysis(photo):
    required_fields = {
        "image_description",
        "is_photo_of_property",
        "is_indoor",
        "is_human_in_photo",
        "is_violating_policy",
        "have_watermark",
        "quality_score",
        "room_type",
    }
    return photo.get("status") == "photo_analysed" and required_fields.issubset(photo)


def finish_property(collection, source_id, worker_id, status):
    collection.update_one(
        {"source_id": source_id, "photo_realtime_extracting_by": worker_id},
        {
            "$set": {"status": status},
            "$unset": {
                "photo_realtime_extracting_by": "",
                "photo_realtime_extracting_at": "",
            },
        },
    )


def process_property(llm_client, model, collection, photo_collection, property_data, worker_id):
    source_id = property_data.get("source_id", "unknown")
    extracted_data = property_data.get("v1_extracted_data")
    if not isinstance(extracted_data, dict):
        extracted_data = {}

    links = get_photo_urls(property_data)
    photo_limit = min(len(links), MAX_PHOTOS_PER_PROPERTY)
    analyzed_count = 0
    failed_count = 0

    for photo_url in links[:photo_limit]:
        existing_photo = photo_collection.find_one(
            {"prop_source_id": source_id, "photo_url": photo_url}
        )
        if existing_photo and has_complete_analysis(existing_photo):
            continue

        try:
            image_data_url = download_as_data_url(photo_url)
            analysis_result = analyze_photo(llm_client, model, image_data_url)
            photo_id = existing_photo.get("photo_id") if existing_photo else str(uuid.uuid4())
            photo_document = create_photo_document(
                property_data, extracted_data, photo_id, photo_url, worker_id
            )
            photo_collection.update_one(
                {"photo_id": photo_id},
                {"$set": {**photo_document, **analysis_result, "status": "photo_analysed"}},
                upsert=True,
            )
            analyzed_count += 1
            print(f"Updated {source_id}: {photo_id}")
        except Exception as error:
            failed_count += 1
            if existing_photo:
                photo_collection.update_one(
                    {"photo_id": existing_photo["photo_id"]},
                    {"$set": {"status": "photo_analysis_failed", "api_error": str(error)}},
                )
            print(f"Error processing photo for {source_id}: {photo_url}: {error}")

    if analyzed_count > 0:
        finish_property(collection, source_id, worker_id, "photo_analysed")
    elif failed_count > 0:
        finish_property(collection, source_id, worker_id, "photo_analysis_failed")
    else:
        finish_property(collection, source_id, worker_id, "photo_analysed")

    return analyzed_count, failed_count


def main():
    if not MONGODB_CONNECTION_STRING:
        raise RuntimeError("Missing MONGODB_CONNECTION_STRING in environment.")

    llm_client, model = create_client()
    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    collection = mongo_client[MONGODB_DATABASE]["props"]
    photo_collection = mongo_client[MONGODB_DATABASE]["prop_photos"]
    worker_id = str(uuid.uuid4())
    property_count = 0
    analyzed_count = 0
    failed_count = 0

    print(f"Starting real-time photo analysis with {model}.")
    try:
        while PROCESS_LIMIT <= 0 or property_count < PROCESS_LIMIT:
            property_data = claim_property(collection, worker_id)
            if not property_data:
                print("No unclaimed properties found.")
                break
            try:
                print(f"Processing property {property_data.get('source_id', 'unknown')}...")
                analyzed, failed = process_property(
                    llm_client, model, collection, photo_collection, property_data, worker_id
                )
                analyzed_count += analyzed
                failed_count += failed
            except Exception as error:
                source_id = property_data.get("source_id", "unknown")
                finish_property(collection, source_id, worker_id, "photo_analysis_failed")
                print(f"Error processing property {source_id}: {error}")
            property_count += 1
    finally:
        mongo_client.close()

    print(
        f"Finished: {property_count} properties, "
        f"{analyzed_count} photos analyzed, {failed_count} photos failed."
    )


if __name__ == "__main__":
    main()