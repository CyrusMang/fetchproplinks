import datetime
import json
import os
import re
import uuid

from dotenv import load_dotenv
from openai import OpenAI
from pymongo import MongoClient, ReturnDocument


load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "prop_main")
QWEN_API_KEY = os.getenv("QWEN_API_KEY") or os.getenv("DASHSCOPE_API_KEY")
QWEN_API_URL = os.getenv("QWEN_API_URL") or os.getenv("DASHSCOPE_BASE_URL")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen3.7-flash")

PROCESS_LIMIT = int(os.getenv("PROPERTY_SUMMARY_REALTIME_LIMIT", "0"))
MAX_PHOTOS_PER_PROPERTY = int(os.getenv("PROPERTY_SUMMARY_MAX_PHOTOS_PER_PROPERTY", "3"))
CLAIM_TIMEOUT_SECONDS = int(os.getenv("PROPERTY_SUMMARY_CLAIM_TIMEOUT_SECONDS", "7200"))


def create_client():
    if QWEN_API_KEY and QWEN_API_URL:
        return OpenAI(api_key=QWEN_API_KEY, base_url=QWEN_API_URL), QWEN_MODEL
    raise RuntimeError(
        "Missing QWEN_API_KEY/DASHSCOPE_API_KEY and DASHSCOPE_BASE_URL."
    )


def create_system_prompt():
    return """You are a senior Hong Kong property analyst.

Produce one concise property summary in English only, based only on structured listing data and photo observations.
Do not translate.

Rules:
- Use only evidence in the input. Do not invent facts.
- If information is missing, use null or an empty array.
- Keep the writing concise and practical for home seekers.
- Mention both strengths and potential concerns.
- Use photo evidence in the narrative.
- Return one top-level JSON object, never an array.

Return JSON with this schema:
{
  "headline_en": "string",
  "executive_summary_en": "string",
  "key_highlights": ["string", "..."],
  "possible_concerns": ["string", "..."],
  "price_analysis": {"value_comment": "string|null"},
  "layout_and_space": {"space_comment": "string|null"},
  "location_and_transport": {"location_comment": "string|null"},
  "photo_insights": {
    "overall_condition": "string",
    "cleanliness_comment": "string|null",
    "brightness_comment": "string|null"
  },
  "recommended_for": ["string", "..."],
  "confidence_score": number (0-100)
}"""


def sanitize_property(property_data):
    extracted = property_data.get("v1_extracted_data", {})
    if not isinstance(extracted, dict):
        extracted = {}
    return {
        "source_id": property_data.get("source_id"),
        "source_channel": property_data.get("source_channel"),
        "source_url": property_data.get("source_url"),
        "estate_or_building_name": extracted.get("estate_or_building_name"),
        "district": extracted.get("district"),
        "floor": extracted.get("floor"),
        "features": extracted.get("features", []),
        "rent_price": extracted.get("rent_price"),
        "sell_price": extracted.get("sell_price"),
        "net_size_sqft": extracted.get("net_size_sqft"),
        "gross_size_sqft": extracted.get("gross_size_sqft"),
        "number_of_bedrooms": extracted.get("number_of_bedrooms"),
        "number_of_bathrooms": extracted.get("number_of_bathrooms"),
        "building_age": extracted.get("building_age"),
        "nearby_places": extracted.get("nearby_places", []),
        "transportation_options": extracted.get("transportation_options", []),
        "additional_notes": extracted.get("additional_notes"),
    }


def create_summary_prompt(property_payload, photo_payloads):
    input_payload = {
        "property_data": property_payload,
        "photo_analyses": photo_payloads,
    }
    return [
        {"role": "system", "content": create_system_prompt()},
        {
            "role": "user",
            "content": (
                "Generate one complete property summary JSON for this listing data:\n"
                f"{json.dumps(input_payload, ensure_ascii=False)}"
            ),
        },
    ]


def parse_json_response(content):
    cleaned = content.strip()
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.IGNORECASE)
    result = json.loads(cleaned)
    if isinstance(result, list):
        if not result or not all(isinstance(item, dict) for item in result):
            raise ValueError("Expected a summary JSON object or an array of summary objects")
        result = result[0]
    if not isinstance(result, dict):
        raise ValueError(f"Expected a summary JSON object, got {type(result).__name__}")
    return result


def generate_summary(client, model, property_data, photo_payloads):
    response = client.chat.completions.create(
        model=model,
        messages=create_summary_prompt(sanitize_property(property_data), photo_payloads),
        temperature=0.3,
        max_tokens=1200,
        response_format={"type": "json_object"},
    )
    return parse_json_response(response.choices[0].message.content)


def claim_property(collection, worker_id):
    stale_before = datetime.datetime.now(datetime.timezone.utc).timestamp() - CLAIM_TIMEOUT_SECONDS
    return collection.find_one_and_update(
        {
            "v1_extracted_data": {"$exists": True},
		        "summary_batch_code": {"$exists": False},
            "$or": [
                {
                    "status": "photo_analysed",
                    "summary_realtime_extracting_by": {"$exists": False},
                },
                {
                    "status": "summary_failed",
                    "summary_realtime_extracting_by": {"$exists": False},
                },
                {
                    "status": "summary_generating_realtime",
                    "summary_realtime_extracting_at": {"$lt": stale_before},
                },
            ],
        },
        {
            "$set": {
                "status": "summary_generating_realtime",
                "summary_realtime_extracting_by": worker_id,
                "summary_realtime_extracting_at": datetime.datetime.now(datetime.timezone.utc).timestamp(),
            }
        },
        sort=[("created_at", -1)],
        return_document=ReturnDocument.AFTER,
    )


def get_photo_payloads(photo_collection, source_id):
    photo_filter = {
        "prop_source_id": source_id,
        "status": "photo_analysed",
        "is_photo_of_property": True,
        "is_violating_policy": False,
        "is_human_in_photo": False,
    }
    photos = photo_collection.find(photo_filter).sort("quality_score", -1).limit(MAX_PHOTOS_PER_PROPERTY)
    return [
        {
            "room": photo.get("room_type"),
            "desc": photo.get("image_description"),
            "q": photo.get("quality_score"),
            "indoor": photo.get("is_indoor"),
        }
        for photo in photos
    ]


def finish_property(collection, source_id, worker_id, status, summary=None, error=None):
    update = {
        "$set": {"summary_status": status},
        "$unset": {
            "summary_realtime_extracting_by": "",
            "summary_realtime_extracting_at": "",
        },
    }
    if summary is not None:
        update["$set"].update(
            {
                "v1_summary_data": summary,
                "summary_generated_at": datetime.datetime.now(datetime.timezone.utc).timestamp(),
            }
        )
    if error is not None:
        update["$set"]["summary_error"] = str(error)
    else:
        update["$unset"]["summary_error"] = ""

    collection.update_one(
        {"source_id": source_id, "summary_realtime_extracting_by": worker_id},
        update,
    )


def main():
    if not MONGODB_CONNECTION_STRING:
        raise RuntimeError("Missing MONGODB_CONNECTION_STRING in environment.")

    llm_client, model = create_client()
    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    collection = mongo_client[MONGODB_DATABASE]["props"]
    photo_collection = mongo_client[MONGODB_DATABASE]["prop_photos"]
    worker_id = str(uuid.uuid4())
    processed_count = 0
    failed_count = 0

    print(f"Starting real-time property summaries with {model}.")
    try:
        while PROCESS_LIMIT <= 0 or processed_count + failed_count < PROCESS_LIMIT:
            property_data = claim_property(collection, worker_id)
            if not property_data:
                print("No unclaimed properties found.")
                break

            source_id = property_data.get("source_id", "unknown")
            try:
                photo_payloads = get_photo_payloads(photo_collection, source_id)
                if not photo_payloads:
                    raise ValueError("No eligible analyzed photos found")
                summary = generate_summary(llm_client, model, property_data, photo_payloads)
                finish_property(collection, source_id, worker_id, "summary_ready", summary=summary)
                processed_count += 1
                print(f"Updated summary for {source_id}.")
            except Exception as error:
                finish_property(collection, source_id, worker_id, "summary_failed", error=error)
                failed_count += 1
                print(f"Error processing summary for {source_id}: {error}")
    finally:
        mongo_client.close()

    print(f"Finished: {processed_count} successful, {failed_count} failed.")


if __name__ == "__main__":
    main()