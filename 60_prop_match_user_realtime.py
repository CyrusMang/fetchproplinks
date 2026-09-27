import datetime
import json
import math
import os
import re
import time

import requests
from dotenv import load_dotenv
from openai import OpenAI
from pymongo import MongoClient


load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "prop_main")
QWEN_API_KEY = os.getenv("QWEN_API_KEY") or os.getenv("DASHSCOPE_API_KEY")
QWEN_API_URL = os.getenv("QWEN_API_URL") or os.getenv("DASHSCOPE_BASE_URL")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen3.7-flash")

PROCESS_LIMIT = int(os.getenv("PROP_MATCH_REALTIME_LIMIT", "0"))
MAX_CANDIDATES = int(os.getenv("PROP_MATCH_MAX_CANDIDATES", "6"))
PUSH_TTL_SECONDS = 2 * 24 * 60 * 60
RADIUS_DEG = 1 / 69


def create_client():
    if QWEN_API_KEY and QWEN_API_URL:
        return OpenAI(api_key=QWEN_API_KEY, base_url=QWEN_API_URL), QWEN_MODEL
    raise RuntimeError(
        "Missing QWEN_API_KEY/DASHSCOPE_API_KEY and DASHSCOPE_BASE_URL."
    )


def get_yesterday_timestamps():
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - datetime.timedelta(days=1)
    return int(yesterday.timestamp()), int(today.timestamp())


def get_yesterday_props(db):
    start_ts, end_ts = get_yesterday_timestamps()
    query = {
        "indexing_status": "indexed",
        "status": {"$ne": "archived"},
        "created_at": {"$gte": start_ts, "$lt": end_ts},
        "v1_summary_data.confidence_score": {"$gte": 80},
    }
    print(f"Querying properties with filter: {query}")
    return list(db["props"].find(query))


def is_push_true_for_last_5_messages(conv):
    messages = conv.get("messages", [])
    if len(messages) < 5:
        return False
    for message in messages[-5:]:
        push_prop = message.get("data", {}).get("additional_kwargs", {}).get("push_prop", False)
        if message.get("type") != "ai" or push_prop is False:
            return False
    return True


def sanitize_conv(conv):
    meaningful_messages = []
    for message in conv.get("messages", []):
        content = message.get("data", {}).get("content", message.get("content", ""))
        if message.get("type") in ["human", "system"] or (message.get("type") == "ai" and content != ""):
            meaningful_messages.append({
                "type": message.get("type"),
                "content": content,
            })
    return {
        "old_conversation_summary": conv.get("summary"),
        "recent_messages": meaningful_messages,
    }


def sanitize_prop(prop):
    extracted = prop.get("v1_extracted_data", {})
    summary = prop.get("v1_summary_data", {})
    if not isinstance(extracted, dict):
        extracted = {}
    if not isinstance(summary, dict):
        summary = {}
    return {
        "source_id": prop.get("source_id"),
        "rent_price": extracted.get("rent_price"),
        "net_size_sqft": extracted.get("net_size_sqft"),
        "number_of_bedrooms": extracted.get("number_of_bedrooms"),
        "headline_en": summary.get("headline_en"),
        "executive_summary_en": summary.get("executive_summary_en"),
        "key_highlights": summary.get("key_highlights", []),
        "possible_concerns": summary.get("possible_concerns", []),
        "price_analysis": summary.get("price_analysis", {}),
        "layout_and_space": summary.get("layout_and_space", {}),
        "location_and_transport": summary.get("location_and_transport", {}),
        "photo_insights": summary.get("photo_insights", {}),
        "recommended_for": summary.get("recommended_for", []),
        "confidence_score": summary.get("confidence_score"),
    }


def create_system_prompt():
    return (
        "You are a Hong Kong property matching assistant.\n"
        "Given a subscriber's search preferences and a list of new property listings, "
        "identify the best matching listings (up to 2) for the subscriber.\n\n"
        "Rules:\n"
        "- Match based on user conversation summary.\n"
        "- If no listings match well, return an empty matched_source_ids array.\n"
        "- Output only valid JSON with no extra text.\n\n"
        "Return JSON:\n"
        "{\n"
        '  "matched_source_ids": ["source_id_1", "source_id_2"]\n'
        "}"
    )


def create_match_prompt(conv, listings):
    return [
        {"role": "system", "content": create_system_prompt()},
        {
            "role": "user",
            "content": json.dumps(
                {
                    "subscriber_conversations": sanitize_conv(conv),
                    "user_preference": conv.get("userPreferences", {}),
                    "new_listings": listings,
                },
                ensure_ascii=False,
            ),
        },
    ]


def lookup_hk_address(keyword):
    try:
        response = requests.get(
            "https://www.als.gov.hk/lookup",
            params={"q": keyword, "n": 5},
            headers={
                "Accept": "application/json",
                "Accept-Language": "en,zh-Hant",
            },
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except Exception as error:
        print(f"ALS address lookup failed for keyword '{keyword}': {error}")
        return None


def number_or_none(value):
    try:
        if value is None:
            return None
        if isinstance(value, str):
            cleaned = value.strip().lower()
            if cleaned == "":
                return None
            cleaned = cleaned.replace(",", "").replace("$", "").replace("hkd", "")
            return int(float(cleaned))
        return int(value)
    except (ValueError, TypeError):
        return None


def prematch_by_search_criteria(conv, listings):
    search_criteria = conv.get("userPreferences", {})
    if not search_criteria:
        return []

    districts = []
    district_keywords = search_criteria.get("districts") or []
    for keyword in district_keywords:
        lookup_result = lookup_hk_address(keyword)
        if lookup_result and "SuggestedAddress" in lookup_result:
            suggestion = lookup_result["SuggestedAddress"][0]
            district_info = suggestion.get("Address", {}).get("PremisesAddress", {}).get("GeospatialInformation", {})
            if district_info:
                districts.append(district_info)

    min_bedrooms = number_or_none(search_criteria.get("minBedrooms"))
    max_bedrooms = number_or_none(search_criteria.get("maxBedrooms"))
    min_price = number_or_none(search_criteria.get("minPrice"))
    max_price = number_or_none(search_criteria.get("maxPrice"))
    min_size = number_or_none(search_criteria.get("minSize"))
    max_size = number_or_none(search_criteria.get("maxSize"))
    min_building_age = number_or_none(search_criteria.get("minBuildingAge"))
    max_building_age = number_or_none(search_criteria.get("maxBuildingAge"))
    with_car_park = search_criteria.get("haveCar", search_criteria.get("withCarPark", False))
    is_village_house = search_criteria.get("likeVillageHouse", search_criteria.get("isVillageHouse", False))
    allow_pets = search_criteria.get("havePets", search_criteria.get("allowPets", False))
    has_maid_rooms = search_criteria.get("needMaidRooms", search_criteria.get("hasMaidRooms", False))
    is_direct_owner_listing = search_criteria.get("preferDirectOwnerListing", search_criteria.get("isDirectOwnerListing", False))
    accept_short_term_rental = search_criteria.get("acceptShortTermRental", False)

    def matches(prop):
        extracted = prop.get("v1_extracted_data", {})
        if not isinstance(extracted, dict):
            extracted = {}
        subdistrict = prop.get("address", {}).get("subdistrict", {})
        latitude = subdistrict.get("latitude")
        longitude = subdistrict.get("longitude")
        if districts:
            if latitude is None or longitude is None:
                return False
            in_district = False
            lat_per_lng = math.cos(math.radians(latitude))
            if lat_per_lng == 0:
                lat_per_lng = 0.0001
            adjusted_lng_radius = RADIUS_DEG / lat_per_lng
            for district in districts:
                district_lat = float(district.get("Latitude"))
                district_lng = float(district.get("Longitude"))
                if district_lat is None or district_lng is None:
                    continue
                lat_diff = abs(latitude - district_lat)
                lng_diff = abs(longitude - district_lng)
                if lat_diff <= RADIUS_DEG and lng_diff <= adjusted_lng_radius:
                    in_district = True
                    break
            if not in_district:
                return False

        bedrooms = number_or_none(extracted.get("number_of_bedrooms"))
        if bedrooms is not None:
            if min_bedrooms is not None and bedrooms <= min_bedrooms:
                return False
            if max_bedrooms is not None and bedrooms >= max_bedrooms + 1:
                return False

        price = number_or_none(extracted.get("rent_price"))
        if price is None:
            return False
        if min_price is not None and price < (min_price * 0.8):
            return False
        if max_price is not None and price > (max_price * 1.1):
            return False

        size = number_or_none(extracted.get("net_size_sqft"))
        if size is not None:
            if min_size is not None and size < (min_size * 0.8):
                return False
            if max_size is not None and size > (max_size * 1.2):
                return False

        building_age = number_or_none(extracted.get("building_age"))
        if building_age is not None:
            if min_building_age is not None and building_age < min_building_age:
                return False
            if max_building_age is not None and building_age > max_building_age:
                return False

        if with_car_park and not extracted.get("with_car_park", False):
            return False
        if is_village_house and not extracted.get("is_village_house", False):
            return False
        if allow_pets and not extracted.get("allow_pets", False):
            return False
        if has_maid_rooms and not extracted.get("has_maid_rooms", False):
            return False
        if is_direct_owner_listing and not extracted.get("is_direct_owner_listing", False):
            return False
        if accept_short_term_rental and not extracted.get("accept_short_term_rental", False):
            return False
        return True

    return [prop for prop in listings if matches(prop)]


def active_conversations(db):
    return db["conversations-v2"].find({
        #'threadId': '+85269098658',
        "state": {"$in": ["ACTIVE_TRACKING"]},
    })


def parse_json_response(content):
    cleaned = content.strip()
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.IGNORECASE)
    result = json.loads(cleaned)
    if isinstance(result, list):
        if not result or not isinstance(result[0], dict):
            raise ValueError("Expected a match JSON object")
        result = result[0]
    if not isinstance(result, dict):
        raise ValueError(f"Expected a match JSON object, got {type(result).__name__}")
    return result


def match_listings(client, model, messages):
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.3,
        max_tokens=500,
        response_format={"type": "json_object"},
    )
    return parse_json_response(response.choices[0].message.content)


def matched_source_ids(llm_result):
    matched_ids = llm_result.get("matched_source_ids", [])
    if isinstance(matched_ids, str):
        matched_ids = [matched_ids]
    if not isinstance(matched_ids, list):
        return []
    return [source_id for source_id in matched_ids if isinstance(source_id, str) and source_id][:2]


def append_push_properties(db, conv, candidate_props, source_ids):
    candidates_by_id = {prop.get("source_id"): prop for prop in candidate_props}
    matched_props = [candidates_by_id[source_id] for source_id in source_ids if source_id in candidates_by_id]
    if not matched_props:
        return 0

    now_ts = int(time.time())
    expired_at = now_ts + PUSH_TTL_SECONDS
    push_items = []
    for prop in matched_props[:1]:
        property_id = prop.get("id") or prop.get("source_id") or str(prop.get("_id") or "")
        if not property_id:
            continue
        push_items.append({
            "property_id": property_id,
            "status": "pending",
            "createdAt": now_ts,
            "expired_at": expired_at,
        })

    if not push_items:
        return 0

    update_result = db["conversations-v2"].update_one(
        {"_id": conv["_id"]},
        {"$push": {"push_properties": {"$each": push_items}}},
    )
    return len(push_items) if update_result.modified_count > 0 else 0


def main():
    if not MONGODB_CONNECTION_STRING:
        raise RuntimeError("Missing MONGODB_CONNECTION_STRING in environment.")

    llm_client, model = create_client()
    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    db = mongo_client[MONGODB_DATABASE]

    matched_count = 0
    skipped_count = 0
    failed_count = 0
    considered_count = 0

    print(f"Starting real-time property matching with {model}.")
    try:
        props = get_yesterday_props(db)
        if not props:
            print("No new indexed properties found for yesterday.")
            return

        print(f"Found {len(props)} new properties from yesterday.")
        sorted_listings = sorted(
            props,
            key=lambda prop: (prop.get("v1_summary_data") or {}).get("confidence_score", 0),
            reverse=True,
        )

        for conv in active_conversations(db):
            if PROCESS_LIMIT > 0 and considered_count >= PROCESS_LIMIT:
                break

            conv_id = conv.get("_id")
            if is_push_true_for_last_5_messages(conv):
                print(f"Conversation {conv_id} has push=True for last 5 messages, skipping.")
                skipped_count += 1
                continue

            filtered_listings = prematch_by_search_criteria(conv, sorted_listings)
            if not filtered_listings:
                print(f"No listings match search criteria for conversation {conv_id}, skipping.")
                skipped_count += 1
                continue

            candidates = filtered_listings[:MAX_CANDIDATES]
            candidate_ids = [prop.get("source_id") for prop in candidates]
            print(
                f"Matching conversation {conv_id} against {len(candidates)} candidate listings: {candidate_ids}"
            )
            considered_count += 1

            try:
                llm_result = match_listings(
                    llm_client,
                    model,
                    create_match_prompt(conv, [sanitize_prop(prop) for prop in candidates]),
                )
                source_ids = matched_source_ids(llm_result)
                if not source_ids:
                    print(f"No matches returned for conversation {conv_id}.")
                    skipped_count += 1
                    continue

                pushed = append_push_properties(db, conv, candidates, source_ids)
                if pushed:
                    matched_count += 1
                    print(f"Updated conversation {conv_id}: appended {pushed} push_properties item(s)")
                else:
                    skipped_count += 1
                    print(f"No valid matched properties for conversation {conv_id}")
            except Exception as error:
                failed_count += 1
                print(f"Error matching conversation {conv_id}: {error}")
    finally:
        mongo_client.close()

    print(
        f"Finished: {matched_count} matched, {skipped_count} skipped, {failed_count} failed."
    )


if __name__ == "__main__":
    main()
