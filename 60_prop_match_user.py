import os
import uuid
import json
import requests
import math
from bson import ObjectId
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
from bson.objectid import ObjectId
import send_prop_matched_wtsapp_msg

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

dir = os.path.dirname(os.path.abspath(__file__))
artifacts = os.path.join(dir, ARTIFACTS_FOLDER)
folder = os.path.join(artifacts, 'prop_match')

os.makedirs(os.path.join(folder, 'batch_files'), exist_ok=True)
os.makedirs(os.path.join(folder, 'upload_batches'), exist_ok=True)
os.makedirs(os.path.join(folder, 'results'), exist_ok=True)
os.makedirs(os.path.join(folder, 'data'), exist_ok=True)

user_batch_size = 100


def gen_batch_code():
    return str(uuid.uuid4())


def get_yesterday_timestamps():
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    return int(yesterday.timestamp()), int(today.timestamp())


def get_yesterday_props(db):
    start_ts, end_ts = get_yesterday_timestamps()
    f = {
        'indexing_status': 'indexed',
        'status': {'$ne': 'archived'},
        'created_at': {'$gte': start_ts, '$lt': end_ts},
        "v1_summary_data.confidence_score": { '$gte': 80 }
    }
    print(f"Querying properties with filter: {f}")
    return list(db['props'].find(f))


def get_all_result_files(folder_path):
    files = []
    for filename in os.listdir(folder_path):
        if filename.endswith('-result.json'):
            files.append(os.path.join(folder_path, filename))
    return files

def get_langgraph_v2_threads_by_user_id(db, user_id):
    six_hours_ago = int((datetime.now() - timedelta(hours=6)).timestamp())
    conv = db['langgraph_v2_threads'].find_one({ 
      'userId': user_id,
      'v2State': {'$in': ["ACTIVE_TRACKING", "ONBOARDING"]},
      'updatedAt': {'$lte': six_hours_ago},
    })
    return conv

def get_pending_conversation_by_user_id(db, user_id):
    six_hours_ago = int((datetime.now() - timedelta(hours=6)).timestamp())
    conv = db['conversations'].find_one({ 
      'userId': ObjectId(user_id),
      'updatedAt': {'$lte': six_hours_ago},
    })
    return conv


def sanitize_conv(conv):
    meaningful_messages = []
    messages = conv.get('messages', [])
    for m in messages:
        if m.get('type') in ['human', 'system']:
            meaningful_messages.append({
                'type': m.get('type'),
                'content': m.get('data', {}).get('content', m.get('content', ''))
            })
        elif m.get('type') == 'ai' and m.get('data', {}).get('content', m.get('content', '')) != '':
            meaningful_messages.append({
                'type': m.get('type'),
                'content': m.get('data', {}).get('content', m.get('content', ''))
            })
    return {
        'old_conversation_summary': conv.get('summary'),
        'recent_messages': meaningful_messages
    }


def sanitize_prop(prop):
    extracted = prop.get('v1_extracted_data', {})
    summary = prop.get('v1_summary_data', {})
    return {
        'source_id': prop.get('source_id'),
        'headline_en': summary.get('headline_en'),
        'executive_summary_en': summary.get('executive_summary_en'),
        'key_highlights': summary.get('key_highlights', []),
        'possible_concerns': summary.get('possible_concerns', []),
        'price_analysis': summary.get('price_analysis', {}),
        'layout_and_space': summary.get('layout_and_space', {}),
        'location_and_transport': summary.get('location_and_transport', {}),
        'photo_insights': summary.get('photo_insights', {}),
        'recommended_for': summary.get('recommended_for', []),
        'confidence_score': summary.get('confidence_score'),
    }


def create_system_prompt():
    return (
        "You are a Hong Kong property matching assistant.\n"
        "Given a subscriber's search preferences and a list of new property listings, "
        "identify the best matching listings (up to 4) for the subscriber.\n\n"
        "Rules:\n"
        "- Match based on user conversation summary.\n"
        "- If no listings match well, return an empty matched_source_ids array.\n"
        "- Output only valid JSON with no extra text.\n\n"
        "Return JSON:\n"
        "{\n"
        '  "matched_source_ids": ["source_id_1", "source_id_2"],\n'
        "}"
    )

def lookup_hk_address(keyword):
    try:
        response = requests.get(
            'https://www.als.gov.hk/lookup',
            params={'q': keyword, 'n': 5},
            headers={
                'Accept': 'application/json',
                'Accept-Language': 'en,zh-Hant',
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
        return int(value)
    except (ValueError, TypeError):
        return None

RADIUS_DEG = 1 / 69
def prematch_by_search_criteria(user, listings):
    sc = user.get('v2LongTermMemory')
    if not sc:
        sc = user.get('userPreferences', {}).get('propertySearchCriteria', {})
    if not sc:
        return []
    sc_district = sc.get('districts')
    districts = []
    if sc_district:
        district_keywords = [d for d in sc.get('districts')]
        for dk in district_keywords:
            lookup_result = lookup_hk_address(dk)
            if lookup_result and 'SuggestedAddress' in lookup_result:
                suggestion = lookup_result['SuggestedAddress'][0] 
                district_info = suggestion.get('Address', {}).get('PremisesAddress', {}).get('GeospatialInformation', {})
                if district_info:
                    districts.append(district_info)
    min_bedrooms = number_or_none(sc.get('minBedrooms'))
    max_bedrooms = number_or_none(sc.get('maxBedrooms'))

    min_price = number_or_none(sc.get('minPrice'))
    max_price = number_or_none(sc.get('maxPrice'))

    min_size = number_or_none(sc.get('minSize'))
    max_size = number_or_none(sc.get('maxSize'))

    min_building_age = number_or_none(sc.get('minBuildingAge'))
    max_building_age = number_or_none(sc.get('maxBuildingAge'))

    with_car_park = sc.get('haveCar', sc.get('withCarPark', False))
    is_village_house = sc.get('likeVillageHouse', sc.get('isVillageHouse', False))
    allow_pets = sc.get('havePets', sc.get('allowPets', False))

    def matches(prop):
        extracted = prop.get('v1_extracted_data', {})
        subdistrict = prop.get('address', {}).get('subdistrict', {})
        latitude = subdistrict.get('latitude')
        longitude = subdistrict.get('longitude')
        if len(districts) > 0:
            if latitude is None or longitude is None:
                return False
            in_district = False
            lat_per_lng = math.cos(math.radians(latitude))
            if lat_per_lng == 0:
              lat_per_lng = 0.0001
            adjusted_lng_radius = RADIUS_DEG / lat_per_lng
            for d in districts:
                d_lat = float(d.get('Latitude'))
                d_lng = float(d.get('Longitude'))
                if d_lat is None or d_lng is None:
                    continue
                lat_diff = abs(latitude - d_lat)
                lng_diff = abs(longitude - d_lng)
                if lat_diff <= RADIUS_DEG and lng_diff <= adjusted_lng_radius:
                    in_district = True
                    break
            if not in_district:
                return False
        bedrooms = number_or_none(extracted.get('number_of_bedrooms'))
        if bedrooms is not None:
            if min_bedrooms and bedrooms <= min_bedrooms:
                return False
            if max_bedrooms and bedrooms >= max_bedrooms + 1:
                return False
        price = number_or_none(extracted.get('rent_price'))
        if price is not None:
            if min_price and price < (min_price * 0.8):
                return False
            if max_price and price > (max_price * 1.1):
                return False
        size = number_or_none(extracted.get('net_size_sqft'))
        if size is not None:
            if min_size and size < (min_size * 0.8):
                return False
            if max_size and size > (max_size * 1.2):
                return False
        building_age = number_or_none(extracted.get('building_age'))
        if building_age is not None:
            if min_building_age and building_age < min_building_age:
                return False
            if max_building_age and building_age > max_building_age:
                return False
        if with_car_park:
            if not extracted.get('with_car_park', False):
                return False
        if is_village_house:
            if not extracted.get('is_village_house', False):
                return False
        if allow_pets:
            if not extracted.get('allow_pets', False):
                return False
        return True

    return [prop for prop in listings if matches(prop)]


def create_match_prompt(conv, user, listings):
    sc = user.get('propertySearchCriteria', {})
    return [
        {'role': 'system', 'content': create_system_prompt()},
        {
            'role': 'user',
            'content': json.dumps(
                {'subscriber_conversations': sanitize_conv(conv), 'new_listings': listings},
                ensure_ascii=False,
            ),
        },
    ]


def batch_subscribers(db):
    skip = 0
    while True:
        users = list(
            db['users']
            .find({
                '_id': ObjectId('6a2b8592bbefe6a9886f5f27'),
                'identifiers': { '$elemMatch': {'type': 'phone'} },
                'userPreferences.disableNotifications': { '$ne': True },
            })
            .skip(skip)
            .limit(user_batch_size)
        )
        if not users:
            break
        yield users
        skip += user_batch_size


def move_file(src, dst):
    try:
        os.rename(src, dst)
        print(f"Moved: {src} → {dst}")
    except Exception as e:
        print(f"Error moving file {src}: {e}")


def main():
    result_files = get_all_result_files(os.path.join(folder, 'results'))
    if result_files:
        for file_path in result_files:
            print(f"Backup previous result file: {file_path}")
            move_file(file_path, os.path.join(folder, 'backup', os.path.basename(file_path)))

    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    db = mongo_client['prop_main']

    props = get_yesterday_props(db)
    if not props:
        print("No new indexed properties found for yesterday.")
        mongo_client.close()
        return

    print(f"Found {len(props)} new properties from yesterday.")
    sorted_listings = sorted(props, key=lambda x: x.get('v1_summary_data', {}).get('confidence_score', 0), reverse=True)
    
    batch_code = gen_batch_code()
    batch_file_path = os.path.join(folder, 'batch_files', f"batch-{batch_code}.jsonl")
    meta_file_path = os.path.join(folder, 'batch_files', f"batch-{batch_code}-meta.json")

    processed_count = 0
    with open(batch_file_path, 'w', encoding='utf-8') as batch_file:
        for user_batch in batch_subscribers(db):
            for user in user_batch:
                user_id = str(user.get('_id'))
                conv = get_langgraph_v2_threads_by_user_id(db, user_id)
                if not conv:
                    conv = get_pending_conversation_by_user_id(db, user_id)
                if not conv:
                    print(f"No pending conversation found for user {user_id}, skipping.")
                    continue
                filtered_listings = prematch_by_search_criteria(user, sorted_listings)
                if not filtered_listings:
                    print(f"No listings match search criteria for user {user_id}, skipping.")
                    continue
                print(f"Creating match prompt for user {user_id} with {len(filtered_listings)} candidate listings.: {[p['source_id'] for p in filtered_listings[:6]]}")
                messages = create_match_prompt(conv, user, [sanitize_prop(p) for p in filtered_listings[:10]])
                row = {
                    'custom_id': f'match-{user_id}',
                    'method': 'POST',
                    'url': '/chat/completions',
                    'body': {
                        'model': 'gpt-4.1-nano',
                        'messages': messages,
                        'max_tokens': 500,
                        'response_format': {'type': 'json_object'},
                    },
                }
                batch_file.write(f"{json.dumps(row, ensure_ascii=False)}\n")
                processed_count += 1

    if processed_count == 0:
        print("No subscribers with phone numbers found.")
        os.remove(batch_file_path)
        mongo_client.close()
        return

    # Save metadata so later scripts know total new prop count and date
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    with open(meta_file_path, 'w', encoding='utf-8') as meta_file:
        meta_file.write(json.dumps({
            'date': yesterday_date,
            'total_new_props': len(props),
        }, ensure_ascii=False))

    print(f"Batch file created: {batch_file_path} ({processed_count} user requests)")
    mongo_client.close()


if __name__ == '__main__':
    main()