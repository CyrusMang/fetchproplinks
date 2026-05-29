import os
import uuid
import json
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
from bson.objectid import ObjectId

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
    }
    print(f"Querying properties with filter: {f}")
    return list(db['props'].find(f))


def get_conversation_by_user_id(db, user_id):
    conv = db['conversations'].find_one({ 'user_id': user_id })
    return conv.get('conversation_id') if conv else None


def sanitize_conv(conv):
    meaningful_messages = []
    messages = conv.get('messages', [])
    for m in messages:
        if m.get('type') in ['human', 'system']:
            meaningful_messages.append(m)
        elif m.get('type') == 'ai' and m.get('content', '') != '':
            meaningful_messages.append(m)
    return {
        'old_conversation_summary': conv.get('summary'),
        'recent_messages': meaningful_messages
    }


def sanitize_prop(prop):
    extracted = prop.get('v1_summary_data', {})
    return {
        'source_id': prop.get('source_id'),
        'headline_en': extracted.get('headline_en'),
        'executive_summary_en': extracted.get('executive_summary_en'),
        'key_highlights': extracted.get('key_highlights', []),
        'possible_concerns': extracted.get('possible_concerns', []),
        'price_analysis': extracted.get('price_analysis', {}),
        'layout_and_space': extracted.get('layout_and_space', {}),
        'location_and_transport': extracted.get('location_and_transport', {}),
        'photo_insights': extracted.get('photo_insights', {}),
        'recommended_for': extracted.get('recommended_for', []),
        'confidence_score': extracted.get('confidence_score'),
    }


def create_system_prompt():
    return (
        "You are a Hong Kong property matching assistant.\n"
        "Given a subscriber's search preferences and a list of new property listings, "
        "identify the best matching listings (up to 5) for the subscriber.\n\n"
        "Rules:\n"
        "- Match based on user conversation summary.\n"
        "- If no listings match well, return an empty matched_source_ids array.\n"
        "- Output only valid JSON with no extra text.\n\n"
        "Return JSON:\n"
        "{\n"
        '  "matched_source_ids": ["source_id_1", "source_id_2"],\n'
        '  "match_count": 0,\n'
        '  "reason": "brief reason"\n'
        "}"
    )


def prematch_by_search_criteria(user, listings):
    sc = user.get('propertySearchCriteria', {})
    query_text = sc.get('queryText', '').lower()
    district = sc.get('district', '').lower()
    bedrooms = sc.get('bedrooms')
    min_price = sc.get('minPrice')
    max_price = sc.get('maxPrice')

    def matches(prop):
        if district and district not in prop.get('district', '').lower():
            return False
        if bedrooms and prop.get('number_of_bedrooms') != bedrooms:
            return False
        price = prop.get('rent_price') or prop.get('sell_price')
        if price is not None:
            if min_price and price < min_price:
                return False
            if max_price and price > max_price:
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
            .find({'identifiers': { '$elemMatch': {'type': 'phone'} }})
            .skip(skip)
            .limit(user_batch_size)
        )
        if not users:
            break
        yield users
        skip += user_batch_size


def main():
    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    db = mongo_client['prop_main']

    props = get_yesterday_props(db)
    if not props:
        print("No new indexed properties found for yesterday.")
        mongo_client.close()
        return

    print(f"Found {len(props)} new properties from yesterday.")
    listings = [sanitize_prop(p) for p in props]
    sorted_listings = sorted(listings, key=lambda x: x.get('confidence_score', 0), reverse=True)
    
    batch_code = gen_batch_code()
    batch_file_path = os.path.join(folder, 'batch_files', f"batch-{batch_code}.jsonl")
    meta_file_path = os.path.join(folder, 'batch_files', f"batch-{batch_code}-meta.json")

    processed_count = 0
    with open(batch_file_path, 'w', encoding='utf-8') as batch_file:
        for user_batch in batch_subscribers(db):
            for user in user_batch:
                user_id = str(user.get('_id'))
                filtered_listings = prematch_by_search_criteria(user, sorted_listings)
                if not filtered_listings:
                    print(f"No listings match search criteria for user {user_id}, skipping.")
                    continue
                conv = get_conversation_by_user_id(db, user_id)
                if conv:
                    messages = create_match_prompt(conv, user, filtered_listings[:20])
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
                else:
                    phone = next((id.get('key') for id in user.get('identifiers', []) if id.get('type') == 'phone'), '')
                    lang = user.get('userPreferences', {}).get('preferredLanguage', 'en')
                    success = send_prop_matched_wtsapp_msg.send('rent', phone, lang, len(props), filtered_listings)
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