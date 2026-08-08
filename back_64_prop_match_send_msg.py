import os
import time
import uuid
from datetime import datetime, timedelta
from bson import ObjectId
from pymongo import MongoClient
from dotenv import load_dotenv
from models.conversation import Conversation
from models.langgraph_thread import LanggraphThread
import chatwoot_api_helpers

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID")
CHATWOOT_API_TOKEN = os.getenv("CHATWOOT_API_TOKEN")
CHATWOOT_INBOX_ID = os.getenv("CHATWOOT_INBOX_ID")

TOKEN_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
DEFAULT_TOKEN_LENGTH = 7
MAX_INSERT_ATTEMPTS = 20

templates = {
    'zh-cn': ["prop_suggestion", 'UTILITY'],
    'zh-hk': ["prop_suggestion", 'UTILITY'],
    'en': ["prop_suggestion", 'UTILITY'],
}

def random_token(length: int) -> str:
    return "".join(random.choice(TOKEN_ALPHABET) for _ in range(length))

def insert_token_doc(
    collection: Collection,
    conversation_object_id: ObjectId,
    property_id: str,
    token_length: int,
) -> str:
    for _ in range(MAX_INSERT_ATTEMPTS):
        token = random_token(token_length)
        try:
            collection.insert_one(
                {
                    "token": token,
                    "conversationId": conversation_object_id,
                    "propertyId": property_id,
                    "clickCount": 0,
                    "createdAt": int(time.time()),
                }
            )
            return token
        except DuplicateKeyError:
            continue
    raise RuntimeError(f"Failed to generate unique token for propertyId={property_id}")


def normalize_lang(lang):
    if not lang:
        return 'en'
    normalized = str(lang).strip().lower()
    if normalized in templates:
        return normalized
    return 'en'


def format_price(price):
    if price is None:
        return "N/A"
    try:
        return f"${int(price):,}"
    except Exception:
        return str(price)


def build_caption(prop, lang):
    extracted = prop.get('v1_extracted_data', {})
    summary = prop.get('v1_summary_data', {})

    lang_key = lang.replace('-', '_')
    title = summary.get(f'headline_{lang_key}') or extracted.get('title') or prop.get('source_id') or "Property"
    price = extracted.get('rent_price')
    if price is None:
        price = extracted.get('sell_price')
    size = extracted.get('net_size_sqft')
    bedrooms = extracted.get('bedrooms')

    if lang == 'zh-hk':
        details = [f"租金: {format_price(price)}"]
        if bedrooms not in (None, ''):
            details.append(f"{bedrooms}房")
        if size not in (None, ''):
            details.append(f"{size}平方呎")
        return f"{title} - {'，'.join(details)}"

    if lang == 'zh-cn':
        details = [f"租金: {format_price(price)}"]
        if bedrooms not in (None, ''):
            details.append(f"{bedrooms}房")
        if size not in (None, ''):
            details.append(f"{size}平方英尺")
        return f"{title} - {'，'.join(details)}"

    details = [f"Price: {format_price(price)}"]
    if bedrooms not in (None, ''):
        details.append(f"{bedrooms} bed")
    if size not in (None, ''):
        details.append(f"{size} sqft")
    return f"{title} - {', '.join(details)}"


def first_pending_push_item(conv):
    for item in conv.get('push_properties', []):
        if item.get('status') == 'pending' and item.get('property_id'):
            return item
    return None


def find_prop_by_property_id(db, property_id):
    if not property_id:
        return None

    prop = db['props'].find_one({'id': property_id})
    if prop:
        return prop

    prop = db['props'].find_one({'source_id': property_id})
    if prop:
        return prop

    try:
        prop = db['props'].find_one({'_id': ObjectId(property_id)})
        if prop:
            return prop
    except Exception:
        pass

    return None


def rendered_message_text(text, lang):
    if lang == 'zh-hk':
        msg = f"跟據你要求, 新盤搵到有單位可能啱：\n\n"
        msg += f"{text}\n\n"
        msg += "如果想暫停通知隨時話我知"
        return msg
    elif lang == 'zh-cn':
        msg = f"根据您的要求, 新盘中找到了可能适合您的单位：\n\n"
        msg += f"{text}\n\n"
        msg += "如果想暂停通知，请随时告诉我。"
        return msg
    else:
        msg = f"Based on your requirements, a new listing has been found that might suit you:\n\n"
        msg += f"{text}\n\n"
        msg += "Just let me know anytime if you want to pause these notifications."
        return msg

def get_active_convs_with_pending_queue(db):
    three_hours_ago = int((datetime.now() - timedelta(hours=3)).timestamp())
    query = {
        #'threadId': '+85269098658',
        'state': {'$in': ["ACTIVE_TRACKING"]},
        'updatedAt': {'$lte': three_hours_ago},
        'push_properties': {'$elemMatch': {'status': 'pending'}},
    }
    return db['conversations-v2'].find(query)

def get_user_by_user_id(db, user_id):
    if not user_id:
        return None
    try:
        user = db['users'].find_one({'_id': user_id})
        if user:
            return user
    except Exception:
        pass

    return None

def conv_save_push(db, conv, rendered_message, property_id):
    message = {
        'type': 'ai',
        'data': {
            "content": rendered_message,
            "additional_kwargs": {
                'push_prop': True,
                'id': str(uuid.uuid4()),
                'index': conv.get('counter'),
                'createdAt': int(time.time()),
            }
        }
    }
    result = db['conversations-v2'].update_one(
        {
            '_id': conv.get('_id'),
            'counter': conv.get('counter'),
            'push_properties': {
                '$elemMatch': {
                    'property_id': property_id,
                    'status': 'pending',
                }
            },
        },
        {
            '$push': {'messages': message},
            '$inc': {'counter': 1},
            '$set': {
                'push_properties.$.status': 'sent',
                'updatedAt': int(datetime.now().timestamp()),
            },
        },
    )
    return result.modified_count > 0


def main():
    if not all([CHATWOOT_ACCOUNT_ID, CHATWOOT_API_TOKEN, CHATWOOT_INBOX_ID]):
        print("Missing Chatwoot configuration (CHATWOOT_ACCOUNT_ID / CHATWOOT_API_TOKEN / CHATWOOT_INBOX_ID).")
        return
    if not MONGODB_CONNECTION_STRING:
        print("Missing MONGODB_CONNECTION_STRING.")
        return

    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    db = mongo_client['prop_main']

    sent = 0
    skipped = 0
    failed = 0

    for conv in get_active_convs_with_pending_queue(db):
        user = get_user_by_user_id(db, conv['userId'])
        if not user:
            print(f"User not found for conversation {conv.get('_id')}")
            skipped += 1
            continue

        phone = next((i.get('key') for i in user.get('identifiers', []) if i.get('type') == 'phone' and i.get('key') == conv.get('threadId')), '')
        if not phone:
            print(f"No phone for user {user.get('_id')}")
            skipped += 1
            continue

        pending_item = first_pending_push_item(conv)
        if not pending_item:
            skipped += 1
            continue

        property_id = pending_item.get('property_id')
        prop = find_prop_by_property_id(db, property_id)
        if not prop:
            print(f"Property not found for user {user.get('_id')}, property_id={property_id}")
            failed += 1
            continue

        lang = normalize_lang(conv.get('language', 'zh-hk'))
        template_name, template_category = templates.get(lang, templates['zh-hk'])

        contact = chatwoot_api_helpers.get_or_create_contact(phone)
        if not contact:
            print(f"Failed to get or create contact for {user.get('_id')}")
            failed += 1
            continue

        contact_id = contact.get('id')
        if not contact_id:
            print(f"Contact found but missing ID for {user.get('_id')}")
            failed += 1
            continue

        prop_id = prop.get('short_id') or prop.get('id')
        if not prop_id:
            print(f"Property missing id for user {user.get('_id')}, property_id={property_id}")
            failed += 1
            continue

        caption = build_caption(prop, lang)

        collection = db["property_link_tokens"]
        token = insert_token_doc(
            collection=collection,
            conversation_object_id=conv.get('_id'),
            property_id=prop_id,
            token_length=5,
        )

        site_url = f"https://homeable.house/r/{token}"

        template_params = {
            'caption': caption,
            'link': site_url,
        }
        rendered_message = rendered_message_text(f"{caption}\n{site_url}", lang)

        result = conv_save_push(db, conv, rendered_message, property_id)
        if not result:
            print(f"Failed to save push message for user {user.get('_id')}, property_id={property_id}")
            failed += 1
            continue

        success = chatwoot_api_helpers.send_whatsapp_template(
            contact_id,
            lang,
            template_name,
            template_category,
            template_params,
            rendered_message,
        )

        if not success:
            failed += 1
            continue

        sent += 1
        print(f"user: {user.get('_id')}, lang: {lang}, sent_property_id: {property_id}")

    print(f"Done: sent={sent}, skipped={skipped}, failed={failed}")
    mongo_client.close()


if __name__ == '__main__':
    main()
