import os
from datetime import datetime
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

templates = {
    'zh-cn': ["prop_suggestion", 'UTILITY'],
    'zh-hk': ["prop_suggestion", 'UTILITY'],
    'en': ["prop_suggestion", 'UTILITY'],
}


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


def first_pending_push_item(user):
    for item in user.get('push_properties', []):
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


def get_active_users_with_pending_queue(db):
    query = {
        #'_id': ObjectId('6a2b8592bbefe6a9886f5f27'),
        'identifiers': {'$elemMatch': {'type': 'phone'}},
        'userPreferences.disableNotifications': {'$ne': True},
        'v2State': {'$nin': ['MUTED', 'OFFBOARDED']},
        'push_properties': {'$elemMatch': {'status': 'pending'}},
    }
    return db['users'].find(query)


def mark_push_item_sent(db, user_id, property_id):
    result = db['users'].update_one(
        {
            '_id': user_id,
            'push_properties': {
                '$elemMatch': {
                    'property_id': property_id,
                    'status': 'pending',
                }
            },
        },
        {
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

    for user in get_active_users_with_pending_queue(db):
        user_id = user.get('_id')
        user_id_str = str(user_id)

        phone = next((i.get('key') for i in user.get('identifiers', []) if i.get('type') == 'phone'), '')
        if not phone:
            print(f"No phone for user {user_id_str}")
            skipped += 1
            continue

        pending_item = first_pending_push_item(user)
        if not pending_item:
            skipped += 1
            continue

        property_id = pending_item.get('property_id')
        prop = find_prop_by_property_id(db, property_id)
        if not prop:
            print(f"Property not found for user {user_id_str}, property_id={property_id}")
            failed += 1
            continue

        conv = LanggraphThread.get_by_user_id(db, user_id_str)
        is_v2_thread = True
        if not conv:
            conv = Conversation.get_by_user_id(db, user_id)
            is_v2_thread = False
        if not conv:
            print(f"No conversation found for user {user_id_str}")
            skipped += 1
            continue

        lang = normalize_lang(user.get('v2Language', user.get('userPreferences', {}).get('language', 'zh-hk')))
        template_name, template_category = templates.get(lang, templates['zh-hk'])

        contact = chatwoot_api_helpers.get_or_create_contact(phone)
        if not contact:
            print(f"Failed to get or create contact for {user_id_str}")
            failed += 1
            continue

        contact_id = contact.get('id')
        if not contact_id:
            print(f"Contact found but missing ID for {user_id_str}")
            failed += 1
            continue

        prop_id = prop.get('id')
        if not prop_id:
            print(f"Property missing id for user {user_id_str}, property_id={property_id}")
            failed += 1
            continue

        caption = build_caption(prop, lang)
        link = f"https://homeable.house/{lang}/{prop_id}"
        template_params = {
            'caption': caption,
            'link': link,
        }
        rendered_message = rendered_message_text(f"{caption}\n{link}", lang)

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

        updated = mark_push_item_sent(db, user_id, property_id)
        if not updated:
            print(f"Sent but failed to mark sent for user {user_id_str}, property_id={property_id}")

        try:
            if is_v2_thread:
                conv.add_message({'type': 'ai', 'data': {'content': rendered_message}})
            else:
                conv.add_message({'type': 'ai', 'content': rendered_message})
            conv.conversation_summary()
            if not is_v2_thread:
                conv.archive_messages()
        except Exception as e:
            print(f"Failed to add conversation message for user {user_id_str}: {e}")

        sent += 1
        print(f"user: {user_id_str}, lang: {lang}, sent_property_id: {property_id}")

    print(f"Done: sent={sent}, skipped={skipped}, failed={failed}")
    mongo_client.close()


if __name__ == '__main__':
    main()
