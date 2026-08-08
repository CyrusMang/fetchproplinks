import os
import time
import uuid
from datetime import datetime, timedelta

from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

import chatwoot_api_helpers

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID")
CHATWOOT_API_TOKEN = os.getenv("CHATWOOT_API_TOKEN")
CHATWOOT_INBOX_ID = os.getenv("CHATWOOT_INBOX_ID")

# Post-side config
POST_TEMPLATE_NAME = os.getenv("POST_TEMPLATE_NAME", "prop_suggestion")
POST_TEMPLATE_CATEGORY = os.getenv("POST_TEMPLATE_CATEGORY", "UTILITY")
POST_WEB_BASE_URL = os.getenv("POST_WEB_BASE_URL", "https://homeable.house")
# Example: /{lang}/post/{post_id} or /{lang}/{post_id}
POST_WEB_PATH_TEMPLATE = os.getenv("POST_WEB_PATH_TEMPLATE", "/{lang}/posts/{post_id}")

# Property-side config
PROP_TEMPLATE_NAME = os.getenv("PROP_TEMPLATE_NAME", "prop_suggestion")
PROP_TEMPLATE_CATEGORY = os.getenv("PROP_TEMPLATE_CATEGORY", "UTILITY")
PROP_WEB_BASE_URL = os.getenv("PROP_WEB_BASE_URL", "https://homeable.house")
PROP_WEB_PATH_TEMPLATE = os.getenv("PROP_WEB_PATH_TEMPLATE", "/{lang}/{prop_id}")

TEMPLATES = {
    "zh-cn": [POST_TEMPLATE_NAME, POST_TEMPLATE_CATEGORY],
    "zh-hk": [POST_TEMPLATE_NAME, POST_TEMPLATE_CATEGORY],
    "en": [POST_TEMPLATE_NAME, POST_TEMPLATE_CATEGORY],
}

PROP_TEMPLATES = {
    "zh-cn": [PROP_TEMPLATE_NAME, PROP_TEMPLATE_CATEGORY],
    "zh-hk": [PROP_TEMPLATE_NAME, PROP_TEMPLATE_CATEGORY],
    "en": [PROP_TEMPLATE_NAME, PROP_TEMPLATE_CATEGORY],
}


def normalize_lang(lang):
    if not lang:
        return "en"
    normalized = str(lang).strip().lower()
    if normalized in TEMPLATES:
        return normalized
    return "en"


def format_price(price):
    if price is None:
        return "N/A"
    try:
        return f"${int(price):,}"
    except Exception:
        return str(price)


def to_int_or_none(value):
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def post_price(post):
    post_type = post.get("post_type")
    rent_price = to_int_or_none(post.get("rent_price"))
    sell_price = to_int_or_none(post.get("sell_price"))

    if post_type == "rent":
        return rent_price if rent_price is not None else sell_price
    if post_type == "sale":
        return sell_price if sell_price is not None else rent_price
    return rent_price if rent_price is not None else sell_price


def post_size(post):
    net_size = to_int_or_none(post.get("net_size_sqft"))
    if net_size is not None:
        return net_size
    return to_int_or_none(post.get("gross_size_sqft"))


def safe_text(value):
    if value is None:
        return ""
    return str(value).strip()


def compose_post_title(post):
    building = post.get("_estate_building") or {}
    name_obj = building.get("name") or {}

    title = safe_text(post.get("title"))
    if title:
        return title

    name_zh = safe_text(name_obj.get("zh-hk"))
    if name_zh:
        return name_zh

    name_en = safe_text(name_obj.get("en"))
    if name_en:
        return name_en

    return "Post"


def build_post_caption(post, lang):
    title = compose_post_title(post)
    size = post_size(post)
    bedrooms = to_int_or_none(post.get("number_of_bedrooms"))
    bathrooms = to_int_or_none(post.get("number_of_bathrooms"))
    price = post_price(post)

    if lang == "zh-hk":
        post_type_text = "租盤" if post.get("post_type") == "rent" else "售盤"
        details = [f"{post_type_text}: {format_price(price)}"]
        if bedrooms is not None:
            details.append(f"{bedrooms}房")
        if bathrooms is not None:
            details.append(f"{bathrooms}廁")
        if size is not None:
            details.append(f"{size}平方呎")
        return f"{title} - {'，'.join(details)}"

    if lang == "zh-cn":
        post_type_text = "租盘" if post.get("post_type") == "rent" else "售盘"
        details = [f"{post_type_text}: {format_price(price)}"]
        if bedrooms is not None:
            details.append(f"{bedrooms}房")
        if bathrooms is not None:
            details.append(f"{bathrooms}卫")
        if size is not None:
            details.append(f"{size}平方英尺")
        return f"{title} - {'，'.join(details)}"

    post_type_text = "rent" if post.get("post_type") == "rent" else "sale"
    details = [f"{post_type_text.title()}: {format_price(price)}"]
    if bedrooms is not None:
        details.append(f"{bedrooms} bed")
    if bathrooms is not None:
        details.append(f"{bathrooms} bath")
    if size is not None:
        details.append(f"{size} sqft")
    return f"{title} - {', '.join(details)}"


def build_prop_caption(prop, lang):
    extracted = prop.get("v1_extracted_data", {})
    summary = prop.get("v1_summary_data", {})

    lang_key = lang.replace("-", "_")
    title = summary.get(f"headline_{lang_key}") or extracted.get("title") or prop.get("source_id") or "Property"
    price = extracted.get("rent_price")
    if price is None:
        price = extracted.get("sell_price")
    size = extracted.get("net_size_sqft")
    bedrooms = extracted.get("bedrooms")

    if lang == "zh-hk":
        details = [f"租金: {format_price(price)}"]
        if bedrooms not in (None, ""):
            details.append(f"{bedrooms}房")
        if size not in (None, ""):
            details.append(f"{size}平方呎")
        return f"{title} - {'，'.join(details)}"

    if lang == "zh-cn":
        details = [f"租金: {format_price(price)}"]
        if bedrooms not in (None, ""):
            details.append(f"{bedrooms}房")
        if size not in (None, ""):
            details.append(f"{size}平方英尺")
        return f"{title} - {'，'.join(details)}"

    details = [f"Price: {format_price(price)}"]
    if bedrooms not in (None, ""):
        details.append(f"{bedrooms} bed")
    if size not in (None, ""):
        details.append(f"{size} sqft")
    return f"{title} - {', '.join(details)}"


def rendered_message_text(text, lang):
    if lang == "zh-hk":
        msg = "跟據你要求, 新盤搵到有單位可能啱：\n\n"
        msg += f"{text}\n\n"
        msg += "如果想暫停通知隨時話我知"
        return msg

    if lang == "zh-cn":
        msg = "根据您的要求, 新盘中找到了可能适合您的单位：\n\n"
        msg += f"{text}\n\n"
        msg += "如果想暂停通知，请随时告诉我。"
        return msg

    msg = "Based on your requirements, a new listing has been found that might suit you:\n\n"
    msg += f"{text}\n\n"
    msg += "Just let me know anytime if you want to pause these notifications."
    return msg


def build_post_link(lang, post_id):
    base = POST_WEB_BASE_URL.rstrip("/")
    path = POST_WEB_PATH_TEMPLATE.format(lang=lang, post_id=post_id)
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base}{path}"


def build_prop_link(lang, prop_id):
    base = PROP_WEB_BASE_URL.rstrip("/")
    path = PROP_WEB_PATH_TEMPLATE.format(lang=lang, prop_id=prop_id)
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base}{path}"


def first_pending_post_item(conv):
    for item in conv.get("push_posts", []):
        if item.get("status") == "pending" and item.get("post_id"):
            return item
    return None


def first_pending_prop_item(conv):
    for item in conv.get("push_properties", []):
        if item.get("status") == "pending" and item.get("property_id"):
            return item
    return None


def first_pending_item_with_priority(conv):
    pending_post = first_pending_post_item(conv)
    if pending_post:
        return "post", pending_post

    pending_prop = first_pending_prop_item(conv)
    if pending_prop:
        return "property", pending_prop

    return None, None


def find_post_by_post_id(db, post_id):
    if not post_id:
        return None

    post = db["posts"].find_one({"id": post_id})
    if post:
        return post

    try:
        post = db["posts"].find_one({"_id": ObjectId(post_id)})
        if post:
            return post
    except Exception:
        pass

    return None


def find_prop_by_property_id(db, property_id):
    if not property_id:
        return None

    prop = db["props"].find_one({"id": property_id})
    if prop:
        return prop

    prop = db["props"].find_one({"source_id": property_id})
    if prop:
        return prop

    try:
        prop = db["props"].find_one({"_id": ObjectId(property_id)})
        if prop:
            return prop
    except Exception:
        pass

    return None


def enrich_post_with_estate(db, post):
    estate_id = post.get("estate_building_id")
    if not estate_id:
        return post

    estate = db["estate_buildings"].find_one({"_id": estate_id})
    if not estate:
        return post

    return {**post, "_estate_building": estate}


def get_active_convs_with_pending_queue(db):
    three_hours_ago = int((datetime.now() - timedelta(hours=3)).timestamp())
    query = {
        #'threadId': '+85269098658',
        "state": {"$in": ["ACTIVE_TRACKING"]},
        "updatedAt": {"$lte": three_hours_ago},
        "$or": [
            {"push_posts": {"$elemMatch": {"status": "pending"}}},
            {"push_properties": {"$elemMatch": {"status": "pending"}}},
        ],
    }
    return db["conversations-v2"].find(query)


def get_user_by_user_id(db, user_id):
    if not user_id:
        return None
    try:
        user = db["users"].find_one({"_id": user_id})
        if user:
            return user
    except Exception:
        pass
    return None


def conv_save_push_post(db, conv, rendered_message, post_id):
    message = {
        "type": "ai",
        "data": {
            "content": rendered_message,
            "additional_kwargs": {
                "push_prop": True,
                "push_post": True,
                "id": str(uuid.uuid4()),
                "index": conv.get("counter"),
                "createdAt": int(time.time()),
            },
        },
    }

    result = db["conversations-v2"].update_one(
        {
            "_id": conv.get("_id"),
            "counter": conv.get("counter"),
            "push_posts": {
                "$elemMatch": {
                    "post_id": post_id,
                    "status": "pending",
                }
            },
        },
        {
            "$push": {"messages": message},
            "$inc": {"counter": 1},
            "$set": {
                "push_posts.$.status": "sent",
                "updatedAt": int(datetime.now().timestamp()),
            },
        },
    )
    return result.modified_count > 0


def conv_save_push_property(db, conv, rendered_message, property_id):
    message = {
        "type": "ai",
        "data": {
            "content": rendered_message,
            "additional_kwargs": {
                "push_prop": True,
                "id": str(uuid.uuid4()),
                "index": conv.get("counter"),
                "createdAt": int(time.time()),
            },
        },
    }

    result = db["conversations-v2"].update_one(
        {
            "_id": conv.get("_id"),
            "counter": conv.get("counter"),
            "push_properties": {
                "$elemMatch": {
                    "property_id": property_id,
                    "status": "pending",
                }
            },
        },
        {
            "$push": {"messages": message},
            "$inc": {"counter": 1},
            "$set": {
                "push_properties.$.status": "sent",
                "updatedAt": int(datetime.now().timestamp()),
            },
        },
    )
    return result.modified_count > 0


def mark_post_queue_item_skipped(db, conv, post_id):
    result = db["conversations-v2"].update_one(
        {
            "_id": conv.get("_id"),
            "push_posts": {
                "$elemMatch": {
                    "post_id": post_id,
                    "status": "pending",
                }
            },
        },
        {
            "$set": {
                "push_posts.$.status": "skipped",
                "updatedAt": int(datetime.now().timestamp()),
            },
        },
    )
    return result.modified_count > 0


def mark_property_queue_item_skipped(db, conv, property_id):
    result = db["conversations-v2"].update_one(
        {
            "_id": conv.get("_id"),
            "push_properties": {
                "$elemMatch": {
                    "property_id": property_id,
                    "status": "pending",
                }
            },
        },
        {
            "$set": {
                "push_properties.$.status": "skipped",
                "updatedAt": int(datetime.now().timestamp()),
            },
        },
    )
    return result.modified_count > 0


def mark_post_queue_item_expired(db, conv, post_id):
    result = db["conversations-v2"].update_one(
        {
            "_id": conv.get("_id"),
            "push_posts": {
                "$elemMatch": {
                    "post_id": post_id,
                    "status": "pending",
                }
            },
        },
        {
            "$set": {
                "push_posts.$.status": "expired",
                "updatedAt": int(datetime.now().timestamp()),
            },
        },
    )
    return result.modified_count > 0


def mark_property_queue_item_expired(db, conv, property_id):
    result = db["conversations-v2"].update_one(
        {
            "_id": conv.get("_id"),
            "push_properties": {
                "$elemMatch": {
                    "property_id": property_id,
                    "status": "pending",
                }
            },
        },
        {
            "$set": {
                "push_properties.$.status": "expired",
                "updatedAt": int(datetime.now().timestamp()),
            },
        },
    )
    return result.modified_count > 0


def queue_item_is_expired(item, now_ts):
    if not item:
        return False
    expired_at = item.get("expired_at")
    if expired_at is None:
        created_at = item.get("createdAt")
        if created_at is None:
            return False
        try:
            expired_at = int(created_at) + (2 * 24 * 60 * 60)
        except Exception:
            return False

    try:
        return int(expired_at) <= int(now_ts)
    except Exception:
        return False


def is_active_post(post):
    return post.get("status") == "published"


def is_active_property(prop):
    return prop.get("status") != "archived"


def main():
    if not all([CHATWOOT_ACCOUNT_ID, CHATWOOT_API_TOKEN, CHATWOOT_INBOX_ID]):
        print("Missing Chatwoot configuration (CHATWOOT_ACCOUNT_ID / CHATWOOT_API_TOKEN / CHATWOOT_INBOX_ID).")
        return

    if not MONGODB_CONNECTION_STRING:
        print("Missing MONGODB_CONNECTION_STRING.")
        return

    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    db = mongo_client["prop_main"]

    sent = 0
    skipped = 0
    failed = 0

    for conv in get_active_convs_with_pending_queue(db):
        user = get_user_by_user_id(db, conv.get("userId"))
        if not user:
            print(f"User not found for conversation {conv.get('_id')}")
            skipped += 1
            continue

        phone = next(
            (
                i.get("key")
                for i in user.get("identifiers", [])
                if i.get("type") == "phone" and i.get("key") == conv.get("threadId")
            ),
            "",
        )

        if not phone:
            print(f"No phone for user {user.get('_id')}")
            skipped += 1
            continue

        now_ts = int(time.time())

        pending_post_item = first_pending_post_item(conv)
        pending_prop_item = first_pending_prop_item(conv)

        if pending_post_item and queue_item_is_expired(pending_post_item, now_ts):
            queued_post_id = str(pending_post_item.get("post_id"))
            mark_post_queue_item_expired(db, conv, queued_post_id)
            skipped += 1
            print(f"Post queue expired for conversation {conv.get('_id')}, post_id={queued_post_id}")
            pending_post_item = None

        if pending_prop_item and queue_item_is_expired(pending_prop_item, now_ts):
            queued_property_id = str(pending_prop_item.get("property_id"))
            mark_property_queue_item_expired(db, conv, queued_property_id)
            skipped += 1
            print(f"Property queue expired for conversation {conv.get('_id')}, property_id={queued_property_id}")
            pending_prop_item = None

        if pending_post_item:
            queue_type = "post"
            pending_item = pending_post_item
        elif pending_prop_item:
            queue_type = "property"
            pending_item = pending_prop_item
        else:
            skipped += 1
            continue

        lang = normalize_lang(conv.get("language", "zh-hk"))

        contact = chatwoot_api_helpers.get_or_create_contact(phone)
        if not contact:
            print(f"Failed to get or create contact for {user.get('_id')}")
            failed += 1
            continue

        contact_id = contact.get("id")
        if not contact_id:
            print(f"Contact found but missing ID for {user.get('_id')}")
            failed += 1
            continue

        if queue_type == "post":
            queued_post_id = str(pending_item.get("post_id"))
            post = find_post_by_post_id(db, queued_post_id)
            if not post:
                print(f"Post not found for user {user.get('_id')}, post_id={queued_post_id}")
                failed += 1
                continue

            if not is_active_post(post):
                mark_post_queue_item_skipped(db, conv, queued_post_id)
                skipped += 1
                print(f"Post not active for user {user.get('_id')}, post_id={queued_post_id}, status={post.get('status')}")
                continue

            post = enrich_post_with_estate(db, post)
            canonical_post_id = post.get("id") or str(post.get("_id") or "")
            if not canonical_post_id:
                print(f"Post missing usable id for user {user.get('_id')}, post_id={queued_post_id}")
                failed += 1
                continue

            template_name, template_category = TEMPLATES.get(lang, TEMPLATES["zh-hk"])
            caption = build_post_caption(post, lang)
            link = build_post_link(lang, canonical_post_id)
            template_params = {
                "caption": caption,
                "link": link,
            }
            rendered_message = rendered_message_text(f"{caption}\n{link}", lang)

            result = conv_save_push_post(db, conv, rendered_message, queued_post_id)
            if not result:
                print(f"Failed to save push message for user {user.get('_id')}, post_id={queued_post_id}")
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
            print(f"user: {user.get('_id')}, lang: {lang}, sent_post_id: {queued_post_id}")
            continue

        queued_property_id = str(pending_item.get("property_id"))
        prop = find_prop_by_property_id(db, queued_property_id)
        if not prop:
            print(f"Property not found for user {user.get('_id')}, property_id={queued_property_id}")
            failed += 1
            continue

        if not is_active_property(prop):
            mark_property_queue_item_skipped(db, conv, queued_property_id)
            skipped += 1
            print(
                f"Property not active for user {user.get('_id')}, "
                f"property_id={queued_property_id}, status={prop.get('status')}"
            )
            continue

        canonical_prop_id = prop.get("short_id") or prop.get("id")
        if not canonical_prop_id:
            print(f"Property missing usable id for user {user.get('_id')}, property_id={queued_property_id}")
            failed += 1
            continue

        prop_template_name, prop_template_category = PROP_TEMPLATES.get(lang, PROP_TEMPLATES["zh-hk"])
        prop_caption = build_prop_caption(prop, lang)
        prop_link = build_prop_link(lang, canonical_prop_id)
        prop_template_params = {
            "caption": prop_caption,
            "link": prop_link,
        }
        prop_rendered_message = rendered_message_text(f"{prop_caption}\n{prop_link}", lang)

        prop_result = conv_save_push_property(db, conv, prop_rendered_message, queued_property_id)
        if not prop_result:
            print(f"Failed to save push message for user {user.get('_id')}, property_id={queued_property_id}")
            failed += 1
            continue

        prop_success = chatwoot_api_helpers.send_whatsapp_template(
            contact_id,
            lang,
            prop_template_name,
            prop_template_category,
            prop_template_params,
            prop_rendered_message,
        )

        if not prop_success:
            failed += 1
            continue

        sent += 1
        print(f"user: {user.get('_id')}, lang: {lang}, sent_property_id: {queued_property_id}")

    print(f"Done: sent={sent}, skipped={skipped}, failed={failed}")
    mongo_client.close()


if __name__ == "__main__":
    main()
