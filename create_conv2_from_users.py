import os
import time
from typing import Any

from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
DB_NAME = "prop_main"

THREAD_COLLECTION = "conversations-v2"
USER_COLLECTION = "users"

ACTIVE_TRACKING = "ACTIVE_TRACKING"
MUTED = "MUTED"
SUPPORTED_STATES = {ACTIVE_TRACKING, MUTED}


def get_phone_identifier(user: dict[str, Any]) -> str | None:
	identifiers = user.get("identifiers", [])
	for identifier in identifiers:
		if identifier.get("type") == "phone" and identifier.get("key"):
			return str(identifier["key"])
	return None


def normalize_language(user: dict[str, Any]) -> str:
	language = user.get("v2Language") or user.get("userPreferences", {}).get("language")
	if not language:
		return "zh-hk"
	return str(language).strip().lower()


def derive_state(user: dict[str, Any]) -> str:
	v2_state = user.get("v2State")
	if v2_state in SUPPORTED_STATES:
		return str(v2_state)
	if v2_state:
		return MUTED

	if user.get("userPreferences", {}).get("disableNotifications") is True:
		return MUTED

	if user.get("whatsappAIDisabled") is True:
		return MUTED

	return ACTIVE_TRACKING


def derive_user_preferences(user: dict[str, Any]) -> dict[str, Any] | None:
	if user.get("v2LongTermMemory"):
		return user.get("v2LongTermMemory")

	old_criteria = user.get("userPreferences", {}).get("propertySearchCriteria")
	if old_criteria:
		return old_criteria

	return None


def build_conversation_doc(user: dict[str, Any], thread_id: str, now: int) -> dict[str, Any]:
	doc: dict[str, Any] = {
		"threadId": thread_id,
		"preArchiveMessages": [],
		"messages": [],
		"counter": 0,
		"userId": user["_id"],
		"language": normalize_language(user),
		"state": derive_state(user),
		"routerIntent": ["CHAT"],
		"createdAt": int(user.get("createdAt") or now),
	}

	user_preferences = derive_user_preferences(user)
	if user_preferences is not None:
		doc["userPreferences"] = user_preferences

	return doc


def main() -> None:
	if not MONGODB_CONNECTION_STRING:
		print("Missing MONGODB_CONNECTION_STRING.")
		return

	mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
	db = mongo_client[DB_NAME]

	users_cursor = db[USER_COLLECTION].find({"status": {"$ne": "deleted"}})
	conv_collection = db[THREAD_COLLECTION]

	processed = 0
	created = 0
	already_exists = 0
	skipped_no_phone = 0
	skipped_thread_conflict = 0

	now = int(time.time())

	for user in users_cursor:
		processed += 1
		thread_id = get_phone_identifier(user)

		if not thread_id:
			skipped_no_phone += 1
			continue

		existing_by_user = conv_collection.find_one({"userId": user["_id"]}, {"_id": 1})
		if existing_by_user:
			already_exists += 1
			continue

		existing_by_thread = conv_collection.find_one({"threadId": thread_id}, {"_id": 1})
		if existing_by_thread:
			skipped_thread_conflict += 1
			continue

		doc = build_conversation_doc(user, thread_id, now)
		doc["updatedAt"] = now
		conv_collection.insert_one(doc)
		created += 1

	print(
		f"Done. processed={processed}, created={created}, "
		f"already_exists={already_exists}, skipped_no_phone={skipped_no_phone}, "
		f"skipped_thread_conflict={skipped_thread_conflict}"
	)

	mongo_client.close()


if __name__ == "__main__":
	main()
