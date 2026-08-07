import os
import uuid
import json
import math
from datetime import datetime, timedelta

import requests
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")
POST_MATCH_USE_LLM = os.getenv("POST_MATCH_USE_LLM", "false").lower() == "true"

dir = os.path.dirname(os.path.abspath(__file__))
artifacts = os.path.join(dir, ARTIFACTS_FOLDER)
folder = os.path.join(artifacts, "post_match")

os.makedirs(os.path.join(folder, "batch_files"), exist_ok=True)
os.makedirs(os.path.join(folder, "upload_batches"), exist_ok=True)
os.makedirs(os.path.join(folder, "results"), exist_ok=True)
os.makedirs(os.path.join(folder, "data"), exist_ok=True)
os.makedirs(os.path.join(folder, "backup"), exist_ok=True)

RADIUS_DEG = 1 / 69


def gen_batch_code():
	return str(uuid.uuid4())


def get_yesterday_timestamps():
	today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
	yesterday = today - timedelta(days=1)
	return int(yesterday.timestamp()), int(today.timestamp())


def get_yesterday_posts(db):
	start_ts, end_ts = get_yesterday_timestamps()
	f = {
		"status": "published",
		"createdAt": {"$gte": start_ts, "$lt": end_ts},
		"estate_building_id": {"$exists": True, "$ne": None},
	}
	print(f"Querying posts with filter: {f}")
	return list(db["posts"].find(f))


def get_all_result_files(folder_path):
	files = []
	for filename in os.listdir(folder_path):
		if filename.endswith("-result.json"):
			files.append(os.path.join(folder_path, filename))
	return files


def is_push_true_for_last_10_messages(conv):
	messages = conv.get("messages", [])
	if len(messages) < 10:
		return False
	for m in messages[-10:]:
		if m.get("type") != "ai" or m.get("data", {}).get("additional_kwargs", {}).get("push_prop", False) is False:
			return False
	return True


def active_conversation(db):
	query = {
    #'threadId': '+85269098658',
		"state": {"$in": ["ACTIVE_TRACKING"]},
	}
	return db["conversations-v2"].find(query)


def move_file(src, dst):
	try:
		os.rename(src, dst)
		print(f"Moved: {src} -> {dst}")
	except Exception as e:
		print(f"Error moving file {src}: {e}")


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
		return int(value)
	except (ValueError, TypeError):
		return None


def normalize_text(value):
	if value is None:
		return ""
	return str(value).strip().lower()


def bool_pref(value):
	if isinstance(value, bool):
		return value
	if isinstance(value, str):
		return value.strip().lower() in ["1", "true", "yes"]
	return False


def extract_listing_price(post):
	post_type = post.get("post_type")
	rent_price = number_or_none(post.get("rent_price"))
	sell_price = number_or_none(post.get("sell_price"))
	if post_type == "rent":
		return rent_price if rent_price is not None else sell_price
	if post_type == "sale":
		return sell_price if sell_price is not None else rent_price
	return rent_price if rent_price is not None else sell_price


def extract_listing_size(post):
	net_size = number_or_none(post.get("net_size_sqft"))
	if net_size is not None:
		return net_size
	return number_or_none(post.get("gross_size_sqft"))


def extract_building_age(post):
	building = post.get("_estate_building") or {}
	built_in = number_or_none(building.get("builtIn"))
	if built_in is None:
		return None
	current_year = datetime.now().year
	if built_in <= 1800 or built_in > current_year:
		return None
	return current_year - built_in


def district_text_tokens(post):
	building = post.get("_estate_building") or {}
	district = building.get("address", {}).get("district", {})
	return [
		normalize_text(district.get("en")),
		normalize_text(district.get("zh-hk")),
	]


def geo_location(post):
	building = post.get("_estate_building") or {}
	geo = building.get("address", {}).get("geoLocation", {})
	lat = geo.get("latitude")
	lng = geo.get("longitude")
	if lat is None or lng is None:
		return None, None
	try:
		return float(lat), float(lng)
	except Exception:
		return None, None


def features_set(post):
	features = post.get("features") or []
	normalized = set()
	for f in features:
		token = normalize_text(f)
		if token:
			normalized.add(token)
	return normalized


def prematch_by_search_criteria(conv, listings):
	sc = conv.get("userPreferences", {})
	if not sc:
		return []

	district_keywords = [d for d in (sc.get("districts") or []) if d]
	districts_geo = []
	normalized_district_keywords = [normalize_text(d) for d in district_keywords]

	for dk in district_keywords:
		lookup_result = lookup_hk_address(dk)
		if lookup_result and "SuggestedAddress" in lookup_result and lookup_result["SuggestedAddress"]:
			suggestion = lookup_result["SuggestedAddress"][0]
			district_info = suggestion.get("Address", {}).get("PremisesAddress", {}).get("GeospatialInformation", {})
			if district_info:
				districts_geo.append(district_info)

	min_bedrooms = number_or_none(sc.get("minBedrooms"))
	max_bedrooms = number_or_none(sc.get("maxBedrooms"))
	min_price = number_or_none(sc.get("minPrice"))
	max_price = number_or_none(sc.get("maxPrice"))
	min_size = number_or_none(sc.get("minSize"))
	max_size = number_or_none(sc.get("maxSize"))
	min_building_age = number_or_none(sc.get("minBuildingAge"))
	max_building_age = number_or_none(sc.get("maxBuildingAge"))

	with_car_park = bool_pref(sc.get("haveCar", False))
	is_village_house = bool_pref(sc.get("likeVillageHouse", False))
	allow_pets = bool_pref(sc.get("havePets", False))

	def matches(post):
		latitude, longitude = geo_location(post)
		district_tokens = district_text_tokens(post)

		if districts_geo:
			in_district = False
			if latitude is not None and longitude is not None:
				lat_per_lng = math.cos(math.radians(latitude))
				if lat_per_lng == 0:
					lat_per_lng = 0.0001
				adjusted_lng_radius = RADIUS_DEG / lat_per_lng

				for d in districts_geo:
					try:
						d_lat = float(d.get("Latitude"))
						d_lng = float(d.get("Longitude"))
					except Exception:
						continue

					lat_diff = abs(latitude - d_lat)
					lng_diff = abs(longitude - d_lng)
					if lat_diff <= RADIUS_DEG and lng_diff <= adjusted_lng_radius:
						in_district = True
						break

			if not in_district and normalized_district_keywords:
				for keyword in normalized_district_keywords:
					if not keyword:
						continue
					if any(keyword in token for token in district_tokens if token):
						in_district = True
						break

			if not in_district:
				return False

		bedrooms = number_or_none(post.get("number_of_bedrooms"))
		if bedrooms is not None:
			if min_bedrooms is not None and bedrooms < min_bedrooms:
				return False
			if max_bedrooms is not None and bedrooms > max_bedrooms:
				return False

		price = extract_listing_price(post)
    if price is None:
      return False
    if min_price is not None and price < (min_price * 0.8):
      return False
    if max_price is not None and price > (max_price * 1.1):
      return False

		size = extract_listing_size(post)
		if size is not None:
			if min_size is not None and size < (min_size * 0.8):
				return False
			if max_size is not None and size > (max_size * 1.2):
				return False

		building_age = extract_building_age(post)
		if building_age is not None:
			if min_building_age is not None and building_age < min_building_age:
				return False
			if max_building_age is not None and building_age > max_building_age:
				return False

		feature_tokens = features_set(post)

		if with_car_park and feature_tokens:
			has_car = any(k in " ".join(feature_tokens) for k in ["car", "parking", "carpark", "car park", "車位"])
			if not has_car:
				return False

		if allow_pets and feature_tokens:
			has_pets = any(k in " ".join(feature_tokens) for k in ["pet", "pets", "寵物"])
			if not has_pets:
				return False

		if is_village_house:
			building = post.get("_estate_building") or {}
			name_zh = normalize_text((building.get("name") or {}).get("zh-hk"))
			name_en = normalize_text((building.get("name") or {}).get("en"))
			inferred_village = "村屋" in name_zh or "village" in name_en
			if feature_tokens:
				inferred_village = inferred_village or any(
					k in " ".join(feature_tokens) for k in ["village", "村屋"]
				)
			if feature_tokens or name_zh or name_en:
				if not inferred_village:
					return False

		return True

	return [post for post in listings if matches(post)]


def scoring_midpoint(min_value, max_value):
	if min_value is None and max_value is None:
		return None
	if min_value is None:
		return max_value
	if max_value is None:
		return min_value
	return (min_value + max_value) / 2


def closeness_score(value, min_value, max_value, weight, tolerance=0.25):
	if value is None:
		return 0.0
	midpoint = scoring_midpoint(min_value, max_value)
	if midpoint is None or midpoint == 0:
		return 0.0
	distance_ratio = abs(value - midpoint) / midpoint
	scaled = max(0.0, 1.0 - (distance_ratio / tolerance))
	return scaled * weight


def score_post_against_preferences(conv, post):
	sc = conv.get("userPreferences", {})

	min_price = number_or_none(sc.get("minPrice"))
	max_price = number_or_none(sc.get("maxPrice"))
	min_size = number_or_none(sc.get("minSize"))
	max_size = number_or_none(sc.get("maxSize"))
	min_bedrooms = number_or_none(sc.get("minBedrooms"))
	max_bedrooms = number_or_none(sc.get("maxBedrooms"))

	score = 0.0
	score += closeness_score(extract_listing_price(post), min_price, max_price, 45.0)
	score += closeness_score(extract_listing_size(post), min_size, max_size, 25.0)

	bedrooms = number_or_none(post.get("number_of_bedrooms"))
	score += closeness_score(bedrooms, min_bedrooms, max_bedrooms, 20.0, tolerance=1.0)

	updated_at = number_or_none(post.get("updatedAt")) or number_or_none(post.get("createdAt"))
	if updated_at:
		age_seconds = max(0, int(datetime.now().timestamp()) - updated_at)
		# Up to 10 recency points. Fully decays after 7 days.
		recency = max(0.0, 1.0 - (age_seconds / (7 * 24 * 60 * 60))) * 10.0
		score += recency

	return round(score, 2)


def rank_posts(conv, posts):
	with_score = []
	for post in posts:
		with_score.append((score_post_against_preferences(conv, post), post))
	with_score.sort(key=lambda x: x[0], reverse=True)
	return [post for _, post in with_score]


def sanitize_conv(conv):
	meaningful_messages = []
	messages = conv.get("messages", [])
	for m in messages:
		if m.get("type") in ["human", "system"]:
			meaningful_messages.append(
				{
					"type": m.get("type"),
					"content": m.get("data", {}).get("content", m.get("content", "")),
				}
			)
		elif m.get("type") == "ai" and m.get("data", {}).get("content", m.get("content", "")) != "":
			meaningful_messages.append(
				{
					"type": m.get("type"),
					"content": m.get("data", {}).get("content", m.get("content", "")),
				}
			)

	return {
		"old_conversation_summary": conv.get("summary"),
		"recent_messages": meaningful_messages,
		"user_preferences": conv.get("userPreferences", {}),
	}


def sanitize_post(post):
	building = post.get("_estate_building") or {}
	district = building.get("address", {}).get("district", {})

	return {
		"post_id": str(post.get("_id")),
		"post_type": post.get("post_type"),
		"title": post.get("title"),
		"description": post.get("description"),
		"floor": post.get("floor"),
		"flat": post.get("flat"),
		"floor_group": post.get("floor_group"),
		"features": post.get("features", []),
		"rent_price": post.get("rent_price"),
		"sell_price": post.get("sell_price"),
		"net_size_sqft": post.get("net_size_sqft"),
		"gross_size_sqft": post.get("gross_size_sqft"),
		"number_of_bedrooms": post.get("number_of_bedrooms"),
		"number_of_bathrooms": post.get("number_of_bathrooms"),
		"district": {
			"en": district.get("en"),
			"zh-hk": district.get("zh-hk"),
		},
		"estate_building": {
			"name": building.get("name", {}),
			"type": building.get("type"),
			"tower": building.get("tower"),
			"builtIn": building.get("builtIn"),
		},
	}


def create_system_prompt():
	return (
		"You are a Hong Kong post matching assistant.\n"
		"Given a subscriber's conversation context and a list of candidate posts, "
		"identify the best matching posts (up to 2).\n\n"
		"Rules:\n"
		"- Match to the user's preferences and needs from conversation summary.\n"
		"- Prefer candidates with realistic fit on price, size, bedrooms, and district.\n"
		"- If nothing is a good fit, return an empty matched_post_ids array.\n"
		"- Output only valid JSON with no extra text.\n\n"
		"Return JSON:\n"
		"{\n"
		'  "matched_post_ids": ["post_id_1", "post_id_2"]\n'
		"}"
	)


def create_match_prompt(conv, listings):
	return [
		{"role": "system", "content": create_system_prompt()},
		{
			"role": "user",
			"content": json.dumps(
				{"subscriber_conversations": sanitize_conv(conv), "candidate_posts": listings},
				ensure_ascii=False,
			),
		},
	]


def attach_estate_buildings(db, posts):
	estate_ids = []
	for post in posts:
		estate_id = post.get("estate_building_id")
		if estate_id is not None:
			estate_ids.append(estate_id)

	if not estate_ids:
		return []

	estates = list(db["estate_buildings"].find({"_id": {"$in": estate_ids}}))
	estate_map = {estate.get("_id"): estate for estate in estates}

	enriched = []
	for post in posts:
		estate = estate_map.get(post.get("estate_building_id"))
		if not estate:
			continue
		enriched.append({**post, "_estate_building": estate})

	return enriched


def write_direct_matches_file(batch_code, rows, total_new_posts):
	path = os.path.join(folder, "results", f"direct-match-{batch_code}.json")
	payload = {
		"date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
		"total_new_posts": total_new_posts,
		"rows": rows,
	}
	with open(path, "w", encoding="utf-8") as f:
		f.write(json.dumps(payload, ensure_ascii=False))
	return path


def main():
	result_files = get_all_result_files(os.path.join(folder, "results"))
	if result_files:
		for file_path in result_files:
			print(f"Backup previous result file: {file_path}")
			move_file(file_path, os.path.join(folder, "backup", os.path.basename(file_path)))

	if not MONGODB_CONNECTION_STRING:
		print("Missing MONGODB_CONNECTION_STRING")
		return

	mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
	db = mongo_client["prop_main"]

	posts = get_yesterday_posts(db)
	if not posts:
		print("No new published posts found for yesterday.")
		mongo_client.close()
		return

	posts = attach_estate_buildings(db, posts)
	if not posts:
		print("No posts with valid estate building records found.")
		mongo_client.close()
		return

	print(f"Found {len(posts)} new posts from yesterday with estate info.")

	batch_code = gen_batch_code()
	batch_file_path = os.path.join(folder, "batch_files", f"batch-{batch_code}.jsonl")
	meta_file_path = os.path.join(folder, "batch_files", f"batch-{batch_code}-meta.json")

	llm_processed_count = 0
	direct_rows = []

	# Batch file is only needed when LLM selection is enabled.
	batch_file = open(batch_file_path, "w", encoding="utf-8") if POST_MATCH_USE_LLM else None

	try:
		for conv in active_conversation(db):
			if is_push_true_for_last_10_messages(conv):
				print(f"Conversation {conv['_id']} has push=True for last 10 messages, skipping.")
				continue

			filtered_posts = prematch_by_search_criteria(conv, posts)
			if not filtered_posts:
				print(f"No posts match search criteria for conversation {conv['_id']}, skipping.")
				continue

			ranked_posts = rank_posts(conv, filtered_posts)
			top_posts = ranked_posts[:6]

			if POST_MATCH_USE_LLM:
				print(
					f"Creating LLM prompt for conversation {conv['_id']} "
					f"with {len(top_posts)} candidates: {[str(p.get('_id')) for p in top_posts[:6]]}"
				)

				messages = create_match_prompt(conv, [sanitize_post(p) for p in top_posts])
				row = {
					"custom_id": f"match-post-{conv['_id']}",
					"method": "POST",
					"url": "/chat/completions",
					"body": {
						"model": "gpt-4.1-nano",
						"messages": messages,
						"max_tokens": 500,
						"response_format": {"type": "json_object"},
					},
				}
				batch_file.write(f"{json.dumps(row, ensure_ascii=False)}\n")
				llm_processed_count += 1
			else:
				matched_post_ids = [str(p.get("_id")) for p in ranked_posts[:2]]
				direct_rows.append(
					{
						"conversation_id": str(conv.get("_id")),
						"matched_post_ids": matched_post_ids,
						"candidate_count": len(filtered_posts),
						"top_candidate_post_ids": [str(p.get("_id")) for p in top_posts],
					}
				)
				print(
					f"Hard matched conversation {conv['_id']}: "
					f"{matched_post_ids} (candidates={len(filtered_posts)})"
				)
	finally:
		if batch_file:
			batch_file.close()

	if POST_MATCH_USE_LLM:
		if llm_processed_count == 0:
			print("No conversations produced LLM match requests.")
			if os.path.exists(batch_file_path):
				os.remove(batch_file_path)
			mongo_client.close()
			return

		yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
		with open(meta_file_path, "w", encoding="utf-8") as meta_file:
			meta_file.write(
				json.dumps(
					{
						"date": yesterday_date,
						"total_new_posts": len(posts),
					},
					ensure_ascii=False,
				)
			)

		print(f"Batch file created: {batch_file_path} ({llm_processed_count} user requests)")
	else:
		if os.path.exists(batch_file_path):
			os.remove(batch_file_path)

		if not direct_rows:
			print("No direct matches generated.")
			mongo_client.close()
			return

		direct_result_path = write_direct_matches_file(batch_code, direct_rows, len(posts))
		print(f"Direct hard-match file created: {direct_result_path} ({len(direct_rows)} users)")

	mongo_client.close()


if __name__ == "__main__":
	main()
