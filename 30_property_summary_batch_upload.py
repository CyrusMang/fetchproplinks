from datetime import datetime
import json
import os
import uuid

from openai import AzureOpenAI
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

batch_size = 30
max_photos_per_property = 8

dir = os.path.dirname(os.path.abspath(__file__))
artifacts = os.path.join(dir, ARTIFACTS_FOLDER)
folder = os.path.join(artifacts, "property_summary")

os.makedirs(os.path.join(folder, "batch_files"), exist_ok=True)
os.makedirs(os.path.join(folder, "upload_batches"), exist_ok=True)
os.makedirs(os.path.join(folder, "results"), exist_ok=True)


def gen_batch_code():
	return str(uuid.uuid4())


def create_system_prompt():
	return """
You are a senior Hong Kong property analyst.

Your task is to produce one complete property summary based only on:
1) structured listing data, and
2) analyzed photo observations.

Rules:
- Use only evidence in the input. Do not invent facts.
- If information is missing, write null or an empty array where applicable.
- Keep writing clear and practical for home seekers.
- Mention both strengths and potential concerns.
- Include photo-based evidence in the narrative.
- Output only valid JSON.

Return JSON with this schema:
{
  "headline_en": "string",
	"headline_zh_hk": "string",
	"headline_zh_cn": "string",
	"executive_summary_en": "string",
	"executive_summary_zh_hk": "string",
	"executive_summary_zh_cn": "string",
  "key_highlights": ["string", "..."],
  "possible_concerns": ["string", "..."],
  "price_analysis": {
	  "value_comment": "string|null"
  },
  "layout_and_space": {
	  "space_comment": "string|null"
  },
  "location_and_transport": {
	  "location_comment": "string|null"
  },
  "photo_insights": {
    "overall_condition": "string",
    "cleanliness_comment": "string|null",
    "brightness_comment": "string|null",
    "photo_quality_comment": "string|null"
  },
  "recommended_for": ["string", "..."],
  "confidence_score": number
}
""".strip()


def sanitize_prop_data(prop):
	extracted = prop.get("v1_extracted_data", {})
	return {
		"source_id": prop.get("source_id"),
		"source_channel": prop.get("source_channel"),
		"source_url": prop.get("source_url"),
		"title": extracted.get("title"),
		"description": extracted.get("description"),
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


def sanitize_photo_data(photo):
	return {
		"photo_id": photo.get("photo_id"),
		"room_type": photo.get("room_type"),
		"image_description": photo.get("image_description"),
		"detected_objects": photo.get("detected_objects", []),
		"quality_score": photo.get("quality_score"),
		"is_indoor": photo.get("is_indoor"),
		"blob_url": photo.get("blob_url"),
		"photo_url": photo.get("photo_url"),
	}


def create_summary_prompt(prop_payload, photo_payloads):
	user_content = {
		"property_data": prop_payload,
		"photo_analyses": photo_payloads,
	}
	return [
		{"role": "system", "content": create_system_prompt()},
		{
			"role": "user",
			"content": f"Generate one complete property summary JSON for this listing data:\n{json.dumps(user_content, ensure_ascii=False)}",
		},
	]


def remove_file(file_path):
	try:
		os.remove(file_path)
		print(f"Removed file: {file_path}")
	except Exception as e:
		print(f"Error removing file {file_path}: {e}")


def main():
	if not MONGODB_CONNECTION_STRING:
		print("Missing MONGODB_CONNECTION_STRING in environment.")
		return
	if not OPENAI_API_KEY or not OPENAI_API_ENDPOINT or not OPENAI_API_VERSION:
		print("Missing OpenAI Azure configuration in environment.")
		return

	mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
	openai_client = AzureOpenAI(
		azure_endpoint=OPENAI_API_ENDPOINT,
		api_key=OPENAI_API_KEY,
		api_version=OPENAI_API_VERSION,
	)

	db = mongo_client["prop_main"]
	prop_collection = db["props"]
	photo_collection = db["prop_photos"]

	prop_filter = {
		"v1_extracted_data": {"$exists": True},
		"status": {"$in": ["photo_analysed", "summary_failed"]},
		"summary_batch_code": {"$exists": False},
	}

	props = list(
		prop_collection.find(prop_filter)
		.sort("updated_at", -1)
		.limit(batch_size)
	)

	if not props:
		print("No properties found for summary batch creation.")
		mongo_client.close()
		return

	batch_code = gen_batch_code()
	batch_file_path = os.path.join(folder, "batch_files", f"batch-{batch_code}.jsonl")

	processed_count = 0
	skipped_count = 0

	with open(batch_file_path, "w", encoding="utf-8") as batch_file:
		for prop in props:
			source_id = prop.get("source_id")

			photo_filter = {
				"prop_source_id": source_id,
				"status": "photo_analysed",
				"is_photo_of_property": True,
				"is_violating_policy": False,
				"is_human_in_photo": False,
			}
			photo_docs = list(
				photo_collection.find(photo_filter)
				.sort("quality_score", -1)
				.limit(max_photos_per_property)
			)

			if not photo_docs:
				skipped_count += 1
				print(f"Skip {source_id}: no eligible analyzed photos.")
				continue

			prop_payload = sanitize_prop_data(prop)
			photo_payloads = [sanitize_photo_data(p) for p in photo_docs]
			messages = create_summary_prompt(prop_payload, photo_payloads)

			row = {
				"custom_id": f"summary-{source_id}",
				"method": "POST",
				"url": "/chat/completions",
				"body": {
					"model": "gpt-4o-mini-batch",
					"messages": messages,
					"temperature": 0.2,
					"max_tokens": 1800,
					"response_format": {"type": "json_object"},
				},
			}
			batch_file.write(f"{json.dumps(row, ensure_ascii=False)}\n")

			prop_collection.update_one(
				{"source_id": source_id},
				{
					"$set": {
						"summary_batch_code": batch_code,
						"summary_status": "batch_created",
						"summary_batch_created_at": datetime.now().timestamp(),
					}
				},
			)
			processed_count += 1
			print(f"Prepared summary task for {source_id} with {len(photo_payloads)} photos.")

	if processed_count == 0:
		remove_file(batch_file_path)
		print("No eligible properties prepared for summary batch.")
		mongo_client.close()
		return

	uploaded_file = openai_client.files.create(
		file=open(batch_file_path, "rb"),
		purpose="batch",
	)
	batch_response = openai_client.batches.create(
		input_file_id=uploaded_file.id,
		endpoint="/chat/completions",
		completion_window="24h",
	)

	uploaded_batch_file_path = os.path.join(folder, "upload_batches", f"batch-{batch_code}.json")
	with open(uploaded_batch_file_path, "w", encoding="utf-8") as uploaded_batch_file:
		content = {
			"batch_code": batch_code,
			"batch_id": batch_response.id,
			"status": batch_response.status,
			"created_at": batch_response.created_at,
			"input_file_id": uploaded_file.id,
			"input_file_name": uploaded_file.filename,
			"input_file_size": uploaded_file.bytes,
			"prepared_properties": processed_count,
			"skipped_properties": skipped_count,
		}
		uploaded_batch_file.write(json.dumps(content, indent=2, ensure_ascii=False))

	remove_file(batch_file_path)

	prop_collection.update_many(
		{"summary_batch_code": batch_code},
		{
			"$set": {
				"summary_status": "batch_uploaded",
				"summary_upload_batch_id": batch_response.id,
				"summary_batch_uploaded_at": datetime.now().timestamp(),
			}
		},
	)

	print("=" * 50)
	print("SUMMARY BATCH UPLOADED")
	print("=" * 50)
	print(f"Batch code: {batch_code}")
	print(f"Batch id: {batch_response.id}")
	print(f"Prepared properties: {processed_count}")
	print(f"Skipped properties: {skipped_count}")
	print(f"Tracking file: {uploaded_batch_file_path}")
	print("Next: create a batch tracking/update script to write summary output back to props.")

	mongo_client.close()


if __name__ == "__main__":
	main()
