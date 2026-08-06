import os
import json
from datetime import datetime

from bson import ObjectId
from dotenv import load_dotenv
from openai import AzureOpenAI
from pymongo import MongoClient

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

DIR = os.path.dirname(os.path.abspath(__file__))
ARTIFACTS = os.path.join(DIR, ARTIFACTS_FOLDER)
FOLDER = os.path.join(ARTIFACTS, "post_match")


def list_result_files(results_dir):
    direct_files = []
    llm_files = []

    for filename in os.listdir(results_dir):
        full_path = os.path.join(results_dir, filename)
        if filename.startswith("direct-match-") and filename.endswith(".json"):
            direct_files.append(full_path)
        elif filename.startswith("batch-") and filename.endswith("-result.json"):
            llm_files.append(full_path)

    return direct_files, llm_files


def move_file(src, dst):
    try:
        os.rename(src, dst)
        print(f"Moved: {src} -> {dst}")
    except Exception as error:
        print(f"Error moving file {src}: {error}")


def has_existing_post_item(conv, post_id):
    for item in conv.get("push_posts", []):
        if str(item.get("post_id")) == str(post_id):
            return True
    return False


def queue_post_for_conversation(db, conv_oid, post_id):
    conv = db["conversations-v2"].find_one({"_id": conv_oid})
    if not conv:
        return False, "conversation_not_found"

    if has_existing_post_item(conv, post_id):
        return False, "already_queued"

    update_result = db["conversations-v2"].update_one(
        {"_id": conv_oid, "push_posts.post_id": {"$ne": post_id}},
        {
            "$push": {
                "push_posts": {
                    "post_id": post_id,
                    "status": "pending",
                    "createdAt": int(datetime.now().timestamp()),
                }
            }
        },
    )

    if update_result.modified_count > 0:
        return True, "queued"
    return False, "not_modified"


def parse_conv_oid(conv_id_str):
    try:
        return ObjectId(conv_id_str)
    except Exception:
        return None


def normalize_post_id(post):
    post_id = post.get("id")
    if post_id:
        return str(post_id)

    object_id = post.get("_id")
    if object_id:
        return str(object_id)

    return None


def find_post(db, post_id):
    if not post_id:
        return None

    try:
        oid = ObjectId(post_id)
        post = db["posts"].find_one({"_id": oid})
        if post:
            return post
    except Exception:
        pass

    post = db["posts"].find_one({"id": post_id})
    if post:
        return post

    return None


def process_direct_result_file(db, file_path):
    sent = 0
    skipped = 0
    failed = 0

    with open(file_path, "r", encoding="utf-8") as result_file:
        payload = json.load(result_file)

    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        print(f"Invalid direct result format: {file_path}")
        return sent, skipped, failed

    for row in rows:
        conv_id_str = str(row.get("conversation_id") or "")
        matched_post_ids = row.get("matched_post_ids") or []

        if not conv_id_str or not matched_post_ids:
            skipped += 1
            continue

        conv_oid = parse_conv_oid(conv_id_str)
        if not conv_oid:
            print(f"Invalid conversation_id: {conv_id_str}")
            skipped += 1
            continue

        # Keep behavior consistent with prop flow: push first matched candidate only.
        selected_id = str(matched_post_ids[0])
        post = find_post(db, selected_id)
        if not post:
            print(f"Post not found for conv={conv_id_str}, post_id={selected_id}")
            failed += 1
            continue

        normalized_post_id = normalize_post_id(post)
        if not normalized_post_id:
            print(f"Post missing id for conv={conv_id_str}, post_id={selected_id}")
            failed += 1
            continue

        ok, reason = queue_post_for_conversation(db, conv_oid, normalized_post_id)
        if ok:
            sent += 1
            print(f"Queued post for conversation {conv_id_str}: {normalized_post_id}")
        elif reason == "already_queued":
            skipped += 1
            print(f"Conversation {conv_id_str} already has queued post {normalized_post_id}")
        else:
            failed += 1
            print(f"Failed queueing post for conversation {conv_id_str}: {reason}")

    return sent, skipped, failed


def parse_llm_lines(raw_lines):
    records = []
    for line in raw_lines:
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def extract_conv_id_from_custom_id(custom_id):
    if custom_id.startswith("match-post-"):
        return custom_id.replace("match-post-", "", 1)
    if custom_id.startswith("match-"):
        return custom_id.replace("match-", "", 1)
    return ""


def process_llm_result_file(db, openai_client, file_path):
    sent = 0
    skipped = 0
    failed = 0

    with open(file_path, "r", encoding="utf-8") as result_file:
        result_meta = json.load(result_file)

    output_file_id = result_meta.get("output_file_id")
    if not output_file_id:
        print(f"Missing output_file_id in {file_path}")
        return sent, skipped + 1, failed

    file_response = openai_client.files.content(output_file_id)
    raw_lines = file_response.text.strip().split("\n")

    batch_code = os.path.basename(file_path).replace("batch-", "").replace("-result.json", "")
    data_file_path = os.path.join(FOLDER, "data", f"batch-{batch_code}-data.jsonl")
    with open(data_file_path, "w", encoding="utf-8") as data_file:
        for line in raw_lines:
            if line.strip():
                data_file.write(f"{line}\n")

    records = parse_llm_lines(raw_lines)

    for record in records:
        custom_id = record.get("custom_id", "")
        conv_id_str = extract_conv_id_from_custom_id(custom_id)
        if not conv_id_str:
            skipped += 1
            continue

        conv_oid = parse_conv_oid(conv_id_str)
        if not conv_oid:
            print(f"Invalid conv id in custom_id: {custom_id}")
            skipped += 1
            continue

        response_body = record.get("response", {}).get("body", {})
        choices = response_body.get("choices", [])
        if not choices:
            skipped += 1
            continue

        try:
            llm_result = json.loads(choices[0]["message"]["content"])
        except Exception:
            print(f"Failed to parse LLM output for custom_id={custom_id}")
            skipped += 1
            continue

        matched_post_ids = llm_result.get("matched_post_ids", [])
        if not matched_post_ids:
            skipped += 1
            continue

        selected_id = str(matched_post_ids[0])
        post = find_post(db, selected_id)
        if not post:
            print(f"Post not found for conv={conv_id_str}, post_id={selected_id}")
            failed += 1
            continue

        normalized_post_id = normalize_post_id(post)
        if not normalized_post_id:
            print(f"Post missing id for conv={conv_id_str}, post_id={selected_id}")
            failed += 1
            continue

        ok, reason = queue_post_for_conversation(db, conv_oid, normalized_post_id)
        if ok:
            sent += 1
            print(f"Queued post for conversation {conv_id_str}: {normalized_post_id}")
        elif reason == "already_queued":
            skipped += 1
            print(f"Conversation {conv_id_str} already has queued post {normalized_post_id}")
        else:
            failed += 1
            print(f"Failed queueing post for conversation {conv_id_str}: {reason}")

    # Clean up uploaded files from OpenAI when available.
    try:
        openai_client.files.delete(output_file_id)
    except Exception as error:
        print(f"Warning: failed to delete output_file_id {output_file_id}: {error}")

    input_file_id = result_meta.get("input_file_id")
    if input_file_id:
        try:
            openai_client.files.delete(input_file_id)
        except Exception as error:
            print(f"Warning: failed to delete input_file_id {input_file_id}: {error}")

    return sent, skipped, failed


def build_openai_client_if_needed(has_llm_files):
    if not has_llm_files:
        return None

    if not all([OPENAI_API_KEY, OPENAI_API_ENDPOINT, OPENAI_API_VERSION]):
        raise RuntimeError("Missing OpenAI Azure configuration for processing LLM result files.")

    return AzureOpenAI(
        azure_endpoint=OPENAI_API_ENDPOINT,
        api_key=OPENAI_API_KEY,
        api_version=OPENAI_API_VERSION,
    )


def main():
    if not MONGODB_CONNECTION_STRING:
        print("Missing MONGODB_CONNECTION_STRING.")
        return

    os.makedirs(os.path.join(FOLDER, "data"), exist_ok=True)
    os.makedirs(os.path.join(FOLDER, "backup"), exist_ok=True)

    results_dir = os.path.join(FOLDER, "results")
    if not os.path.isdir(results_dir):
        print(f"Results folder not found: {results_dir}")
        return

    direct_files, llm_files = list_result_files(results_dir)
    if not direct_files and not llm_files:
        print("No post match result files found.")
        return

    openai_client = None
    try:
        openai_client = build_openai_client_if_needed(bool(llm_files))
    except RuntimeError as error:
        print(str(error))
        return

    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    db = mongo_client["prop_main"]

    total_sent = 0
    total_skipped = 0
    total_failed = 0

    for file_path in direct_files:
        print(f"\nProcessing direct match file: {os.path.basename(file_path)}")
        sent, skipped, failed = process_direct_result_file(db, file_path)
        total_sent += sent
        total_skipped += skipped
        total_failed += failed
        print(f"Summary: sent={sent}, skipped={skipped}, failed={failed}")
        move_file(file_path, os.path.join(FOLDER, "backup", os.path.basename(file_path)))

    for file_path in llm_files:
        print(f"\nProcessing LLM match result file: {os.path.basename(file_path)}")
        sent, skipped, failed = process_llm_result_file(db, openai_client, file_path)
        total_sent += sent
        total_skipped += skipped
        total_failed += failed
        print(f"Summary: sent={sent}, skipped={skipped}, failed={failed}")
        move_file(file_path, os.path.join(FOLDER, "backup", os.path.basename(file_path)))

    print(f"\nDone: sent={total_sent}, skipped={total_skipped}, failed={total_failed}")
    mongo_client.close()


if __name__ == "__main__":
    main()
