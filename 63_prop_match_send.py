import os
import json
from datetime import datetime, timedelta
from bson import ObjectId
from openai import AzureOpenAI
from pymongo import MongoClient
import requests
from dotenv import load_dotenv
import send_prop_matched_wtsapp_msg

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

CHATWOOT_BASE_URL = os.getenv("CHATWOOT_BASE_URL", "https://chatwoot.snailbutler.com")
CHATWOOT_ACCOUNT_ID = os.getenv("CHATWOOT_ACCOUNT_ID")
CHATWOOT_API_TOKEN = os.getenv("CHATWOOT_API_TOKEN")
CHATWOOT_INBOX_ID = os.getenv("CHATWOOT_INBOX_ID")   # WhatsApp inbox ID in Chatwoot

WHATSAPP_TEMPLATE_NAME = "new_prop_matched"
WHATSAPP_TEMPLATE_LANGUAGE = "zh_HK"

dir = os.path.dirname(os.path.abspath(__file__))
artifacts = os.path.join(dir, ARTIFACTS_FOLDER)
folder = os.path.join(artifacts, 'prop_match')


def get_all_result_files(folder_path):
    files = []
    for filename in os.listdir(folder_path):
        if filename.endswith('-result.json'):
            files.append(os.path.join(folder_path, filename))
    return files


def remove_file(file_path):
    try:
        os.remove(file_path)
        print(f"Removed file: {file_path}")
    except Exception as e:
        print(f"Error removing file {file_path}: {e}")


def move_file(src, dst):
    try:
        os.rename(src, dst)
        print(f"Moved: {src} → {dst}")
    except Exception as e:
        print(f"Error moving file {src}: {e}")


def format_price(price):
    if price is None:
        return "N/A"
    return f"HK${int(price):,}"


def format_size(sqft):
    if sqft is None:
        return "N/A"
    return f"{int(sqft)} sqft"


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

# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def main():
    if not all([CHATWOOT_ACCOUNT_ID, CHATWOOT_API_TOKEN, CHATWOOT_INBOX_ID]):
        print("Missing Chatwoot configuration (CHATWOOT_ACCOUNT_ID / CHATWOOT_API_TOKEN / CHATWOOT_INBOX_ID).")
        return
    if not MONGODB_CONNECTION_STRING:
        print("Missing MONGODB_CONNECTION_STRING.")
        return
    if not all([OPENAI_API_KEY, OPENAI_API_ENDPOINT, OPENAI_API_VERSION]):
        print("Missing OpenAI Azure configuration.")
        return

    openai_client = AzureOpenAI(
        azure_endpoint=OPENAI_API_ENDPOINT,
        api_key=OPENAI_API_KEY,
        api_version=OPENAI_API_VERSION,
    )
    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    db = mongo_client['prop_main']

    total = get_yesterday_props(db)

    os.makedirs(os.path.join(folder, 'data'), exist_ok=True)
    os.makedirs(os.path.join(folder, 'backup'), exist_ok=True)

    result_files = get_all_result_files(os.path.join(folder, 'results'))
    if not result_files:
        print("No completed result files found.")
        mongo_client.close()
        return

    for result_file_path in result_files:
        batch_code = os.path.basename(result_file_path).replace('batch-', '').replace('-result.json', '')
        print(f"\nProcessing batch: {batch_code}")

        with open(result_file_path, 'r', encoding='utf-8') as rf:
            result_meta = json.load(rf)

        output_file_id = result_meta.get('output_file_id')
        total_new_props = result_meta.get('total_new_props', 0)

        if not output_file_id:
            print(f"Missing output_file_id in {result_file_path}")
            move_file(result_file_path, os.path.join(folder, 'backup', os.path.basename(result_file_path)))
            continue

        # Download raw JSONL results from Azure OpenAI
        file_response = openai_client.files.content(output_file_id)
        raw_lines = [l for l in file_response.text.strip().split('\n') if l.strip()]

        # Save raw data for audit
        data_file_path = os.path.join(folder, 'data', f"batch-{batch_code}-data.jsonl")
        with open(data_file_path, 'w', encoding='utf-8') as df:
            for line in raw_lines:
                df.write(f"{line}\n")

        sent = 0
        skipped = 0
        failed = 0

        for raw_line in raw_lines:
            try:
                record = json.loads(raw_line)
            except json.JSONDecodeError:
                continue

            custom_id = record.get('custom_id', '')
            if not custom_id.startswith('match-'):
                continue

            # Extract user ObjectId
            user_id_str = custom_id.replace('match-', '', 1)
            try:
                user_oid = ObjectId(user_id_str)
            except Exception:
                print(f"Invalid user_id in custom_id: {custom_id}")
                skipped += 1
                continue

            # Parse LLM response
            response_body = record.get('response', {}).get('body', {})
            choices = response_body.get('choices', [])
            if not choices:
                skipped += 1
                continue

            try:
                llm_result = json.loads(choices[0]['message']['content'])
            except (json.JSONDecodeError, KeyError, IndexError):
                print(f"Failed to parse LLM response for {custom_id}")
                skipped += 1
                continue

            matched_ids = llm_result.get('matched_source_ids', [])
            if not matched_ids:
                skipped += 1
                continue  # No matches for this user — skip

            # Fetch user from MongoDB
            user = db['users'].find_one({'_id': user_oid})
            if not user:
                print(f"User not found: {user_id_str}")
                skipped += 1
                continue

            phone = next((id.get('key') for id in user.get('identifiers', []) if id.get('type') == 'phone'), '')
            if not phone:
                print(f"No phone for user {user_id_str}")
                skipped += 1
                continue

            # Fetch matched props (up to 5)
            matched_source_ids = matched_ids[:5]
            matched_props = [
                db['props'].find_one({'source_id': sid})
                for sid in matched_source_ids
            ]
            lang = user.get('userPreferences', {}).get('preferredLanguage', 'en')
            success = send_prop_matched_wtsapp_msg.send('rent', phone, lang, total, matched_props)

            if success:
                sent += 1
            else:
                failed += 1

        print(f"Batch {batch_code}: sent={sent}, skipped={skipped}, failed={failed}")

        # Clean up
        openai_client.files.delete(output_file_id)
        input_file_id = result_meta.get('input_file_id')
        if input_file_id:
            openai_client.files.delete(input_file_id)

        move_file(result_file_path, os.path.join(folder, 'backup', os.path.basename(result_file_path)))

    mongo_client.close()


if __name__ == '__main__':
    main()
