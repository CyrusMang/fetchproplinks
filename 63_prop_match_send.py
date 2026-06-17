import os
import json
from datetime import datetime, timedelta
from bson import ObjectId
from openai import AzureOpenAI
from pymongo import MongoClient
import requests
from dotenv import load_dotenv
# import send_prop_matched_wtsapp_msg
from models.conversation import Conversation
import chatwoot_api_helpers

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

import chatwoot_api_helpers

templates = {
  '1_zh-cn': ["new_prop_matched_1", 'UTILITY'],
  '2_zh-cn': ["new_prop_matched_2", 'UTILITY'],
  '3_zh-cn': ["new_prop_matched_3", 'UTILITY'],
  '4_zh-cn': ["new_prop_matched_4", 'UTILITY'],
  # '1_zh-hk': ["new_prop_matched_1", 'UTILITY'],
  '2_zh-hk': ["new_prop_matched_2", 'UTILITY'],
  '3_zh-hk': ["new_prop_matched_3", 'UTILITY'],
  '4_zh-hk': ["new_prop_matched_4", 'UTILITY'],
  # '1_en': ["new_prop_matched_1", 'UTILITY'],
  # '2_en': ["new_prop_matched_3", 'UTILITY'], 
  '3_en': ["new_prop_matched_3", 'UTILITY'],
  # '4_en': ["new_prop_matched_4", 'UTILITY'],
}

# templates = {
#     1: ["new_prop_matched_1", ['zh-cn'], 'UTILITY'],
#     2: ["new_prop_matched_2", ['zh-hk', 'zh-cn'], 'UTILITY'],
#     3: ["new_prop_matched_3", ['en', 'zh-hk', 'zh-cn'], 'UTILITY'],
#     4: ["new_prop_matched_4", ['zh-hk', 'zh-cn'], 'UTILITY'],
#     # 5: ["new_prop_matched_5", ['en', 'zh-hk'], 'UTILITY'],
# }

def get_right_num_of_props(num_props, lang):
    if num_props > 4 or num_props < 1:
        raise ValueError(f"Invalid number of properties: {num_props}")
    template = templates.get(f"{num_props}_{lang}")
    if not template:
        return get_right_num_of_props(num_props - 1, lang)
    return num_props

def get_template_and_props(props, lang):
    print(f"get_template_and_props: num_props={len(props)}, lang={lang}")
    num_props = min(len(props), 4)
    try:
        right_num = get_right_num_of_props(num_props, lang)
        return [templates[f"{right_num}_{lang}"][0], templates[f"{right_num}_{lang}"][1], props[:right_num]]
    except ValueError:
        return None

def render_message(prop_num, params, lang): 
    if lang == 'zh-hk':
        msg = f"Hello, 跟據你要求, 係{params['total']}個新盤入面搵到有{prop_num}個盤啱：\n\n"
        for i in range(1, prop_num + 1):
            msg += f"{i}️⃣ {params[f'prop_{i}_title']}\n"
            msg += f"- 租金：{params[f'prop_{i}_price']}\n"
            msg += f"- 面積：{params[f'prop_{i}_size']}\n"
            msg += f"- {params[f'prop_{i}_link']}\n\n"
        msg += "我會繼續幫你留意住市場, 如果有邊幾個單位睇啱話我知, 我幫你約睇樓😊"
        return msg
    elif lang == 'zh-cn':
        msg = f"你好，根据你的要求，在{params['total']}个新盘里面找到有{prop_num}个盘合适：\n\n"
        for i in range(1, prop_num + 1):
            msg += f"{i}️⃣ {params[f'prop_{i}_title']}\n"
            msg += f"- 租金：{params[f'prop_{i}_price']}\n"
            msg += f"- 面积：{params[f'prop_{i}_size']}\n"
            msg += f"- {params[f'prop_{i}_link']}\n\n"
        msg += "我会继续帮你留意市场，如果有哪个单位看合适告诉我，我帮你约看房😊"
        return msg
    else:
        msg = f"Hello, based on your requirements, we found {prop_num} suitable listings among {params['total']} new properties:\n\n"
        for i in range(1, prop_num + 1):
            msg += f"{i}️⃣ {params[f'prop_{i}_title']}\n"
            msg += f"- Price: {params[f'prop_{i}_price']}\n"
            msg += f"- Size: {params[f'prop_{i}_size']}\n"
            msg += f"- {params[f'prop_{i}_link']}\n\n"
        msg += "I will keep an eye on the market for you. If you are interested in any of these units, let me know and I can help arrange a viewing!😊"
        return msg

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
        total_new_props = result_meta.get('total_new_props', 243)

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

            conv = Conversation.get_by_user_id(db, user_oid)
            if not conv:
                print(f"No conversation found for user {user_id_str}")
                skipped += 1
                continue

            # Fetch matched props (up to 4)
            matched_source_ids = matched_ids[:4]
            matched_props = [
                db['props'].find_one({'source_id': sid})
                for sid in matched_source_ids
            ]
            matched_props = [p for p in matched_props if p]  # Filter out None
            lang = user.get('userPreferences', {}).get('language', 'en')

            contact = chatwoot_api_helpers.get_or_create_contact(phone)
            if not contact:
                print(f"Failed to get or create contact for {user_id_str}")
                continue
            contact_id = contact.get('id')
            if not contact_id:
                print(f"Contact found but missing ID for {user_id_str}")
                continue
            template_params = {
                'total': total_new_props,
            }
            r = get_template_and_props(matched_props, lang)
            if not r:
                print(f"No suitable template and language {lang}")
                continue
            template_name, template_category, selected_props = r

            for i, prop in enumerate(selected_props, start=1):
                extracted = prop.get('v1_extracted_data')
                summary = prop.get('v1_summary_data')
                size = extracted.get('net_size_sqft')
                price = extracted.get(f'rent_price')
                template_params[f'prop_{i}_title'] = summary.get(f'headline_{lang.replace("-", "_")}')
                template_params[f'prop_{i}_price'] = f"${price}"
                template_params[f'prop_{i}_size'] = f"{size} ft²" if size else "N/A"
                template_params[f'prop_{i}_link'] = f"https://homeable.house/{lang}/{prop.get('id')}"
            
            rendered_message = render_message(len(selected_props), template_params, lang)
            aimsg = {
              'type': 'ai',
              'content': rendered_message
            }
            success=1
            try:
                conv.add_message(aimsg)
            except Exception as e:
                print(f"Failed to add message for user {user_id_str}: {e}")
                continue

            print(f"user: {user_id_str}, lang: {lang}, total: {total_new_props}, selected_props: {len(selected_props)}")
            success = chatwoot_api_helpers.send_whatsapp_template(
                contact_id, lang, template_name, template_category, template_params, rendered_message
            )

            conv.conversation_summary()
            conv.archive_messages()

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
