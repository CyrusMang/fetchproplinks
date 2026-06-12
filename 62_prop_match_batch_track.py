import datetime
import os
import json
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

dir = os.path.dirname(os.path.abspath(__file__))
artifacts = os.path.join(dir, ARTIFACTS_FOLDER)
folder = os.path.join(artifacts, 'prop_match')


def get_all_files(folder_path):
    files = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            files.append(os.path.join(folder_path, filename))
    return files


def remove_file(file_path):
    try:
        os.remove(file_path)
        print(f"Removed file: {file_path}")
    except Exception as e:
        print(f"Error removing file {file_path}: {e}")


def main():
    if not OPENAI_API_KEY or not OPENAI_API_ENDPOINT or not OPENAI_API_VERSION:
        print("Missing OpenAI Azure configuration in environment.")
        return

    client = AzureOpenAI(
        azure_endpoint=OPENAI_API_ENDPOINT,
        api_key=OPENAI_API_KEY,
        api_version=OPENAI_API_VERSION,
    )

    files = get_all_files(os.path.join(folder, 'upload_batches'))
    if not files:
        print("No upload tracking files found.")
        return

    for file_path in files:
        batch_code = os.path.basename(file_path).replace('batch-', '').replace('.json', '')

        with open(file_path, 'r+', encoding='utf-8') as tracking_file:
            content = json.loads(tracking_file.read())
            batch_id = content.get('batch_id')
            if not batch_id:
                print(f"Invalid tracking file (missing batch_id): {file_path}")
                continue

            batch_response = client.batches.retrieve(batch_id)
            status = batch_response.status
            print(f"Batch {batch_code}: {status}")

            if status == 'completed':
                result_path = os.path.join(folder, 'results', f"batch-{batch_code}-result.json")
                with open(result_path, 'w', encoding='utf-8') as rf:
                    result_data = json.loads(batch_response.model_dump_json())
                    # Carry forward metadata from tracking file
                    result_data['date'] = content.get('date', '')
                    result_data['total_new_props'] = content.get('total_new_props', 242)
                    rf.write(json.dumps(result_data, ensure_ascii=False))
                remove_file(file_path)
                print(f"Batch {batch_code} completed → {result_path}")
            elif status in ('failed', 'cancelled', 'expired'):
                print(f"Batch {batch_code} ended with status: {status}. Removing tracking file.")
                remove_file(file_path)
            else:
                content['status'] = status
                content['updated_at'] = datetime.datetime.now().timestamp()
                tracking_file.seek(0)
                tracking_file.write(json.dumps(content, ensure_ascii=False))
                tracking_file.truncate()


if __name__ == '__main__':
    main()
