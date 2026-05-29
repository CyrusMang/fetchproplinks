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


def get_batch_jsonl_files(folder_path):
    files = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.jsonl'):
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

    batch_files_dir = os.path.join(folder, 'batch_files')
    files = get_batch_jsonl_files(batch_files_dir)
    if not files:
        print("No batch JSONL files found.")
        return

    for file_path in files:
        batch_code = os.path.basename(file_path).replace('batch-', '').replace('.jsonl', '')
        print(f"Uploading: {file_path}")

        file = client.files.create(
            file=open(file_path, 'rb'),
            purpose='batch',
        )
        batch_response = client.batches.create(
            input_file_id=file.id,
            endpoint='/chat/completions',
            completion_window='24h',
        )

        # Load companion meta file if present
        meta_path = os.path.join(batch_files_dir, f"batch-{batch_code}-meta.json")
        meta = {}
        if os.path.exists(meta_path):
            with open(meta_path, 'r', encoding='utf-8') as mf:
                meta = json.load(mf)

        tracking = {
            'batch_id': batch_response.id,
            'status': batch_response.status,
            'created_at': batch_response.created_at,
            'input_file_id': file.id,
            'input_file_name': file.filename,
            'input_file_size': file.bytes,
            'date': meta.get('date', ''),
            'total_new_props': meta.get('total_new_props', 0),
        }

        tracking_path = os.path.join(folder, 'upload_batches', f"batch-{batch_code}.json")
        with open(tracking_path, 'w', encoding='utf-8') as tf:
            tf.write(json.dumps(tracking, ensure_ascii=False))

        print(f"Batch submitted: {batch_response.id} → {tracking_path}")
        remove_file(file_path)
        if os.path.exists(meta_path):
            remove_file(meta_path)


if __name__ == '__main__':
    main()
