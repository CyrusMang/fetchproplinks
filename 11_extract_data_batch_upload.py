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
folder = os.path.join(artifacts, 'extract_data')

def get_all_files(folder_path):
    files = []
    for root, dirs, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.endswith('.jsonl'):
                files.append(os.path.join(root, filename))
    return files

def remove_file(file_path):
    try:
        os.remove(file_path)
        print(f"Removed file: {file_path}")
    except Exception as e:
        print(f"Error removing file {file_path}: {e}")

def main():
    client = AzureOpenAI(
        azure_endpoint = OPENAI_API_ENDPOINT, 
        api_key=OPENAI_API_KEY, 
        api_version=OPENAI_API_VERSION
    )
    files = get_all_files(os.path.join(folder, 'batch_files'))
    if not files:
        print("No files found in the folder.")
        return
    for file_path in files:
        print(f"Processing file: {file_path}")
        file = client.files.create(
            file=open(file_path, "rb"), 
            purpose="batch"
        )
        batch_response = client.batches.create(
            input_file_id=file.id,
            endpoint="/chat/completions",
            completion_window="24h"
        )
        batch_code = file_path.split('/')[-1].split('.')[0].replace('batch-', '')
        uploaded_batch_file_path = os.path.join(folder, 'upload_batches', f"batch-{batch_code}.json")
        with open(uploaded_batch_file_path, 'w', encoding='utf-8') as uploaded_batch_file:
            content = {
                "batch_id": batch_response.id,
                "status": batch_response.status,
                "created_at": batch_response.created_at,
                "input_file_id": file.id,
                "input_file_name": file.filename,
                "input_file_size": file.bytes,
            }
            uploaded_batch_file.write(f"{json.dumps(content)}")
        print(f"Uploaded batch file created: {uploaded_batch_file_path}")
        remove_file(file_path)

if __name__ == '__main__':
    main()