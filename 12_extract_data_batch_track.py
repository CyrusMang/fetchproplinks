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
folder = os.path.join(artifacts, 'extract_data')

def get_all_files(folder_path):
    files = []
    for root, dirs, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.endswith('.json'):
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
    files = get_all_files(os.path.join(folder, 'upload_batchs'))
    if not files:
        print("No files found in the folder.")
        return
    for file_path in files:
        batch_code = file_path.split('/')[-1].split('.')[0].replace('batch-', '')
        with open(file_path, 'r+') as uploaded_batch_file:
            data = uploaded_batch_file.read()
            content = json.loads(data)
            batch_response = client.batches.retrieve(content['batch_id'])
            if batch_response.status == "completed":
                with open(os.path.join(folder, 'results', f"batch-{batch_code}-result.json"), 'w') as result_file:
                    result_file.write(batch_response.model_dump_json())
                remove_file(file_path)
                print(f"Batch {batch_code} completed and results saved.")
            else:
                content.update({
                    "status": batch_response.status,
                    "updated_at": datetime.datetime.now().timestamp(),
                })
                uploaded_batch_file.seek(0)
                uploaded_batch_file.write(f"{json.dumps(content)}")
                uploaded_batch_file.truncate()
                print(f"Batch {batch_code} is not completed yet, status: {batch_response.status}")

if __name__ == '__main__':
    main()