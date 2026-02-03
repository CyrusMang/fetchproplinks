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
    files = get_all_files(os.path.join(folder, 'results'))
    if not files:
        print("No files found in the folder.")
        return
    for file_path in files:
        batch_code = file_path.split('/')[-1].split('.')[0].replace('batch-', '')
        with open(file_path, 'r') as result_batch_file:
            content = json.load(result_batch_file)
            file_response = client.files.content(content['output_file_id'])
            raw_responses = file_response.text.strip().split('\n') 
            data_file_path = os.path.join(folder, 'data', f"batch-{batch_code}-data.jsonl")
            with open(data_file_path, 'w', encoding='utf-8') as data_file: 
                for raw_response in raw_responses:
                    json_response = json.loads(raw_response)  
                    formatted_json = json.dumps(json_response, ensure_ascii=False)  
                    data_file.write(f"{formatted_json}\n")
            remove_file(file_path)
            client.files.delete(content['output_file_id'])
            client.files.delete(content['input_file_id'])
            print(f"Processed batch {batch_code} and saved data to {data_file_path}")


if __name__ == '__main__':
    main()