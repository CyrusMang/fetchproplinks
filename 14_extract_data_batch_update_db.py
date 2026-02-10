import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
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

def move_file(source, destination):
    try:
        os.rename(source, destination)
        print(f"Moved file: {source}")
    except Exception as e:
        print(f"Error removing file {source}: {e}")

def main():
    client = MongoClient(MONGODB_CONNECTION_STRING)
    db = client['prop_main']
    collection = db['props']

    files = get_all_files(os.path.join(folder, 'data'))
    if not files:
        print("No files found in the folder.")
        return
    for file_path in files:
        batch_code = file_path.split('/')[-1].split('.')[0].replace('batch-', '')
        with open(file_path, 'r+') as data_file:
            for row in data_file:
                try:
                    content = json.loads(row)
                    if content.get('error'):
                        raise Exception(f"Error in content: {content['error']}")
                    source_id = content.get('custom_id').replace('task-', '')
                    if not source_id:
                        raise Exception(f"No source_id found in content: {content}")
                    res_str = content['response']['body']['choices'][0]['message']['content']
                    res_json = json.loads(res_str)
                    collection.update_one(
                        { 'source_id': source_id },
                        { 
                            '$set': { 'v1_extracted_data': res_json, 'status': 'data_extracted' },
                        }
                    )
                    print(f"Updated source {source_id} with extracted data.")
                except Exception as e:
                    collection.update_one(
                        { 'source_id': source_id },
                        { '$set': { 'v1_extract_data_error': f"{e}" } }
                    )
                    print(f"Error processing content for source {source_id}: {e}")
        move_file(file_path, os.path.join(folder, 'backup', f"batch-{batch_code}-data.jsonl"))

if __name__ == '__main__':
    main()