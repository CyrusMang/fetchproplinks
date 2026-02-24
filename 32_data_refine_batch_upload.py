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
folder = os.path.join(artifacts, 'data_refine')

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
        print("No batch files found to upload.")
        return
    
    for file_path in files:
        print(f"Processing file: {file_path}")
        
        # Upload file to Azure OpenAI
        file = client.files.create(
            file=open(file_path, "rb"), 
            purpose="batch"
        )
        
        print(f"Uploaded file ID: {file.id}")
        
        # Create batch job
        batch_job = client.batches.create(
            input_file_id=file.id,
            endpoint="/chat/completions",
            completion_window="24h"
        )
        
        print(f"Created batch job ID: {batch_job.id}")
        
        # Save batch info
        batch_code = file_path.split('/')[-1].replace('batch-', '').replace('.jsonl', '')
        batch_info = {
            'batch_code': batch_code,
            'batch_id': batch_job.id,
            'input_file_id': file.id,
            'status': batch_job.status
        }
        
        upload_info_path = os.path.join(folder, 'upload_batches', f"batch-{batch_code}.json")
        with open(upload_info_path, 'w') as f:
            json.dump(batch_info, f, indent=2)
        
        print(f"Saved batch info to: {upload_info_path}")
        
        # Remove processed batch file
        remove_file(file_path)
        print("-" * 50)
    
    print("\nAll batch files uploaded successfully.")
    print("Next step: Run python 33_data_refine_batch_track.py to check status.")

if __name__ == '__main__':
    main()
