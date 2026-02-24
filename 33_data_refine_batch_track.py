import os
import json
import time
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
            if filename.endswith('.json'):
                files.append(os.path.join(root, filename))
    return files

def main():
    client = AzureOpenAI(
        azure_endpoint = OPENAI_API_ENDPOINT, 
        api_key=OPENAI_API_KEY, 
        api_version=OPENAI_API_VERSION
    )
    
    files = get_all_files(os.path.join(folder, 'upload_batches'))
    if not files:
        print("No batch tracking files found.")
        return
    
    print(f"Found {len(files)} batch(es) to track.\n")
    
    for file_path in files:
        with open(file_path, 'r') as f:
            batch_info = json.load(f)
        
        batch_id = batch_info['batch_id']
        batch_code = batch_info['batch_code']
        
        print(f"Checking batch: {batch_code}")
        print(f"Batch ID: {batch_id}")
        
        # Retrieve batch status
        batch = client.batches.retrieve(batch_id)
        
        print(f"Status: {batch.status}")
        print(f"Created at: {batch.created_at}")
        
        if batch.request_counts:
            print(f"Total requests: {batch.request_counts.total}")
            print(f"Completed: {batch.request_counts.completed}")
            print(f"Failed: {batch.request_counts.failed}")
        
        # Update batch info
        batch_info['status'] = batch.status
        batch_info['request_counts'] = {
            'total': batch.request_counts.total if batch.request_counts else 0,
            'completed': batch.request_counts.completed if batch.request_counts else 0,
            'failed': batch.request_counts.failed if batch.request_counts else 0
        }
        
        if batch.status == 'completed':
            batch_info['output_file_id'] = batch.output_file_id
            print(f"✓ Batch completed! Output file ID: {batch.output_file_id}")
        elif batch.status == 'failed':
            print(f"✗ Batch failed!")
            if batch.errors:
                print(f"Errors: {batch.errors}")
        else:
            print(f"⏳ Batch still processing...")
        
        # Save updated info
        with open(file_path, 'w') as f:
            json.dump(batch_info, f, indent=2)
        
        print("-" * 50)
    
    print("\nBatch tracking completed.")
    print("Once all batches are completed, run: python 34_data_refine_batch_update.py")

if __name__ == '__main__':
    main()
