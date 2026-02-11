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
folder = os.path.join(artifacts, 'prop_summary')

def get_all_batch_files(folder_path):
    files = []
    for root, dirs, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.endswith('.json'):
                files.append(os.path.join(root, filename))
    return files

def main():
    client = AzureOpenAI(
        azure_endpoint=OPENAI_API_ENDPOINT,
        api_key=OPENAI_API_KEY,
        api_version=OPENAI_API_VERSION
    )
    
    batch_files = get_all_batch_files(os.path.join(folder, 'upload_batches'))
    
    if not batch_files:
        print("No batch files found to track.")
        return
    
    print(f"Tracking {len(batch_files)} batch(es)...\n")
    
    pending_batches = []
    completed_batches = []
    failed_batches = []
    
    for batch_file_path in batch_files:
        with open(batch_file_path, 'r', encoding='utf-8') as f:
            batch_info = json.load(f)
        
        batch_id = batch_info['batch_id']
        
        try:
            # Retrieve batch status
            batch = client.batches.retrieve(batch_id)
            
            print(f"Batch: {batch_id}")
            print(f"Status: {batch.status}")
            
            # Update batch info
            batch_info['status'] = batch.status
            batch_info['completed_at'] = batch.completed_at
            batch_info['failed_at'] = batch.failed_at
            batch_info['output_file_id'] = batch.output_file_id
            batch_info['error_file_id'] = batch.error_file_id
            
            # Save updated info
            with open(batch_file_path, 'w', encoding='utf-8') as f:
                f.write(json.dumps(batch_info, indent=2))
            
            if batch.status == 'completed':
                print(f"✓ Completed!")
                print(f"  Output file ID: {batch.output_file_id}")
                completed_batches.append(batch_id)
            elif batch.status == 'failed':
                print(f"✗ Failed!")
                if batch.error_file_id:
                    print(f"  Error file ID: {batch.error_file_id}")
                failed_batches.append(batch_id)
            else:
                print(f"⏳ Still processing...")
                pending_batches.append(batch_id)
            
            print()
            
        except Exception as e:
            print(f"Error tracking batch {batch_id}: {e}\n")
    
    # Summary
    print("=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Total batches: {len(batch_files)}")
    print(f"Completed: {len(completed_batches)}")
    print(f"Pending: {len(pending_batches)}")
    print(f"Failed: {len(failed_batches)}")
    
    if completed_batches:
        print("\nNext step: Run python 33_prop_summary_batch_update.py to process results")
    elif pending_batches:
        print("\nSome batches are still processing. Run this script again later to check status.")

if __name__ == '__main__':
    main()
