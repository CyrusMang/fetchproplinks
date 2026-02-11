import os
import json
from openai import AzureOpenAI
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

dir = os.path.dirname(os.path.abspath(__file__))
artifacts = os.path.join(dir, ARTIFACTS_FOLDER)
folder = os.path.join(artifacts, 'prop_summary')

def get_completed_batches(folder_path):
    """Get all completed batch files."""
    files = []
    for root, dirs, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.endswith('.json'):
                file_path = os.path.join(root, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    batch_info = json.load(f)
                    if batch_info.get('status') == 'completed':
                        files.append(file_path)
    return files

def download_batch_results(client, batch_info, batch_code):
    """Download and save batch results."""
    output_file_id = batch_info.get('output_file_id')
    
    if not output_file_id:
        print("No output file ID found.")
        return None
    
    try:
        # Download the result file content
        file_response = client.files.content(output_file_id)
        
        # Save to results folder
        result_file_path = os.path.join(folder, 'results', f"batch-{batch_code}-result.jsonl")
        with open(result_file_path, 'w', encoding='utf-8') as f:
            f.write(file_response.text)
        
        print(f"Results saved to: {result_file_path}")
        return result_file_path
        
    except Exception as e:
        print(f"Error downloading results: {e}")
        return None

def process_summary_result(result_line, collection):
    """Process a single summary result and update MongoDB."""
    try:
        data = json.loads(result_line)
        custom_id = data.get('custom_id')
        
        # Extract source_id from custom_id (format: summary-{source_id})
        source_id = custom_id.replace('summary-', '')
        
        # Get the response
        response = data.get('response', {})
        body = response.get('body', {})
        choices = body.get('choices', [])
        
        if not choices:
            print(f"No choices found for {source_id}")
            return False
        
        message = choices[0].get('message', {})
        content = message.get('content', '{}')
        
        # Parse the JSON response
        try:
            summary_data = json.loads(content)
            
            overall_score = summary_data.get('overall_score', 0)

            # Update MongoDB
            update_data = {
                'status': 'summarized',
                'property_summary': summary_data.get('summary', ''),
                'quality_scores': {
                    'data_completeness': summary_data.get('data_completeness_score', 0),
                    'data_quality': summary_data.get('data_quality_score', 0),
                    'photo_quality': summary_data.get('photo_quality_score', 0),
                    'overall': overall_score
                }
            }
            
            collection.update_one(
                {'source_id': source_id},
                {'$set': update_data}
            )
            
            print(f"✓ Updated {source_id}: Score {overall_score}/100 ")
            return True
            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response for {source_id}: {e}")
            return False
            
    except Exception as e:
        print(f"Error processing result: {e}")
        return False

def main():
    # Initialize clients
    openai_client = AzureOpenAI(
        azure_endpoint=OPENAI_API_ENDPOINT,
        api_key=OPENAI_API_KEY,
        api_version=OPENAI_API_VERSION
    )
    
    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    db = mongo_client['prop_main']
    collection = db['props']
    
    # Get completed batches
    completed_batches = get_completed_batches(os.path.join(folder, 'upload_batches'))
    
    if not completed_batches:
        print("No completed batches found.")
        print("Run 32_prop_summary_batch_track.py to check batch status first.")
        return
    
    print(f"Found {len(completed_batches)} completed batch(es)\n")
    
    for batch_file_path in completed_batches:
        with open(batch_file_path, 'r', encoding='utf-8') as f:
            batch_info = json.load(f)
        
        batch_code = batch_file_path.split('/')[-1].replace('batch-', '').replace('.json', '')
        batch_id = batch_info['batch_id']
        
        print(f"Processing batch: {batch_id}")
        
        # Download results
        result_file_path = download_batch_results(openai_client, batch_info, batch_code)
        
        if not result_file_path:
            continue
        
        # Process each result
        with open(result_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    process_summary_result(line, collection)
    
    mongo_client.close()

if __name__ == '__main__':
    main()
