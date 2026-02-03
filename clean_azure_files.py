import os

from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

# Replace with your actual Azure resource details
client = AzureOpenAI(
    azure_endpoint = OPENAI_API_ENDPOINT, 
    api_key=OPENAI_API_KEY, 
    api_version=OPENAI_API_VERSION
)

# 1. Fetch the list of all files
files = client.files.list()

if not files.data:
    print("No files found to delete.")
else:
    print(f"Found {len(files.data)} files. Starting deletion...")
    
    # 2. Loop through and delete each file by ID
    for file_obj in files.data:
        try:
            client.files.delete(file_obj.id)
            print(f"Successfully deleted: {file_obj.filename} ({file_obj.id})")
        except Exception as e:
            print(f"Failed to delete {file_obj.id}: {e}")

print("Cleanup complete.")
