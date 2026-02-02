import os
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv

load_dotenv()

AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

account_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
blob_service_client = BlobServiceClient(account_url, credential=AZURE_STORAGE_ACCOUNT_KEY)

def upload(container_name, blob_name, content, content_type):
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    if not blob_client.exists():
        blob_client.upload_blob(content, blob_type="BlockBlob", content_settings=ContentSettings(content_type=content_type))
    return {
        "blob_url": blob_client.url
    }

# def file_exist(container_name, blob_name):
#     container_client = blob_service_client.get_container_client(container_name)
#     blob_client = container_client.get_blob_client(blob_name)
#     if blob_client.exists():
#         return {
#             "blob_url": blob_client.url
#         }
#     else:
#         return False
