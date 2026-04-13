import datetime
import json
import os

from dotenv import load_dotenv
from openai import AzureOpenAI
from pymongo import MongoClient

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_ENDPOINT = os.getenv("OPENAI_API_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
ARTIFACTS_FOLDER = os.getenv("ARTIFACTS_FOLDER")

dir = os.path.dirname(os.path.abspath(__file__))
artifacts = os.path.join(dir, ARTIFACTS_FOLDER)
folder = os.path.join(artifacts, "property_summary")

os.makedirs(os.path.join(folder, "upload_batches"), exist_ok=True)
os.makedirs(os.path.join(folder, "results"), exist_ok=True)


def get_all_files(folder_path):
    files = []
    for root, dirs, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.endswith(".json"):
                files.append(os.path.join(root, filename))
    return files


def remove_file(file_path):
    try:
        os.remove(file_path)
        print(f"Removed file: {file_path}")
    except Exception as e:
        print(f"Error removing file {file_path}: {e}")


def main():
    if not OPENAI_API_KEY or not OPENAI_API_ENDPOINT or not OPENAI_API_VERSION:
        print("Missing OpenAI Azure configuration in environment.")
        return
    if not MONGODB_CONNECTION_STRING:
        print("Missing MONGODB_CONNECTION_STRING in environment.")
        return

    openai_client = AzureOpenAI(
        azure_endpoint=OPENAI_API_ENDPOINT,
        api_key=OPENAI_API_KEY,
        api_version=OPENAI_API_VERSION,
    )
    mongo_client = MongoClient(MONGODB_CONNECTION_STRING)
    db = mongo_client["prop_main"]
    prop_collection = db["props"]

    files = get_all_files(os.path.join(folder, "upload_batches"))
    if not files:
        print("No summary upload tracking files found.")
        mongo_client.close()
        return

    for file_path in files:
        batch_code = file_path.split("/")[-1].replace("batch-", "").replace(".json", "")

        with open(file_path, "r+", encoding="utf-8") as uploaded_batch_file:
            content = json.loads(uploaded_batch_file.read())
            batch_id = content.get("batch_id")
            if not batch_id:
                print(f"Invalid tracking file (missing batch_id): {file_path}")
                continue

            batch_response = openai_client.batches.retrieve(batch_id)
            status = batch_response.status

            if status == "completed":
                result_file_path = os.path.join(folder, "results", f"batch-{batch_code}-result.json")
                with open(result_file_path, "w", encoding="utf-8") as result_file:
                    result_file.write(batch_response.model_dump_json())

                prop_collection.update_many(
                    {"summary_batch_code": batch_code},
                    {
                        "$set": {
                            "summary_status": "batch_completed",
                            "summary_batch_completed_at": datetime.datetime.now().timestamp(),
                        }
                    },
                )

                remove_file(file_path)
                print(f"Batch {batch_code} completed and results saved: {result_file_path}")
            else:
                content.update(
                    {
                        "status": status,
                        "updated_at": datetime.datetime.now().timestamp(),
                    }
                )
                uploaded_batch_file.seek(0)
                uploaded_batch_file.write(json.dumps(content, indent=2, ensure_ascii=False))
                uploaded_batch_file.truncate()

                if status in ["failed", "cancelled", "expired"]:
                    prop_collection.update_many(
                        {"summary_batch_code": batch_code},
                        {
                            "$set": {
                                "summary_status": "summary_failed",
                                "summary_batch_error": f"Batch terminal state: {status}",
                            }
                        },
                    )

                print(f"Batch {batch_code} is not completed yet, status: {status}")

    mongo_client.close()


if __name__ == "__main__":
    main()
