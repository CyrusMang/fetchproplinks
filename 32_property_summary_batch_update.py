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

os.makedirs(os.path.join(folder, "results"), exist_ok=True)
os.makedirs(os.path.join(folder, "data"), exist_ok=True)
os.makedirs(os.path.join(folder, "backup"), exist_ok=True)


def get_all_files(folder_path):
    files = []
    for root, dirs, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.endswith(".json"):
                files.append(os.path.join(root, filename))
    return files


def move_file(source, destination):
    try:
        os.rename(source, destination)
        print(f"Moved file: {source} -> {destination}")
    except Exception as e:
        print(f"Error moving file {source}: {e}")


def parse_source_id(custom_id):
    if not custom_id:
        return None
    if custom_id.startswith("summary-"):
        return custom_id.replace("summary-", "", 1)
    return None


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

    files = get_all_files(os.path.join(folder, "results"))
    if not files:
        print("No summary result files found.")
        mongo_client.close()
        return

    for file_path in files:
        batch_code = file_path.split("/")[-1].replace("batch-", "").replace("-result.json", "")

        with open(file_path, "r", encoding="utf-8") as result_batch_file:
            result_meta = json.load(result_batch_file)

        output_file_id = result_meta.get("output_file_id")
        input_file_id = result_meta.get("input_file_id")

        if not output_file_id:
            print(f"Missing output_file_id in result meta: {file_path}")
            move_file(file_path, os.path.join(folder, "backup", os.path.basename(file_path)))
            continue

        file_response = openai_client.files.content(output_file_id)
        raw_lines = [line for line in file_response.text.strip().split("\n") if line.strip()]

        data_file_path = os.path.join(folder, "data", f"batch-{batch_code}-data.jsonl")
        with open(data_file_path, "w", encoding="utf-8") as data_file:
            for raw_line in raw_lines:
                data_file.write(f"{raw_line}\n")

        total = 0
        success = 0
        failed = 0

        for raw_line in raw_lines:
            total += 1
            source_id = None
            try:
                row = json.loads(raw_line)
                source_id = parse_source_id(row.get("custom_id"))
                if not source_id:
                    raise Exception("Invalid custom_id, cannot parse source_id")

                if row.get("error"):
                    raise Exception(f"Batch row error: {row['error']}")

                choices = row.get("response", {}).get("body", {}).get("choices", [])
                if not choices:
                    raise Exception("No choices in response")

                content = choices[0].get("message", {}).get("content", "{}")
                summary_json = json.loads(content)

                prop_collection.update_one(
                    {"source_id": source_id},
                    {
                        "$set": {
                            "v1_summary_data": summary_json,
                            "summary_status": "summary_ready",
                            "summary_generated_at": row.get("response", {}).get("body", {}).get("created"),
                            "summary_batch_code": batch_code,
                        }
                    },
                )
                print(f"Updated summary for {source_id}")
                success += 1
            except Exception as e:
                failed += 1
                if source_id:
                    prop_collection.update_one(
                        {"source_id": source_id},
                        {
                            "$set": {
                                "summary_status": "summary_failed",
                                "summary_error": str(e),
                                "summary_batch_code": batch_code,
                            }
                        },
                    )
                print(f"Error processing summary row for {source_id}: {e}")

        print("=" * 50)
        print(f"Batch {batch_code} summary update")
        print(f"Total rows: {total}")
        print(f"Successful: {success}")
        print(f"Failed: {failed}")

        backup_result_path = os.path.join(folder, "backup", os.path.basename(file_path))
        backup_data_path = os.path.join(folder, "backup", os.path.basename(data_file_path))
        move_file(file_path, backup_result_path)
        move_file(data_file_path, backup_data_path)

        try:
            openai_client.files.delete(output_file_id)
            if input_file_id:
                openai_client.files.delete(input_file_id)
            print(f"Deleted OpenAI files for batch {batch_code}")
        except Exception as e:
            print(f"Error deleting OpenAI files for batch {batch_code}: {e}")

    mongo_client.close()


if __name__ == "__main__":
    main()
