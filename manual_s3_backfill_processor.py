import boto3
import json
import os
import urllib.parse
from botocore.exceptions import ClientError

# --- Configuration ---
# Replace with your S3 bucket name and raw data prefix
S3_BUCKET_NAME = "wistia-video-analytics-de-project"
RAW_DATA_PREFIX = "wistia-pipeline/raw/"
PROCESSED_DATA_PREFIX = "wistia-pipeline/processed/"

# Initialize the S3 client
s3 = boto3.client('s3')

def process_existing_files():
    """
    Lists all files in a specific S3 prefix, converts JSON files to JSON Lines
    format, and uploads them to a processed prefix. This function handles files
    that contain either a top-level JSON list or a single JSON object.
    """
    print(f"Starting backfill process for bucket: {S3_BUCKET_NAME}")
    paginator = s3.get_paginator('list_objects_v2')

    file_count = 0
    
    # Use a paginator to handle a large number of files
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=RAW_DATA_PREFIX)

    for page in pages:
        if "Contents" not in page:
            print("No files found in the specified prefix.")
            return

        for obj in page['Contents']:
            key = obj['Key']
            # Skip folders and non-JSON files
            if key.endswith('/') or not key.lower().endswith('.json'):
                continue
            
            file_count += 1
            print(f"Processing file: {key}")

            try:
                # Read the file from S3
                response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                file_content = response['Body'].read().decode('utf-8')
                
                # Load the JSON content
                data = json.loads(file_content)

                # Convert the data to a list for processing, regardless of its original type
                items_to_process = []
                if isinstance(data, list):
                    items_to_process = data
                else:
                    items_to_process = [data]

                # Convert the list of JSON objects to JSON Lines
                json_lines_content = ""
                for item in items_to_process:
                    json_lines_content += json.dumps(item) + '\n'
                
                key = os.path.splitext(key)[0]
                # Define the new key for the processed file
                output_key = key.replace(RAW_DATA_PREFIX, PROCESSED_DATA_PREFIX) + '.jsonl'

                # Upload the new content to S3
                s3.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=output_key,
                    Body=json_lines_content,
                    ContentType='application/jsonl'
                )

                print(f"Successfully converted and uploaded to {output_key}")
            
            except ClientError as e:
                print(f"Error processing {key}: {e}")
            except json.JSONDecodeError:
                print(f"Invalid JSON in file: {key}")
            except Exception as e:
                print(f"An unexpected error occurred with {key}: {e}")

    print(f"\nBackfill process completed. Processed {file_count} files.")

if __name__ == "__main__":
    process_existing_files()
