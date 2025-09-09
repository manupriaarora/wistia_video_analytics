import requests
import json
import boto3
import time
from botocore.exceptions import ClientError
from datetime import datetime, timezone

# Wistia API Token
SECRET_NAME = "wistia-api-token"
# List of Wistia media IDs to process
MEDIA_IDS = ["gskhw4w4lm", "v08dlrgr7v"]

# S3 bucket and key for the last run timestamp
S3_BUCKET_NAME = "wistia-video-analytics-de-project"
S3_LAST_RUN_KEY = "wistia-pipeline/last_run_timestamp.txt"
# Base Wistia Stats API Endpoint
BASE_URL = "https://api.wistia.com/v1/stats/medias"

# Initialize the S3 client
s3_client = boto3.client('s3')
# Initialize the Secrets Manager client
secrets_client = boto3.client('secretsmanager')

# --- Helper Functions ---

def get_secret(secret_name):
    """
    Retrieves a secret from AWS Secrets Manager.
    """
    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(f"Error retrieving secret '{secret_name}': {e}")
        raise e
    
    # Secrets Manager returns a dictionary. Save the api token in the following format in secrets manager
    # e.g., {"api_token": "YOUR_TOKEN"}
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)['api_token']
    else:
        # If the secret is stored as a binary blob, handle it here.
        raise ValueError("Secret is not stored as a string.")


def load_last_run_timestamp():
    """Loads the last run timestamp from an S3 bucket."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=S3_LAST_RUN_KEY)
        timestamp_str = response['Body'].read().decode('utf-8').strip()
        # Convert ISO 8601 string back to datetime object
        return datetime.fromisoformat(timestamp_str)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Last run timestamp object '{S3_LAST_RUN_KEY}' not found in S3 bucket. Performing a full historical pull.")
            return None
        else:
            print(f"Error loading timestamp from S3: {e}")
            raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def save_current_timestamp():
    """Saves the current timestamp to an S3 bucket for the next run."""
    current_timestamp = datetime.now(timezone.utc)
    timestamp_str = current_timestamp.isoformat()
    try:
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=S3_LAST_RUN_KEY, Body=timestamp_str)
        print(f"Saved new timestamp for next run to S3: {timestamp_str}")
    except Exception as e:
        print(f"Error saving timestamp to S3: {e}")
        raise
    return current_timestamp

def make_api_request(url, headers, params=None, max_retries=5, backoff_factor=1.5):
    """
    Makes a GET request with retry logic and exponential backoff.
    Retries on 429 (Too Many Requests) and 5xx status codes.
    
    Args:
        url (str): The API endpoint URL.
        headers (dict): The authorization headers.
        params (dict, optional): URL parameters.
        max_retries (int): Maximum number of retries.
        backoff_factor (float): The multiplier for the exponential backoff delay.
    
    Returns:
        requests.Response or None: The response object on success, or None on failure.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            
            # If the response is successful or a non-retryable error, return it.
            if 200 <= response.status_code < 300 or response.status_code not in [429, 500, 502, 503, 504]:
                return response
            
            # Log the retryable error and wait
            print(f"Received status code {response.status_code}. Retrying in {backoff_factor**attempt} seconds...")
            time.sleep(backoff_factor**attempt)
            
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}. Retrying...")
            time.sleep(backoff_factor**attempt)
    
    print(f"Failed to fetch data from {url} after {max_retries} attempts.")
    return None

def fetch_media_show(media_id, headers):
    """Fetches media-level stats for a given Wistia media ID."""
    url = f"https://api.wistia.com/v1/medias/{media_id}.json"
    response = make_api_request(url, headers)
    if response and response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching stats for media ID {media_id}: {response.status_code if response else 'No Response'} - {response.text if response else ''}")
        return None
    
def fetch_media_stats(media_id, headers):
    """Fetches media-level stats for a given Wistia media ID."""
    url = f"{BASE_URL}/{media_id}.json"
    response = make_api_request(url, headers)
    if response and response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching stats for media ID {media_id}: {response.status_code if response else 'No Response'} - {response.text if response else ''}")
        return None

def fetch_incremental_data(media_id, last_run_timestamp, headers, prefix):
    """
    Fetches new visitor stats since the last run, handling pagination.
    This function demonstrates the core incremental logic.
    """
    url = f"https://api.wistia.com/v1/stats/{prefix}"
    all_new_data = []
    page = 1
    per_page = 100

    print(f"Starting incremental pull for media ID {media_id} from {last_run_timestamp or 'beginning of time'}.")

    while True:
        params = {"page": page, "per_page": per_page}
        response = make_api_request(url, headers, params=params)
        
        if not response or response.status_code != 200 or not response.json():
            print("--- Reached end of data or encountered an error. ---")
            break

        new_records_on_page = []
        for record in response.json():
            # Wistia timestamps are in UTC, we must parse them correctly
            if prefix == "visitors":
                record_timestamp = datetime.fromisoformat(record['created_at'].replace('Z', '+00:00'))
            else:
                record_timestamp = datetime.fromisoformat(record['received_at'].replace('Z', '+00:00'))

            if last_run_timestamp and record_timestamp <= last_run_timestamp:
                # We have found a record older than or equal to our last run.
                # Since records are returned in reverse chronological order by default,
                # we can stop processing this page and all subsequent pages.
                print(f"--- Found old record from {record_timestamp}. Stopping pull for this media ID. ---")
                break
            
            # This is a new record, add it to our list
            new_records_on_page.append(record)
        
        # Add the new records from this page to our main list
        all_new_data.extend(new_records_on_page)

        # If the inner loop broke, it means we found old data, so we should break the outer loop too.
        if len(new_records_on_page) < per_page:
            # If we didn't fill a full page with new records, we must be at the end of new data
            break

        page += 1
    
    return all_new_data

def upload_data_to_s3(data, media_id, timestamp, prefix):
    """Uploads the retrieved data to an S3 bucket as a JSON file."""
    if not data:
        print(f"No new data to upload for media ID {media_id}.")
        return

    # Create a file name based on the media ID and a timestamp
    file_key = f"wistia-pipeline/raw/{prefix}/{media_id}-{timestamp.strftime('%Y-%m-%d-%H-%M-%S')}.json"
    
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        print(f"Successfully uploaded {len(data)} records to S3 at s3://{S3_BUCKET_NAME}/{file_key}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")
        raise

def upload_media_stats_to_s3(data, media_id, prefix="media-stats"):
    """Uploads the retrieved media stats to an S3 bucket as a JSON file."""
    if not data:
        print(f"No media stats to upload for media ID {media_id}.")
        return

    # Create a file name based on the media ID
    file_key = f"wistia-pipeline/raw/{prefix}/{media_id}.json"
    
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=file_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        print(f"Successfully uploaded media stats to S3 at s3://{S3_BUCKET_NAME}/{file_key}")
    except Exception as e:
        print(f"Error uploading media stats to S3: {e}")
        raise

# --- Main Logic ---
def main():
    """The main function to orchestrate the data pull process."""
    # 1. Retrieve the API token from Secrets Manager
    try:
        api_token = get_secret(SECRET_NAME)
        headers = {"Authorization": f"Bearer {api_token}"}
    except Exception as e:
        print(f"Failed to retrieve API token. Exiting.")
        return # Exit the function

    # 2. Load the last successful run timestamp
    last_run_timestamp = load_last_run_timestamp()

    # Get the current timestamp to use for the output file name
    current_run_timestamp = datetime.now(timezone.utc)

    # 3. Iterate through each media ID
    for media_id in MEDIA_IDS:
        print(f"\n--- Processing Media ID: {media_id} ---")

        # Fetch and upload media-level stats
        media_show = fetch_media_show(media_id, headers)
        if media_show:
            upload_media_stats_to_s3(media_show, media_id, "media-show")

        # Fetch and upload media-level stats
        media_stats = fetch_media_stats(media_id, headers)
        if media_stats:
            upload_media_stats_to_s3(media_stats, media_id)

        # Fetch and upload all new visitor stats since the last run, passing the headers
        new_visitor_stats = fetch_incremental_data(media_id, last_run_timestamp, headers, "visitors")

        if new_visitor_stats:
            # 4. Upload the new data to S3
            upload_data_to_s3(new_visitor_stats, media_id, current_run_timestamp, "visitor-stats")
        else:
            print("No new visitor data found.")
        

        # Fetch and upload all new event stats since the last run, passing the headers
        new_event_stats = fetch_incremental_data(media_id, last_run_timestamp, headers, "events")

        if new_event_stats:
            # 4. Upload the new data to S3
            upload_data_to_s3(new_event_stats, media_id, current_run_timestamp, "event-stats")
        else:
            print("No new events data found.")

    # 5. After all data is processed, save the new timestamp for the next run
    save_current_timestamp()

# --- AWS Lambda Handler ---
def lambda_handler(event, context):
    """
    Entry point for lambda function
    """
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Wistia ingestion process completed successfully!')
    }

if __name__ == "__main__":
    # Call the main function to start the process
    main()
