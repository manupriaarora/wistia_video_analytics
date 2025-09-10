import json
import urllib.parse
import boto3
import os

# Initialize the S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    This Lambda function is triggered by an S3 PutObject event.
    It reads a JSON file containing a list of objects, converts it
    to JSON Lines format, and saves the new file to a 'processed'
    folder in the same S3 bucket.
    """
    try:
        # Get the bucket and file key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

        print(f"Reading file '{key}' from bucket '{bucket_name}'...")

        # Read the file content from S3
        response = s3.get_object(Bucket=bucket_name, Key=key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Load the JSON content
        data = json.loads(file_content)

        # Ensure the top-level element is a list
        if not isinstance(data, list):
            print("Error: The top-level element in the JSON file is not a list. Skipping conversion.")
            return {
                'statusCode': 400,
                'body': json.dumps('File is not a list of objects.')
            }

        # Convert the list of JSON objects to a JSON Lines string
        json_lines_content = ""
        for item in data:
            json_lines_content += json.dumps(item) + '\n'
            
        # Define the new key for the processed file
        # This places the new file in a 'processed' folder with a '.jsonl' extension
        output_key = f"wistia-pipeline/processed/{os.path.basename(key).split('.')[0]}.jsonl"
        
        # Upload the new content to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=output_key,
            Body=json_lines_content,
            ContentType='application/jsonl'
        )

        print(f"Successfully converted and uploaded to '{output_key}'")

        return {
            'statusCode': 200,
            'body': json.dumps('File converted successfully!')
        }

    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'An error occurred: {e}')
        }
