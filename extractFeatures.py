import boto3
import json
import re
from collections import defaultdict


def lambda_handler(event, context):
    # Step 1: Get the file from the event

    # Get the name of the S3 bucket where the file is located
    bucket_name = event['Records'][0]['s3']['bucket']['name']

    # Get the key (path) of the file in the S3 bucket and extracting file name from the key
    file_key = event['Records'][0]['s3']['object']['key']
    file_name = file_key.split('/')[-1]

    # Remove the '.txt' extension from the file name
    file_name = file_name.replace('.txt', '')

    # Step 2: Extract named entities and create a JSON array

    # Call the 'extract_named_entities' function to extract named entities from the file
    extracted_entities = extract_named_entities(bucket_name, file_key)
    # Creating a JSON array with the file name in the format speicfied
    json_array = {file_name + 'ne': extracted_entities}

    # Step 3: Save the JSON array in the destination bucket (second S3 bucket)
    destination_bucket = 'tagsb00891974'
    # Create the destination key by appending 'ne.txt' to the file name
    destination_key = file_name + 'ne.txt'
    saving_json(destination_bucket, destination_key, json_array)

    return {
        'statusCode': 200,
        'body': 'JSON array created and saved successfully.'
    }


def extract_named_entities(bucket, key):
    # Read the file content from the bucket/key
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    file_content = response['Body'].read().decode('utf-8')

    # Extract named entities and count occurrences
    named_entities = defaultdict(int)
    words = re.findall(r'\b[A-Z][A-Za-z]+\b', file_content)
    for word in words:
        named_entities[word] += 1

    return named_entities


def saving_json(bucket, key, json_array):
    # Save the JSON array to the specified bucket/key
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(json_array)
    )