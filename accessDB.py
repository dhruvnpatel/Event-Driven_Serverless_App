import boto3
import json


def lambda_handler(event, context):
    # Step 1: Get the file from the event

    # Extract the bucket name from the event
    bucket = event['Records'][0]['s3']['bucket']['name']

    # Extract the file key (path) and file name from the event
    file_key = event['Records'][0]['s3']['object']['key']
    file_name = file_key.split('/')[-1]

    # Step 2: Read the JSON file
    json_data = read_json_file(bucket, file_key)

    # Step 3: Update the DynamoDB table
    table_name = 'entitiesb00891974'
    update_DDB(table_name, json_data)

    return {
        'statusCode': 200,
        'body': 'DynamoDB table updated successfully.'
    }


def read_json_file(bucket, key):
    # Read the JSON file content from the bucket/key
    s3_client = boto3.client('s3')

    # Get the object (JSON file) from the specified bucket and key
    response = s3_client.get_object(Bucket=bucket, Key=key)

    # Read the content of the file and convert it to dictionary
    json_content = response['Body'].read().decode('utf-8')
    json_data = json.loads(json_content)

    return json_data


def update_DDB(table_name, json_data):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    for _, entities in json_data.items():   # Iterate over the top-level items (keys) in the JSON data
        for entity, count in entities.items():  # Iterate over the nested items (keys and values) in each top-level item
            table.update_item(
                Key={'key': entity},
                # Define the update expression to increment the 'value' attribute by the 'count' value
                UpdateExpression='SET #value = if_not_exists(#value, :initial) + :count',
                ExpressionAttributeNames={'#value': 'value'},
                ExpressionAttributeValues={':initial': 0, ':count': count},
            )