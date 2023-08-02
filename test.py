import boto3
import time

region_name = "us-east-1"
s3_bucket_name = "sampledatab00891974"
tags_bucket_name = "tagsb00891974"
ddb_table_name = "entitiesb00891974"

s3_client = boto3.client("s3", region_name=region_name)
lambda_client = boto3.client("lambda", region_name=region_name)
ddb_client = boto3.client("dynamodb", region_name=region_name)

def upload_file_to_s3(file_path, file_key):
    print(f"Uploading file '{file_path}' to S3 with key '{file_key}'")
    s3_client.upload_file(file_path, s3_bucket_name, file_key)

def get_file_content_from_s3(bucket, file_key):
    print(f"Retrieving content of file '{file_key}' from S3 bucket '{bucket}'")
    response = s3_client.get_object(Bucket=bucket, Key=file_key)
    content = response["Body"].read().decode("utf-8")
    return content

def get_ddb_item(key):
    print(f"Retrieving item with key '{key}' from DynamoDB table '{ddb_table_name}'")
    response = ddb_client.get_item(TableName=ddb_table_name, Key={"key": {"S": key}})
    return response.get("Item", None)

def test():
    # Replace 'test.txt' with the actual name of your test file
    file_path = "test.txt"
    file_key = "001.txt"  # Replace with the desired key name in the S3 bucket

    upload_file_to_s3(file_path, file_key)

    time.sleep(5)

    ne_file_key = "001ne.txt"
    ne_file_content = get_file_content_from_s3(tags_bucket_name, ne_file_key)
    print(f"Named Entities File Contents: {ne_file_content}")

    time.sleep(5)

    item = get_ddb_item("Asia")
    if item:
        value = item["value"]["N"]
        print(f'Value for key "Asia" in DynamoDB: {value}')
    else:
        print('Key "Asia" not found in DynamoDB')

if __name__ == "__main__":
    test()