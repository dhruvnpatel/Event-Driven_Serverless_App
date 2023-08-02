# [1] "Unzipping files in Python", GeeksForGeeks. [Online], Available: https://www.geeksforgeeks.org/unzipping-files-in-python/ [Accessed: July 17, 2023].
# [2] ""
import boto3
import time
import zipfile
import os

# S3 bucket name
bucket_name = 'sampledatab00891974'

# Path to the zip file
zip_file_path = 'tech.zip'

# Create an S3 client using default credentials
s3_client = boto3.client('s3')

# Extract files from the zip archive
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall()

# Get the list of extracted files
folder_path = os.path.join('tech')
file_paths = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path)]

# Variables for tracking
total_uploaded = 0
start_time = time.time()

# Upload files to S3 bucket with a delay
for file_path in file_paths:
    # Generate a unique key for the S3 object
    key = os.path.basename(file_path)

    # Upload the file to S3 bucket
    s3_client.upload_file(file_path, bucket_name, key)

    total_uploaded += 1
    print(f"Uploaded {file_path} to S3 bucket {bucket_name}")

    # Delay for 100 milliseconds
    time.sleep(0.1)

# Remove the extracted files folder
for file_path in file_paths:
    os.remove(file_path)
os.rmdir(folder_path)

# Calculate and print total uploaded files count and execution time
execution_time = time.time() - start_time
print(f"Total uploaded files: {total_uploaded}")
print(f"Total execution time (seconds): {execution_time}")
