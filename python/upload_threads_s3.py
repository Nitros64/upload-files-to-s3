from concurrent.futures import ThreadPoolExecutor
import logging
from threading import Thread
import time
from dotenv import load_dotenv
import boto3
import os

load_dotenv()

BUCKET_NAME = os.environ['BUCKET_NAME']
BUCKET_KEY = os.environ['BUCKET_KEY']
client = boto3.client(
    's3', 
    aws_access_key_id = os.environ['ACCESS_KEY_ID'],
    aws_secret_access_key = os.environ['SECRET_ACCESS_KEY'])

def list_files(directory):
    try: # Get list of all files in the directory
        return os.listdir(directory)
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def upload_file(file_path, s3_bucket, s3_key):
    try:
        client.upload_file(file_path, s3_bucket, s3_key)
        logging.info(f"File uploaded successfully: {file_path} → s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        logging.error(f"Error uploading {file_path} a S3: {e}")

def upload_file_from_folder(directory):
    files = list_files(directory)
    for filename in files:
        filePath = f"{directory}/{filename}"
        s3_key = os.path.normpath(os.path.join(BUCKET_KEY, filename)).replace("\\", "/")
        client.upload_file(filePath,BUCKET_NAME,s3_key,)

def upload_file_from_folder_ThreadPoolExecutor(directory):
    files = list_files(directory)    
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = []
        for filename in files:
            file_path = os.path.join(directory, filename)
            s3_key = os.path.normpath(os.path.join(BUCKET_KEY, filename)).replace("\\", "/")
            futures.append(executor.submit(upload_file, file_path, BUCKET_NAME, s3_key))

        for future in futures:
            future.result()

def upload_file_from_folder_threads(directory):
    files = list_files(directory)
    threads = []
    for filename in files:
        file_path = os.path.join(directory, filename)
        s3_key = os.path.normpath(os.path.join(BUCKET_KEY, filename)).replace("\\", "/")
        t = Thread(target=upload_file, args=(file_path, BUCKET_NAME, s3_key))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == "__main__":
    tic = time.time()
    upload_file_from_folder_threads("directory")
    toc = time.time()
    print(f'Elapsed time computing: {toc - tic:.2f} seconds')
    