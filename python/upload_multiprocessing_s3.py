import logging
from multiprocessing import Process
import time
from dotenv import load_dotenv
import boto3
import os

load_dotenv()

BUCKET_NAME = os.environ['BUCKET_NAME']
BUCKET_KEY = os.environ['BUCKET_KEY']

client = boto3.client('s3', 
            aws_access_key_id = os.environ['ACCESS_KEY_ID'],
            aws_secret_access_key = os.environ['SECRET_ACCESS_KEY'])

def list_files(directory):
    try:
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

def upload_file_from_folder_process(directory):
    files = list_files(directory)
    processes = []

    for filename in files:
        file_path = os.path.join(directory, filename)
        s3_key = os.path.normpath(os.path.join(BUCKET_KEY, filename)).replace("\\", "/")
        p = Process(target=upload_file, args=(file_path, BUCKET_NAME, s3_key))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

if __name__ == "__main__":
    tic = time.time()
    upload_file_from_folder_process("directory")
    toc = time.time()
    print(f'Elapsed time computing: {toc - tic:.2f} seconds')
    