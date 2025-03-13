import logging
import time
from dotenv import load_dotenv
import boto3
import os

from joblib import Parallel, delayed

load_dotenv()

BUCKET_NAME = os.environ['BUCKET_NAME']
BUCKET_KEY = os.environ['BUCKET_KEY']
ACCESS_KEY_ID = os.environ['ACCESS_KEY_ID']
SECRET_ACCESS_KEY = os.environ['SECRET_ACCESS_KEY']


def list_files(directory):
    try: # Get list of all files in the directory
        return os.listdir(directory)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return []

def upload_file(file_path, s3_bucket, s3_key):
    try:
        client = boto3.client('s3', 
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY)
        
        client.upload_file(file_path, s3_bucket, s3_key)
        logging.info(f"File uploaded successfully: {file_path} → s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        logging.error(f"Error uploading {file_path} a S3: {e}")

def upload_file_from_folder_parallel(directory):
    files = list_files(directory)
    
    Parallel(n_jobs=-1)(delayed(upload_file)(
        os.path.join(directory, filename), 
        BUCKET_NAME,
        os.path.normpath(os.path.join(BUCKET_KEY, filename)).replace("\\", "/")
    ) for filename in files)

if __name__ == "__main__":
    tic = time.time()
    upload_file_from_folder_parallel("directory")
    toc = time.time()
    print(f'Elapsed time computing: {toc - tic:.2f} seconds')