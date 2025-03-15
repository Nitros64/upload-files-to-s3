import logging
import time
import os
import asyncio
import aioboto3
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.environ['BUCKET_NAME']
BUCKET_KEY = os.environ['BUCKET_KEY']

def list_files(directory):
    try:
        return os.listdir(directory)
    except Exception as e:
        logging.error(f"Error listing files in {directory}: {e}")
        return []

async def upload_file(file_path, s3_key):
    session = aioboto3.Session()
    try:
        async with session.client('s3',
            aws_access_key_id=os.environ['ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['SECRET_ACCESS_KEY']) as client:

            await client.upload_file(file_path, BUCKET_NAME, s3_key)
            logging.info(f"File uploaded successfully: {file_path} → s3://{BUCKET_NAME}/{s3_key}")

    except Exception as e:
        logging.error(f"Error uploading {file_path} a S3: {e}")

async def upload_file_from_folder_async(directory):
    files = list_files(directory)

    tasks = [
        upload_file(
            os.path.join(directory, filename), 
            os.path.normpath(os.path.join(BUCKET_KEY, filename)).replace("\\", "/")
        ) for filename in files
    ]

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    tic = time.time()
    asyncio.run(upload_file_from_folder_async("directory"))
    toc = time.time()
    print(f'Elapsed time computing with asyncio: {toc - tic:.2f} seconds')
