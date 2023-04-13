import os
import requests
import argparse
from io import BytesIO
from google.cloud import storage
import datetime
import time
from google.cloud.exceptions import GoogleCloudError
from tqdm import tqdm
import math



now = datetime.datetime.now()

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Download taxi trip data and upload to GCS bucket')
parser.add_argument('--year', type=int, default=2019, help='The year of the taxi trip data (default: 2019)')
parser.add_argument('--taxi_type', type=str, default='yellow', help='The type of taxi trip data (default: yellow)')

args = parser.parse_args()
year = args.year
taxi_type = args.taxi_type

# Set GOOGLE_APPLICATION_CREDENTIALS environment variable
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/david/.google/credentials/google_credentials.json"

# GCP bucket name
bucket_name = "nytaxi_raw"

# Initialize the GCP storage client
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)

# Base URL for the Parquet files
url_base = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-"



for m in range(1, 13):
    fn = f"{taxi_type}_tripdata_{year}-{m:02d}.parquet"
    url = url_base + f"{m:02d}.parquet"
    print(now,'loading file: ', fn, 'from url: ', url)

    # Download the Parquet file
    response = requests.get(url, stream=True)

    # Check if the download was successful (HTTP status code 200)
    if response.status_code == 200:
        buffer = BytesIO()
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        for data in tqdm(response.iter_content(block_size), total=math.ceil(total_size//block_size), unit='KB', unit_scale=True):
            buffer.write(data)

        # Reset buffer to the beginning
        buffer.seek(0)

        # Upload the Parquet file to the GCP bucket
        blob = bucket.blob(f'd-{fn}')
        # print(blob.self_link)
        blob.upload_from_file(buffer)
        print(f"Uploaded {fn} to GCP bucket {bucket_name}")
