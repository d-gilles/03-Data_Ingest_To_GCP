import requests
import argparse
from io import BytesIO
from google.cloud import storage
import datetime
from tqdm import tqdm
import math
from upload_in_chunks import upload_blob_in_chunks_from_buffer

now = datetime.datetime.now()

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Download taxi trip data and upload to GCS bucket')
parser.add_argument('--year', type=int, default=2020, help='The year of the taxi trip data (default: 2019)')
parser.add_argument('--taxi_type', type=str, default='yellow', help='The type of taxi trip data (default: yellow)')

args = parser.parse_args()
year = args.year
taxi_type = args.taxi_type


# GCP bucket name
bucket_name = "nytaxi_raw"

# Initialize the GCP storage client
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)

# Base URL for the Parquet files
url_base = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-"

# Iterate over the months of the year
for month in range(1, 13):
    fn = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url = url_base + f"{month:02d}.parquet"
    print(now,'loading file: ', fn, 'from url: ', url)

    # Download the Parquet file
    response = requests.get(url, stream=True)

    # Check if the download was successful (HTTP status code 200)
    if response.status_code == 200:

        # initate a buffer to store the downloaded file
        buffer = BytesIO()

        # Get the size of the file from the HTTP headers
        total_size = int(response.headers.get('content-length', 0))

        # Define the size of each block to be downloaded, here 1024 KB = 1 MB
        block_size = 1024

        # Iterate over the response data and save it to the buffer, tqdm shows the progress bar
        for data in tqdm(response.iter_content(block_size), total=math.ceil(total_size//block_size), unit='KB', unit_scale=True):
            buffer.write(data)

        print(now,'uploading file: ', fn, 'to GCP bucket: ', bucket_name)

        # create blob object with filename
        blob = bucket.blob(f'd-{fn}')
        buffer.seek(0)  # set the pointer to the beginning of the buffer
        upload_blob_in_chunks_from_buffer(bucket_name, f'd-{fn}', buffer, chunk_size= 5)
