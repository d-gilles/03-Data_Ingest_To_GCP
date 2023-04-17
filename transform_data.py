
import os
import pandas as pd
from google.cloud import storage
from io import BytesIO
import argparse

# Set GOOGLE_APPLICATION_CREDENTIALS environment variable
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/david/.google/credentials/google_credentials.json"

# GCP bucket name
bucket_name = "nytaxi_raw"

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Download taxi trip data and upload to GCS bucket')
parser.add_argument('--year', type=int, default=2019, help='The year of the taxi trip data (default: 2019)')
parser.add_argument('--taxi_type', type=str, default='yellow', help='The type of taxi trip data (default: yellow)')
parser.add_argument('--verbose', type=bool, default=True, help='Print verbose output (default: True)')
parser.add_argument('--delete', type=bool, default=False, help='Delete the downloaded file (default: False)')

args = parser.parse_args()
year = args.year
taxi_type = args.taxi_type
verbose = args.verbose
delete = args.delete

# Initialize the GCP storage client
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)

# Required columns and data types
if taxi_type == 'yellow':
    required_columns = {
    'VendorID': 'int64',
    'tpep_pickup_datetime': 'datetime64[ns]',
    'tpep_dropoff_datetime': 'datetime64[ns]',
    'passenger_count': 'float64',
    'trip_distance': 'float64',
    'RatecodeID': 'float64',
    'store_and_fwd_flag': 'string',
    'PULocationID': 'int64',
    'DOLocationID': 'int64',
    'payment_type': 'int64',
    'fare_amount': 'float64',
    'extra': 'float64',
    'mta_tax': 'float64',
    'tip_amount': 'float64',
    'tolls_amount': 'float64',
    'improvement_surcharge': 'float64',
    'total_amount': 'float64',
    'congestion_surcharge': 'float64',
    'airport_fee': 'float64'
    }

if taxi_type == 'green':
    required_columns = {
    'VendorID': 'int64',
    'lpep_pickup_datetime': 'datetime64[ns]',
    'lpep_dropoff_datetime': 'datetime64[ns]',
    'store_and_fwd_flag': 'string',
    'RatecodeID': 'float64',
    'PULocationID': 'int64',
    'DOLocationID': 'int64',
    'passenger_count': 'float64',
    'trip_distance': 'float64',
    'fare_amount': 'float64',
    'extra': 'float64',
    'mta_tax': 'float64',
    'tip_amount': 'float64',
    'tolls_amount': 'float64',
    'ehail_fee': 'float64',
    'improvement_surcharge': 'float64',
    'total_amount': 'float64',
    'payment_type': 'float64',
    'trip_type': 'float64',
    'congestion_surcharge': 'float64'
    }

def check_dtypes_match(df, required_columns):
    for column, expected_dtype in required_columns.items():
        if df[column].dtype != expected_dtype:
            return False
    return True

# Loop through each month
for m in range(1, 13):
    input_fn = f"d-{taxi_type}_tripdata_{year}-{m:02d}.parquet"
    output_fn = f"{taxi_type}_tripdata_{year}-{m:02d}.parquet"

    # Download the Parquet file from the GCP bucket
    blob = bucket.blob(input_fn)
    buffer = BytesIO()
    blob.download_to_file(buffer)
    if verbose:
        print(f"Downloaded {input_fn} from GCP bucket")
    buffer.seek(0)

    # Read the Parquet file using pandas
    df = pd.read_parquet(buffer)
    if verbose:
        print(f"Read {input_fn} into pandas dataframe")

    # Check columns and data types and update data types if necessary
    for column, dtype in required_columns.items():
        if column not in df.columns or df[column].dtype.name != dtype:
            df[column] = df[column].astype(dtype)
    if verbose:
        print(f"Updated {input_fn} columns and data types")

    # Test if dtypes match, if so, save the transformed data to the GCP bucket
    if check_dtypes_match(df, required_columns):
        upload_buffer = df.to_parquet(None, engine='pyarrow', index=False)
        blob = bucket.blob(output_fn)
        blob.upload_from_string(upload_buffer)
        if verbose:
            print(f"Uploaded {output_fn} to GCP bucket")

        # Delete the downloaded file from the GCP bucket
        if delete:
            blob_to_delete = bucket.blob(input_fn)
            blob_to_delete.delete()
            if verbose:
                print(f"Deleted {input_fn} from GCP bucket")
        else:
            if verbose:
                print(f"Did not delete {input_fn} from GCP bucket")
