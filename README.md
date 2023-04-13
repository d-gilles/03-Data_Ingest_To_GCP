# New York Taxi Trip Data Ingestion to GCP BigQuery

This repository contains the scripts for downloading and transforming the New York City taxi trip data and ingesting it into Google Cloud Storage (GCS) and BigQuery. The data includes records for both yellow and green taxis from 2019 and 2020.

## Table of Contents

- [Getting Started](#getting-started)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Scripts](#scripts)
  - [download_data.py](#download_datapy)
  - [transform_data.py](#transform_datapy)
  - [big_query.sql](#big_querysql)
- [Contributing](#contributing)


## Getting Started

These instructions will guide you on how to set up the project on your local machine.

### Prerequisites

- Python 3.7 or later
- A Google Cloud Platform (GCP) account
- GCP credentials file (JSON format)

### Setup

1. Clone the repository.
2. Install the required Python packages:
`pip install -r requirements.txt`
3. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your GCP credentials file (JSON format).

### Scripts

#### download_data.py

This script downloads the taxi trip data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page in Parquet format and uploads it to a specified GCS bucket. The script takes the following command-line arguments:

- `--year`: The year of the taxi trip data (default: 2019)
- `--taxi_type`: The type of taxi trip data (default: yellow)

#### transform_data.py

This script downloads the Parquet files from the GCS bucket, reads them into pandas DataFrames, ensures that the columns and data types match the expected schema, and then uploads the transformed data back to the GCS bucket. The script takes the following command-line arguments:

- `--year`: The year of the taxi trip data (default: 2019)
- `--taxi_type`: The type of taxi trip data (default: yellow)
- `--verbose`: Print verbose output (default: True)
- `--delete`: Delete the input file in bucket file (default: False)

#### big_query.sql

This SQL file contains queries for creating external tables in BigQuery that reference the GCS Parquet files and for creating partitioned and clustered tables in BigQuery. This is to understand the impact of clustering and partitioning on performance and cost.

### Contributing

Contributions to this repository are welcome. Please submit a pull request or open an issue with your suggestions or improvements.
