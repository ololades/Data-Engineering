from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import datetime as dt  # Import the datetime library
import os
import pyarrow.parquet as pq

# Extract Block
@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    try:
        df = pd.read_csv(dataset_url)
        return df
    except Exception as e:
        raise Exception(f"Error fetching data from {dataset_url}: {e}")

# Transformer block
@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues and perform cleaning"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    df["lpep_pickup_date"] = df["lpep_pickup_datetime"].dt.date
    df["pickup_year"] = df["lpep_pickup_datetime"].dt.year
    df["pickup_month"] = df["lpep_pickup_datetime"].dt.month
    df.columns = [col.lower() for col in df.columns]  # Convert column names to snake_case
    df = df[(df["passenger_count"] > 0) & (df["trip_distance"] > 0)]  # Remove rows with zero values

    # Assertions for data quality checks
    assert df["vendorid"].isin([1, 2]).all(), "Assertion failed: vendor_id values not in expected range"
    assert (df["passenger_count"] > 0).all(), "Assertion failed: passenger_count should be greater than 0"
    assert (df["trip_distance"] > 0).all(), "Assertion failed: trip_distance should be greater than 0"

    return df

# Task to write locally
@task
def write_local(df: pd.DataFrame, color: str, month: int, year: int) -> Path:
    """Write DataFrame to a local Parquet file"""
    data_folder = Path("data")
    color_folder = data_folder / color
    year_folder = color_folder / f"year={year}"
    month_folder = year_folder / f"month={month}"
    month_folder.mkdir(parents=True, exist_ok=True)

    filename = f"{color}_tripdata_{year}-{month:02}.parquet"
    path = month_folder / filename
    df.to_parquet(path, compression="gzip")
    return path

# # Load Block (cloud)
# @task(log_prints=True)
# def load_to_gcs(df: pd.DataFrame, path: Path, bucket_name: str) -> None:
#     """Write DataFrame to GCS in partitioned Parquet format"""

#     file_path = f"gs://{bucket_name}/{path.name}"
#     partitions = df["lpep_pickup_date"].dt.strftime("%Y-%m-%d")

#     pq.write_to_dataset(
#         df,
#         root_path=file_path,
#         partition_cols=["lpep_pickup_date"],
#         coerce_timestamps="datetime",
#         compression="snappy",  # Optional compression
#     )

# Load Block (local to cloud)
@task(log_prints=True)
def upload_local_to_gcs(local_path: Path, bucket_name: str) -> None:
    """Upload a local Parquet file to GCS"""
    gcs_client = GcsBucket.load("etl-gcs")
    gcs_client.upload_from_path(local_path, f"gs://{bucket_name}/{local_path.name}")

# Main ETL flow
@flow(log_prints=True)
def green_taxi_etl(bucket_name: str) -> None:
    """The main ETL function"""

    color = "green"
    year = 2020
    months = [10, 11, 12]

    # Loop through months and download, clean, write local, and upload to GCS
    for month in months:
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02}.csv.gz"

        try:
            df = fetch(dataset_url)
            df_clean = clean(df)

            # Write locally
            local_path = write_local(df_clean, color, month, year)

            # Upload local file to GCS
            upload_local_to_gcs(local_path, bucket_name)

        except Exception as e:
            print(f"Error processing data for month {month}: {e}")

if __name__ == "__main__":
    bucket_name = "etl_gcs"  # Replace with your actual bucket name
    green_taxi_etl(bucket_name)
