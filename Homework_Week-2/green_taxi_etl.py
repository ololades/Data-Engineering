from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import datetime as dt  # Import the datetime library
import os
import pyarrow.parquet as pq

# Extract Block
@task()
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    try:
        df = pd.read_csv(dataset_url)
        return df
    except Exception as e:
        raise Exception(f"Error fetching data from {dataset_url}: {e}")  # Informative error message

# Transformer block
@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues and perform cleaning"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    df["lpep_pickup_date"] = df["lpep_pickup_datetime"].dt.date
    df["lpep_pickup_date"] = pd.to_datetime(df["lpep_pickup_date"])
    df["pickup_year"] = df["lpep_pickup_datetime"].dt.year
    df["pickup_month"] = df["lpep_pickup_datetime"].dt.month
    df.columns = [col.lower() for col in df.columns]  # Convert column names to snake_case
    df = df[(df["passenger_count"] > 0) & (df["trip_distance"] > 0)]  # Remove rows with zero values

    # Assertions for data quality checks
    assert df["vendorid"].isin([1, 2]).all(), "Assertion failed: vendor_id values not in expected range"
    assert (df["passenger_count"] > 0).all(), "Assertion failed: passenger_count should be greater than 0"
    assert (df["trip_distance"] > 0).all(), "Assertion failed: trip_distance should be greater than 0"

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as a Parquet file"""
    data_folder = Path("data")
    color_folder = data_folder / color  # Create a subdirectory for the color
    try:
        data_folder.mkdir(parents=True, exist_ok=True)
        color_folder.mkdir(parents=True, exist_ok=True)
        path = color_folder / f"{dataset_file}.parquet"
        df.to_parquet(path, compression="gzip")
        return path
    except Exception as e:
        raise Exception(f"Error while writing to the local directory: {e}")
    

@task(log_prints=True)
def write_gcs(df: pd.DataFrame, path_local: Path, partition_date: str) -> None:
    """Write DataFrame to GCS in partitioned Parquet format"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("etl-gcs")

    # Save to GCS without partitioning
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=str(path_local), to_path=Path
    )

    # Save to GCS with partitioning
    partition_path = path_local.parent / f"{partition_date}/{path_local.name}"
    partition_df = df[df['lpep_pickup_datetime'].dt.strftime('%Y-%m-%d') != partition_date]
    partition_df.to_parquet(partition_path, compression='snappy')


@flow(log_prints=True)
def green_taxi_etl() -> None:
    """The main ETL function"""

    color = "green"
    year = 2020
    months = [10, 11, 12]
    
    df_all = pd.DataFrame()  # Initialize an empty DataFrame for concatenation
    
    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month:02}.csv.gz"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}"

        df = fetch(dataset_url)
        df_clean = clean(df)
        df_all = pd.concat([df_all, df_clean], ignore_index=True)  # Concatenate data

        # Moved write_local and write_gcs inside the loop for individual month uploads
    path = write_local(df_all, color, dataset_file)
    write_gcs(df_all, path, partition_date=f"{year}-{months[-1]:02}-01")

# Removed unnecessary print statements and assertions, as they were used for debugging

if __name__ == "__main__":
    green_taxi_etl()  # Call the ETL function