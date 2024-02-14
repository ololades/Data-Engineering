### Ingest data without concatenating
# from pathlib import Path
# import pandas as pd  # Not strictly necessary if not analyzing locally
# from prefect import task, flow
# from prefect_gcp.cloud_storage import GcsBucket
# import requests

# @task
# def fetch_and_save(url, local_dir=Path.cwd() / "data"):
#     """Downloads data from the URL and saves it locally."""
#     response = requests.get(url)
#     response.raise_for_status()  # Raise an error for HTTP errors
#     filename = Path(url).name
#     local_file = local_dir / filename

#     local_dir.mkdir(parents=True, exist_ok=True)  # Create directory if needed
#     with open(local_file, "wb") as f:
#         f.write(response.content)
#     return local_file

# @task
# def write_gcs(filename: str) -> None:
#     """Uploads the local parquet file to GCS."""
#     path = Path.cwd() / f"data/{filename}"  # Assume files are downloaded to data/ dir
#     gcs_bucket = GcsBucket.load("etl-gcs")
#     gcs_bucket.upload_from_path(from_path=str(path), to_path=str(filename))

# @flow(log_prints=True)
# def etl_web_to_gcs_local() -> None:
#     """The main ETL function that also downloads locally."""
#     color = "green"
#     year = 2022
#     months = [f"{month:02}" for month in range(1, 13)]  # Generate month strings

#     for month in months:
#         dataset_file = f"{color}_tripdata_{year}-{month}.parquet"
#         dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"

#         try:
#             local_file = fetch_and_save(dataset_url)
#             write_gcs(dataset_file)
#             print(f"Successfully downloaded {dataset_url} to {local_file}.")
#             print(f"Successfully uploaded {dataset_url} to GCS.")
#         except requests.exceptions.HTTPError as e:
#             print(f"Error downloading {dataset_url}: {e}")

# if __name__ == "__main__":
#     etl_web_to_gcs_local()




from pathlib import Path
import pandas as pd  # Not strictly necessary if not analyzing locally
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
import requests

@task
def fetch_and_save(url, local_dir=Path.cwd() / "data"):
    """Downloads data from the URL and saves it locally."""
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for HTTP errors
    filename = Path(url).name
    local_file = local_dir / filename

    local_dir.mkdir(parents=True, exist_ok=True)  # Create directory if needed
    with open(local_file, "wb") as f:
        f.write(response.content)
    return local_file

@task
def write_gcs(filename: str) -> None:
    """Uploads the local parquet file to GCS."""
    path = Path.cwd() / f"data/{filename}"  # Assume files are downloaded to data/ dir
    gcs_bucket = GcsBucket.load("etl-gcs")
    gcs_bucket.upload_from_path(from_path=str(path), to_path=str(filename), timeout=360)


@flow(log_prints=True)
def etl_web_to_gcs_local() -> None:
    """The main ETL function that also downloads locally."""
    color = "green"
    year = 2022
    months = [f"{month:02}" for month in range(1, 13)]  # Generate month strings

    concatenated_data = pd.DataFrame()  # Initialize an empty DataFrame for concatenation

    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month}.parquet"
        dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"

        try:
            local_file = fetch_and_save(dataset_url)
            print(f"Successfully downloaded {dataset_url} to {local_file}.")
            data = pd.read_parquet(local_file)  # Read the downloaded parquet file
            concatenated_data = pd.concat([concatenated_data, data], ignore_index=True)  # Concatenate the data
            print(f"Successfully concatenated {dataset_url}.")

        except requests.exceptions.HTTPError as e:
            print(f"Error downloading {dataset_url}: {e}")

    # After looping through all months, write the concatenated DataFrame to a local file
    concatenated_filename = f"{color}_tripdata_{year}.parquet"
    concatenated_filepath = Path.cwd() / "data" / concatenated_filename
    concatenated_data.to_parquet(concatenated_filepath)

    # Upload the concatenated file to GCS
    try:
        write_gcs(concatenated_filename)
        print(f"Successfully uploaded concatenated file to GCS.")
    except Exception as e:
        print(f"Error uploading concatenated file to GCS: {e}")

if __name__ == "__main__":
    etl_web_to_gcs_local()