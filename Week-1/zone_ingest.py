
import os
import pandas as pd
from sqlalchemy import create_engine
import argparse


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'taxi+_zone_lookup.csv'

    # Download file
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read Parquet data into DataFrame
    df = pd.read_csv(csv_name)

    # Import data to PostgreSQL
    with engine.begin() as connection:
        df.to_sql(name=table_name, con=connection, if_exists='append')

    # Clean up downloaded file
    os.remove(csv_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to Postgres.')
    parser.add_argument('--user', help='user name for Postgres')
    parser.add_argument('--password', help='password for Postgres')
    parser.add_argument('--host', help='host name for Postgres')
    parser.add_argument('--port', help='port name for Postgres')
    parser.add_argument('--db', help='database name for Postgres')
    parser.add_argument('--table_name', help='name of the table where the results will be written')
    parser.add_argument('--url', help='URL of the Parquet file')

    args = parser.parse_args()

    main(args)






# ### pipeline run in the terminal, to connect the data into the database
# python3 zone_ingest.py \
#     --user=root \
#     --password=root \
#     --host=localhost \
#     --port=5433 \
#     --db=ny_taxi \
#     --table_name=trips_zones \
#     --url=https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
