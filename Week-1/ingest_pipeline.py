
import os #to download file
import pandas as pd 
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time
import argparse


def main(params):
      user=params.user
      password=params.password
      host=params.host
      port=params.port
      db=params.db
      table_name=params.table_name
      url = params.url
      parquet_name ='output.parquet'
      
      os.system(f"wget {url} -O {parquet_name}") #download file
      
      engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
      df_iter = pq.ParquetFile(parquet_name).iter_batches(batch_size=200000)

      df = next(df_iter)
      df = df.to_pandas()
      df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
      df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
      
      df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
      df.to_sql(name = table_name, con=engine, if_exists='append')
      
      while True:
            t_start = time()
            try:
                  df = next(df_iter)
            except StopIteration:
                  break

            df = df.to_pandas()
            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
            df.to_sql(name='ny_taxi', con=engine, if_exists='append')
            
            t_end = time()
            print('Inserted another chunk..., took %.3f seconds' % (t_end - t_start))

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









