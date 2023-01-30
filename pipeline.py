
#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine


def ingest_data(user, password, host, port, db, table_name,table_name_2, url,url_2):
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    csv_name_2="ny_zones.csv"

    os.system(f"wget {url} -O {csv_name}")
    os.system(f"wget {url_2} -O {csv_name_2}")

    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(postgres_url)
    
    if engine.connect():
        print("Connected to Database")

        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.tlep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.to_sql(name=table_name, con=engine, if_exists='append')


        while True: 

            try:
                t_start = time()
                
                df = next(df_iter)

                df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

                df.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()

                print('inserted another chunk, took %.3f second' % (t_end - t_start))

            except StopIteration:
                print("Finished ingesting data into the postgres database")
                break

        
        print(f"Creating table: {table_name_2}")
        zones=pd.read_csv(csv_name_2)
        zones.columns=zones.columns.str.lower()

        zones.head(n=0).to_sql(name=table_name_2, con=engine, if_exists='replace')
        zones.to_sql(name=table_name_2, con=engine, if_exists='append')


    else:
        print("Connection failed")

    

    

    

if __name__ == '__main__':
    user = "root"
    password = "root"
    host = "localhost"
    port = "5433"
    db = "ny_taxi_hw"
    table_name = "taxi_data_ny"
    table_name_2="ny_zones"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
    csv_url_2="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

    ingest_data(user, password, host, port, db, table_name,table_name_2 , csv_url,csv_url_2)