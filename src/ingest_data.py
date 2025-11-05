#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine

file_path = "C:/Users/diana/jupyter_notebooks/spotify-analysis/data/raw/tracks_features.csv"
db_url = 'postgresql://root:root@localhost:5432/spotify'
engine = create_engine('postgresql://root:root@localhost:5432/spotify')
table_name = 'spotify'
    
def ingest_to_postgres(file_path, db_url, table_name, chunksize=100000):
    engine = create_engine(db_url)
    df_iter = pd.read_csv(file_path, iterator=True, chunksize=chunksize)
    df = next(df_iter)
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df.head(0).to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            df = next(df_iter)
            df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
            df.to_sql(name=table_name, con=engine, if_exists='append')
            print("Inserted another chunk...")
        except StopIteration:
            print("Ingestion completed.")
            break




