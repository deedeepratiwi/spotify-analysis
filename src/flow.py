from prefect import flow, task
from ingest_data import ingest_to_postgres
import os

@task
def ingest_task():
    ingest_to_postgres(
        file_path="data/raw/tracks_features.csv",
        db_url="postgresql://root:root@localhost:5432/spotify",
        table_name="spotify"
    )

@task
def run_dbt_models():
    os.system("dbt run")

@flow(name="spotify_pipeline")
def main_flow():
    ingest_task()
    run_dbt_models()

if __name__ == "__main__":
    main_flow()
