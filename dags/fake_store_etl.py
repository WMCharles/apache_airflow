import os
import logging
import requests
import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime
from sqlalchemy import inspect, text
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, text, inspect
from sqlalchemy.dialects.postgresql import insert


# ---------------- CONFIG ----------------
API_URL = "https://fakestoreapi.com/products"

BASE_DATA_DIR = "/opt/airflow/data"
INPUT_DIR = os.path.join(BASE_DATA_DIR, "fake_store")
CSV_FILENAME = "fake_store_products.csv"
OUTPUT_PATH = os.path.join(INPUT_DIR, CSV_FILENAME)

POSTGRES_CONN_ID = "farmlytics_postgres"
TABLE_NAME = "fake_store_products"

# ---------------- DAG ----------------
@dag(
    dag_id="fake_store_etl_db_dag",
    description="Fetch Fake Store products, save CSV, and load to DB",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fake_store", "etl", "db"],
)
def fake_store_etl_db():

    @task
    def extract_products() -> list[dict]:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        return response.json()

    @task
    def save_products_csv(products: list[dict]) -> str:
        os.makedirs(INPUT_DIR, exist_ok=True)
        df = pd.DataFrame(products)
        df.to_csv(OUTPUT_PATH, index=False)
        return OUTPUT_PATH

    @task
    def load_to_db(csv_path: str):
        df = pd.read_csv(csv_path)

        if df["rating"].dtype == "object":
            df["rating"] = df["rating"].apply(lambda x: str(x) if isinstance(x, dict) else x)

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        
        # 1. Create table if missing (Keep your existing logic)
        inspector = inspect(engine)
        if not inspector.has_table(TABLE_NAME):
            df.head(0).to_sql(
                TABLE_NAME,
                engine,
                index=False,
                if_exists="replace",
                dtype={
                    "id": Integer,
                    "title": String,
                    "price": Float,
                    "description": String,
                    "category": String,
                    "image": String,
                    "rating": String,
                },
            )
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (id);"))

        # 2. Reflect the table for the upsert logic
        metadata = MetaData()
        # This replaces the broken engine.dialect.get_table call
        table = Table(TABLE_NAME, metadata, autoload_with=engine)

        # 3. Bulk upsert
        with engine.begin() as conn:
            stmt = insert(table).values(df.to_dict(orient="records"))
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "title": stmt.excluded.title,
                    "price": stmt.excluded.price,
                    "description": stmt.excluded.description,
                    "category": stmt.excluded.category,
                    "image": stmt.excluded.image,
                    "rating": stmt.excluded.rating,
                },
            )
            conn.execute(upsert_stmt)

        logging.info(f"Loaded {len(df)} rows into {TABLE_NAME}")

        products = extract_products()
        csv_file = save_products_csv(products)
        load_to_db(csv_file)


fake_store_etl_db()
