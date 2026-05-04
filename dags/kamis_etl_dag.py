from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from farmlytics_etl.extract.kamis_scraper import scrape_market_page
from farmlytics_etl.db.facts import insert_product_market_prices
from farmlytics_etl.load.csv_loader import load_csv_to_staging
from farmlytics_etl.db.staging import clean_staging_data
from farmlytics_etl.utils.logger import setup_logger
from concurrent.futures import ThreadPoolExecutor
from farmlytics_etl.db.dimensions import *
from farmlytics_etl.config.settings import (
    INPUT_DIR,
    OUTPUT_DIR,
    MAX_WORKERS,
    PAGES_TO_CHECK,
)
from airflow.decorators import dag, task
from datetime import datetime
import logging
import shutil
import re
import os

default_args = {
    'owner': 'masinde',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': 10  # 10 seconds
}

@dag(
    dag_id="kamis_etl_dag_v3",
    description="ETL DAG for Kamis market data",
    schedule="@daily",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=True,
    tags=["kamis", "etl", "farmlytics"],
)

def kamis_etl_dag():

    @task
    def extract():
        context = get_current_context()
        run_id = re.sub(r'[^\w\-]', '_', context['run_id'])
        input_path = os.path.join(INPUT_DIR, f"kamis_{run_id}.csv")

        setup_logger()
        os.makedirs(INPUT_DIR, exist_ok=True)

        if os.path.exists(input_path):
            os.remove(input_path)

        logging.info("Starting scrape")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            list(executor.map(lambda p: scrape_market_page(p, input_path), range(PAGES_TO_CHECK)))

        if not os.path.exists(input_path):
            raise ValueError("No data scraped")

        return input_path

    @task
    def load_to_db(input_path: str):
        hook = PostgresHook(postgres_conn_id="farmlytics_postgres")
        engine = hook.get_sqlalchemy_engine()

        load_csv_to_staging(engine, input_path)
        clean_staging_data(engine)

        create_products(engine)
        create_product_classes(engine)
        create_product_grades(engine)
        create_product_sexes(engine)
        create_markets(engine)

        insert_product_market_prices(engine)

        return input_path

    @task
    def archive(input_path: str):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = f"{OUTPUT_DIR}/kamis_{ts}.csv"
        shutil.move(input_path, dest)
        logging.info(f"Archived CSV to {dest}")

    path = extract()
    loaded = load_to_db(path)
    archive(loaded)


kamis_etl_dag()
