import os
import datetime

INPUT_DIR = "/opt/airflow/data/input"
OUTPUT_DIR = "/opt/airflow/data/output"
CSV_FILENAME = "kamis_all_data.csv"
INPUT_PATH = f"{INPUT_DIR}/{CSV_FILENAME}"

BASE_URL = "https://kamis.kilimo.go.ke/site/market"
MAX_WORKERS = 5
PAGES_TO_CHECK = 5

today = datetime.date.today()
seven_days_ago = today - datetime.timedelta(days=7)

DATE_START = seven_days_ago.strftime('%Y-%m-%d')
DATE_END = today.strftime('%Y-%m-%d')