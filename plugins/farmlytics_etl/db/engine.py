from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from farmlytics_etl.config.settings import *

def get_engine():
    url = URL.create(
        "postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )
    return create_engine(url, client_encoding="utf8")
