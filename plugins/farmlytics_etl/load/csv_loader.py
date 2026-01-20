import pandas as pd
import logging

def load_csv_to_staging(engine, csv_path):
    logging.info("Loading CSV to staging")
    df = pd.read_csv(csv_path).drop_duplicates()
    df.to_sql('kamis_raw_data', engine, if_exists='replace', index=False)
