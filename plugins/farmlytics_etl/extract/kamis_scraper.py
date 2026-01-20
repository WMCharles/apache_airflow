from farmlytics_etl.config.settings import INPUT_PATH, DATE_START, DATE_END
from io import StringIO
import pandas as pd
import requests
import logging
import os


def scrape_market_page(page_index):
    offset = page_index * 3000
    url = f"https://kamis.kilimo.go.ke/site/market_search"

    params = {
        "start": DATE_START,
        "end": DATE_END,
        "per_page": 3000
    }

    try:
        logging.info(f"Scraping offset {offset}")
        res = requests.get(url, params=params, timeout=300)
        res.raise_for_status()

        tables = pd.read_html(StringIO(res.text))
        if tables and not tables[0].empty:
            df = tables[0]
            df.to_csv(
                INPUT_PATH,
                mode='a',
                index=False,
                header=not os.path.exists(INPUT_PATH),
                encoding='utf-8'
            )
            return len(df)
    except Exception as e:
        logging.error(f"Scrape error @ {offset}: {e}")

    return 0
