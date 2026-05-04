from farmlytics_etl.config.settings import INPUT_PATH, DATE_START, DATE_END
from io import StringIO
import pandas as pd
import requests
import logging
import urllib3
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def scrape_market_page(page_index, output_path=INPUT_PATH):
    offset = page_index * 3000
    url = f"https://kamis.kilimo.go.ke/site/market_search"

    params = {
        "start": DATE_START,
        "end": DATE_END,
        "per_page": 3000
    }

    try:
        logging.info(f"Scraping offset {offset}")
        res = requests.get(url, params=params, timeout=300, verify=False)
        res.raise_for_status()

        tables = pd.read_html(StringIO(res.text))
        if tables and not tables[0].empty:
            df = tables[0]
            df.to_csv(
                output_path,
                mode='a',
                index=False,
                header=not os.path.exists(output_path),
                encoding='utf-8'
            )
            return len(df)
    except Exception as e:
        logging.error(f"Scrape error @ {offset}: {e}")

    return 0
