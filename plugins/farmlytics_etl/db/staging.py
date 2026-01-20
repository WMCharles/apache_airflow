from sqlalchemy import text
import logging

def clean_staging_data(engine):
    logging.info("Cleaning staging data")
    query = text("""
        UPDATE kamis_raw_data SET
            "Commodity" = NULLIF("Commodity", '-'),
            "Classification" = NULLIF("Classification", '-'),
            "Grade" = NULLIF("Grade", '-'),
            "Sex" = NULLIF("Sex", '-'),
            "Wholesale" = NULLIF("Wholesale", '-'),
            "Retail" = NULLIF("Retail", '-')
    """)
    with engine.begin() as conn:
        conn.execute(query)
