from sqlalchemy import text
import logging

def insert_product_market_prices(engine):
    logging.info("Inserting product market prices...")
    query = text("""
        INSERT INTO product_market_prices (
            product_id,
            product_class_id,
            product_grade_id,
            product_sex_id,
            wholesale_price,
            retail_price,
            unit,
            market_id,
            date,
            source_id,
            created_at,
            updated_at
        )
        SELECT
            p.id,
            pc.id,
            pg.id,
            ps.id,
            split_part(krd."Wholesale", '/', 1)::numeric,
            split_part(krd."Retail", '/', 1)::numeric,
            COALESCE(
                split_part(krd."Wholesale", '/', 2),
                split_part(krd."Retail", '/', 2)
            ),
            m.id,
            krd."Date"::date,
            NULL,
            NOW(),
            NOW()
        FROM kamis_raw_data krd
        LEFT JOIN products p ON p.name = krd."Commodity"
        LEFT JOIN product_classes pc ON pc.name = krd."Classification"
        LEFT JOIN product_grades pg ON pg.name = krd."Grade"
        LEFT JOIN product_sexes ps ON ps.name = krd."Sex"
        LEFT JOIN markets m ON m.name = krd."Market"
        ON CONFLICT ON CONSTRAINT pmp_unique_observation 
        DO NOTHING;
    """)
    with engine.begin() as conn:
        conn.execute(query)
