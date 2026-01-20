from sqlalchemy import text
import logging

def create_products(engine):
    logging.info("Creating missing products...")
    query = text("""
        INSERT INTO products (name, created_at, updated_at)
        SELECT DISTINCT
            krd."Commodity",
            NOW(),
            NOW()
        FROM kamis_raw_data krd
        LEFT JOIN products p ON p.name = krd."Commodity"
        WHERE
            krd."Commodity" IS NOT NULL
            AND krd."Commodity" != '-'
            AND p.id IS NULL
    """)
    with engine.begin() as conn:
        conn.execute(query)

def create_product_classes(engine):
    logging.info("Creating missing product classes...")
    query = text("""
        INSERT INTO product_classes (name, created_at, updated_at)
        SELECT DISTINCT
            krd."Classification",
            NOW(),
            NOW()
        FROM kamis_raw_data krd
        LEFT JOIN product_classes pc ON pc.name = krd."Classification"
        WHERE
            krd."Classification" IS NOT NULL
            AND krd."Classification" != '-'
            AND pc.id IS NULL
    """)
    with engine.begin() as conn:
        conn.execute(query)

def create_product_grades(engine):
    logging.info("Creating missing product grades...")
    query = text("""
        INSERT INTO product_grades (name, created_at, updated_at)
        SELECT DISTINCT
            krd."Grade",
            NOW(),
            NOW()
        FROM kamis_raw_data krd
        LEFT JOIN product_grades pg ON pg.name = krd."Grade"
        WHERE
            krd."Grade" IS NOT NULL
            AND krd."Grade" != '-'
            AND pg.id IS NULL
    """)
    with engine.begin() as conn:
        conn.execute(query)

def create_product_sexes(engine):
    logging.info("Creating missing product sexes...")
    query = text("""
        INSERT INTO product_sexes (name, created_at, updated_at)
        SELECT DISTINCT
            krd."Sex",
            NOW(),
            NOW()
        FROM kamis_raw_data krd
        LEFT JOIN product_sexes ps ON ps.name = krd."Sex"
        WHERE
            krd."Sex" IS NOT NULL
            AND krd."Sex" != '-'
            AND ps.id IS NULL
    """)
    with engine.begin() as conn:
        conn.execute(query)

def create_markets(engine):
    logging.info("Creating markets")
    query = text("""
        INSERT INTO markets (county_id, name, created_at, updated_at)
        SELECT DISTINCT
            c.id,
            krd."Market",
            NOW(),
            NOW()
        FROM kamis_raw_data krd
        LEFT JOIN markets m ON m.name = krd."Market"
        LEFT JOIN counties c ON c.name = krd."County"
        WHERE
            krd."Market" IS NOT NULL
            AND krd."Market" != '-'
            AND m.id IS NULL
    """)
    with engine.begin() as conn:
        conn.execute(query)