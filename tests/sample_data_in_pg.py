
from sqlalchemy import create_engine,text
import pandas as pd
import logging
from dbmerge import format_ms
import time
from datetime import date

logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(message)s")



con_str = """postgresql+psycopg2://postgres:@localhost:5432/dbmerge"""
engine_src = create_engine(con_str)
conn_src = engine_src.connect()

def recreate_test_table():
    start_time = time.perf_counter()
    logger.info('Drop table Facts_source')
    SQL = """
         DROP TABLE IF EXISTS "Facts_source";
     """
    conn_src.execute(text(SQL))
    conn_src.execute('CREATE SCHEMA IF NOT EXISTS "target"')
    conn_src.execute('CREATE SCHEMA IF NOT EXISTS "temp"')
    conn_src.execute('CREATE SCHEMA IF NOT EXISTS "source"')

    logger.info('Create table Facts_source')
    SQL = """
        CREATE TABLE IF NOT EXISTS "Facts_source"
            ("Date" DATE,
            "Product" TEXT,
            "Shop" TEXT,
            "Qty" INT,
            "Price" NUMERIC,
            CONSTRAINT pg_facts_source PRIMARY KEY ("Date","Product","Shop")
        );
    """
    conn_src.execute(text(SQL))
    conn_src.commit()
    
    end_time = time.perf_counter()
    logger.debug('Time: '+format_ms(end_time - start_time))

def generate_test_data(products_no,shops_no):
    start_time = time.perf_counter()
    logger.info(f'Generate data for products_no {products_no} and shops_no {shops_no}')
    SQL = f"""
        WITH
        time_series
        AS
        (
        SELECT 
        generate_series::date "Date"
        FROM generate_series('2024-01-01'::date, '2025-07-31'::date, '1 day'::interval)
        ),

        products
        AS 
        (SELECT 
        'Product' || s.n::varchar "Product"
        FROM generate_series(1,{products_no}) as s(n)),

        shops
        AS 
        (SELECT 
        'Shop' || s.n::varchar "Shop"
        FROM generate_series(1,{shops_no}) as s(n)),

        facts
        AS
        (SELECT
            ts."Date",
            p."Product",
            s."Shop",
            random(1,100)::int "Qty",
            random(50000,150000)/100::numeric "Price"
        FROM products p
        CROSS JOIN shops s
        CROSS JOIN time_series ts
        )

        INSERT INTO "Facts_source"
        SELECT * FROM facts
        ON CONFLICT DO NOTHING;
        ;

    """

    conn_src.execute(text(SQL))
    conn_src.commit()
    end_time = time.perf_counter()
    logger.debug('Time: '+format_ms(end_time - start_time))

def get_data(shops:list=None, start_date: date=None, end_date: date=None, limit:int=None):
    start_time = time.perf_counter()

    if limit is None:
        limit_str = ''
    else:
        limit_str = f'LIMIT {limit}'

    if shops is not None:
        shops_cond = "'" + "','".join(shops) + "'"
        data = pd.read_sql(f"""SELECT * FROM "Facts_source" 
                           WHERE "Shop" in ({shops_cond})
                           ORDER BY "Date","Product","Shop"  {limit_str} """, engine_src)
    elif start_date is not None and end_date is not None:
        data = pd.read_sql(f"""SELECT * FROM "Facts_source" 
                           WHERE "Date" between '{start_date}' and '{end_date}'
                           ORDER BY "Date","Product","Shop" {limit_str} """, engine_src)
    else:
        data = pd.read_sql(f"""SELECT * FROM "Facts_source"
                               ORDER BY "Date","Product","Shop"  {limit_str}""", engine_src)
    
    end_time = time.perf_counter()
    logger.debug(f'Get data: {format_ms(end_time - start_time)}, Shops: {str(shops)}, '+\
                 f'Period: from {start_date} to {end_date}, Limit: {limit}, Count: {len(data)}')
    return data

def get_modified_data(shops:list=None, start_date: date=None, end_date: date=None, limit:int=None):
    start_time = time.perf_counter()

    if limit is None:
        limit_str = ''
    else:
        limit_str = f'LIMIT {limit}'


    if shops is not None:
        shops_cond = "'" + "','".join(shops) + "'"
        where = f""" AND "Shop" in ({shops_cond})"""
    elif start_date is not None and end_date is not None:
        where = f""" AND "Date" between '{start_date}' and '{end_date}' """
    else:
        where = ''

    SQL = f"""
        WITH 
        random_data_updates
        AS
        (
        SELECT 
            "Date",
            "Product",
            "Shop",
            "Qty",
            "Price",
            CASE WHEN random(1,100) % 5 = 0 
                THEN 
                    CASE WHEN random(1,100) % 8 = 0
                    THEN NULL
                    ELSE random(50000,150000)/100 END 
                ELSE "Price" END::NUMERIC "New Price",
            CASE WHEN random(1,100) % 7 = 0 THEN 1 ELSE 0 END::NUMERIC "Delete"
        FROM "Facts_source" ORDER BY "Date","Product","Shop"  {limit_str})

        SELECT
            "Date",
            "Product",
            "Shop",
            "Qty",
            "New Price" "Price"
        FROM random_data_updates
        WHERE "Delete"=0  {where}


        """   
    data = pd.read_sql(text(SQL), engine_src)
    end_time = time.perf_counter()
    logger.debug(f'Get data with update_delete: {format_ms(end_time - start_time)}, Shops: {str(shops)}, '+\
                 f'Period: from {start_date} to {end_date}, Limit: {limit}, Count: {len(data)}')
    return data

if __name__ == '__main__':
    recreate_test_table()

    generate_test_data(50,50)