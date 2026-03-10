from sqlalchemy import create_engine, text, types
import pandas as pd
import logging
from dbmerge import format_ms
import time
from datetime import date
import sqlite3
import os

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(message)s")

logger = logging.getLogger()
logger.setLevel(level=logging.DEBUG)

# Get the project root directory (parent of tests folder)
script_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(script_dir)
db_path = os.path.join(project_dir, 'data', 'sample_data.sqlite')
con_str = f"sqlite:///{db_path}"
engine_src = create_engine(con_str)
conn_src = engine_src.connect()


def recreate_test_table():
    logger.info('Drop table Facts_source')
    SQL = """
         DROP TABLE IF EXISTS "Facts_source";
     """
    conn_src.execute(text(SQL))

    logger.info('Create table Facts_source')
    SQL = """
        CREATE TABLE IF NOT EXISTS "Facts_source"
            ("Date" DATE,
            "Product" TEXT,
            "Shop" TEXT,
            "Qty" INTEGER,
            "Price" REAL,
            PRIMARY KEY ("Date","Product","Shop")
        );
    """
    conn_src.execute(text(SQL))
    conn_src.commit()

    end_time = time.perf_counter()



def generate_test_data(products_no, shops_no):
    start_time = time.perf_counter()
    logger.info(f'Generate data for products_no {products_no} and shops_no {shops_no}')

    # Generate data in Python and insert via pandas
    import random as py_random
    from datetime import timedelta

    start_date = date(2024, 1, 1)
    end_date = date(2025, 7, 31)

    products = [f'Product{i}' for i in range(1, products_no + 1)]
    shops = [f'Shop{i}' for i in range(1, shops_no + 1)]

    # Generate all dates
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)

    # Generate all combinations
    data = []
    for product in products:
        for shop in shops:
            for d in dates:
                qty = py_random.randint(1, 100)
                price = round(py_random.uniform(500.00, 1500.00), 2)
                data.append({
                    'Date': d,
                    'Product': product,
                    'Shop': shop,
                    'Qty': qty,
                    'Price': price
                })

    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date']).dt.date

    # Insert in chunks to avoid memory issues
    # Use dtype to ensure Date is stored properly in SQLite
    df.to_sql('Facts_source', engine_src, if_exists='append', index=False)

    end_time = time.perf_counter()
    logger.debug('Time: ' + format_ms(end_time - start_time))


def get_data(shops: list = None, start_date: date = None, end_date: date = None, limit: int = None):
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
    
    data['Date'] = pd.to_datetime(data['Date']).dt.date

    end_time = time.perf_counter()
    logger.debug(f'Get data: {format_ms(end_time - start_time)}, Shops: {str(shops)}, ' +
                 f'Period: from {start_date} to {end_date}, Limit: {limit}, Count: {len(data)}')
    
    return data


def get_modified_data(shops: list = None, start_date: date = None, end_date: date = None, limit: int = None):
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
        SELECT
            "Date",
            "Product",
            "Shop",
            "Qty",
            CASE WHEN (ABS(RANDOM()) % 100) + 1 > 95
                THEN
                    CASE WHEN (ABS(RANDOM()) % 100) + 1 > 87
                    THEN NULL
                    ELSE (ABS(RANDOM()) % 100001 + 50000) / 100.0 END
                ELSE "Price" END "Price"
        FROM "Facts_source"
        WHERE "Price" < 1200 {where}
        ORDER BY "Date","Product","Shop"
        {limit_str}

        """

    data = pd.read_sql(text(SQL), engine_src)
    data['Date'] = pd.to_datetime(data['Date']).dt.date

    end_time = time.perf_counter()
    logger.debug(f'Get data with update_delete: {format_ms(end_time - start_time)}, Shops: {str(shops)}, ' +
                 f'Period: from {start_date} to {end_date}, Limit: {limit}, Count: {len(data)}')
    return data


if __name__ == '__main__':
    recreate_test_table()
    generate_test_data(50, 50)
