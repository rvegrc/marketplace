import psycopg2
import clickhouse_connect
import pandas as pd
import os
import sys
import re
import numpy as np
import json
import logging
from typing import List, Union

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app.log"),          # File output
        logging.StreamHandler()                  # Console output
    ],
    force=True  # This overrides any prior logging config
)


# Connect to PostgreSQL
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

def conn_pg():
    '''Открытие подключения к БД в случае его разрыва'''
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )

# Create SQLAlchemy engine using psycopg2
from sqlalchemy import create_engine, text, Engine
engine = create_engine(f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')

# Load data
logging.info("Load data")
ozon_orders = pd.read_json('../data/test_task_ozon/ozon_orders.json')
logging.info("Data loaded successfully")

logging.info("Creating customers DataFrame")
customers = pd.DataFrame(list(ozon_orders['customer'])).rename(columns={'id': 'customer_id'})
logging.info("Customers DataFrame created successfully")

logging.info("Creating orders DataFrame")
orders = ozon_orders[['order_id', 'status', 'date', 'amount']].copy()
ozon_orders_all = pd.concat([ozon_orders[['order_id', 'status', 'date', 'amount']], customers], axis='columns') \
.rename(columns={'customer_id': 'fk_customer_id'})
orders = ozon_orders_all[['order_id', 'status', 'date', 'amount', 'fk_customer_id']]
logging.info("Orders DataFrame created successfully")

# create tables in PostgreSQL if they do not exist
create_customers_query = '''
        CREATE TABLE  if not exists customers (
            customer_id char(50) PRIMARY KEY,
            region varchar(50)
        );
'''

create_orders_query = '''
    CREATE TABLE IF NOT EXISTS orders (
        order_id BIGINT PRIMARY KEY,  
        status VARCHAR(50),
        date TIMESTAMP,
        amount DOUBLE PRECISION,
        fk_customer_id VARCHAR(50),    
        FOREIGN KEY (fk_customer_id) REFERENCES customers(customer_id)
    );
'''

# check if tables already exist
logging.info("Checking if tables exist in PostgreSQL")
conn = conn_pg()
cur = conn.cursor()
try:
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'customers')")
    customers_exists = cur.fetchone()[0]
    if not customers_exists:
        logging.info("Customers table does not exist, will create it")
        cur.execute(create_customers_query)
    else:
        logging.info("Customers table already exists, skipping creation")

    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'orders')")
    orders_exists = cur.fetchone()[0]
    if not orders_exists:
        logging.info("Orders table does not exist, will create it")
        cur.execute(create_orders_query)
    else:
        logging.info("Orders table already exists, skipping creation")
    conn.commit()
    logging.info("Tables checked and created if necessary")
except Exception as e:
    logging.error(f"Error creating tables: {e}")
    raise
finally:
    conn.close()    

conn_alch = engine.connect()
existing_customer_ids = pd.read_sql(text("SELECT customer_id FROM customers"), conn_alch)
existing_ids_clean = existing_customer_ids['customer_id'].str.strip().values

def batch_insert_dataframe(
    engine: Engine,
    df: pd.DataFrame,
    table_name: str,
    conflict_columns: Union[str, List[str]],
    schema: str = "public"
):
    """
    Batch insert a DataFrame into a database table, skipping duplicate rows.
    
    Args:
        engine: SQLAlchemy engine
        df: DataFrame to insert
        table_name: Target table name
        conflict_columns: Column(s) that define uniqueness constraint
        schema: Database schema (default 'public')
    
    Returns:
        Tuple of (attempted_count, inserted_count)
    """
    if df.empty:
        logging.info(f"No data to insert into {schema}.{table_name}.")
        return (0, 0)

    df = df.copy()
    attempted_count = len(df)

    # Strip whitespace from string columns
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    # Verify conflict columns exist in DataFrame
    if isinstance(conflict_columns, str):
        conflict_columns = [conflict_columns]
    
    missing_cols = [col for col in conflict_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Conflict columns not in DataFrame: {missing_cols}")

    # Build insert query
    columns = list(df.columns)
    column_str = ', '.join(f'"{col}"' for col in columns)
    placeholders = ', '.join([f':{col}' for col in columns])

    # Build conflict clause
    conflict_clause = ""
    if conflict_columns:
        quoted_cols = [f'"{col}"' for col in conflict_columns]
        conflict_clause = f"ON CONFLICT ({', '.join(quoted_cols)}) DO NOTHING"

    insert_query = text(f"""
        INSERT INTO {schema}.{table_name} ({column_str})
        VALUES ({placeholders})
        {conflict_clause}
    """)

    # Convert to list of dicts for executemany
    values = df.to_dict(orient='records')

    with engine.begin() as conn:
        # Get count before insertion
        count_before = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}")).scalar()
        
        # Perform the insertion
        conn.execute(insert_query, values)
        
        # Get count after insertion
        count_after = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}")).scalar()

    inserted_count = count_after - count_before
    skipped_count = attempted_count - inserted_count

    logging.info(
        f"Inserted into {schema}.{table_name}: "
        f"Attempted={attempted_count}, Actual={inserted_count}, "
        f"Skipped={skipped_count}"
    )

    return (attempted_count, inserted_count)

# Insert customers into PostgreSQL
batch_insert_dataframe(
    engine=engine,
    df=customers,
    table_name='customers',
    conflict_columns=['customer_id'],
    schema='public'
)

# Insert orders into PostgreSQL
batch_insert_dataframe(
    engine=engine,
    df=orders,
    table_name='orders',
    conflict_columns=['order_id'],
    schema='public'
)