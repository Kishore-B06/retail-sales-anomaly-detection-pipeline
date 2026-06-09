"""
ingest.py
=========
Task 1 of the retail anomaly pipeline.

What it does:
  - Reads all 4 CSV files from /opt/airflow/data/
  - Truncates the 4 staging tables in Snowflake
  - Bulk-inserts every row using executemany (fast)
  - Logs row counts + load status to data_load_log
"""

import os
import csv
import logging
from datetime import datetime
from snowflake_conn import get_connection, execute, executemany

log = logging.getLogger(__name__)

DATA_DIR = '/opt/airflow/data'

# ── CSV → Snowflake table mapping ─────────────────────────────────────────────
TABLES = {
    'dim_date': {
        'file': 'dim_date.csv',
        'columns': ['date', 'year', 'month', 'day', 'weekday'],
        'insert_sql': """
            INSERT INTO dim_date (date, year, month, day, weekday)
            VALUES (%s, %s, %s, %s, %s)
        """,
    },
    'dim_store': {
        'file': 'dim_store.csv',
        'columns': ['store_id', 'store_name', 'region', 'city', 'store_open_date'],
        'insert_sql': """
            INSERT INTO dim_store (store_id, store_name, region, city, store_open_date)
            VALUES (%s, %s, %s, %s, %s)
        """,
    },
    'dim_product': {
        'file': 'dim_product.csv',
        'columns': ['product_id', 'product_name','category', 'price'],
        'insert_sql': """
            INSERT INTO dim_product (product_id, product_name, category, price)
            VALUES (%s, %s, %s, %s)
        """,
    },
    'fact_daily_sales': {
        'file': 'fact_daily_sales.csv',
        'columns': ['date', 'store_id', 'product_id', 'units_sold',
                    'net_sales', 'transaction_count', 'load_timestamp'],
        'insert_sql': """
            INSERT INTO fact_daily_sales
                (date, store_id, product_id, units_sold,
                 net_sales, transaction_count, load_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
    },
}


def read_csv(filepath, columns):
    """Read a CSV and return list of tuples matching columns order."""
    rows = []
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(tuple(row[c] for c in columns))
    return rows


def log_load(conn, table_name, row_count, status):
    """Write a record to data_load_log."""
    execute(conn, """
        INSERT INTO data_load_log (table_name, load_timestamp, row_count, load_status)
        VALUES (%s, %s, %s, %s)
    """, (table_name, datetime.utcnow().isoformat(), row_count, status))


def run_ingest():
    """
    Main entry point called by Airflow PythonOperator.
    Loads all 4 CSV files into Snowflake.
    """
    log.info("=== TASK 1: INGESTION STARTED ===")
    conn = get_connection()

    try:
        for table_name, cfg in TABLES.items():
            filepath = os.path.join(DATA_DIR, cfg['file'])
            log.info(f"Reading {filepath} ...")

            rows = read_csv(filepath, cfg['columns'])
            log.info(f"  → {len(rows)} rows read from CSV")

            # Truncate first to avoid double-loading on re-runs
            log.info(f"  → Truncating {table_name} ...")
            execute(conn, f"TRUNCATE TABLE {table_name}")

            # Bulk insert
            log.info(f"  → Inserting into {table_name} ...")
            inserted = executemany(conn, cfg['insert_sql'], rows)
            log.info(f"  → {inserted} rows inserted into {table_name}")

            # Log to audit table
            log_load(conn, table_name, len(rows), 'SUCCESS')

        log.info("=== TASK 1: INGESTION COMPLETE ===")

    except Exception as e:
        log.error(f"Ingestion failed: {e}")
        # Log failure
        try:
            log_load(conn, 'UNKNOWN', 0, f'FAILED: {str(e)[:200]}')
        except Exception:
            pass
        raise

    finally:
        conn.close()
