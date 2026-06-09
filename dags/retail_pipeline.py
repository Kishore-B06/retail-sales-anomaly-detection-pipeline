"""
retail_pipeline.py
==================
Airflow DAG that orchestrates the full retail anomaly detection pipeline.

Tasks (in order):
  1. ingest        → Load CSVs into Snowflake staging tables
  2. dq_checks     → Validate raw data with Great Expectations
  3. transform     → Dedup + rolling avg + anomaly detection (Snowflake SQL)
  4. export        → Pull results and write dashboard-ready JSON

Schedule: daily at 06:00 UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ── Task imports ─────────────────────────────────────────────────────────────
import sys
sys.path.insert(0, '/opt/airflow/tasks')

from ingest import run_ingest
from dq_checks import run_dq_checks
from transform import run_transform
from export import run_export

# ── Default args ─────────────────────────────────────────────────────────────
default_args = {
    'owner': 'retail-de',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id='retail_anomaly_pipeline',
    description='End-to-end retail sales anomaly detection pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='0 6 * * *',   # runs daily at 06:00 UTC
    catchup=False,
    tags=['retail', 'data-engineering', 'anomaly-detection'],
) as dag:

    start = EmptyOperator(task_id='start')

    ingest = PythonOperator(
        task_id='ingest_csvs_to_snowflake',
        python_callable=run_ingest,
        doc_md="""
        **Ingestion Task**
        - Reads 4 CSV files (fact_daily_sales, dim_store, dim_product, dim_date)
        - Truncates staging tables in Snowflake
        - Bulk-inserts all rows using executemany
        - Logs row counts to data_load_log table
        """,
    )

    dq = PythonOperator(
        task_id='data_quality_checks',
        python_callable=run_dq_checks,
        doc_md="""
        **Data Quality Task (Great Expectations)**
        - Checks for nulls in critical columns
        - Validates value ranges (units_sold > 0, net_sales > 0)
        - Checks referential integrity (store_id, product_id exist in dims)
        - Raises exception and halts pipeline if any expectation fails
        """,
    )

    transform = PythonOperator(
        task_id='transform_and_detect_anomalies',
        python_callable=run_transform,
        doc_md="""
        **Transform Task**
        - Deduplicates fact table using ROW_NUMBER() OVER PARTITION BY
        - Writes clean data to fact_daily_sales_clean
        - Computes 7-day rolling average per day
        - Flags SPIKE (>+7%) and DROP (<-7%) days
        - Writes results to anomaly_results table
        """,
    )

    export = PythonOperator(
        task_id='export_dashboard_json',
        python_callable=run_export,
        doc_md="""
        **Export Task**
        - Queries anomaly_results, store performance, category breakdown
        - Writes dashboard_data.json for the frontend dashboard
        - Logs pipeline completion summary
        """,
    )

    end = EmptyOperator(task_id='end')

    # ── Task dependencies (linear pipeline) ──────────────────────────────────
    start >> ingest >> dq >> transform >> export >> end
