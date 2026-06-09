"""
transform.py
============
Task 3 of the retail anomaly pipeline.

Runs your Snowflake SQL transformations in sequence:
  1. Deduplication  → creates fact_daily_sales_clean
  2. Rolling avg    → 7-day window per date
  3. Anomaly flag   → SPIKE (>+7%) / DROP (<-7%) / NORMAL
  4. Writes results → anomaly_results table (created if not exists)

All SQL here is a direct translation of the Snowflake SQL you wrote,
now executed programmatically by Airflow so it runs on a schedule.
"""

import logging
from snowflake_conn import get_connection, execute, fetchall

log = logging.getLogger(__name__)


# ── SQL Definitions ───────────────────────────────────────────────────────────

# Step 1: Deduplicate using ROW_NUMBER (your exact logic)
SQL_DEDUP = """
CREATE OR REPLACE TABLE fact_daily_sales_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY date, store_id, product_id
               ORDER BY load_timestamp DESC
           ) AS rn
    FROM fact_daily_sales
)
WHERE rn = 1
"""

# Step 2 + 3: Compute rolling avg and flag anomalies, write to results table
SQL_CREATE_ANOMALY_TABLE = """
CREATE TABLE IF NOT EXISTS anomaly_results (
    run_date        TIMESTAMP,
    sale_date       DATE,
    weekday         STRING,
    daily_sales     NUMBER(15,2),
    rolling_7day    NUMBER(15,2),
    pct_deviation   NUMBER(8,4),
    anomaly_type    STRING
)
"""

SQL_TRUNCATE_ANOMALY = "TRUNCATE TABLE anomaly_results"

SQL_INSERT_ANOMALY = """
INSERT INTO anomaly_results
    (run_date, sale_date, weekday, daily_sales, rolling_7day, pct_deviation, anomaly_type)
WITH daily AS (
    SELECT
        f.date,
        d.weekday,
        SUM(f.net_sales) AS daily_sales,
        AVG(SUM(f.net_sales)) OVER (
            ORDER BY f.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7day_avg
    FROM fact_daily_sales_clean f
    JOIN dim_date d ON f.date = d.date
    GROUP BY f.date, d.weekday
),
scored AS (
    SELECT
        date,
        weekday,
        daily_sales,
        rolling_7day_avg,
        ((daily_sales - rolling_7day_avg) / rolling_7day_avg) * 100 AS pct_deviation
    FROM daily
)
SELECT
    CURRENT_TIMESTAMP()           AS run_date,
    date                          AS sale_date,
    weekday,
    daily_sales,
    rolling_7day_avg              AS rolling_7day,
    ROUND(pct_deviation, 4)       AS pct_deviation,
    CASE
        WHEN pct_deviation >  7 THEN 'SPIKE'
        WHEN pct_deviation < -7 THEN 'DROP'
        ELSE 'NORMAL'
    END                           AS anomaly_type
FROM scored
ORDER BY date
"""

# Step 4: Validation queries (logged after transform)
SQL_VALIDATE_COUNTS = """
SELECT
    'RAW'   AS source, COUNT(*) AS row_count, SUM(net_sales) AS total_revenue
FROM fact_daily_sales
UNION ALL
SELECT
    'CLEAN' AS source, COUNT(*) AS row_count, SUM(net_sales) AS total_revenue
FROM fact_daily_sales_clean
"""

SQL_ANOMALY_SUMMARY = """
SELECT
    anomaly_type,
    COUNT(*)                              AS days_count,
    ROUND(SUM(daily_sales - rolling_7day), 2) AS total_rupees_impact
FROM anomaly_results
WHERE anomaly_type != 'NORMAL'
GROUP BY anomaly_type
ORDER BY anomaly_type
"""


def run_transform():
    """
    Main entry point called by Airflow PythonOperator.
    Runs deduplication, rolling average, and anomaly detection in Snowflake.
    """
    log.info("=== TASK 3: TRANSFORM STARTED ===")
    conn = get_connection()

    try:
        # Step 1: Deduplicate
        log.info("Step 1: Deduplicating fact_daily_sales → fact_daily_sales_clean ...")
        execute(conn, SQL_DEDUP)
        log.info("  → Deduplication complete")

        # Step 2: Ensure anomaly_results table exists
        log.info("Step 2: Ensuring anomaly_results table exists ...")
        execute(conn, SQL_CREATE_ANOMALY_TABLE)

        # Step 3: Clear previous run's results
        log.info("Step 3: Truncating previous anomaly_results ...")
        execute(conn, SQL_TRUNCATE_ANOMALY)

        # Step 4: Compute rolling avg + anomaly detection → insert
        log.info("Step 4: Computing 7-day rolling avg and anomaly flags ...")
        execute(conn, SQL_INSERT_ANOMALY)
        log.info("  → Anomaly detection complete, results written to anomaly_results")

        # Step 5: Log validation summary
        log.info("Step 5: Validation summary ...")
        counts = fetchall(conn, SQL_VALIDATE_COUNTS)
        for row in counts:
            log.info(
                f"  [{row['source']}] rows={row['row_count']:,}  "
                f"revenue=₹{float(row['total_revenue']):,.2f}"
            )

        raw_rev  = next(r['total_revenue'] for r in counts if r['source'] == 'RAW')
        clean_rev = next(r['total_revenue'] for r in counts if r['source'] == 'CLEAN')
        inflation = ((float(raw_rev) - float(clean_rev)) / float(clean_rev)) * 100
        log.info(f"  Revenue inflation from duplicates: {inflation:.2f}%")

        anomaly_summary = fetchall(conn, SQL_ANOMALY_SUMMARY)
        for row in anomaly_summary:
            log.info(
                f"  {row['anomaly_type']}: {row['days_count']} days, "
                f"₹{float(row['total_rupees_impact']):,.0f} impact"
            )

        log.info("=== TASK 3: TRANSFORM COMPLETE ===")

    except Exception as e:
        log.error(f"Transform failed: {e}")
        raise

    finally:
        conn.close()
