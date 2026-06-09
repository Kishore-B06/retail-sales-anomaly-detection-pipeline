"""
export.py
=========
Task 4 of the retail anomaly pipeline.

Queries the Snowflake results tables and writes a single
dashboard_data.json file that the HTML dashboard reads.

Output file: /opt/airflow/data/dashboard_data.json
"""

import json
import logging
from datetime import datetime
from snowflake_conn import get_connection, fetchall

log = logging.getLogger(__name__)

OUTPUT_PATH = '/opt/airflow/data/dashboard_data.json'


# ── Queries ───────────────────────────────────────────────────────────────────

SQL_TIMELINE = """
SELECT
    sale_date::STRING  AS date,
    weekday,
    daily_sales        AS sales,
    rolling_7day       AS rolling,
    pct_deviation      AS dev,
    anomaly_type       AS type
FROM anomaly_results
ORDER BY sale_date
"""

SQL_STORE_PERF = """
SELECT
    s.store_id,
    s.store_name   AS name,
    s.region,
    s.city,
    SUM(f.net_sales)   AS sales,
    SUM(f.units_sold)  AS units
FROM fact_daily_sales_clean f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY s.store_id, s.store_name, s.region, s.city
ORDER BY sales DESC
"""

SQL_CATEGORY = """
SELECT
    p.category          AS cat,
    SUM(f.net_sales)    AS sales,
    SUM(f.units_sold)   AS units
FROM fact_daily_sales_clean f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY sales DESC
"""

SQL_REGION = """
SELECT
    s.region,
    SUM(f.net_sales) AS sales
FROM fact_daily_sales_clean f
JOIN dim_store s ON f.store_id = s.store_id
GROUP BY s.region
ORDER BY sales DESC
"""

SQL_MONTHLY = """
SELECT
    TO_CHAR(date, 'YYYY-MM') AS month,
    SUM(net_sales)            AS sales
FROM fact_daily_sales_clean
GROUP BY TO_CHAR(date, 'YYYY-MM')
ORDER BY month
"""

SQL_SUMMARY = """
SELECT
    (SELECT COUNT(*) FROM fact_daily_sales)       AS raw_count,
    (SELECT COUNT(*) FROM fact_daily_sales_clean) AS clean_count,
    (SELECT SUM(net_sales) FROM fact_daily_sales_clean) AS total_revenue
"""

SQL_ANOMALY_COUNTS = """
SELECT
    anomaly_type,
    COUNT(*)                                           AS days_count,
    SUM(daily_sales - rolling_7day)                   AS rupees_impact
FROM anomaly_results
WHERE anomaly_type != 'NORMAL'
GROUP BY anomaly_type
"""


def to_float(val):
    """Safely convert Decimal/None to float."""
    return float(val) if val is not None else 0.0


def run_export():
    """
    Main entry point called by Airflow PythonOperator.
    Pulls results from Snowflake and writes dashboard_data.json.
    """
    log.info("=== TASK 4: EXPORT STARTED ===")
    conn = get_connection()

    try:
        log.info("Querying timeline data ...")
        timeline = fetchall(conn, SQL_TIMELINE)
        for row in timeline:
            row['sales']   = to_float(row['sales'])
            row['rolling'] = to_float(row['rolling'])
            row['dev']     = to_float(row['dev'])

        log.info("Querying store performance ...")
        store_data = fetchall(conn, SQL_STORE_PERF)
        for row in store_data:
            row['sales'] = to_float(row['sales'])
            row['units'] = int(row['units']) if row['units'] else 0

        log.info("Querying category breakdown ...")
        cat_data = fetchall(conn, SQL_CATEGORY)
        for row in cat_data:
            row['sales'] = to_float(row['sales'])
            row['units'] = int(row['units']) if row['units'] else 0

        log.info("Querying region breakdown ...")
        region_data = fetchall(conn, SQL_REGION)
        for row in region_data:
            row['sales'] = to_float(row['sales'])

        log.info("Querying monthly trend ...")
        monthly_data = fetchall(conn, SQL_MONTHLY)
        for row in monthly_data:
            row['sales'] = to_float(row['sales'])

        log.info("Querying pipeline summary ...")
        summary_rows = fetchall(conn, SQL_SUMMARY)
        summary = summary_rows[0] if summary_rows else {}
        raw_count     = int(summary.get('raw_count', 0))
        clean_count   = int(summary.get('clean_count', 0))
        total_revenue = to_float(summary.get('total_revenue', 0))

        anomaly_counts = fetchall(conn, SQL_ANOMALY_COUNTS)
        spike_row = next((r for r in anomaly_counts if r['anomaly_type'] == 'SPIKE'), {})
        drop_row  = next((r for r in anomaly_counts if r['anomaly_type'] == 'DROP'),  {})

        # ── Assemble final JSON ───────────────────────────────────────────────
        dashboard_data = {
            'generated_at': datetime.utcnow().isoformat(),
            'raw_count':     raw_count,
            'clean_count':   clean_count,
            'dupes':         raw_count - clean_count,
            'total_revenue': total_revenue,
            'spike_days':    int(spike_row.get('days_count', 0)),
            'drop_days':     int(drop_row.get('days_count', 0)),
            'spike_impact':  to_float(spike_row.get('rupees_impact', 0)),
            'drop_impact':   to_float(drop_row.get('rupees_impact', 0)),
            'timeline':      timeline,
            'store_data':    store_data,
            'cat_data':      cat_data,
            'region_data':   region_data,
            'monthly_data':  monthly_data,
        }

        # ── Write JSON ────────────────────────────────────────────────────────
        with open(OUTPUT_PATH, 'w') as f:
            json.dump(dashboard_data, f, indent=2, default=str)

        log.info(f"Dashboard JSON written to {OUTPUT_PATH}")
        log.info(f"  Raw rows: {raw_count:,} → Clean rows: {clean_count:,}")
        log.info(f"  Dupes removed: {raw_count - clean_count:,}")
        log.info(f"  Total revenue: ₹{total_revenue:,.2f}")
        log.info(f"  Spikes: {spike_row.get('days_count', 0)} days")
        log.info(f"  Drops:  {drop_row.get('days_count', 0)} days")
        log.info("=== TASK 4: EXPORT COMPLETE ===")

    except Exception as e:
        log.error(f"Export failed: {e}")
        raise

    finally:
        conn.close()
