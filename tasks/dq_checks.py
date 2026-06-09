"""
dq_checks.py - Data Quality Checks using pandas only
"""
import logging
import pandas as pd
from snowflake_conn import get_connection, fetchall

log = logging.getLogger(__name__)

def run_dq_checks():
    log.info("=== TASK 2: DATA QUALITY CHECKS STARTED ===")
    conn = get_connection()
    try:
        rows = fetchall(conn, "SELECT * FROM fact_daily_sales")
        df = pd.DataFrame(rows)
        df.columns = [c.lower() for c in df.columns]
        df['units_sold'] = pd.to_numeric(df['units_sold'], errors='coerce')
        df['net_sales']  = pd.to_numeric(df['net_sales'],  errors='coerce')

        # Check 1: Row count
        assert len(df) >= 1000, f"Too few rows: {len(df)}"
        log.info(f"  ✓ Row count OK: {len(df)}")

        # Check 2: No nulls in key columns
        for col in ['date','store_id','product_id','units_sold','net_sales']:
            nulls = df[col].isnull().sum()
            assert nulls == 0, f"Column {col} has {nulls} nulls"
            log.info(f"  ✓ No nulls in {col}")

        # Check 3: Positive values (units_sold can be 0, net_sales must be positive)
        assert df['units_sold'].min() >= 0, "units_sold has negative values"
        assert df['net_sales'].min()  >= 0, "net_sales has negative values"
        log.info("  ✓ Value ranges OK")

        # Check 4: Duplicate rate below 5%
        total = len(df)
        dupes = total - df.drop_duplicates(subset=['date','store_id','product_id']).shape[0]
        rate  = (dupes / total) * 100
        assert rate <= 5.0, f"Duplicate rate too high: {rate:.2f}%"
        log.info(f"  ✓ Duplicate rate OK: {rate:.2f}%")

        log.info("=== TASK 2: ALL CHECKS PASSED ===")

    except AssertionError as e:
        log.error(f"DATA QUALITY FAILURE: {e}")
        raise
    finally:
        conn.close()