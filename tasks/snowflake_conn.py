"""
snowflake_conn.py
=================
Shared Snowflake connection helper used by all pipeline tasks.
Reads credentials from environment variables set in docker-compose.yml.
"""

import os
import snowflake.connector


def get_connection():
    """
    Returns an open Snowflake connection.
    Credentials come from environment variables injected by docker-compose.
    """
    conn = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        role=os.environ.get('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
    )
    return conn


def execute(conn, sql, params=None):
    """Run a single SQL statement."""
    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        return cur
    finally:
        cur.close()


def executemany(conn, sql, data):
    """Bulk insert using executemany."""
    cur = conn.cursor()
    try:
        cur.executemany(sql, data)
        conn.commit()
        return cur.rowcount
    finally:
        cur.close()


def fetchall(conn, sql):
    """Run a SELECT and return all rows as list of dicts."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()
