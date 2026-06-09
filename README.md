# Retail Sales Anomaly Detection Pipeline

## The Problem I Was Solving

I had 6 months of daily retail sales data across 10 stores and 50 products — roughly 90,000 rows. Before I could do any meaningful analysis on it, I noticed two things:

First, the raw data had duplicate records. The same sale for the same store, product, and date appeared more than once. This meant any revenue total I calculated from the raw data was wrong — it was being inflated by those duplicates. I needed to clean this before anything else.

Second, even after cleaning, I wanted to know which days had abnormal sales — not just eyeball it, but detect it systematically. A day that is 10% above its recent trend means something (a promotion, a successful campaign). A day that drops 8% below trend also means something (a stockout, an operational issue). I wanted to flag every such day automatically.

The challenge was to do both of these things reliably, on a schedule, without manual intervention.

---

## What I Built

An automated data pipeline that runs daily and does four things in order:

1. Loads the raw CSV data into Snowflake
2. Validates the data quality before any transformation touches it
3. Deduplicates the data and runs anomaly detection entirely in SQL
4. Exports the clean results

Every step is a separate task in Apache Airflow. If any step fails — for example, if the data quality check catches something unexpected — the pipeline stops there and logs the failure. Nothing bad reaches the next step.

---

## Why I Made the Tool Choices I Did

**Snowflake for storage and transformation:** I could have done the deduplication and anomaly detection in Python, but I deliberately kept that logic in Snowflake SQL. The reason is that SQL window functions — specifically `ROW_NUMBER()` for dedup and a rolling `AVG()` for the baseline — are exactly the right tool for this kind of work. They are readable, fast, and run directly on the data warehouse without pulling everything into memory. It also gave me a chance to demonstrate that I can write analytical SQL at a production level, not just basic queries.

**Airflow for orchestration:** I did not want a pipeline that I have to manually trigger every day. Airflow lets me define the four tasks, set their dependencies, and schedule the whole thing to run at 06:00 UTC daily. If something fails, Airflow retries it and logs exactly what went wrong. This is how real pipelines work in production — they run on a schedule and alert you when something breaks, rather than requiring someone to press a button.

**Docker to run Airflow:** Airflow has many dependencies and setting it up manually on Windows is painful and error-prone. Docker gives me a clean, reproducible environment — anyone can clone this repo, add their Snowflake credentials, and have the pipeline running with two commands. This is also closer to how Airflow is deployed in real environments.

**Python for ingestion and quality checks:** The ingestion task reads the CSV files and bulk-loads them into Snowflake. The quality check task validates the raw data — checking for nulls, value ranges, and duplicate rates — before the transformation runs. I used pandas here because it makes these checks simple and readable, and because catching data issues early (before transformation) is a deliberate architectural decision. You do not want bad data silently flowing through to your analytics layer.

---

## The Technical Work

### Deduplication
The raw data had 433 duplicate rows — the same `(date, store_id, product_id)` combination appearing more than once with different load timestamps. I used `ROW_NUMBER()` partitioned by those three columns and ordered by `load_timestamp DESC`, keeping only the row with rank 1. This dropped 433 rows and brought the dataset from 90,033 to 89,600 clean records. The duplicates were inflating total revenue by approximately 0.5%.

### Anomaly Detection
I computed a 7-day rolling average for daily net sales using a SQL window function:
```sql
AVG(SUM(net_sales)) OVER (
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)
```
This gives each day a baseline — what revenue looked like over the recent week. I then calculated how much each day deviated from that baseline as a percentage. Days above +7% were flagged as SPIKE, days below −7% were flagged as DROP.

I chose 7% as the threshold because it filters out normal day-to-day noise while catching genuinely unusual movements. The result was 12 flagged days across 180 days of data — 7 spikes and 5 drops.

### What the Anomalies Revealed
- The 7 spike days added ₹38.6 Lakh in revenue above the baseline
- The 5 drop days represented ₹23.7 Lakh in lost revenue below the baseline
- Sundays consistently spiked — weekend demand effect
- Mondays consistently dropped — post-weekend fall-off
- Wednesdays spiked repeatedly — likely promotional activity

---

## Results Summary

| Metric | Value |
|---|---|
| Raw rows ingested | 90,033 |
| Duplicates removed | 433 |
| Revenue inflation from duplicates | ~0.5% |
| Clean rows | 89,600 |
| Total revenue H1 2025 | ₹111.2 Crore |
| Anomaly days detected | 12 (7 spikes, 5 drops) |
| Spike revenue impact | +₹38.6 Lakh |
| Drop revenue impact | −₹23.7 Lakh |
| Top category | Electronics — ₹34.6 Crore (31%) |
| Top region | South — ₹33.6 Crore |

---

## Project Structure

```
retail_pipeline/
├── docker-compose.yml              # Runs Airflow locally via Docker
├── requirements.txt                # Python dependencies
├── dags/
│   └── retail_pipeline.py          # DAG definition — 4 tasks, daily at 06:00 UTC
├── tasks/
│   ├── snowflake_conn.py           # Shared Snowflake connection helper
│   ├── ingest.py                   # Task 1: Load CSVs into Snowflake
│   ├── dq_checks.py                # Task 2: Validate raw data before transformation
│   ├── transform.py                # Task 3: Dedup + rolling avg + anomaly detection
│   └── export.py                   # Task 4: Export results to JSON
└── data/
    ├── dim_date.csv
    ├── dim_product.csv
    ├── dim_store.csv
    ├── fact_daily_sales.csv
    └── dashboard_data.json         # Auto-generated on each pipeline run
```

---

## How to Run This

### Prerequisites
- Docker Desktop — docker.com
- Free Snowflake account — snowflake.com
- Git

### Step 1 — Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/retail-anomaly-pipeline.git
cd retail-anomaly-pipeline
```

### Step 2 — Add Snowflake credentials to docker-compose.yml
```yaml
SNOWFLAKE_ACCOUNT: your_account
SNOWFLAKE_USER: your_username
SNOWFLAKE_PASSWORD: your_password
SNOWFLAKE_DATABASE: retail_anomaly_db
SNOWFLAKE_SCHEMA: analytics
SNOWFLAKE_WAREHOUSE: retail_wh
```

### Step 3 — Create tables in Snowflake
```sql
CREATE DATABASE IF NOT EXISTS retail_anomaly_db;
USE DATABASE retail_anomaly_db;
CREATE SCHEMA IF NOT EXISTS analytics;
USE SCHEMA analytics;

CREATE TABLE IF NOT EXISTS dim_store (
    store_id INT, store_name STRING, region STRING,
    city STRING, store_open_date DATE
);
CREATE TABLE IF NOT EXISTS dim_product (
    product_id INT, product_name STRING,
    category STRING, price NUMBER(10,2)
);
CREATE TABLE IF NOT EXISTS dim_date (
    date DATE, year INT, month INT, day INT, weekday STRING
);
CREATE TABLE IF NOT EXISTS fact_daily_sales (
    date DATE, store_id INT, product_id INT, units_sold INT,
    net_sales NUMBER(12,2), transaction_count INT, load_timestamp TIMESTAMP
);
CREATE TABLE IF NOT EXISTS data_load_log (
    table_name STRING, load_timestamp TIMESTAMP,
    row_count INT, load_status STRING
);
```

### Step 4 — Start Airflow
```bash
docker compose up airflow-init   # first time only
docker compose up -d
```

### Step 5 — Trigger the pipeline
1. Open http://localhost:8080
2. Login: admin / admin
3. Find `retail_anomaly_pipeline` → toggle ON → click ▶ Trigger
4. All 4 tasks turn green in 2–5 minutes

### Step 6 — Verify in Snowflake
```sql
SELECT COUNT(*) FROM fact_daily_sales_clean;   -- 89,600
SELECT COUNT(*) FROM anomaly_results;           -- 180
SELECT * FROM anomaly_results WHERE anomaly_type != 'NORMAL' ORDER BY sale_date;  -- 12 rows
```

### Stop the pipeline
```bash
docker compose down
```
