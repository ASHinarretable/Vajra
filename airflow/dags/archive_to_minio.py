"""MinIO raw-event archival DAG.

Runs hourly. Reads raw_transactions from Postgres for the previous complete
hour, converts to Parquet (columnar, compressed), and uploads to MinIO at:

    s3://vajra-raw/year=YYYY/month=MM/day=DD/hour=HH/raw.parquet

Why Parquet on object storage?
  - Append-only raw events become large fast; a database is expensive storage.
  - Parquet is columnar + Snappy-compressed: ~10x smaller than JSON, much
    faster for analytical scans (Spark, Athena, DuckDB can query it directly).
  - MinIO is S3-compatible, so the same boto3 code works against AWS S3 in
    production with only an endpoint-URL change.
"""
from __future__ import annotations

import io
import os
from datetime import datetime, timedelta

try:
    import boto3
    import pandas as pd
    import psycopg2
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False

BUCKET = "vajra-raw"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY",  "vajra")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY",  "vajra2026")

PG = dict(
    host     = os.getenv("POSTGRES_HOST",     "postgres"),
    dbname   = os.getenv("POSTGRES_DB",       "vajra"),
    user     = os.getenv("POSTGRES_USER",     "vajra"),
    password = os.getenv("POSTGRES_PASSWORD", "vajra"),
)


def _s3():
    return boto3.client(
        "s3",
        endpoint_url          = MINIO_ENDPOINT,
        aws_access_key_id     = MINIO_ACCESS,
        aws_secret_access_key = MINIO_SECRET,
    )


def _ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        print(f"[archive] created bucket {BUCKET}")


def archive_hour(**ctx) -> None:
    """Export the previous hour's raw_transactions to MinIO as Parquet."""
    # Airflow logical date = start of the interval being processed.
    logical: datetime = ctx["data_interval_start"]
    hour_start = logical.replace(minute=0, second=0, microsecond=0)
    hour_end   = hour_start + timedelta(hours=1)

    print(f"[archive] exporting {hour_start} -> {hour_end}", flush=True)

    conn = psycopg2.connect(**PG)
    df = pd.read_sql(
        "SELECT * FROM raw_transactions WHERE ingested_at >= %s AND ingested_at < %s",
        conn, params=(hour_start, hour_end),
    )
    conn.close()

    if df.empty:
        print("[archive] no rows for this window — skipping", flush=True)
        return

    # Serialize payload column (JSONB comes back as dict) to string for Parquet.
    if "payload" in df.columns:
        import json
        df["payload"] = df["payload"].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else x
        )

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)

    key = (f"year={hour_start.year}/month={hour_start.month:02d}/"
           f"day={hour_start.day:02d}/hour={hour_start.hour:02d}/raw.parquet")

    s3 = _s3()
    _ensure_bucket(s3)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf)
    print(f"[archive] uploaded s3://{BUCKET}/{key} ({len(df)} rows)", flush=True)


if DEPS_AVAILABLE:
    with DAG(
        dag_id    = "vajra_archive_to_minio",
        description = "Hourly Parquet archival of raw_transactions to MinIO",
        schedule  = "@hourly",
        start_date= datetime(2026, 1, 1),
        catchup   = False,
        default_args = {"owner": "vajra", "retries": 2,
                        "retry_delay": timedelta(minutes=5)},
        tags      = ["vajra", "archival"],
    ) as dag:
        PythonOperator(
            task_id         = "archive_hour_to_parquet",
            python_callable = archive_hour,
        )
