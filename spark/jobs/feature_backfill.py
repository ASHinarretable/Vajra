"""Vajra batch feature backfill (PySpark).

The streaming consumer computes per-user features approximately, from limited
in-memory state on a single instance. This batch job reads the FULL history
from Postgres and computes accurate, expensive baselines:

  * customer_profile  — per-user spending baseline + fraud rate
  * merchant_risk     — per-merchant volume, fraud rate, risk tier

These curated tables are the "golden" baseline: the streaming layer can read
them to warm-start a user's average instead of starting cold, and dashboards
can rank risky merchants. This is the batch layer of a lambda architecture.

Run:  docker compose run --rm spark
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "vajra")
PG_USER = os.getenv("POSTGRES_USER", "vajra")
PG_PW = os.getenv("POSTGRES_PASSWORD", "vajra")

URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PROPS = {"user": PG_USER, "password": PG_PW, "driver": "org.postgresql.Driver"}


def main() -> None:
    spark = SparkSession.builder.appName("vajra-feature-backfill").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def read(table):
        return spark.read.jdbc(URL, table, properties=PROPS)

    proc = read("processed_transactions").cache()
    fraud = read("fraud_events").cache()

    n_proc, n_fraud = proc.count(), fraud.count()
    print(f"[backfill] read processed={n_proc} fraud_events={n_fraud}", flush=True)

    # ---- per-user fraud counts (distinct txns, since one txn can fire many rules)
    fraud_by_user = fraud.groupBy("user_id").agg(
        F.countDistinct("transaction_id").alias("fraud_txns")
    )

    # ---- customer_profile -------------------------------------------------
    customer = (
        proc.groupBy("user_id")
        .agg(
            F.first("user_name", ignorenulls=True).alias("user_name"),
            F.count("*").alias("txn_count"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.round(F.stddev_pop("amount"), 2).alias("std_amount"),
            F.round(F.expr("percentile_approx(amount, 0.95)"), 2).alias("p95_amount"),
            F.max("amount").alias("max_amount"),
            F.countDistinct("city").alias("distinct_cities"),
            F.countDistinct("device").alias("distinct_devices"),
            F.min("event_time").alias("first_seen"),
            F.max("event_time").alias("last_seen"),
        )
        .join(fraud_by_user, "user_id", "left")
        .fillna({"fraud_txns": 0})
        .withColumn(
            "fraud_rate_pct",
            F.round(100.0 * F.col("fraud_txns") / F.col("txn_count"), 2),
        )
        .withColumn("computed_at", F.current_timestamp())
    )

    # ---- merchant_risk ----------------------------------------------------
    # fraud_events has no merchant column -> join back to processed for it.
    fraud_with_merchant = fraud.join(
        proc.select("transaction_id", "merchant"), "transaction_id", "left"
    )
    fraud_by_merchant = fraud_with_merchant.groupBy("merchant").agg(
        F.countDistinct("transaction_id").alias("fraud_txns")
    )

    # Group by merchant ONLY (a merchant maps to one category; using the modal
    # category keeps this 1:1 with the fraud-by-merchant join, so rates can't
    # exceed 100%).
    merchant = (
        proc.groupBy("merchant")
        .agg(
            F.mode("merchant_category").alias("merchant_category"),
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
        .join(fraud_by_merchant, "merchant", "left")
        .fillna({"fraud_txns": 0})
        .withColumn(
            "fraud_rate_pct",
            F.round(100.0 * F.col("fraud_txns") / F.col("txn_count"), 2),
        )
        .withColumn(
            "risk_tier",
            F.when(F.col("fraud_rate_pct") >= 25, F.lit("HIGH"))
            .when(F.col("fraud_rate_pct") >= 10, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW")),
        )
        .withColumn("computed_at", F.current_timestamp())
    )

    # ---- write curated tables (overwrite = full recompute each run) --------
    (customer.write.jdbc(URL, "customer_profile", mode="overwrite", properties=PROPS))
    (merchant.write.jdbc(URL, "merchant_risk", mode="overwrite", properties=PROPS))

    print(f"[backfill] wrote customer_profile rows={customer.count()} "
          f"merchant_risk rows={merchant.count()}", flush=True)

    print("[backfill] top 5 riskiest merchants:", flush=True)
    (merchant.orderBy(F.col("fraud_rate_pct").desc())
             .select("merchant", "txn_count", "fraud_txns", "fraud_rate_pct", "risk_tier")
             .show(5, truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
