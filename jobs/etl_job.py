"""
UrbanGear Daily Sales ETL Job
==============================
PySpark job: reads raw JSON orders from the data lake, applies quality checks,
routes clean/rejected records, writes Parquet, and emits metrics.

Run locally (inside the Airflow container):
    spark-submit /opt/airflow/jobs/etl_job.py --process_date 2026-07-01
On AWS Glue: deployed as a Glue job; the date is passed as a job argument.
"""

import argparse
import json
import logging
import os
from datetime import datetime

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType,
    TimestampType,
)
from pyspark.sql.window import Window

# ── Logging ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Configuration ───────────────────────────────────────────────
class PipelineConfig:
    """
    All configuration lives here — paths, thresholds, business rules.
    Change environment by swapping this object, not the code.
    """
    # Data-lake paths. s3a:// is Spark's S3 connector (MinIO locally, S3 on AWS).
    S3_BUCKET    = "urbangear-data-lake"
    S3_RAW       = f"s3a://{S3_BUCKET}/raw"
    S3_PROCESSED = f"s3a://{S3_BUCKET}/processed"
    S3_REJECTED  = f"s3a://{S3_BUCKET}/rejected"
    S3_METRICS   = f"s3a://{S3_BUCKET}/metrics"

    # Quality thresholds
    MAX_REJECT_RATE  = 0.05     # Airflow's quality gate stops the load above 5%
    MIN_RECORD_COUNT = 1_000    # local sample is ~2,000 lines/day (production: 100_000)

    # Business rules
    VALID_PLATFORMS = ["website", "mobile_app", "marketplace"]
    MAX_ORDER_VALUE = 50_000.0  # flag orders > $50K for review

    # Spark tuning
    SHUFFLE_PARTITIONS = 8      # small local data (production: 200)


# ── Schema ──────────────────────────────────────────────────────
RAW_ORDER_SCHEMA = StructType([
    StructField("order_id",         StringType(),    nullable=False),
    StructField("customer_id",      StringType(),    nullable=True),
    StructField("platform",         StringType(),    nullable=True),
    StructField("order_date",       TimestampType(), nullable=True),
    StructField("product_id",       StringType(),    nullable=True),
    StructField("product_name",     StringType(),    nullable=True),
    StructField("category",         StringType(),    nullable=True),
    StructField("quantity",         IntegerType(),   nullable=True),
    StructField("unit_price",       DoubleType(),    nullable=True),
    StructField("discount_pct",     DoubleType(),    nullable=True),
    StructField("shipping_cost",    DoubleType(),    nullable=True),
    StructField("tax_amount",       DoubleType(),    nullable=True),
    StructField("payment_method",   StringType(),    nullable=True),
    StructField("shipping_country", StringType(),    nullable=True),
    StructField("shipping_state",   StringType(),    nullable=True),
    StructField("shipping_city",    StringType(),    nullable=True),
    StructField("is_returned",      BooleanType(),   nullable=True),
    # PERMISSIVE mode puts the raw text of unparseable lines here.
    # It MUST be declared in an explicit schema, or Spark silently drops it.
    StructField("_corrupt_record",  StringType(),    nullable=True),
])


def create_spark_session(app_name: str) -> SparkSession:
    """
    Create a SparkSession with production-tuned configuration.
    AQE is enabled: Spark will auto-coalesce shuffle partitions and
    auto-detect skew at runtime.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        # Adaptive Query Execution — let Spark optimize at runtime
        .config("spark.sql.adaptive.enabled",                    "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions",                  str(PipelineConfig.SHUFFLE_PARTITIONS))
        .config("spark.sql.sources.partitionOverwriteMode",      "dynamic")
        # Compression and serialization
        .config("spark.sql.parquet.compression.codec",           "snappy")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.session.timeZone",                    "UTC")
    )
    # Local stack: point the s3a connector at MinIO. (On AWS Glue/EMR these
    # env vars are absent and Spark uses the real S3 endpoint + IAM role.)
    if os.environ.get("MINIO_ENDPOINT"):
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint",          os.environ["MINIO_ENDPOINT"])
            .config("spark.hadoop.fs.s3a.access.key",        os.environ["MINIO_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.secret.key",        os.environ["MINIO_SECRET_KEY"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
        )
    return builder.getOrCreate()


# ── Extract ─────────────────────────────────────────────────────
def extract_raw_data(spark: SparkSession, process_date: str) -> DataFrame:
    """
    Read raw order JSON for one date partition.

    WHY PERMISSIVE mode: we never want the job to crash on a single bad
    record. PERMISSIVE puts the bad line's text in _corrupt_record so we
    can route it to the dead-letter zone.

    WHY fail on low record count: if far fewer records arrive than expected,
    the source export probably failed — not a quiet day. Better to abort
    than to load misleading data.
    """
    raw_path = f"{PipelineConfig.S3_RAW}/orders/dt={process_date}/"
    logger.info(f"Reading raw data from: {raw_path}")

    df = (
        spark.read
        .schema(RAW_ORDER_SCHEMA)
        .option("mode",                      "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(raw_path)
        .cache()   # Spark refuses some queries on _corrupt_record unless the data is cached
    )

    count = df.count()
    logger.info(f"[EXTRACT] {count:,} records read")

    if count < PipelineConfig.MIN_RECORD_COUNT:
        raise ValueError(
            f"Record count {count:,} is below the minimum threshold "
            f"{PipelineConfig.MIN_RECORD_COUNT:,}. "
            "Source export may have failed. Aborting pipeline."
        )
    return df


# ── Quality tagging and routing ─────────────────────────────────
def tag_and_route(df: DataFrame, process_date: str) -> tuple[DataFrame, DataFrame]:
    """
    Tag each record with quality flags and route to clean or rejected.

    Returns: (clean_df, rejected_df)

    Every flag is wrapped in coalesce(..., False/True) so it is never NULL.
    (In SQL, `NULL OR FALSE` is NULL, and a NULL filter keeps the row in
    NEITHER output — it would silently vanish.)
    """
    platform_clean = F.lower(F.trim(F.col("platform")))
    tagged = (
        df
        # Individual quality flags
        .withColumn("_null_critical",
            F.col("order_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("order_date").isNull()
            | F.col("quantity").isNull()
            | F.col("unit_price").isNull()
        )
        .withColumn("_negative_qty",
            F.coalesce(F.col("quantity") <= 0, F.lit(False)))
        .withColumn("_future_date",
            F.coalesce(F.col("order_date") > F.current_timestamp(), F.lit(False)))
        .withColumn("_invalid_platform",            # missing platform counts as invalid
            F.coalesce(~platform_clean.isin(PipelineConfig.VALID_PLATFORMS), F.lit(True)))
        .withColumn("_corrupt_json", F.col("_corrupt_record").isNotNull())
        # Composite rejection flag
        .withColumn("_is_rejected",
            F.col("_null_critical")
            | F.col("_negative_qty")
            | F.col("_future_date")
            | F.col("_invalid_platform")
            | F.col("_corrupt_json")
        )
        # Human-readable rejection reason (first matching rule wins)
        .withColumn("rejection_reason",
            F.when(F.col("_corrupt_json"),     F.lit("corrupt_json"))
             .when(F.col("_null_critical"),    F.lit("null_critical_field"))
             .when(F.col("_negative_qty"),     F.lit("negative_quantity"))
             .when(F.col("_future_date"),      F.lit("future_order_date"))
             .when(F.col("_invalid_platform"), F.lit("invalid_platform"))
             .otherwise(None)
        )
    )

    # Route: clean vs rejected
    rejected_df = tagged.filter(F.col("_is_rejected"))
    clean_df    = tagged.filter(~F.col("_is_rejected"))

    logger.info(f"[QUALITY] Clean: {clean_df.count():,} | Rejected: {rejected_df.count():,}")
    return clean_df, rejected_df


# ── Transform ───────────────────────────────────────────────────
def transform_clean(df: DataFrame, process_date: str) -> DataFrame:
    """Apply business transformations to clean records."""
    window_order = Window.partitionBy("order_id")

    return (
        df
        # Calculated revenue measures
        .withColumn("gross_revenue",
            F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("discount_amount",
            F.round(
                F.col("gross_revenue") * F.coalesce(F.col("discount_pct"), F.lit(0.0)) / 100,
                2
            ))
        .withColumn("net_revenue",
            F.round(F.col("gross_revenue") - F.col("discount_amount"), 2))
        .withColumn("total_amount",
            F.round(
                F.col("net_revenue")
                + F.coalesce(F.col("shipping_cost"), F.lit(0.0))
                + F.coalesce(F.col("tax_amount"),    F.lit(0.0)),
                2
            ))
        # Standardize text fields
        .withColumn("platform",          F.lower(F.trim(F.col("platform"))))
        .withColumn("payment_method",    F.upper(F.trim(F.col("payment_method"))))
        .withColumn("shipping_country",  F.upper(F.trim(F.col("shipping_country"))))
        # Date parts for partitioning
        .withColumn("order_year",  F.year(F.col("order_date")))
        .withColumn("order_month", F.month(F.col("order_date")))
        .withColumn("order_day",   F.dayofmonth(F.col("order_date")))
        .withColumn("order_hour",  F.hour(F.col("order_date")))
        .withColumn("day_of_week", F.dayofweek(F.col("order_date")))
        .withColumn("is_weekend",  F.col("day_of_week").isin(1, 7))
        # Order-level aggregates via window (no groupBy needed)
        .withColumn("order_item_count",
            F.count("*").over(window_order))
        .withColumn("order_total_revenue",
            F.round(F.sum("net_revenue").over(window_order), 2))
        .withColumn("is_high_value",
            F.col("order_total_revenue") > PipelineConfig.MAX_ORDER_VALUE)
        # ETL metadata
        .withColumn("etl_processed_at",  F.current_timestamp())
        .withColumn("etl_process_date",  F.lit(process_date).cast("date"))
        # Drop internal flag columns
        .drop("_null_critical", "_negative_qty", "_future_date",
              "_invalid_platform", "_corrupt_json", "_is_rejected",
              "_corrupt_record", "rejection_reason")
    )


# ── Metrics and write ───────────────────────────────────────────
def collect_metrics(raw_df: DataFrame, clean_df: DataFrame,
                    rejected_df: DataFrame, process_date: str) -> dict:
    """
    Collect quality metrics and return them as a dict.
    This dict is written as a JSON "sidecar" file and read by the
    Airflow quality gate task to decide whether to proceed or abort.
    """
    total    = raw_df.count()
    clean    = clean_df.count()
    rejected = rejected_df.count()

    # Per-rule rejection counts (each rejected row counted once, by its reason)
    rule_counts = {
        row["rejection_reason"]: row["count"]
        for row in rejected_df.groupBy("rejection_reason").count().collect()
    }

    metrics = {
        "process_date":   process_date,
        "total_count":    total,
        "clean_count":    clean,
        "rejected_count": rejected,
        "reject_rate":    round(rejected / total, 4) if total > 0 else 0.0,
        "rule_counts":    dict(sorted(rule_counts.items())),
        "generated_at":   datetime.now().isoformat(),
    }
    logger.info(f"[METRICS] {metrics}")
    return metrics


def write_outputs(clean_df: DataFrame, rejected_df: DataFrame,
                  metrics: dict, process_date: str) -> None:
    """Write clean records, rejected records, and metrics to the data lake."""
    # Clean records: partitioned Parquet. With partitionOverwriteMode=dynamic,
    # "overwrite" replaces ONLY the partitions present in clean_df.
    (
        clean_df
        .repartition(F.col("order_year"), F.col("order_month"), F.col("order_day"))
        .write
        .mode("overwrite")
        .partitionBy("order_year", "order_month", "order_day")
        .parquet(f"{PipelineConfig.S3_PROCESSED}/orders/")
    )
    logger.info(f"[WRITE] Clean data written to {PipelineConfig.S3_PROCESSED}/orders/")

    # Rejected records: one flat folder per run date, for investigation
    n_rejected = rejected_df.count()
    if n_rejected > 0:
        (
            rejected_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .parquet(f"{PipelineConfig.S3_REJECTED}/orders/dt={process_date}/")
        )
        logger.info(f"[WRITE] {n_rejected:,} rejected records written to dead-letter")

    # Metrics sidecar (read by the Airflow quality gate) — written with boto3.
    # MINIO_ENDPOINT is set locally; on AWS it is absent and boto3 uses real S3.
    metrics_key = f"metrics/orders/dt={process_date}/metrics.json"
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY"),
    )
    s3.put_object(Bucket=PipelineConfig.S3_BUCKET, Key=metrics_key,
                  Body=json.dumps(metrics, indent=2))
    logger.info(f"[WRITE] Quality metrics written to s3://{PipelineConfig.S3_BUCKET}/{metrics_key}")


# ── Main ────────────────────────────────────────────────────────
def main(process_date: str) -> None:
    logger.info("=" * 60)
    logger.info("URBANGEAR DAILY SALES ETL")
    logger.info(f"Processing date: {process_date}")
    logger.info(f"Started: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    spark = create_spark_session("urbangear_daily_sales_etl")

    try:
        raw_df                = extract_raw_data(spark, process_date)
        clean_df, rejected_df = tag_and_route(raw_df, process_date)
        transformed_df        = transform_clean(clean_df, process_date)
        metrics               = collect_metrics(raw_df, transformed_df, rejected_df, process_date)
        write_outputs(transformed_df, rejected_df, metrics, process_date)

        logger.info("=" * 60)
        logger.info(f"ETL COMPLETE — {process_date}")
        logger.info(f"Clean: {metrics['clean_count']:,} | Rejected: {metrics['rejected_count']:,} "
                    f"| Rate: {metrics['reject_rate']:.2%}")
        logger.info("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Required on purpose: a batch job must be a pure function of its date.
    # Never default to "yesterday" — backfills and retries need explicit dates.
    parser.add_argument("--process_date", required=True, help="YYYY-MM-DD")
    # parse_known_args: AWS Glue adds its own arguments (e.g. --JOB_NAME); ignore them
    args, _ = parser.parse_known_args()
    main(args.process_date)
