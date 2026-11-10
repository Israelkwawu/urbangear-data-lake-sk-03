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
