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
