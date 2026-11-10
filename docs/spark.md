# Spark job

`jobs/etl_job.py` is the start of the daily transform. It has logging, `PipelineConfig`, `RAW_ORDER_SCHEMA`, and `create_spark_session()`. Later labs add the quality flags, the Parquet writes, and the metrics file. The imports it does not use yet stay in the file so that growth does not reshuffle the top of the module.

The daily job runs in local mode inside `airflow-scheduler`: driver and executors are one process. The same function on AWS Glue skips the MinIO block, because `MINIO_ENDPOINT` is unset, and Spark uses the real S3 endpoint with an IAM role. Paths in the config use `s3a://`. Glue accepts `s3://`. Open-source Spark needs `s3a://`.

## Why the settings live in one session factory

| Setting | Why |
|---|---|
| Explicit `StructType`, with `order_date` as `TimestampType` | The schema is the contract with the export. Spark parses `2026-07-01T20:05:41` as a timestamp. Inference guessed `string`, and it guessed `quantity` as `long`. |
| `_corrupt_record` on the schema | PERMISSIVE mode stores the raw text of a broken line here. If the field is missing from an explicit schema, Spark drops that text and the line becomes a silent all-null row. |
| `mode=PERMISSIVE` | Keep every line. `DROPMALFORMED` deletes bad lines with no record. `FAILFAST` stops the day on the first broken line. |
| `spark.sql.adaptive.enabled` | Adaptive Query Execution changes the plan while the job runs, using real sizes. |
| `spark.sql.adaptive.coalescePartitions.enabled` | AQE merges tiny shuffle partitions. |
| `spark.sql.shuffle.partitions=8` | A shuffle otherwise uses Spark's default of 200 partitions. That is too many for about 2,000 rows. Production in the guided lab uses 200. |
| `partitionOverwriteMode=dynamic` | A later overwrite replaces the partitions written that day, not the whole table. |
| Parquet codec `snappy` | Fast compression for the clean and rejected output. |
| `KryoSerializer` | Smaller, faster serialization when Spark ships objects between JVMs. |
| Session time zone `UTC` | A timestamp means the same instant on every machine. |
| `fs.s3a.endpoint`, keys, `path.style.access=true` | Only when `MINIO_ENDPOINT` is set. Path-style access is required because the bucket name contains hyphens and MinIO is not AWS. |

`PipelineConfig` holds the paths and the rules in one class: bucket `urbangear-data-lake`, reject rate `0.05`, minimum count `1_000` for this sample (production comment says `100_000`), valid platforms, and `MAX_ORDER_VALUE` of `50000`. Changing environment is a change to this object.

Check the session from the scheduler:

```bash
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler python -c "import sys; sys.path.insert(0,'/opt/airflow/jobs'); from etl_job import create_spark_session; s=create_spark_session('verify'); print('SPARK', s.version, s.conf.get('spark.sql.adaptive.enabled'), s.conf.get('spark.sql.shuffle.partitions'), s.conf.get('spark.sql.sources.partitionOverwriteMode')); s.stop()"
```

Expected: `SPARK 3.5.1 true 8 dynamic`.

## What a read of 2026-07-01 showed

An inferred read sorted columns alphabetically, left `order_date` as a string, and typed `quantity` as `long`. It also scanned the file just to guess.

The explicit read, in PERMISSIVE mode, kept source order. `order_date` is a timestamp and `quantity` is an integer. Spark still prints `order_id` as nullable: a file cannot promise a value, so the quality checks, not the schema, enforce non-null. `count()` is the action that actually reads MinIO. It returned **1,968** lines. The first three lines are order `ORD-20260701-00000`, platform ` Website `, products P100, P600, and P200.

A filter on `_corrupt_record` alone fails. Spark refuses a query that touches only the corrupt-record column on a raw JSON scan, because it would skip parsing. Cache the DataFrame first. After `cache()`, July 1 has **15** corrupt lines, the same count as the plain Python read. Other columns on those rows are null. The original text is in `_corrupt_record`.

`groupBy("platform")` is a shuffle. `explain()` shows two `Exchange` operators and `hashpartitioning(..., 8)`, which is `SHUFFLE_PARTITIONS`. `AdaptiveSparkPlan` means AQE is on. `InMemoryTableScan` means the second action read the cache. The raw platform values are still messy: `website` (640), `marketplace` (555), `mobile_app` (538), plus ` Website ` (80), ` Marketplace ` (63), ` Mobile_App ` (62), `fax` (15), and null (15). The next lab trims and lowercases before it decides a platform is invalid.

## Cluster job and the Spark UI

`jobs/lab02_agg.py` sums `quantity * unit_price` by category across every `raw/orders/` day and runs on the standalone cluster, whose UI is http://localhost:8081. It uses inference on purpose, so the 99 corrupt lines across five days show up as a null category. That revenue still includes dirty rows. The script waits on Enter so the application stays under Running Applications.

```bash
MSYS_NO_PATHCONV=1 docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /opt/jobs/lab02_agg.py
```

The verified totals are Shelter 835402.00 (1,690 lines), Sleeping 509034.00 (1,620), Bags 361759.80 (1,635), Cooking 169032.50 (1,553), Hydration 149551.05 (1,708), Lighting 82538.71 (1,660), and null category 99 lines. While it waits, the master UI shows application `lab02-agg`, one alive worker, and both cores in use. Jobs, stages, and shuffle read/write are on the application detail page. Press Enter to stop it.

The Spark containers run as root. The image's default user is uid 1001, which has no name in `/etc/passwd`. `docker exec` does not pass through the image entrypoint, so the name wrapper never applies, and Hadoop crashes with `invalid null input: name` before it can open `s3a://`. Root has a passwd entry. `HADOOP_USER_NAME=spark` is set as well. This is a lab accommodation. The Airflow containers are unchanged and still run as the `airflow` user.
