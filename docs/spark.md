# Spark job

`jobs/etl_job.py` reads one day, checks that enough rows arrived, tags every line as clean or rejected, computes revenue and an order-level window, and writes three outputs: partitioned Parquet, a dead-letter Parquet file, and `metrics.json`. `--process_date` is required.

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

`groupBy("platform")` is a shuffle. `explain()` shows two `Exchange` operators and `hashpartitioning(..., 8)`, which is `SHUFFLE_PARTITIONS`. `AdaptiveSparkPlan` means AQE is on. `InMemoryTableScan` means the second action read the cache. The raw platform values are still messy: `website` (640), `marketplace` (555), `mobile_app` (538), plus ` Website ` (80), ` Marketplace ` (63), ` Mobile_App ` (62), `fax` (15), and null (15).

## Quality flags and routing

`extract_raw_data(spark, process_date)` builds `raw/orders/dt=<date>/` from the argument, never from today's date, so a rerun of an old day reads that day. The read is PERMISSIVE and ends in `cache()`, which is what lets later steps query `_corrupt_record`. If the count is below `MIN_RECORD_COUNT` (1,000 here), the function raises `ValueError`. A day of 50 lines that "succeeds" would look like a sales collapse. Raising the threshold to 5,000 against 1 July aborts with `Record count 1,968 is below the minimum threshold 5,000`.

`tag_and_route` adds a boolean column per rule, then one `_is_rejected` column, then `rejection_reason`. Both outputs filter on that same column, so a row cannot land in both or in neither.

A comparison with null is null, not false. `quantity <= 0` is null when quantity is null, and a filter keeps a row only when the condition is true, so that row would disappear from both outputs. Each comparison is wrapped in `coalesce(..., false)`. A missing platform is treated as invalid, so that flag defaults to true. `isNull()` is already true or false, so the critical-null flag does not need coalesce.

`rejection_reason` uses the first matching rule. A corrupt line is also full of nulls, and the reason recorded for it is `corrupt_json`.

Platforms are validated after `lower(trim(...))`, so `" Website "` stays. Only `fax` and a null platform are invalid.

Verified split for 1 July: **1,909 clean + 59 rejected = 1,968**.

| `rejection_reason` | Rows |
|---|---:|
| `corrupt_json` | 15 |
| `future_order_date` | 15 |
| `invalid_platform` | 15 |
| `negative_quantity` | 2 |
| `null_critical_field` | 12 |

Clean platforms after normalisation: website 709, marketplace 609, mobile_app 591.

| Date | Total | Clean | Rejected | Reject rate |
|---|---:|---:|---:|---:|
| 2026-07-01 | 1,968 | 1,909 | 59 | 3.00% |
| 2026-07-02 | 1,989 | 1,924 | 65 | 3.27% |
| 2026-07-03 | 2,004 | 1,950 | 54 | 2.69% |
| 2026-07-04 | 2,000 | 1,933 | 67 | 3.35% |
| 2026-07-05 | 2,004 | 1,768 | 236 | 11.78% |

July 5 is above `MAX_REJECT_RATE` (5%). The Airflow gate that refuses to load it comes in a later lab. That day is still raw only. The four normal days below are the ones this job has written.

## Transforms, windows, and writes

`transform_clean` runs on the clean frame. Gross revenue is `quantity * unit_price`, rounded to cents. A null `discount_pct`, `shipping_cost`, or `tax_amount` becomes 0 before the arithmetic, so a missing discount is a full-price line. `platform` is lowercased and trimmed. `payment_method` and `shipping_country` are uppercased. Year, month, day, hour, and `day_of_week` come from `order_date`. Spark's `dayofweek` counts Sunday as 1 and Saturday as 7, and `is_weekend` is true for those two values. 4 July 2026 is a Saturday, so that day's clean rows have `is_weekend` true. 1–3 July are Wednesday through Friday.

The window is `partitionBy("order_id")`. Every product line stays its own row and also receives `order_item_count` and `order_total_revenue`, the sum of `net_revenue` on that order. A `groupBy` would have collapsed the order to one row and lost the line grain `staging.daily_orders` expects. `is_high_value` is true when that order total exceeds `MAX_ORDER_VALUE` (50,000). `etl_processed_at` and `etl_process_date` stamp the run. The internal flags, `rejection_reason`, and `_corrupt_record` are dropped before the write.

Order `ORD-20260701-00000` has three lines. Net revenue is 161.98, 122.85, and 29.71. All three rows show `order_item_count` 3 and `order_total_revenue` 314.54. Across 1 July, 344 orders have one line, 722 have two, and 843 have three. No order that day exceeds 50,000, so `is_high_value` is false on every clean line.

`collect_metrics` counts the raw, clean, and rejected frames, rounds `reject_rate` to four decimals, and sorts the rejection reasons. That dict is the sidecar the later Airflow gate reads.

`write_outputs` does three things:

- Clean rows are repartitioned by year, month, and day, then overwritten as Parquet under `processed/orders/`. `partitionOverwriteMode=dynamic` replaces the partitions present in that DataFrame and leaves the other days in place. Running 1 July a second time left the four-day counts unchanged.
- Rejected rows are coalesced to one file and overwritten under `rejected/orders/dt=<date>/`.
- Metrics go to `metrics/orders/dt=<date>/metrics.json` through boto3. The log line prints an `s3://` URI because that is the key the gate will read. The client still uses `MINIO_ENDPOINT` when it is set.

`--process_date` is required, and `parse_known_args` ignores extra arguments Glue adds, such as `--JOB_NAME`. Omitting the date prints usage and exits.

```bash
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler spark-submit \
  --master "local[2]" /opt/airflow/jobs/etl_job.py --process_date 2026-07-01
```

After 1–4 July, and after the second run of 1 July:

| `order_day` | `is_weekend` | Clean rows |
|---:|---|---:|
| 1 | false | 1,909 |
| 2 | false | 1,924 |
| 3 | false | 1,950 |
| 4 | true | 1,933 |

July 1 metrics: `reject_rate` 0.03, with the same five reason counts as the routing table above. July 2 is 0.0327 (65 rejected), July 3 is 0.0269 (54), July 4 is 0.0335 (67).

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
