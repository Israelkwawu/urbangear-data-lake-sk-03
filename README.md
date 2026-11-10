# UrbanGear daily sales pipeline

Local batch platform for the SK-03 labs: a daily sales pipeline for UrbanGear, an online retailer taking orders from a website, a mobile app, and a marketplace.

Analysts need yesterday's sales before the 9 AM dashboard. The old process was a person running a dozen SQL scripts by hand at 6 AM. A missed run left the dashboard stale, a broken script could sit unnoticed for days, nothing enforced extract-then-clean-then-load order, and bad rows landed straight in the reports.

This repo replaces that with Airflow (order of work and retries), PySpark (clean millions of order lines), and dbt (tested SQL models). Everything here is the local stand-in for the AWS services used in production.

## Services

| Local service | Production equivalent | What it does | How you reach it |
|---|---|---|---|
| MinIO | Amazon S3 | Data lake. One bucket, `urbangear-data-lake`, with prefixes `raw/`, `processed/`, `rejected/`, and `metrics/`. | Console http://localhost:9001 · code `http://minio:9000` |
| PySpark inside Airflow | AWS Glue | Reads a day's JSON, tags bad rows, writes clean Parquet, rejects, and metrics. | `docker exec` into `airflow-scheduler` |
| Spark master + worker | A Glue cluster | Spark UI for learning the cluster model. Pipeline jobs run in local mode inside Airflow. | http://localhost:8081 |
| Airflow 2.9.3 | Amazon MWAA | Waits for files, runs Spark, applies the quality gate, loads the warehouse, runs dbt, alerts. | http://localhost:8080 |
| Postgres `airflow-db` | MWAA metadata store | DAG runs, task states, connections. Not for analysts. | internal only |
| Postgres `warehouse` | Amazon Redshift | Analytics database. Spark loads `staging.daily_orders`; dbt builds `analytics`. | Windows `localhost:5433` · code `warehouse:5432` |
| dbt 1.8 inside Airflow | dbt Cloud / dbt in CI | Versioned SQL models and tests. | `docker exec ... dbt` |
| An `[ALERT]` log line | Slack | Success summary or failure with task name, date, and log link. | Airflow task logs |

From Windows, use `localhost` and the published port. From inside a container, use the Compose service name (`minio`, `warehouse`). `localhost` inside a container is that container.

## Daily commands

Run these from this folder.

```bash
docker compose up -d                 # start
docker compose ps                    # status; airflow-init Exited (0) is correct
docker compose stop                  # pause, keep data
docker compose down                  # remove containers, keep volumes
docker compose down -v               # delete buckets, warehouse, and Airflow history
docker compose logs -f airflow-scheduler
```

The first `up` builds `sk03-airflow:2.9.3` (Java 17, PySpark 3.5.1, dbt 1.8.10). Later starts reuse that image. Pass `--build` after a Dockerfile change.

Git Bash rewrites `/opt/...` into a Windows path. Prefix container commands with `MSYS_NO_PATHCONV=1`, or use PowerShell.

```bash
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler python /opt/airflow/jobs/setup_buckets.py
```

## Local credentials

These values live in `docker-compose.yml` because this is a laptop lab. Production reads them from a secrets manager.

| System | User | Password |
|---|---|---|
| Airflow | `admin` | `admin` |
| MinIO | `minioadmin` | `minioadmin123` |
| Warehouse database `warehouse` | `dwh_user` | `dwh_pass` |

The warehouse is on host port **5433** so it does not collide with another Postgres on 5432.

## Layout

| Path | Contents |
|---|---|
| `docker-compose.yml` | The platform: images, ports, env vars, volumes. |
| `docker/Dockerfile` | Airflow plus Java, PySpark, the S3A JARs, and dbt. |
| `sql/warehouse_init.sql` | Runs once, when the warehouse volume is first created. Creates `staging`, `analytics`, and `staging.daily_orders`. |
| `jobs/` | Python and PySpark. `setup_buckets.py` and `generate_orders.py` load the lake. `etl_job.py` is the session, schema, and config. `lab02_agg.py` is the cluster UI example. |
| `dags/` | Airflow DAGs. Bind-mounted to `/opt/airflow/dags`. |
| `dbt/` | dbt project and profiles. |
| `logs/` | Airflow logs. Gitignored. |
| `plugins/`, `data/` | Airflow plugins and scratch files. |

`dbt-core` and `dbt-postgres` are pinned to 1.8.10 and 1.8.2. The open `~=1.8.0` range no longer resolves on the Airflow image's pip, and `dbt-postgres` 1.8.1 tries to compile `psycopg2` from source on Linux. 1.8.2 uses `psycopg2-binary`, which the base image already has. dbt 1.8 needs protobuf 5; the Google libraries in the base image expect protobuf 4. Those libraries are unused by this pipeline, and the Airflow webserver still starts.

## Data already loaded

`jobs/setup_buckets.py` creates `urbangear-data-lake` and is safe to run twice.

`jobs/generate_orders.py` writes one JSON Lines file per day to `raw/orders/dt=YYYY-MM-DD/part-00000.json`. The random seed is the date plus the dirty rate, so the same arguments always rebuild the same file. About 3% of lines are dirty (null customer, negative quantity, a date in 2099, platform `fax`, or truncated JSON). 2026-07-05 is the bad day at 12% dirt, used later to trip the 5% quality gate.

```bash
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler python /opt/airflow/jobs/setup_buckets.py
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler python /opt/airflow/jobs/generate_orders.py --process_date 2026-07-01
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler python /opt/airflow/jobs/generate_orders.py --process_date 2026-07-05 --dirty_rate 0.12
```

Do not edit files under MinIO's data directory. Read and write only through the S3 API.

## Documentation

Longer notes live in [docs](docs/README.md): [architecture](docs/architecture.md) (with diagrams), [data lake](docs/data-lake.md), [Spark job](docs/spark.md), [warehouse](docs/warehouse.md), and [operations](docs/operations.md).
