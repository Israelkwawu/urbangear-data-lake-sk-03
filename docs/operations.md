# Operations

Commands assume the current directory is the project root, the folder that contains `docker-compose.yml`.

## Start and stop

```bash
docker compose up -d                 # start; builds the Airflow image on first run
docker compose ps                    # airflow-init should be Exited (0)
docker compose stop                  # pause, keep volumes
docker compose down                  # remove containers, keep volumes
docker compose down -v               # delete MinIO, the warehouse, and Airflow history
docker compose logs -f airflow-scheduler
```

After a Dockerfile change:

```bash
docker compose up -d --build
```

Compose on this machine may refuse to build and ask for an extra file-read privilege. Build the image directly, then start without another build:

```bash
docker build -t sk03-airflow:2.9.3 -f docker/Dockerfile docker
docker compose up -d --no-build
```

`airflow-webserver` can take a minute to become healthy. `airflow-init` exiting 0 is success: it migrated the metadata database and created `admin`.

## Where to look

| What | URL or address | Login |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| MinIO API, from Windows | http://localhost:9000 | same keys |
| MinIO API, from a container | http://minio:9000 | `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` |
| Spark master UI | http://localhost:8081 | none; expect Alive Workers: 1 |
| Warehouse, from Windows | `localhost:5433`, database `warehouse` | `dwh_user` / `dwh_pass` |
| Warehouse, from a container | `warehouse:5432` | same user |

These passwords are in `docker-compose.yml` on purpose for the lab. Do not reuse them anywhere else.

## Run a job

Python, Spark, and dbt run inside `airflow-scheduler`. Git Bash rewrites a path that starts with `/opt`. Set `MSYS_NO_PATHCONV=1` in Git Bash. PowerShell does not need it.

```bash
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler python /opt/airflow/jobs/setup_buckets.py
MSYS_NO_PATHCONV=1 docker exec -it airflow-scheduler bash
```

Check the tools:

```bash
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler python -c "import pyspark; print(pyspark.__version__)"
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler spark-submit --version
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler dbt --version
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler bash -c 'echo $MINIO_ENDPOINT $WAREHOUSE_HOST'
```

Expected: PySpark 3.5.1, dbt Core 1.8.10 with the postgres plugin 1.8.2, and `http://minio:9000 warehouse`. `spark-submit` may print `ps: command not found` while it looks up the process environment. The version banner after that line is the result that matters.

Try the teaching DAG without the UI. It stays paused, so this command is the way to run it:

```bash
MSYS_NO_PATHCONV=1 docker exec airflow-scheduler airflow dags test urbangear_hello 2026-07-01
```

Look for `[REPORT] date=2026-07-01 files=1` and `state=success`. The daily sales DAG is also paused. Both are described in [Airflow](airflow.md). The MinIO connection it uses is `aws_default`, stored in the Airflow database.

## When something fails

| What you see | What it usually means | What to do |
|---|---|---|
| `docker: command not found` or the daemon is unreachable | Docker Desktop is not running | Start it, wait until the engine is up, open a new terminal |
| `port is already allocated` | Another process has 8080, 5433, 9000, 9001, or 8081 | Find it with `netstat -ano \| findstr :5433`. Stop it, or change only the left side of the port mapping |
| `python: can't open file '/opt/airflow/C:/Program Files/Git/...'` | Git Bash rewrote the container path | `MSYS_NO_PATHCONV=1` in front of `docker exec` |
| `KeyError: 'MINIO_ENDPOINT'` | The script ran on Windows | Run it with `docker exec` into `airflow-scheduler` |
| Connection errors to `localhost:9000` from inside a container | `localhost` there is the container itself | Use `http://minio:9000` |
| `NoSuchBucket` | The bucket script has not been run | `jobs/setup_buckets.py` |
| `staging.daily_orders` is missing | The warehouse volume already existed, so the init SQL never ran | Re-apply `sql/warehouse_init.sql` as in [Warehouse](warehouse.md) |
| Webserver keeps restarting | `airflow-init` failed | `docker compose logs airflow-init`, then `docker compose down` and `docker compose up -d` |
| Line or byte counts differ from [Data lake](data-lake.md) | The generator or `--dirty_rate` changed | Use the script and the dates in that doc |
| File locks while containers run | The project is inside OneDrive or a similar sync folder | Keep the repo on a normal disk path such as `C:\DataEngineering` |
| `invalid null input: name` from `spark-submit` on `spark-master` | The container user has no passwd entry | Recreate the Spark services from this Compose file. They run as root and set `HADOOP_USER_NAME=spark`. See [Spark job](spark.md) |

`docker compose ps` and `docker compose logs <service>` are the first two commands when a container is unhappy.
