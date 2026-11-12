# Documentation

| Document | What it answers |
|---|---|
| [Architecture](architecture.md) | Which containers exist, how a sales day moves through them, and which AWS service each one stands in for. |
| [Data lake](data-lake.md) | The bucket, the four zones, the JSON Lines contract, and the five days already loaded. |
| [Spark job](spark.md) | The session, the explicit schema, the quality flags, the windowed transforms, the three lake writes, and the cluster UI job. |
| [Airflow](airflow.md) | The teaching DAG, and the daily sales DAG that waits for a file and runs Spark. |
| [Warehouse](warehouse.md) | `staging` and `analytics`, and the `daily_orders` landing table. |
| [Operations](operations.md) | Start, stop, reset, hostnames, ports, and the failures that show up first. |

The project [README](../README.md) is the short version: what this is, the logins, and the daily commands.
