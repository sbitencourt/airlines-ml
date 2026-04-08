# ETL Pipelines

This document describes how to execute ETL pipelines in the `dst_airlines` project.

## Description

The ETL (Extract, Transform, Load) process consists of:

1. Extract → Retrieve data from external APIs (e.g., Aviationstack)
2. Transform → Clean and structure the data
3. Load → Store the processed data into target systems (MongoDB, Neo4j, etc.)

The pipeline is executed through the CLI defined in `cli.py`.

---

## Execution

Pipelines are executed inside the `airflow-webserver` container using Docker Compose.

### General structure

```bash
docker compose exec airflow-webserver python -m dst_airlines.cli run-etl \
  --source <source> \
  --endpoint <endpoint>
```

---

## Aviationstack

### Airports

Run the pipeline to extract and process airport data:

```bash
docker compose exec airflow-webserver python -m dst_airlines.cli run-etl --source aviationstack --endpoint airports
```

---

### Flights

Run the pipeline to extract and process flight data:

```bash
docker compose exec airflow-webserver python -m dst_airlines.cli run-etl --source aviationstack --endpoint flights
```

---

## Parameters

- `--source` → Data source (e.g., `aviationstack`)
- `--endpoint` → Data type (e.g., `airports`, `flights`)
- `--run-id` (optional) → Unique execution identifier  
  - If not provided, it is generated automatically

Example with run-id:

```bash
docker compose exec airflow-webserver python -m dst_airlines.cli run-etl \
  --source aviationstack \
  --endpoint flights \
  --run-id manual_run_001
```

---

## Pipeline flow

Each execution follows this order:

1. `extract_fn`
2. `transform_fn`
3. `load_fn`

---

## Notes

- Logs are recorded using `log_event`
- Pipelines are registered in `pipelines/registry.py`
- New pipelines can be added by extending the registry and implementing the corresponding ETL functions

---
