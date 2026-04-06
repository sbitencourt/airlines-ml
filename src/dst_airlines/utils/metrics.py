from __future__ import annotations

from datetime import datetime, timezone
import json

from dst_airlines.utils.prometheus import delete_metrics, push_metric


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_push_context(metric_name: str, labels: dict):
    if metric_name.startswith("etl_dag_"):
        job = "etl_dag"
        grouping_key = {
            "dag_id": str(labels["dag_id"]),
            "source": str(labels["source"]),
            "endpoint": str(labels["endpoint"]),
        }
        return job, grouping_key

    if metric_name.startswith("etl_flight_"):
        job = "etl_flight_snapshot"
        grouping_key = {
            "source": str(labels["source"]),
            "endpoint": str(labels["endpoint"]),
        }
        return job, grouping_key

    job = "etl_pipeline_run"
    grouping_key = {
        "source": str(labels["source"]),
        "endpoint": str(labels["endpoint"]),
        "stage": str(labels["stage"]),
        "run_id": str(labels["run_id"]),
    }
    return job, grouping_key


def emit_metric(metric_name: str, value, **labels):
    job, grouping_key = _resolve_push_context(metric_name, labels)

    push_metric(
        metric_name,
        value,
        labels,
        job=job,
        grouping_key=grouping_key,
    )

    payload = {
        "timestamp": utc_now_iso(),
        "type": "metric",
        "metric": metric_name,
        "value": value,
        "labels": labels,
        "pushgateway_job": job,
        "grouping_key": grouping_key,
    }
    print(json.dumps(payload, ensure_ascii=False))


def delete_metric_group_for_dag(*, dag_id: str, source: str, endpoint: str) -> None:
    delete_metrics(
        job="etl_dag",
        grouping_key={
            "dag_id": dag_id,
            "source": source,
            "endpoint": endpoint,
        },
    )


def delete_metric_group_for_flights(*, source: str, endpoint: str) -> None:
    delete_metrics(
        job="etl_flight_snapshot",
        grouping_key={
            "source": source,
            "endpoint": endpoint,
        },
    )