from __future__ import annotations

from dst_airlines.utils.prometheus import delete_metrics, push_metrics


JOB_NAME = "etl_pipeline"

VALID_STATUSES = {"success", "failed"}


def emit_pipeline_run_metrics(
    *,
    source: str,
    endpoint: str,
    stage: str,
    status: str,
    duration_seconds: float,
    records_processed: int | None = None,
) -> None:
    """
    Emit a minimal and stable set of ETL pipeline metrics.

    Metrics:
    - etl_pipeline_status:
        Gauge. 1 if the last run for this source/endpoint/stage succeeded,
        0 if it failed.

    - etl_pipeline_duration_seconds:
        Gauge. Duration of the last run for this source/endpoint/stage.

    - etl_pipeline_records_processed:
        Gauge. Number of records processed in the last run.

    - etl_pipeline_runs_total:
        Counter. Total number of runs by source/endpoint/stage/status.

    Labels:
    - source
    - endpoint
    - stage
    - status only for etl_pipeline_runs_total
    """
    source = source.strip().lower()
    endpoint = endpoint.strip().lower()
    stage = stage.strip().lower()
    status = status.strip().lower()

    if status not in VALID_STATUSES:
        raise ValueError(
            f"Invalid status '{status}'. "
            f"Expected one of: {', '.join(sorted(VALID_STATUSES))}"
        )

    base_labels = {
        "source": source,
        "endpoint": endpoint,
        "stage": stage,
    }

    metrics = [
        (
            "etl_pipeline_status",
            1 if status == "success" else 0,
            base_labels,
        ),
        (
            "etl_pipeline_duration_seconds",
            duration_seconds,
            base_labels,
        ),
        (
            "etl_pipeline_runs_total",
            1,
            {
                **base_labels,
                "status": status,
            },
        ),
    ]

    if records_processed is not None:
        metrics.append(
            (
                "etl_pipeline_records_processed",
                records_processed,
                base_labels,
            )
        )

    push_metrics(
        metrics,
        job=JOB_NAME,
        grouping_key={
            "source": source,
            "endpoint": endpoint,
            "stage": stage,
        },
    )


def delete_pipeline_metrics(
    *,
    source: str,
    endpoint: str,
    stage: str,
) -> None:
    """
    Delete ETL pipeline metrics for a specific source, endpoint and stage.
    """
    source = source.strip().lower()
    endpoint = endpoint.strip().lower()
    stage = stage.strip().lower()

    delete_metrics(
        job=JOB_NAME,
        grouping_key={
            "source": source,
            "endpoint": endpoint,
            "stage": stage,
        },
    )