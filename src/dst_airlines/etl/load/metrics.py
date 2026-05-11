from dst_airlines.utils.prometheus import push_metrics, delete_metrics


JOB_NAME = "etl_pipeline"


def emit_pipeline_run_metrics(
    *,
    source: str,
    endpoint: str,
    stage: str,
    status: str,
    duration_seconds: float,
    records_processed: int | None = None,
) -> None:
    source = source.strip().lower()
    endpoint = endpoint.strip().lower()
    stage = stage.strip().lower()
    status = status.strip().lower()

    if status not in {"success", "failed"}:
        raise ValueError("status must be either 'success' or 'failed'")

    labels = {
        "source": source,
        "endpoint": endpoint,
        "stage": stage,
    }

    metrics = [
        (
            "etl_pipeline_status",
            1 if status == "success" else 0,
            labels,
        ),
        (
            "etl_pipeline_duration_seconds",
            duration_seconds,
            labels,
        ),
        (
            "etl_pipeline_runs_total",
            1,
            {
                **labels,
                "status": status,
            },
        ),
    ]

    if records_processed is not None:
        metrics.append(
            (
                "etl_pipeline_records_processed",
                records_processed,
                labels,
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
    delete_metrics(
        job=JOB_NAME,
        grouping_key={
            "source": source.strip().lower(),
            "endpoint": endpoint.strip().lower(),
            "stage": stage.strip().lower(),
        },
    )