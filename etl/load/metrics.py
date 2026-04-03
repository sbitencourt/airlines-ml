
from dst_airlines.utils.metrics import emit_metric



def emit_pipeline_status(
    *,
    source: str,
    endpoint: str,
    stage: str,
    run_id: str,
    status: str,
    value: int,
) -> None:
    emit_metric(
        "etl_pipeline_status",
        value,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
        status=status,
    )


def emit_file_metrics(
    *,
    source: str,
    endpoint: str,
    stage: str,
    run_id: str,
    file_name: str,
    records: int,
    valid: int,
    invalid: int,
    inserted: int,
    updated: int,
    unchanged: int,
    duration_seconds: float,
) -> None:
    emit_metric(
        "etl_file_records_total",
        records,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
        file_name=file_name,
    )
    emit_metric(
        "etl_file_valid_records_total",
        valid,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
        file_name=file_name,
    )
    emit_metric(
        "etl_file_invalid_records_total",
        invalid,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
        file_name=file_name,
    )
    emit_metric(
        "etl_file_inserted_total",
        inserted,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
        file_name=file_name,
    )
    emit_metric(
        "etl_file_updated_total",
        updated,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
        file_name=file_name,
    )
    emit_metric(
        "etl_file_unchanged_total",
        unchanged,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
        file_name=file_name,
    )
    emit_metric(
        "etl_file_duration_seconds",
        duration_seconds,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
        file_name=file_name,
    )



def emit_pipeline_metrics(
    *,
    source: str,
    endpoint: str,
    stage: str,
    run_id: str,
    files: int,
    records: int,
    valid: int,
    invalid: int,
    inserted: int,
    updated: int,
    unchanged: int,
    duration_seconds: float,
) -> None:
    emit_metric(
        "etl_pipeline_files_total",
        files,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
    )
    emit_metric(
        "etl_pipeline_records_total",
        records,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
    )
    emit_metric(
        "etl_pipeline_valid_records_total",
        valid,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
    )
    emit_metric(
        "etl_pipeline_invalid_records_total",
        invalid,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
    )
    emit_metric(
        "etl_pipeline_inserted_total",
        inserted,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
    )
    emit_metric(
        "etl_pipeline_updated_total",
        updated,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
    )
    emit_metric(
        "etl_pipeline_unchanged_total",
        unchanged,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
    )
    emit_metric(
        "etl_pipeline_duration_seconds",
        duration_seconds,
        source=source,
        endpoint=endpoint,
        stage=stage,
        run_id=run_id,
    )