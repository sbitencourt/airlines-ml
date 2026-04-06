from dst_airlines.utils.metrics import emit_metric
from dst_airlines.utils.prometheus import push_metrics


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


def build_flight_status_metric(
    *,
    source: str,
    endpoint: str,
    run_id: str,
    flight_number: str,
    airline_iata: str | None,
    departure_iata: str | None,
    arrival_iata: str | None,
    status: str,
) -> tuple[str, int, dict]:
    # run_id se mantiene en la firma por compatibilidad, pero no se usa como label
    # porque esta métrica representa un snapshot actual de vuelos.
    return (
        "etl_flight_status",
        1,
        {
            "source": source,
            "endpoint": endpoint,
            "flight_number": flight_number,
            "airline_iata": airline_iata,
            "departure_iata": departure_iata,
            "arrival_iata": arrival_iata,
            "status": status,
        },
    )


def push_flight_status_metrics(
    *,
    source: str,
    endpoint: str,
    metrics: list[tuple[str, int, dict]],
) -> None:
    if not metrics:
        return

    push_metrics(
        metrics,
        job="etl_flight_snapshot",
        grouping_key={
            "source": source,
            "endpoint": endpoint,
        },
    )


def emit_flight_status(
    *,
    source: str,
    endpoint: str,
    run_id: str,
    flight_number: str,
    airline_iata: str | None,
    departure_iata: str | None,
    arrival_iata: str | None,
    status: str,
):
    # Compatibilidad hacia atrás.
    # Para snapshots de muchos vuelos, preferir build_flight_status_metric()
    # + push_flight_status_metrics().
    emit_metric(
        "etl_flight_status",
        1,
        source=source,
        endpoint=endpoint,
        flight_number=flight_number,
        airline_iata=airline_iata,
        departure_iata=departure_iata,
        arrival_iata=arrival_iata,
        status=status,
    )


def emit_flight_position(
    *,
    source: str,
    endpoint: str,
    run_id: str,
    flight_number: str,
    airline_iata: str | None,
    altitude: float | None,
    speed: float | None,
):
    if altitude is not None:
        emit_metric(
            "etl_flight_altitude_feet",
            altitude,
            source=source,
            endpoint=endpoint,
            flight_number=flight_number,
            airline_iata=airline_iata,
        )

    if speed is not None:
        emit_metric(
            "etl_flight_ground_speed_knots",
            speed,
            source=source,
            endpoint=endpoint,
            flight_number=flight_number,
            airline_iata=airline_iata,
        )


def emit_dag_active(
    *,
    dag_id: str,
    source: str,
    endpoint: str,
    value: int = 1,
) -> None:
    emit_metric(
        "etl_dag_active",
        value,
        dag_id=dag_id,
        source=source,
        endpoint=endpoint,
    )


def emit_dag_last_run_timestamp(
    *,
    dag_id: str,
    source: str,
    endpoint: str,
    timestamp_seconds: float | int,
) -> None:
    emit_metric(
        "etl_dag_last_run_timestamp_seconds",
        timestamp_seconds,
        dag_id=dag_id,
        source=source,
        endpoint=endpoint,
    )


def emit_dag_last_success_timestamp(
    *,
    dag_id: str,
    source: str,
    endpoint: str,
    timestamp_seconds: float | int,
) -> None:
    emit_metric(
        "etl_dag_last_success_timestamp_seconds",
        timestamp_seconds,
        dag_id=dag_id,
        source=source,
        endpoint=endpoint,
    )


def emit_dag_schedule_interval(
    *,
    dag_id: str,
    source: str,
    endpoint: str,
    schedule_interval_seconds: int,
) -> None:
    emit_metric(
        "etl_dag_schedule_interval_seconds",
        schedule_interval_seconds,
        dag_id=dag_id,
        source=source,
        endpoint=endpoint,
    )


def emit_dag_run_status(
    *,
    dag_id: str,
    source: str,
    endpoint: str,
    status: str,
) -> None:
    for candidate in ("running", "success", "failed"):
        emit_metric(
            "etl_dag_run_status",
            1 if candidate == status else 0,
            dag_id=dag_id,
            source=source,
            endpoint=endpoint,
            status=candidate,
        )