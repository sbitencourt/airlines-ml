from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Callable


@dataclass(frozen=True)
class PipelineDefinition:
    source: str
    endpoint: str
    extract_fn: Callable[..., None]
    transform_fn: Callable[..., None]
    load_fn: Callable[..., None]


def _run_extract_module(module_path: str, *, run_id: str) -> None:
    module = import_module(module_path)
    module.main(run_id=run_id)


def _run_load_function(
    module_path: str,
    function_name: str,
    *,
    source: str,
    endpoint: str,
    run_id: str,
) -> None:
    module = import_module(module_path)
    load_fn = getattr(module, function_name)
    load_fn(
        source=source,
        endpoint=endpoint,
        run_id=run_id,
    )


def aviationstack_transform(*, source: str, endpoint: str, run_id: str) -> None:
    from dst_airlines.etl.transform.aviationstack_to_incoming import main

    main(
        source=source,
        endpoint=endpoint,
        run_id=run_id,
    )


def aviationstack_flights_extract(*, source: str, endpoint: str, run_id: str) -> None:
    _run_extract_module(
        "dst_airlines.etl.extract.aviationstack_flights",
        run_id=run_id,
    )


def aviationstack_airlines_extract(*, source: str, endpoint: str, run_id: str) -> None:
    _run_extract_module(
        "dst_airlines.etl.extract.aviationstack_airlines",
        run_id=run_id,
    )


def aviationstack_airports_extract(*, source: str, endpoint: str, run_id: str) -> None:
    _run_extract_module(
        "dst_airlines.etl.extract.aviationstack_airports",
        run_id=run_id,
    )


def aviationstack_flights_load(*, source: str, endpoint: str, run_id: str) -> None:
    _run_load_function(
        "dst_airlines.etl.load.to_mongo_flights",
        "sync_flights_to_mongo",
        source=source,
        endpoint=endpoint,
        run_id=run_id,
    )


def aviationstack_airlines_load(*, source: str, endpoint: str, run_id: str) -> None:
    _run_load_function(
        "dst_airlines.etl.load.to_mongo_airlines",
        "sync_airlines_to_mongo",
        source=source,
        endpoint=endpoint,
        run_id=run_id,
    )


def aviationstack_airports_load(*, source: str, endpoint: str, run_id: str) -> None:
    _run_load_function(
        "dst_airlines.etl.load.to_mongo_airports",
        "sync_airports_to_mongo",
        source=source,
        endpoint=endpoint,
        run_id=run_id,
    )


PIPELINES: dict[tuple[str, str], PipelineDefinition] = {
    ("aviationstack", "flights"): PipelineDefinition(
        source="aviationstack",
        endpoint="flights",
        extract_fn=aviationstack_flights_extract,
        transform_fn=aviationstack_transform,
        load_fn=aviationstack_flights_load,
    ),
    ("aviationstack", "airlines"): PipelineDefinition(
        source="aviationstack",
        endpoint="airlines",
        extract_fn=aviationstack_airlines_extract,
        transform_fn=aviationstack_transform,
        load_fn=aviationstack_airlines_load,
    ),
    ("aviationstack", "airports"): PipelineDefinition(
        source="aviationstack",
        endpoint="airports",
        extract_fn=aviationstack_airports_extract,
        transform_fn=aviationstack_transform,
        load_fn=aviationstack_airports_load,
    ),
}


def get_pipeline(source: str, endpoint: str) -> PipelineDefinition:
    key = (source.strip().lower(), endpoint.strip().lower())

    if key not in PIPELINES:
        available = ", ".join(f"{s}/{e}" for s, e in PIPELINES)
        raise ValueError(
            f"Pipeline not registered: {source}/{endpoint}. "
            f"Available: {available}"
        )

    return PIPELINES[key]