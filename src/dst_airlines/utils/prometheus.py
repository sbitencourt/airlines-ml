from __future__ import annotations

from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge
from prometheus_client import delete_from_gateway, push_to_gateway

PUSHGATEWAY_URL = "http://pushgateway:9091"


def _metric_factory(
    registry: CollectorRegistry,
    metric_name: str,
    labelnames: list[str],
):
    if metric_name.endswith("_total"):
        return Counter(
            metric_name,
            metric_name,
            labelnames=labelnames,
            registry=registry,
        )
    return Gauge(
        metric_name,
        metric_name,
        labelnames=labelnames,
        registry=registry,
    )


def push_metrics(
    metrics: list[tuple[str, float | int, dict[str, Any]]],
    *,
    job: str,
    grouping_key: dict[str, str] | None = None,
) -> None:
    registry = CollectorRegistry()
    created: dict[tuple[str, tuple[str, ...]], Any] = {}

    for metric_name, value, labels in metrics:
        labelnames = list(labels.keys())
        cache_key = (metric_name, tuple(labelnames))

        if cache_key not in created:
            created[cache_key] = _metric_factory(
                registry=registry,
                metric_name=metric_name,
                labelnames=labelnames,
            )

        metric = created[cache_key]

        normalized_labels = {
            k: "" if v is None else str(v)
            for k, v in labels.items()
        }

        if metric_name.endswith("_total"):
            metric.labels(**normalized_labels).inc(float(value))
        else:
            metric.labels(**normalized_labels).set(float(value))

    push_to_gateway(
        PUSHGATEWAY_URL,
        job=job,
        grouping_key=grouping_key or {},
        registry=registry,
    )


def push_metric(
    metric_name: str,
    value: float | int,
    labels: dict[str, Any],
    *,
    job: str,
    grouping_key: dict[str, str] | None = None,
) -> None:
    push_metrics(
        [(metric_name, value, labels)],
        job=job,
        grouping_key=grouping_key,
    )


def delete_metrics(
    *,
    job: str,
    grouping_key: dict[str, str] | None = None,
) -> None:
    delete_from_gateway(
        PUSHGATEWAY_URL,
        job=job,
        grouping_key=grouping_key or {},
    )