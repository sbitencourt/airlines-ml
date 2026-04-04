from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway

PUSHGATEWAY_URL = "http://pushgateway:9091"


def push_metric(metric_name: str, value, labels: dict):
    registry = CollectorRegistry()

    # detectar tipo
    if metric_name.endswith("_total"):
        metric = Counter(metric_name, metric_name, labelnames=list(labels.keys()), registry=registry)
        metric.labels(**labels).inc(value)
    else:
        metric = Gauge(metric_name, metric_name, labelnames=list(labels.keys()), registry=registry)
        metric.labels(**labels).set(value)

    push_to_gateway(
        PUSHGATEWAY_URL,
        job="etl_pipeline",
        registry=registry,
    )