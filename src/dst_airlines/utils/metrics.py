from datetime import datetime, timezone
import json




def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_metric(metric_name: str, value, **labels) -> None:
    payload = {
        "timestamp": utc_now_iso(),
        "type": "metric",
        "metric": metric_name,
        "value": value,
        "labels": labels,
    }
    print(json.dumps(payload, ensure_ascii=False))