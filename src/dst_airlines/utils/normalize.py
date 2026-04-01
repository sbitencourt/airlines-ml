from __future__ import annotations

from typing import Any


def prune(obj: Any) -> Any:
    if isinstance(obj, dict):
        cleaned = {}
        for key, value in obj.items():
            pruned_value = prune(value)
            if pruned_value in (None, "", [], {}):
                continue
            cleaned[key] = pruned_value
        return cleaned if cleaned else None

    if isinstance(obj, list):
        cleaned_list = [prune(item) for item in obj]
        cleaned_list = [item for item in cleaned_list if item not in (None, "", [], {})]
        return cleaned_list if cleaned_list else None

    return obj