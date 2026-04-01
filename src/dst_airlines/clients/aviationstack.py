from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from dst_airlines.clients.base import BaseClient
from dst_airlines.utils.get_tool_fernet import get_credentials


@dataclass
class AviationstackClient(BaseClient):
    access_key: str = ""

    @classmethod
    def from_env(
        cls,
        api_url_env_var: str = "API_URL_AVIATIONSTACK_FLIGHTS",
        timeout: int = 15,
    ) -> "AviationstackClient":
        host, token, _ = get_credentials(api_url_env_var=api_url_env_var)
        provider_base_url = host.split("/v1/")[0] + "/v1"
        return cls(base_url=provider_base_url, access_key=token, timeout=timeout)

    def _fetch_all_paginated(
        self,
        fetch_page_fn: Callable[..., Dict[str, Any]],
        limit: int,
        max_pages: Optional[int],
        extra_params: Optional[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        payloads: List[Dict[str, Any]] = []
        offset = 0
        page_count = 0

        while True:
            if max_pages is not None and page_count >= max_pages:
                break

            payload = fetch_page_fn(
                limit=limit,
                offset=offset,
                extra_params=extra_params,
            )
            payloads.append(payload)
            page_count += 1

            records = self._extract_records(payload)
            if not records or len(records) < limit:
                break

            offset += limit

        return payloads

    def fetch_flights_raw(
        self,
        *,
        limit: Optional[int] = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"access_key": self.access_key}

        if limit is not None:
            params["limit"] = limit

        if extra_params:
            params.update(extra_params)

        return self.get_json(endpoint="flights", params=params)

    def fetch_airports_raw_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "access_key": self.access_key,
            "limit": limit,
            "offset": offset,
        }

        if extra_params:
            params.update(extra_params)

        return self.get_json(endpoint="airports", params=params)

    def fetch_airports_raw_all(
        self,
        *,
        limit: int = 100,
        max_pages: Optional[int] = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        return self._fetch_all_paginated(
            self.fetch_airports_raw_page,
            limit,
            max_pages,
            extra_params,
        )

    def fetch_airlines_raw_page(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "access_key": self.access_key,
            "limit": limit,
            "offset": offset,
        }

        if extra_params:
            params.update(extra_params)

        return self.get_json(endpoint="airlines", params=params)

    def fetch_airlines_raw_all(
        self,
        *,
        limit: int = 100,
        max_pages: Optional[int] = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        return self._fetch_all_paginated(
            self.fetch_airlines_raw_page,
            limit,
            max_pages,
            extra_params,
        )

    @staticmethod
    def _extract_records(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

        results = payload.get("results")
        if isinstance(results, list):
            return [item for item in results if isinstance(item, dict)]

        return []