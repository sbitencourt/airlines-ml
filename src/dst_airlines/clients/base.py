from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


class ApiClientError(Exception):
    """Base exception for API client errors."""


class ApiConnectionError(ApiClientError):
    """Raised when the API cannot be reached."""


class ApiTimeoutError(ApiClientError):
    """Raised when the API request times out."""


class ApiHttpError(ApiClientError):
    """Raised when the API returns a non-200 HTTP status."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message


class ApiInvalidResponseError(ApiClientError):
    """Raised when the API response is not valid JSON."""


class ApiPayloadError(ApiClientError):
    """Raised when the API returns a valid JSON payload with an error field."""


@dataclass
class BaseClient:
    base_url: str
    timeout: int = 15

    def get_json(
        self,
        endpoint: str = "",
        *,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = self._build_url(endpoint)

        try:
            response = requests.get(url, params=params, timeout=self.timeout)
        except requests.exceptions.Timeout as exc:
            raise ApiTimeoutError("Request timed out.") from exc
        except requests.exceptions.ConnectionError as exc:
            raise ApiConnectionError("Could not reach the API.") from exc
        except requests.exceptions.RequestException as exc:
            raise ApiClientError(f"Unexpected request error: {exc}") from exc

        if response.status_code != 200:
            raise ApiHttpError(response.status_code, response.text[:300])

        try:
            payload = response.json()
        except ValueError as exc:
            raise ApiInvalidResponseError("Response is not valid JSON.") from exc

        if not isinstance(payload, dict):
            raise ApiInvalidResponseError("Expected JSON object response.")

        self._raise_if_api_error(payload)
        return payload

    def _build_url(self, endpoint: str) -> str:
        if not endpoint:
            return self.base_url

        base = self.base_url.rstrip("/")
        suffix = endpoint.lstrip("/")
        return f"{base}/{suffix}"

    def _raise_if_api_error(self, payload: Dict[str, Any]) -> None:
        error = payload.get("error")
        if error:
            if isinstance(error, dict):
                code = error.get("code")
                message = error.get("message", "Unknown API error")
                raise ApiPayloadError(f"API error {code}: {message}")
            raise ApiPayloadError(f"API error: {error}")