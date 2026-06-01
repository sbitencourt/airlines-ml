import os
from typing import Any

import requests
import streamlit as st


API_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")


@st.cache_data(ttl=60)
def call_api(endpoint: str, params: dict[str, Any] | None = None):
    """Call a FastAPI endpoint and return a tuple: (json_data, error_message)."""
    url = f"{API_URL}{endpoint}"

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json(), None
    except requests.exceptions.RequestException as exc:
        return None, str(exc)
