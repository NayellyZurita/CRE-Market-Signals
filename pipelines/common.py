"""Shared utilities for retrieving and normalizing external API responses."""

from __future__ import annotations

from typing import Any, Mapping, MutableMapping

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

DEFAULT_TIMEOUT_SECONDS = 30.0
_DEFAULT_WAIT = wait_exponential(min=1, max=16)
_DEFAULT_STOP = stop_after_attempt(5)


Headers = Mapping[str, str] | None
Params = Mapping[str, Any] | None
Data = MutableMapping[str, Any] | bytes | str | None
JsonData = Any


@retry(wait=_DEFAULT_WAIT, stop=_DEFAULT_STOP, reraise=True)
async def fetch_json(
    url: str,
    *,
    headers: Headers = None,
    params: Params = None,
    method: str = "GET",
    data: Data = None,
    json: JsonData = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> Any:
    """Execute an HTTP request and return the decoded JSON payload.

    Retries with exponential backoff when transient failures occur. The helper keeps
    the interface close to ``httpx.AsyncClient.request`` so downstream ingestors can
    forward API-specific requirements (headers, params, JSON body, etc.) without
    reimplementing networking concerns.
    """

    request_method = method.upper()
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.request(
            request_method,
            url,
            headers=headers,
            params=params,
            data=data,
            json=json,
        )

    response.raise_for_status()
    return response.json()


__all__ = ["fetch_json", "DEFAULT_TIMEOUT_SECONDS"]
