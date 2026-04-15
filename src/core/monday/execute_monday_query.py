from __future__ import annotations

import random
import time
from typing import Any, Dict, Optional

import requests

from src.config.settings import (
    API_URL,
    BACKOFF_FACTOR,
    BASE_DELAY,
    HEADERS,
    JITTER_MAX,
    JITTER_MIN,
    LOG_PREFIX,
    MAX_DELAY,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)


def log_info(message: str) -> None:
    print(f"{LOG_PREFIX} [INFO] {message}")


def log_warn(message: str) -> None:
    print(f"{LOG_PREFIX} [WARN] {message}")


def log_error(message: str) -> None:
    print(f"{LOG_PREFIX} [ERROR] {message}")


def sleep_with_jitter(base_delay: float) -> float:
    delay = min(base_delay, MAX_DELAY)
    jitter = random.uniform(JITTER_MIN, JITTER_MAX)
    final_delay = delay + jitter
    time.sleep(final_delay)
    return final_delay


def build_backoff_delay(attempt: int) -> float:
    return min(BASE_DELAY * (BACKOFF_FACTOR**attempt), MAX_DELAY)


def should_retry_http_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


def should_retry_exception(exc: Exception) -> bool:
    return isinstance(
        exc,
        (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ),
    )


def format_graphql_payload(
    query: str,
    variables: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables
    return payload


def execute_monday_query(
    query: str,
    variables: Optional[Dict[str, Any]] = None,
    operation_name: str = "graphql_request",
    timeout: int = REQUEST_TIMEOUT,
) -> Dict[str, Any]:
    payload = format_graphql_payload(query=query, variables=variables)
    last_error: Optional[Exception] = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = requests.post(
                API_URL,
                headers=HEADERS,
                json=payload,
                timeout=timeout,
            )

            if response.status_code == 200:
                result = response.json()

                if "errors" in result:
                    errors = result["errors"]
                    error_text = str(errors)

                    if "JsonParseException" in error_text or "Syntax error in JSON input" in error_text:
                        log_error(f"{operation_name} payload invalido: {errors}")
                        raise ValueError(f"{operation_name} payload invalido")

                    if "CursorExpiredError" in error_text:
                        raise ValueError(f"{operation_name} CursorExpiredError: {error_text}")

                    log_error(f"{operation_name} retornou erros GraphQL: {errors}")
                    raise ValueError(f"{operation_name} retornou erros GraphQL")

                return result.get("data", {})

            if should_retry_http_status(response.status_code):
                if attempt < MAX_RETRIES:
                    delay = sleep_with_jitter(build_backoff_delay(attempt))
                    log_warn(
                        f"{operation_name} HTTP {response.status_code}. "
                        f"Tentativa {attempt + 1}/{MAX_RETRIES}. Retry em {delay:.2f}s."
                    )
                    continue
                response.raise_for_status()

            log_error(
                f"{operation_name} falhou com HTTP {response.status_code}: {response.text[:500]}"
            )
            response.raise_for_status()

        except Exception as exc:
            last_error = exc

            if isinstance(exc, requests.exceptions.HTTPError):
                status_code = exc.response.status_code if exc.response is not None else None
                if (
                    status_code is not None
                    and should_retry_http_status(status_code)
                    and attempt < MAX_RETRIES
                ):
                    delay = sleep_with_jitter(build_backoff_delay(attempt))
                    log_warn(
                        f"{operation_name} HTTPError {status_code}. "
                        f"Tentativa {attempt + 1}/{MAX_RETRIES}. Retry em {delay:.2f}s."
                    )
                    continue
                raise

            if should_retry_exception(exc) and attempt < MAX_RETRIES:
                delay = sleep_with_jitter(build_backoff_delay(attempt))
                log_warn(
                    f"{operation_name} falhou por {type(exc).__name__}. "
                    f"Tentativa {attempt + 1}/{MAX_RETRIES}. Retry em {delay:.2f}s."
                )
                continue

            raise

    raise RuntimeError(f"{operation_name} falhou apos retries") from last_error
