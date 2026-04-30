from __future__ import annotations

import json
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Protocol, TypeVar
from urllib.parse import parse_qs, urlparse

from openai import APIConnectionError, APITimeoutError, AzureOpenAI, InternalServerError, RateLimitError
from pydantic import BaseModel

from ..settings import settings

ModelT = TypeVar("ModelT", bound=BaseModel)
RETRYABLE_ERRORS = (RateLimitError, APITimeoutError, APIConnectionError, InternalServerError)
_CAPABILITY_CACHE: tuple[float, "AzureOpenAICapabilities"] | None = None
_CAPABILITY_CACHE_LOCK = Lock()
_CAPABILITY_CACHE_TTL_SECONDS = 300


class StructuredModelClient(Protocol):
    provider_name: str

    def call_json_model(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        response_model: type[ModelT],
        model: str,
        temperature: float = 0.1,
        max_retries: int = 3,
    ) -> ModelT: ...

    def healthcheck(self) -> tuple[bool, str]: ...


def extract_output_text(response: object) -> str:
    if hasattr(response, "output_text") and getattr(response, "output_text"):
        return getattr(response, "output_text")
    try:
        return json.dumps(response.model_dump(), indent=2)  # type: ignore[call-arg]
    except Exception:
        return str(response)


def parse_json_output(text: str) -> Any:
    stripped = text.strip()
    if not stripped:
        return {}

    try:
        return json.loads(stripped)
    except json.JSONDecodeError as exc:
        decoder = json.JSONDecoder()
        try:
            payload, _index = decoder.raw_decode(stripped)
            return payload
        except json.JSONDecodeError:
            raise ValueError(f"Unable to parse model JSON output: {exc}") from exc


def patch_schema_for_strict(schema: dict) -> dict:
    schema = schema.copy()

    if "$defs" in schema:
        schema["$defs"] = {key: patch_schema_for_strict(value) for key, value in schema["$defs"].items()}

    if schema.get("type") == "object" and "properties" in schema:
        schema["additionalProperties"] = False
        schema["required"] = list(schema["properties"].keys())
        schema["properties"] = {key: patch_schema_for_strict(value) for key, value in schema["properties"].items()}

    if schema.get("type") == "array" and "items" in schema:
        schema["items"] = patch_schema_for_strict(schema["items"])

    if "anyOf" in schema:
        schema["anyOf"] = [patch_schema_for_strict(value) for value in schema["anyOf"]]

    return schema


@dataclass(slots=True)
class ProviderHealth:
    provider_name: str
    configured: bool
    message: str


@dataclass(slots=True)
class AzureOpenAICapabilities:
    files_supported: bool
    vector_stores_supported: bool
    file_search_supported: bool
    message: str


def normalize_azure_endpoint(raw_endpoint: str | None) -> str | None:
    if not raw_endpoint:
        return None
    parsed = urlparse(raw_endpoint.strip())
    if not parsed.scheme or not parsed.netloc:
        return raw_endpoint.strip()
    return f"{parsed.scheme}://{parsed.netloc}"


def endpoint_api_version(raw_endpoint: str | None) -> str | None:
    if not raw_endpoint:
        return None
    parsed = urlparse(raw_endpoint.strip())
    versions = parse_qs(parsed.query).get("api-version")
    return versions[0] if versions else None


def resolved_azure_endpoint() -> str | None:
    return normalize_azure_endpoint(settings.azure_openai_endpoint)


def resolved_azure_api_version() -> str:
    return endpoint_api_version(settings.azure_openai_endpoint) or settings.azure_openai_api_version


def get_azure_client() -> AzureOpenAI:
    return AzureOpenAI(
        api_key=settings.azure_openai_api_key,
        azure_endpoint=resolved_azure_endpoint(),
        api_version=resolved_azure_api_version(),
    )


class AzureStructuredModelClient:
    provider_name = "azure_openai"

    def __init__(self) -> None:
        self._client = get_azure_client()

    def call_json_model(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        response_model: type[ModelT],
        model: str,
        temperature: float = 0.1,
        max_retries: int = 3,
    ) -> ModelT:
        schema = patch_schema_for_strict(response_model.model_json_schema())

        for attempt in range(1, max_retries + 1):
            try:
                response = self._client.responses.create(
                    model=model,
                    temperature=temperature,
                    input=[
                        {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
                        {"role": "user", "content": [{"type": "input_text", "text": user_prompt}]},
                    ],
                    text={
                        "format": {
                            "type": "json_schema",
                            "name": response_model.__name__,
                            "schema": schema,
                            "strict": True,
                        }
                    },
                )
                payload = parse_json_output(extract_output_text(response))
                return response_model.model_validate(payload)
            except RETRYABLE_ERRORS:
                if attempt == max_retries:
                    raise
                time.sleep(2**attempt)

        raise RuntimeError("Provider exhausted retries without returning a result.")

    def healthcheck(self) -> tuple[bool, str]:
        if not resolved_azure_endpoint() or not settings.azure_openai_api_key:
            return False, "Azure OpenAI endpoint or API key is not configured."
        return True, "Azure OpenAI credentials are configured."


def get_provider_client() -> StructuredModelClient:
    return AzureStructuredModelClient()


def get_azure_capabilities(force_refresh: bool = False) -> AzureOpenAICapabilities:
    global _CAPABILITY_CACHE

    configured = bool(resolved_azure_endpoint() and settings.azure_openai_api_key)
    if not configured:
        return AzureOpenAICapabilities(
            files_supported=False,
            vector_stores_supported=False,
            file_search_supported=False,
            message="Azure OpenAI endpoint or API key is not configured.",
        )

    with _CAPABILITY_CACHE_LOCK:
        if not force_refresh and _CAPABILITY_CACHE:
            cached_at, cached_value = _CAPABILITY_CACHE
            if time.time() - cached_at < _CAPABILITY_CACHE_TTL_SECONDS:
                return cached_value

    files_supported = False
    vector_stores_supported = False
    message = "Azure OpenAI files and vector stores are available."

    try:
        client = get_azure_client()
        client.files.list(limit=1)
        files_supported = True
    except Exception as exc:
        message = f"Azure OpenAI files API is unavailable: {exc}"

    try:
        client = get_azure_client()
        client.vector_stores.list(limit=1)
        vector_stores_supported = True
    except Exception as exc:
        detail = f"Azure OpenAI vector stores API is unavailable: {exc}"
        message = f"{message} {detail}" if files_supported else detail

    capabilities = AzureOpenAICapabilities(
        files_supported=files_supported,
        vector_stores_supported=vector_stores_supported,
        file_search_supported=files_supported and vector_stores_supported,
        message=message,
    )

    with _CAPABILITY_CACHE_LOCK:
        _CAPABILITY_CACHE = (time.time(), capabilities)

    return capabilities


def get_provider_health() -> ProviderHealth:
    configured = bool(resolved_azure_endpoint() and settings.azure_openai_api_key)
    message = "Azure OpenAI credentials are configured." if configured else "Azure OpenAI endpoint or API key is not configured."
    return ProviderHealth(provider_name="azure_openai", configured=configured, message=message)
