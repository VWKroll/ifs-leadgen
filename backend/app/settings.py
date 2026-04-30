from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "IDC Event Intelligence API"
    api_prefix: str = "/api"
    cors_origins: list[str] = ["http://localhost:3000", "http://127.0.0.1:3000"]
    db_host: str | None = None
    db_profile: str | None = None
    db_auth_type: Literal["auto", "oauth", "pat"] = "oauth"
    pat_token: str | None = None
    generation_runner: Literal["auto", "local", "job"] = "auto"
    generation_job_id: int | None = None
    generation_job_name: str = "IDC Event Intelligence Generation"
    generation_poll_seconds: int = 10
    openai_model: str = "gpt-5.4"
    azure_openai_endpoint: str | None = None
    azure_openai_api_key: str | None = None
    azure_openai_api_version: str = "2025-03-01-preview"
    chat_model: str = "gpt-5.4"
    kb_storage_root: str = "/tmp/idc-event-intelligence/knowledge_base"
    kb_vector_store_id: str | None = None
    kb_max_results: int = 6
    kb_cleanup_mode: Literal["off", "dedupe", "aggressive"] = "dedupe"
    kb_cleanup_on_sync: bool = True
    kb_document_retention_days: int = 45
    pipeline_target_region: str = "Europe"
    pipeline_recency_days: int = 30
    pipeline_dedup_days: int = 7
    pipeline_max_peers: int = 5
    pipeline_max_ownership_nodes: int = 3
    pipeline_run_timeout_seconds: int = 1800
    admin_api_key: str | None = None
    generation_rate_limit_per_minute: int = 5

    model_config = SettingsConfigDict(
        env_prefix="IDC_",
        env_file=("backend/.env", ".env"),
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
    )

    @property
    def resolved_generation_runner(self) -> Literal["local", "job"]:
        if self.generation_runner == "local":
            return "local"
        if self.generation_runner == "job":
            return "job"
        return "job" if self.generation_job_id else "local"


settings = Settings()
