from __future__ import annotations

from .knowledge_base import sync_knowledge_base
from .pipeline import GenerationPipelineResult
from .pipeline.orchestrator import run_generation_pipeline
from .pipeline.provider import get_provider_health
from .services import invalidate_read_caches
from .store import get_pipeline_settings_record, update_generation_run_record, utcnow


def _default_step_statuses() -> dict[str, str]:
    return {
        "discovery": "pending",
        "expansion": "pending",
        "scoring": "pending",
        "role_recommendation": "pending",
        "persistence": "pending",
        "knowledge_base_sync": "pending",
    }


def execute_generation_run(app_run_id: str, settings_record: dict | None = None) -> GenerationPipelineResult:
    config_row = settings_record if settings_record is not None else get_pipeline_settings_record()

    # Pre-run provider validation — fail fast with a clear message
    provider = get_provider_health()
    if not provider.configured:
        update_generation_run_record(
            app_run_id,
            {
                "status": "failed",
                "started_at": utcnow(),
                "finished_at": utcnow(),
                "error_message": (
                    "Azure OpenAI provider is not configured. "
                    "Set IDC_AZURE_OPENAI_ENDPOINT and IDC_AZURE_OPENAI_API_KEY "
                    "in your Databricks App secrets or backend/.env file."
                ),
            },
        )
        return GenerationPipelineResult(
            status="failed",
            error_message=provider.message,
        )

    step_statuses = _default_step_statuses()
    update_generation_run_record(
        app_run_id,
        {
            "status": "running",
            "started_at": utcnow(),
            "step_statuses": step_statuses.copy(),
        },
    )

    def update_step(step: str, status: str) -> None:
        step_statuses[step] = status
        update_generation_run_record(app_run_id, {"step_statuses": step_statuses.copy()})

    try:
        result = run_generation_pipeline(app_run_id, config_row, update_step)
        final_status = "skipped" if result.duplicate_skipped else result.status
        final_error = result.error_message

        if result.status == "succeeded":
            invalidate_read_caches()
            if result.created_cluster_id:
                update_step("knowledge_base_sync", "queued")
                try:
                    sync_knowledge_base(
                        cluster_id=result.created_cluster_id,
                        source_run_id=app_run_id,
                        on_lock_acquired=lambda: update_step("knowledge_base_sync", "running"),
                    )
                    update_step("knowledge_base_sync", "succeeded")
                except Exception as exc:
                    update_step("knowledge_base_sync", "failed")
                    final_status = "failed"
                    final_error = str(exc)
            else:
                update_step("knowledge_base_sync", "skipped")
        else:
            update_step("knowledge_base_sync", "skipped")

        update_generation_run_record(
            app_run_id,
            {
                "status": final_status,
                "finished_at": utcnow(),
                "created_cluster_id": result.created_cluster_id,
                "duplicate_skipped": result.duplicate_skipped,
                "error_message": final_error,
                "step_statuses": step_statuses.copy(),
            },
        )
        return result
    except Exception as exc:
        update_generation_run_record(
            app_run_id,
            {
                "status": "failed",
                "finished_at": utcnow(),
                "error_message": str(exc),
                "step_statuses": step_statuses.copy(),
            },
        )
        raise
