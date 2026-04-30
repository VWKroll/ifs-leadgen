from __future__ import annotations

import json
import os
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Protocol

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, PauseStatus, PeriodicTriggerConfiguration, PeriodicTriggerConfigurationTimeUnit, TriggerSettings

from .generation_runtime import execute_generation_run
from .knowledge_base import cleanup_knowledge_base, get_knowledge_base_status, sync_knowledge_base
from .pipeline.orchestrator import make_run_id
from .pipeline.provider import get_provider_health
from .settings import settings
from .store import (
    get_generation_run_record,
    get_pipeline_settings_record,
    list_active_generation_run_records,
    list_generation_run_records,
    save_generation_run_record,
    save_pipeline_settings_record,
    update_generation_run_record,
    utcnow,
)

TERMINAL_STATUSES = {"succeeded", "failed", "skipped", "cancelled"}
_KB_SYNC_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="idc-kb-sync")
_KB_SYNC_LOCK = Lock()
_KB_SYNC_FUTURE: Future | None = None


def _step_statuses() -> dict[str, str]:
    return {
        "discovery": "pending",
        "expansion": "pending",
        "scoring": "pending",
        "role_recommendation": "pending",
        "persistence": "pending",
        "knowledge_base_sync": "pending",
    }


def _dt_from_millis(value: int | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _status_from_run(run) -> tuple[str, str | None]:
    state = getattr(run, "state", None)
    life_cycle = getattr(state, "life_cycle_state", None)
    result = getattr(state, "result_state", None)
    message = getattr(state, "state_message", None)

    life_cycle_value = getattr(life_cycle, "value", str(life_cycle or "")).upper()
    result_value = getattr(result, "value", str(result or "")).upper()

    if life_cycle_value in {"PENDING", "QUEUED", "RUNNING", "TERMINATING", "WAITING_FOR_RETRY", "BLOCKED"}:
        return "running", message
    if life_cycle_value == "SKIPPED":
        return "skipped", message
    if result_value in {"SUCCESS", "SUCCEEDED"}:
        return "succeeded", message
    if result_value in {"CANCELED", "CANCELLED"}:
        return "cancelled", message
    if life_cycle_value == "INTERNAL_ERROR" or result_value in {"FAILED", "TIMEDOUT", "TIMED_OUT", "INTERNAL_ERROR"}:
        return "failed", message
    return "running", message


def _active_run_anchor(record: dict) -> datetime | None:
    anchor = record.get("started_at") or record.get("requested_at")
    if isinstance(anchor, datetime):
        return anchor if anchor.tzinfo is not None else anchor.replace(tzinfo=timezone.utc)
    return None


def _active_run_timed_out(record: dict) -> bool:
    anchor = _active_run_anchor(record)
    if anchor is None:
        return False
    return (utcnow() - anchor).total_seconds() > settings.pipeline_run_timeout_seconds


def _timeout_message(record: dict) -> str:
    app_run_id = str(record.get("app_run_id") or "unknown")
    return (
        f"Generation run {app_run_id} exceeded the timeout window "
        f"({settings.pipeline_run_timeout_seconds} seconds)."
    )


def _mark_run_failed(record: dict, message: str) -> dict:
    return update_generation_run_record(
        str(record["app_run_id"]),
        {
            "status": "failed",
            "finished_at": utcnow(),
            "error_message": message,
        },
    )


def _sync_run_record(record: dict) -> dict:
    if record.get("status") in TERMINAL_STATUSES:
        return record

    if _active_run_timed_out(record):
        return _mark_run_failed(record, _timeout_message(record))

    try:
        synced = get_generation_runner(record).sync(str(record["app_run_id"]))
    except Exception as exc:
        # Do not permanently mutate run history during a transient read-path
        # failure such as auth expiry or a temporary Databricks outage.
        return {
            **record,
            "error_message": record.get("error_message") or f"Unable to sync generation run status: {exc}",
        }

    if synced.get("status") not in TERMINAL_STATUSES and _active_run_timed_out(synced):
        return _mark_run_failed(synced, _timeout_message(synced))

    return synced


def get_workspace_client() -> WorkspaceClient:
    host = settings.db_host or os.getenv("DATABRICKS_HOST")
    profile = settings.db_profile or os.getenv("DATABRICKS_CONFIG_PROFILE")
    auth_type = settings.db_auth_type

    if auth_type == "auto":
        auth_type = "pat" if settings.pat_token else "oauth"

    if auth_type == "pat" and settings.pat_token:
        return WorkspaceClient(host=host, token=settings.pat_token)

    kwargs: dict[str, str] = {}
    if host:
        kwargs["host"] = host
    if profile:
        kwargs["profile"] = profile
    return WorkspaceClient(**kwargs)


class GenerationRunner(Protocol):
    runner_type: str

    def submit(self, app_run_id: str, settings_record: dict, requested_by: str, trigger_source: str) -> dict: ...

    def sync(self, app_run_id: str) -> dict: ...

    def cancel(self, app_run_id: str) -> dict: ...


class LocalRunner:
    runner_type = "local"

    def __init__(self) -> None:
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="idc-generation")
        self._futures: dict[str, Future] = {}
        self._lock = Lock()

    def _finalize_future(self, app_run_id: str, future: Future) -> None:
        try:
            future.result()
        except Exception as exc:
            try:
                update_generation_run_record(
                    app_run_id,
                    {
                        "status": "failed",
                        "finished_at": utcnow(),
                        "error_message": str(exc),
                    },
                )
            except KeyError:
                pass
        finally:
            with self._lock:
                self._futures.pop(app_run_id, None)

    def submit(self, app_run_id: str, settings_record: dict, requested_by: str, trigger_source: str) -> dict:
        future = self._executor.submit(execute_generation_run, app_run_id, settings_record)
        future.add_done_callback(lambda completed: self._finalize_future(app_run_id, completed))
        with self._lock:
            self._futures[app_run_id] = future
        return get_generation_run_record(app_run_id) or {}

    def sync(self, app_run_id: str) -> dict:
        record = get_generation_run_record(app_run_id)
        if record is None:
            raise KeyError(app_run_id)
        if record.get("status") in TERMINAL_STATUSES:
            return record
        with self._lock:
            future = self._futures.get(app_run_id)
        if future and future.done():
            try:
                future.result()
            except Exception as exc:
                return update_generation_run_record(
                    app_run_id,
                    {
                        "status": "failed",
                        "finished_at": utcnow(),
                        "error_message": str(exc),
                    },
                )
        return get_generation_run_record(app_run_id) or record

    def cancel(self, app_run_id: str) -> dict:
        with self._lock:
            future = self._futures.get(app_run_id)
        if future and future.cancel():
            return update_generation_run_record(app_run_id, {"status": "cancelled", "finished_at": utcnow()})
        raise RuntimeError("Local runs cannot be cancelled once execution has started.")


class DatabricksJobRunner:
    runner_type = "job"

    def submit(self, app_run_id: str, settings_record: dict, requested_by: str, trigger_source: str) -> dict:
        if not settings.generation_job_id:
            raise RuntimeError("IDC_GENERATION_JOB_ID is not configured.")
        client = get_workspace_client()
        waiter = client.jobs.run_now(
            settings.generation_job_id,
            idempotency_token=app_run_id,
            job_parameters={
                "app_run_id": app_run_id,
                "trigger_source": trigger_source,
                "requested_by": requested_by,
                "mode": "single",
            },
        )
        databricks_run_id = getattr(waiter, "run_id", None) or getattr(getattr(waiter, "response", None), "run_id", None)
        if not databricks_run_id:
            raise RuntimeError(
                "Databricks accepted the run request but did not return a run ID. "
                "Check the job configuration, task parameters, and workspace permissions."
            )
        job_url = f"{(settings.db_host or '').rstrip('/')}/jobs/{settings.generation_job_id}/runs/{databricks_run_id}" if databricks_run_id else None
        return update_generation_run_record(
            app_run_id,
            {
                "runner_type": self.runner_type,
                "databricks_job_id": settings.generation_job_id,
                "databricks_run_id": databricks_run_id,
                "job_url": job_url,
                "status": "queued",
            },
        )

    def sync(self, app_run_id: str) -> dict:
        record = get_generation_run_record(app_run_id)
        if record is None:
            raise KeyError(app_run_id)
        if record.get("status") in TERMINAL_STATUSES or not record.get("databricks_run_id"):
            return record
        client = get_workspace_client()
        run = client.jobs.get_run(int(record["databricks_run_id"]))
        status, message = _status_from_run(run)
        patch = {
            "status": status,
            "started_at": record.get("started_at") or _dt_from_millis(getattr(run, "start_time", None)),
            "finished_at": _dt_from_millis(getattr(run, "end_time", None)),
            "error_message": message if status == "failed" else record.get("error_message"),
        }
        return update_generation_run_record(app_run_id, patch)

    def cancel(self, app_run_id: str) -> dict:
        record = get_generation_run_record(app_run_id)
        if record is None:
            raise KeyError(app_run_id)
        if not record.get("databricks_run_id"):
            raise RuntimeError("This run has not been handed off to Databricks Jobs.")
        client = get_workspace_client()
        client.jobs.cancel_run(int(record["databricks_run_id"]))
        return update_generation_run_record(app_run_id, {"status": "running", "error_message": "Cancellation requested."})


def get_generation_runner(settings_record: dict | None = None) -> GenerationRunner:
    mode = str((settings_record or {}).get("runner_type") or (settings_record or {}).get("generation_runner") or settings.resolved_generation_runner)
    if mode == "job":
        return DatabricksJobRunner()
    return LocalRunnerSingleton.instance()


class LocalRunnerSingleton:
    _instance: LocalRunner | None = None
    _lock = Lock()

    @classmethod
    def instance(cls) -> LocalRunner:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = LocalRunner()
        return cls._instance


def _sync_job_schedule(settings_record: dict) -> dict:
    if not settings.generation_job_id:
        return settings_record

    client = get_workspace_client()
    job = client.jobs.get(settings.generation_job_id)
    job_settings = job.settings or JobSettings(name=settings.generation_job_name)
    interval = int(settings_record.get("schedule_interval_hours") or 2)
    pause_status = PauseStatus.UNPAUSED if settings_record.get("schedule_enabled") else PauseStatus.PAUSED
    job_settings.trigger = TriggerSettings(
        periodic=PeriodicTriggerConfiguration(interval=interval, unit=PeriodicTriggerConfigurationTimeUnit.HOURS),
        pause_status=pause_status,
    )
    job_settings.schedule = None
    client.jobs.reset(settings.generation_job_id, new_settings=job_settings)
    next_run = utcnow() + timedelta(hours=interval) if settings_record.get("schedule_enabled") else None
    return save_pipeline_settings_record(
        {
            "databricks_job_id": settings.generation_job_id,
            "next_scheduled_run": next_run,
        },
        updated_by="job-sync",
    )


def serialize_run_record(record: dict) -> dict:
    return {
        **record,
        "step_statuses": record.get("step_statuses") or json.loads(record.get("step_statuses_json") or "{}"),
    }


def get_pipeline_settings_response() -> dict:
    settings_record = get_pipeline_settings_record()
    provider = get_provider_health()
    knowledge_base = get_knowledge_base_status()
    runner_mode = str(settings_record.get("generation_runner") or settings.resolved_generation_runner)
    schedule_enabled = bool(settings_record.get("schedule_enabled"))
    databricks_job_id = settings_record.get("databricks_job_id") or settings.generation_job_id

    return {
        **settings_record,
        "generation_runner": runner_mode,
        "databricks_job_id": databricks_job_id,
        "kb_max_results": int(settings_record.get("kb_max_results") or settings.kb_max_results),
        "kb_cleanup_mode": str(settings_record.get("kb_cleanup_mode") or settings.kb_cleanup_mode),
        "kb_cleanup_on_sync": bool(settings_record.get("kb_cleanup_on_sync") if settings_record.get("kb_cleanup_on_sync") is not None else settings.kb_cleanup_on_sync),
        "kb_document_retention_days": int(settings_record.get("kb_document_retention_days") or settings.kb_document_retention_days),
        "chat_model": settings_record.get("chat_model") or settings.chat_model,
        "provider": {
            "provider_name": provider.provider_name,
            "configured": provider.configured,
            "status": "ready" if provider.configured else "missing_config",
            "message": provider.message,
        },
        "job": {
            "configured": bool(databricks_job_id),
            "runner_type": runner_mode,
            "message": "Databricks job control is configured." if databricks_job_id else "No Databricks generation job is configured.",
        },
        "last_successful_run_id": settings_record.get("last_successful_run_id"),
        "last_successful_run_at": settings_record.get("last_successful_run_at"),
        "next_scheduled_run": settings_record.get("next_scheduled_run") if schedule_enabled else None,
        "knowledge_base": knowledge_base,
    }


def update_pipeline_settings(patch: dict, updated_by: str = "app") -> dict:
    allowed_keys = {
        "schedule_enabled",
        "schedule_interval_hours",
        "target_region",
        "recency_days",
        "dedup_days",
        "max_peers",
        "max_ownership_nodes",
        "generation_runner",
        "openai_model",
        "kb_max_results",
        "kb_cleanup_mode",
        "kb_cleanup_on_sync",
        "kb_document_retention_days",
    }
    clean_patch = {key: value for key, value in patch.items() if key in allowed_keys}
    next_record = save_pipeline_settings_record(clean_patch, updated_by=updated_by)
    if str(next_record.get("generation_runner") or settings.resolved_generation_runner) == "job" and settings.generation_job_id:
        next_record = _sync_job_schedule(next_record)
    return get_pipeline_settings_response()


def _normalize_target_region(value: str | None, settings_record: dict) -> str:
    candidate = (value or "").strip()
    if candidate:
        return candidate
    fallback = str(settings_record.get("target_region") or settings.pipeline_target_region).strip()
    return fallback or settings.pipeline_target_region


def _prepare_manual_generation_settings(
    settings_record: dict,
    *,
    research_mode: str,
    target_region: str | None,
    company_name: str | None,
) -> tuple[dict, dict]:
    normalized_mode = "company" if research_mode == "company" else "region"
    next_settings = dict(settings_record)
    metadata: dict[str, str | None] = {
        "research_mode": normalized_mode,
        "research_target": None,
        "target_region": None,
        "company_name": None,
    }

    if normalized_mode == "company":
        normalized_company = (company_name or "").strip()
        if not normalized_company:
            raise ValueError("Company name is required for company research.")
        next_settings["research_mode"] = "company"
        next_settings["company_name"] = normalized_company
        next_settings["target_region"] = _normalize_target_region(target_region, settings_record)
        metadata["research_target"] = normalized_company
        metadata["target_region"] = str(next_settings["target_region"])
        metadata["company_name"] = normalized_company
        return next_settings, metadata

    normalized_region = _normalize_target_region(target_region, settings_record)
    next_settings["research_mode"] = "region"
    next_settings["target_region"] = normalized_region
    next_settings["company_name"] = None
    metadata["research_target"] = normalized_region
    metadata["target_region"] = normalized_region
    return next_settings, metadata


def start_manual_generation_run(
    requested_by: str = "app",
    *,
    research_mode: str = "region",
    target_region: str | None = None,
    company_name: str | None = None,
) -> dict:
    settings_record = get_pipeline_settings_record()
    effective_settings, run_metadata = _prepare_manual_generation_settings(
        settings_record,
        research_mode=research_mode,
        target_region=target_region,
        company_name=company_name,
    )
    if run_metadata.get("target_region"):
        save_pipeline_settings_record({"target_region": run_metadata["target_region"]}, updated_by="research-request")
    app_run_id = make_run_id()
    runner = get_generation_runner(effective_settings)
    trigger_source = "manual_company" if run_metadata["research_mode"] == "company" else "manual_region"
    saved_record = save_generation_run_record(
        {
            "app_run_id": app_run_id,
            "trigger_source": trigger_source,
            "requested_by": requested_by,
            **run_metadata,
            "runner_type": runner.runner_type,
            "status": "queued",
            "step_statuses": _step_statuses(),
            "databricks_job_id": settings.generation_job_id if runner.runner_type == "job" else None,
        }
    )
    try:
        submitted_record = runner.submit(app_run_id, effective_settings, requested_by=requested_by, trigger_source=trigger_source)
    except Exception as exc:
        failed_record = update_generation_run_record(
            app_run_id,
            {
                "status": "failed",
                "finished_at": utcnow(),
                "error_message": str(exc),
            },
        )
        return serialize_run_record(failed_record)
    return serialize_run_record(submitted_record or saved_record)


def trigger_knowledge_base_sync(cluster_id: str | None = None, full_refresh: bool = True) -> dict:
    global _KB_SYNC_FUTURE

    def run_sync() -> None:
        sync_knowledge_base(cluster_id=cluster_id, full_refresh=full_refresh)
        settings_record = get_pipeline_settings_record()
        cleanup_mode = str(settings_record.get("kb_cleanup_mode") or settings.kb_cleanup_mode)
        if settings_record.get("kb_cleanup_on_sync") and cleanup_mode != "off":
            cleanup_knowledge_base(mode=cleanup_mode)

    with _KB_SYNC_LOCK:
        if _KB_SYNC_FUTURE and not _KB_SYNC_FUTURE.done():
            current_status = get_knowledge_base_status()
            return {
                **current_status,
                "status": "syncing",
            }
        save_pipeline_settings_record(
            {
                "kb_status": "syncing",
                "kb_last_error": None,
            },
            updated_by="knowledge-base",
        )
        _KB_SYNC_FUTURE = _KB_SYNC_EXECUTOR.submit(run_sync)

    current_status = get_knowledge_base_status()
    return {
        **current_status,
        "status": "syncing",
        "last_error": None,
    }


def run_knowledge_base_cleanup(mode: str | None = None) -> dict:
    return cleanup_knowledge_base(mode=mode)


def get_generation_run(app_run_id: str) -> dict:
    record = get_generation_run_record(app_run_id)
    if record is None:
        raise KeyError(app_run_id)
    synced = _sync_run_record(record)
    return serialize_run_record(synced)


def list_generation_runs(limit: int = 25) -> list[dict]:
    records = list_generation_run_records(limit=limit)
    items: list[dict] = []
    for record in records:
        synced = _sync_run_record(record)
        items.append(serialize_run_record(synced))
    return items


def cancel_generation_run(app_run_id: str) -> dict:
    record = get_generation_run_record(app_run_id)
    if record is None:
        raise KeyError(app_run_id)
    runner = get_generation_runner(record)
    return serialize_run_record(runner.cancel(app_run_id))


def reconcile_orphaned_local_runs() -> int:
    recovered = 0
    for record in list_active_generation_run_records(runner_type="local"):
        app_run_id = str(record.get("app_run_id") or "")
        if not app_run_id:
            continue
        requested_status = str(record.get("status") or "running")
        message = (
            "Local run was interrupted by an application restart."
            if requested_status == "running"
            else "Local run never started before the application restarted."
        )
        try:
            update_generation_run_record(
                app_run_id,
                {
                    "status": "failed",
                    "finished_at": utcnow(),
                    "error_message": message,
                },
            )
            recovered += 1
        except KeyError:
            continue
    return recovered
