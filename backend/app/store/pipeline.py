"""Pipeline settings and generation run persistence."""
from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from pyspark.sql import functions as F

from idc_app.spark import get_spark

from ..settings import settings
from ._core import (
    GENERATION_RUNS_TABLE,
    PIPELINE_SETTINGS_TABLE,
    TERMINAL_STATUSES,
    _clean_value,
    _table_append,
    _upsert_rows,
    ensure_control_plane_tables,
    utcnow,
)


def build_default_pipeline_settings() -> dict[str, Any]:
    now = utcnow()
    interval = 2
    return {
        "settings_id": "default",
        "schedule_enabled": False,
        "schedule_interval_hours": interval,
        "target_region": settings.pipeline_target_region,
        "recency_days": settings.pipeline_recency_days,
        "dedup_days": settings.pipeline_dedup_days,
        "max_peers": settings.pipeline_max_peers,
        "max_ownership_nodes": settings.pipeline_max_ownership_nodes,
        "generation_runner": settings.resolved_generation_runner,
        "databricks_job_id": settings.generation_job_id,
        "openai_model": settings.openai_model,
        "chat_model": settings.chat_model,
        "provider_name": "azure_openai",
        "kb_vector_store_id": settings.kb_vector_store_id,
        "kb_max_results": settings.kb_max_results,
        "kb_cleanup_mode": settings.kb_cleanup_mode,
        "kb_cleanup_on_sync": settings.kb_cleanup_on_sync,
        "kb_document_retention_days": settings.kb_document_retention_days,
        "kb_last_synced_at": None,
        "kb_last_error": None,
        "kb_status": "not_synced",
        "updated_at": now,
        "updated_by": "system",
        "last_successful_run_id": None,
        "last_successful_run_at": None,
        "next_scheduled_run": now + timedelta(hours=interval),
    }


def get_pipeline_settings_record() -> dict[str, Any]:
    ensure_control_plane_tables()
    spark = get_spark()
    pdf = spark.table(PIPELINE_SETTINGS_TABLE).filter("settings_id = 'default'").limit(1).toPandas()
    if pdf.empty:
        record = build_default_pipeline_settings()
        _table_append(PIPELINE_SETTINGS_TABLE, [record])
        return record
    return {key: _clean_value(value) for key, value in pdf.iloc[0].to_dict().items()}


def save_pipeline_settings_record(patch: dict[str, Any], updated_by: str = "app") -> dict[str, Any]:
    current = get_pipeline_settings_record()
    next_record = {
        **current,
        **patch,
        "settings_id": "default",
        "updated_at": utcnow(),
        "updated_by": updated_by,
    }
    if next_record.get("schedule_enabled"):
        interval_hours = int(next_record.get("schedule_interval_hours") or 2)
        anchor = utcnow()
        next_record["next_scheduled_run"] = anchor + timedelta(hours=interval_hours)
    else:
        next_record["next_scheduled_run"] = None
    _upsert_rows(PIPELINE_SETTINGS_TABLE, ["settings_id"], [next_record])
    return next_record


def list_generation_run_records(limit: int = 25) -> list[dict[str, Any]]:
    ensure_control_plane_tables()
    spark = get_spark()
    pdf = spark.table(GENERATION_RUNS_TABLE).orderBy("requested_at", ascending=False).limit(limit).toPandas()
    return [{key: _clean_value(value) for key, value in row.items()} for row in pdf.to_dict(orient="records")]


def get_generation_run_record(app_run_id: str) -> dict[str, Any] | None:
    ensure_control_plane_tables()
    spark = get_spark()
    pdf = spark.table(GENERATION_RUNS_TABLE).filter(F.col("app_run_id") == F.lit(app_run_id)).limit(1).toPandas()
    if pdf.empty:
        return None
    return {key: _clean_value(value) for key, value in pdf.iloc[0].to_dict().items()}


def save_generation_run_record(record: dict[str, Any]) -> dict[str, Any]:
    ensure_control_plane_tables()
    now = utcnow()
    next_record = {
        "app_run_id": record["app_run_id"],
        "trigger_source": record.get("trigger_source", "manual"),
        "requested_by": record.get("requested_by", "app"),
        "research_mode": record.get("research_mode"),
        "research_target": record.get("research_target"),
        "target_region": record.get("target_region"),
        "company_name": record.get("company_name"),
        "runner_type": record.get("runner_type", settings.resolved_generation_runner),
        "status": record.get("status", "queued"),
        "requested_at": record.get("requested_at", now),
        "started_at": record.get("started_at"),
        "finished_at": record.get("finished_at"),
        "updated_at": now,
        "created_cluster_id": record.get("created_cluster_id"),
        "duplicate_skipped": bool(record.get("duplicate_skipped", False)),
        "error_message": record.get("error_message"),
        "step_statuses_json": json.dumps(record.get("step_statuses", {})),
        "databricks_job_id": record.get("databricks_job_id"),
        "databricks_run_id": record.get("databricks_run_id"),
        "job_url": record.get("job_url"),
    }
    _upsert_rows(GENERATION_RUNS_TABLE, ["app_run_id"], [next_record])
    return next_record


def update_generation_run_record(app_run_id: str, patch: dict[str, Any]) -> dict[str, Any]:
    current = get_generation_run_record(app_run_id)
    if current is None:
        raise KeyError(app_run_id)
    merged = {
        **current,
        **patch,
        "app_run_id": app_run_id,
        "step_statuses": patch.get("step_statuses")
        or json.loads(current.get("step_statuses_json") or "{}"),
    }
    saved = save_generation_run_record(merged)
    if saved["status"] == "succeeded":
        save_pipeline_settings_record(
            {
                "last_successful_run_id": app_run_id,
                "last_successful_run_at": saved.get("finished_at") or utcnow(),
            },
            updated_by="pipeline",
        )
    return saved


def recent_distinct_values(table_fqn: str, column: str, timestamp_column: str, days: int) -> list[str]:
    spark = get_spark()
    cutoff = utcnow() - timedelta(days=days)
    rows = (
        spark.table(table_fqn)
        .filter(F.col(timestamp_column) >= F.lit(cutoff))
        .select(column)
        .distinct()
        .collect()
    )
    return [getattr(row, column) for row in rows if getattr(row, column, None)]


def list_active_generation_run_records(runner_type: str | None = None) -> list[dict[str, Any]]:
    ensure_control_plane_tables()
    spark = get_spark()
    frame = spark.table(GENERATION_RUNS_TABLE).filter(~F.col("status").isin(*TERMINAL_STATUSES))
    if runner_type:
        frame = frame.filter(F.col("runner_type") == F.lit(runner_type))
    pdf = frame.orderBy("requested_at", ascending=False).toPandas()
    return [{key: _clean_value(value) for key, value in row.items()} for row in pdf.to_dict(orient="records")]
