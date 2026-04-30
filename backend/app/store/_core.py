"""Shared helpers, table constants, and schema definitions for the store layer."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any

import pandas as pd
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from idc_app.config import CATALOG, SCHEMA, TABLES
from idc_app.spark import get_spark

from ..pipeline.models import normalize_company_name
from ..settings import settings

# ---------------------------------------------------------------------------
# Table FQNs
# ---------------------------------------------------------------------------

PIPELINE_SETTINGS_TABLE = f"{CATALOG}.{SCHEMA}.pipeline_settings"
GENERATION_RUNS_TABLE = f"{CATALOG}.{SCHEMA}.generation_runs"
KNOWLEDGE_BASE_DOCUMENTS_TABLE = f"{CATALOG}.{SCHEMA}.knowledge_base_documents"
CHAT_NOTES_TABLE = f"{CATALOG}.{SCHEMA}.cluster_chat_notes"
SALES_CLAIMS_TABLE = f"{CATALOG}.{SCHEMA}.sales_claims"
SALES_DRAFTS_TABLE = f"{CATALOG}.{SCHEMA}.salesforce_drafts"
SALES_DRAFT_MESSAGES_TABLE = f"{CATALOG}.{SCHEMA}.sales_draft_messages"
USER_MEMORY_TABLE = f"{CATALOG}.{SCHEMA}.user_memory"

# ---------------------------------------------------------------------------
# DDL schemas
# ---------------------------------------------------------------------------

PIPELINE_SETTINGS_SCHEMA = """
    settings_id STRING,
    schedule_enabled BOOLEAN,
    schedule_interval_hours INT,
    target_region STRING,
    recency_days INT,
    dedup_days INT,
    max_peers INT,
    max_ownership_nodes INT,
    generation_runner STRING,
    databricks_job_id BIGINT,
    openai_model STRING,
    chat_model STRING,
    provider_name STRING,
    kb_vector_store_id STRING,
    kb_max_results INT,
    kb_cleanup_mode STRING,
    kb_cleanup_on_sync BOOLEAN,
    kb_document_retention_days INT,
    kb_last_synced_at TIMESTAMP,
    kb_last_error STRING,
    kb_status STRING,
    updated_at TIMESTAMP,
    updated_by STRING,
    last_successful_run_id STRING,
    last_successful_run_at TIMESTAMP,
    next_scheduled_run TIMESTAMP
"""

GENERATION_RUNS_SCHEMA = """
    app_run_id STRING,
    trigger_source STRING,
    requested_by STRING,
    research_mode STRING,
    research_target STRING,
    target_region STRING,
    company_name STRING,
    runner_type STRING,
    status STRING,
    requested_at TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    updated_at TIMESTAMP,
    created_cluster_id STRING,
    duplicate_skipped BOOLEAN,
    error_message STRING,
    step_statuses_json STRING,
    databricks_job_id BIGINT,
    databricks_run_id BIGINT,
    job_url STRING
"""

KNOWLEDGE_BASE_DOCUMENTS_SCHEMA = """
    document_id STRING,
    cluster_id STRING,
    document_kind STRING,
    title STRING,
    file_path STRING,
    content_sha STRING,
    source_run_id STRING,
    entity_id STRING,
    source_id STRING,
    linked_entity_id STRING,
    attributes_json STRING,
    vector_store_id STRING,
    uploaded_file_id STRING,
    vector_store_file_id STRING,
    sync_status STRING,
    synced_at TIMESTAMP,
    error_message STRING,
    updated_at TIMESTAMP
"""

CHAT_NOTES_SCHEMA = """
    note_id STRING,
    cluster_id STRING,
    title STRING,
    summary_markdown STRING,
    source_response_id STRING,
    source_message_count INT,
    committed_at TIMESTAMP,
    committed_by STRING,
    updated_at TIMESTAMP
"""

SALES_CLAIMS_SCHEMA = """
    claim_id STRING,
    cluster_id STRING,
    sales_item_id STRING,
    cluster_entity_id STRING,
    event_subject_company_name STRING,
    event_headline STRING,
    subject_company_name STRING,
    branch_type STRING,
    entity_type STRING,
    normalized_company_name STRING,
    claimed_by_user_id STRING,
    claimed_by_name STRING,
    claimed_by_email STRING,
    claimed_at TIMESTAMP,
    updated_at TIMESTAMP,
    status STRING,
    salesforce_stage STRING,
    salesforce_record_type STRING,
    salesforce_record_id STRING,
    salesforce_owner_name STRING,
    salesforce_owner_id STRING,
    salesforce_account_name STRING,
    salesforce_account_id STRING,
    salesforce_contact_count INT,
    salesforce_open_opportunity_count INT,
    last_activity_note STRING,
    last_activity_at TIMESTAMP,
    last_pushed_at TIMESTAMP,
    next_step STRING,
    draft_id STRING
"""

SALES_DRAFTS_SCHEMA = """
    draft_id STRING,
    claim_id STRING,
    cluster_id STRING,
    sales_item_id STRING,
    draft_payload_json STRING,
    draft_status STRING,
    last_generated_at TIMESTAMP,
    last_pushed_at TIMESTAMP,
    pushed_by_name STRING,
    updated_at TIMESTAMP
"""

SALES_DRAFT_MESSAGES_SCHEMA = """
    message_id STRING,
    draft_id STRING,
    cluster_id STRING,
    sales_item_id STRING,
    role STRING,
    channel STRING,
    content STRING,
    created_at TIMESTAMP
"""

USER_MEMORY_SCHEMA = """
    user_id STRING,
    memory_key STRING,
    memory_value STRING,
    updated_at TIMESTAMP
"""

# ---------------------------------------------------------------------------
# Column maps (for _ensure_columns)
# ---------------------------------------------------------------------------

EVENT_CLUSTER_LOCATION_COLUMNS = {
    "subject_state": "STRING",
    "subject_city": "STRING",
    "subject_address": "STRING",
    "subject_latitude": "DOUBLE",
    "subject_longitude": "DOUBLE",
}

ENTITY_LOCATION_COLUMNS = {
    "entity_country": "STRING",
    "entity_region": "STRING",
    "entity_state": "STRING",
    "entity_city": "STRING",
    "entity_address": "STRING",
    "entity_latitude": "DOUBLE",
    "entity_longitude": "DOUBLE",
}

TERMINAL_STATUSES = ("succeeded", "failed", "skipped", "cancelled")

_CONTROL_PLANE_TABLES_READY = False
_CONTROL_PLANE_TABLES_LOCK = Lock()
_PIPELINE_OUTPUT_TABLES_READY = False
_PIPELINE_OUTPUT_TABLES_LOCK = Lock()

PIPELINE_SETTINGS_COLUMNS = {
    "settings_id": "STRING",
    "schedule_enabled": "BOOLEAN",
    "schedule_interval_hours": "INT",
    "target_region": "STRING",
    "recency_days": "INT",
    "dedup_days": "INT",
    "max_peers": "INT",
    "max_ownership_nodes": "INT",
    "generation_runner": "STRING",
    "databricks_job_id": "BIGINT",
    "openai_model": "STRING",
    "chat_model": "STRING",
    "provider_name": "STRING",
    "kb_vector_store_id": "STRING",
    "kb_max_results": "INT",
    "kb_cleanup_mode": "STRING",
    "kb_cleanup_on_sync": "BOOLEAN",
    "kb_document_retention_days": "INT",
    "kb_last_synced_at": "TIMESTAMP",
    "kb_last_error": "STRING",
    "kb_status": "STRING",
    "updated_at": "TIMESTAMP",
    "updated_by": "STRING",
    "last_successful_run_id": "STRING",
    "last_successful_run_at": "TIMESTAMP",
    "next_scheduled_run": "TIMESTAMP",
}

KNOWLEDGE_BASE_DOCUMENT_COLUMNS = {
    "document_id": "STRING",
    "cluster_id": "STRING",
    "document_kind": "STRING",
    "title": "STRING",
    "file_path": "STRING",
    "content_sha": "STRING",
    "source_run_id": "STRING",
    "entity_id": "STRING",
    "source_id": "STRING",
    "linked_entity_id": "STRING",
    "attributes_json": "STRING",
    "vector_store_id": "STRING",
    "uploaded_file_id": "STRING",
    "vector_store_file_id": "STRING",
    "sync_status": "STRING",
    "synced_at": "TIMESTAMP",
    "error_message": "STRING",
    "updated_at": "TIMESTAMP",
}

SALES_CLAIM_COLUMNS = {
    "claim_id": "STRING",
    "cluster_id": "STRING",
    "sales_item_id": "STRING",
    "cluster_entity_id": "STRING",
    "event_subject_company_name": "STRING",
    "event_headline": "STRING",
    "subject_company_name": "STRING",
    "branch_type": "STRING",
    "entity_type": "STRING",
    "normalized_company_name": "STRING",
    "claimed_by_user_id": "STRING",
    "claimed_by_name": "STRING",
    "claimed_by_email": "STRING",
    "claimed_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
    "status": "STRING",
    "salesforce_stage": "STRING",
    "salesforce_record_type": "STRING",
    "salesforce_record_id": "STRING",
    "salesforce_owner_name": "STRING",
    "salesforce_owner_id": "STRING",
    "salesforce_account_name": "STRING",
    "salesforce_account_id": "STRING",
    "salesforce_contact_count": "INT",
    "salesforce_open_opportunity_count": "INT",
    "last_activity_note": "STRING",
    "last_activity_at": "TIMESTAMP",
    "last_pushed_at": "TIMESTAMP",
    "next_step": "STRING",
    "draft_id": "STRING",
}

SALES_DRAFT_COLUMNS = {
    "draft_id": "STRING",
    "claim_id": "STRING",
    "cluster_id": "STRING",
    "sales_item_id": "STRING",
    "draft_payload_json": "STRING",
    "draft_status": "STRING",
    "last_generated_at": "TIMESTAMP",
    "last_pushed_at": "TIMESTAMP",
    "pushed_by_name": "STRING",
    "updated_at": "TIMESTAMP",
}

SALES_DRAFT_MESSAGE_COLUMNS = {
    "message_id": "STRING",
    "draft_id": "STRING",
    "cluster_id": "STRING",
    "sales_item_id": "STRING",
    "role": "STRING",
    "channel": "STRING",
    "content": "STRING",
    "created_at": "TIMESTAMP",
}

USER_MEMORY_COLUMNS = {
    "user_id": "STRING",
    "memory_key": "STRING",
    "memory_value": "STRING",
    "updated_at": "TIMESTAMP",
}

GENERATION_RUN_COLUMNS = {
    "app_run_id": "STRING",
    "trigger_source": "STRING",
    "requested_by": "STRING",
    "research_mode": "STRING",
    "research_target": "STRING",
    "target_region": "STRING",
    "company_name": "STRING",
    "runner_type": "STRING",
    "status": "STRING",
    "requested_at": "TIMESTAMP",
    "started_at": "TIMESTAMP",
    "finished_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
    "created_cluster_id": "STRING",
    "duplicate_skipped": "BOOLEAN",
    "error_message": "STRING",
    "step_statuses_json": "STRING",
    "databricks_job_id": "BIGINT",
    "databricks_run_id": "BIGINT",
    "job_url": "STRING",
}


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if pd.isna(value):
        return None
    return value


def _normalize_timestamp(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.tz_localize("UTC").to_pydatetime()
        return value.tz_convert("UTC").to_pydatetime()
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return value


def _coerce_row_to_schema(row: dict[str, Any], schema: StructType) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field in schema.fields:
        value = row.get(field.name)
        cleaned = _clean_value(value)
        if field.dataType.typeName() == "timestamp":
            normalized[field.name] = _normalize_timestamp(cleaned)
        else:
            normalized[field.name] = cleaned
    return normalized


def _quote_sql(value: str) -> str:
    return value.replace("'", "''")


def _quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def _quote_table(table_fqn: str) -> str:
    return ".".join(_quote_identifier(part) for part in table_fqn.split("."))


def _ensure_table(table_fqn: str, ddl: str) -> None:
    spark = get_spark()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_fqn} ({ddl}) USING DELTA")


def _ensure_columns(table_fqn: str, columns: dict[str, str]) -> None:
    spark = get_spark()
    existing = {field.name for field in spark.table(table_fqn).schema.fields}
    missing = {name: dtype for name, dtype in columns.items() if name not in existing}
    if not missing:
        return
    ddl = ", ".join(f"{name} {dtype}" for name, dtype in missing.items())
    spark.sql(f"ALTER TABLE {table_fqn} ADD COLUMNS ({ddl})")


def ensure_control_plane_tables() -> None:
    global _CONTROL_PLANE_TABLES_READY
    with _CONTROL_PLANE_TABLES_LOCK:
        if _CONTROL_PLANE_TABLES_READY:
            return
        _ensure_table(PIPELINE_SETTINGS_TABLE, PIPELINE_SETTINGS_SCHEMA)
        _ensure_table(GENERATION_RUNS_TABLE, GENERATION_RUNS_SCHEMA)
        _ensure_table(KNOWLEDGE_BASE_DOCUMENTS_TABLE, KNOWLEDGE_BASE_DOCUMENTS_SCHEMA)
        _ensure_table(CHAT_NOTES_TABLE, CHAT_NOTES_SCHEMA)
        _ensure_table(SALES_CLAIMS_TABLE, SALES_CLAIMS_SCHEMA)
        _ensure_table(SALES_DRAFTS_TABLE, SALES_DRAFTS_SCHEMA)
        _ensure_table(SALES_DRAFT_MESSAGES_TABLE, SALES_DRAFT_MESSAGES_SCHEMA)
        _ensure_table(USER_MEMORY_TABLE, USER_MEMORY_SCHEMA)
        _ensure_columns(PIPELINE_SETTINGS_TABLE, PIPELINE_SETTINGS_COLUMNS)
        _ensure_columns(GENERATION_RUNS_TABLE, GENERATION_RUN_COLUMNS)
        _ensure_columns(KNOWLEDGE_BASE_DOCUMENTS_TABLE, KNOWLEDGE_BASE_DOCUMENT_COLUMNS)
        _ensure_columns(SALES_CLAIMS_TABLE, SALES_CLAIM_COLUMNS)
        _ensure_columns(SALES_DRAFTS_TABLE, SALES_DRAFT_COLUMNS)
        _ensure_columns(SALES_DRAFT_MESSAGES_TABLE, SALES_DRAFT_MESSAGE_COLUMNS)
        _ensure_columns(USER_MEMORY_TABLE, USER_MEMORY_COLUMNS)
        _CONTROL_PLANE_TABLES_READY = True


def ensure_pipeline_output_tables() -> None:
    global _PIPELINE_OUTPUT_TABLES_READY
    with _PIPELINE_OUTPUT_TABLES_LOCK:
        if _PIPELINE_OUTPUT_TABLES_READY:
            return
        _ensure_columns(TABLES["event_clusters"], EVENT_CLUSTER_LOCATION_COLUMNS)
        _ensure_columns(TABLES["cluster_entities"], ENTITY_LOCATION_COLUMNS)
        _ensure_columns(TABLES["cluster_role_recommendations"], {"hypothesized_services_json": "STRING"})
        _PIPELINE_OUTPUT_TABLES_READY = True


def ensure_pipeline_tables() -> None:
    ensure_control_plane_tables()
    ensure_pipeline_output_tables()


def _table_append(table_fqn: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    spark = get_spark()
    schema = spark.table(table_fqn).schema
    normalized_rows = [_coerce_row_to_schema(row, schema) for row in rows]
    spark.createDataFrame([Row(**row) for row in normalized_rows], schema=schema).write.format("delta").mode("append").saveAsTable(
        table_fqn
    )


def append_rows(table_fqn: str, rows: list[dict[str, Any]]) -> None:
    _table_append(table_fqn, rows)


def _upsert_rows(table_fqn: str, key_names: list[str], rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    spark = get_spark()
    schema = spark.table(table_fqn).schema
    normalized_rows = [_coerce_row_to_schema(row, schema) for row in rows]
    temp_view = f"idc_upsert_{uuid.uuid4().hex}"
    source_df = spark.createDataFrame([Row(**row) for row in normalized_rows], schema=schema)
    source_df.createOrReplaceTempView(temp_view)

    assignments = ", ".join(
        f"target.{_quote_identifier(field.name)} = source.{_quote_identifier(field.name)}"
        for field in schema.fields
    )
    insert_columns = ", ".join(_quote_identifier(field.name) for field in schema.fields)
    insert_values = ", ".join(f"source.{_quote_identifier(field.name)}" for field in schema.fields)
    join_condition = " AND ".join(
        f"target.{_quote_identifier(key_name)} <=> source.{_quote_identifier(key_name)}"
        for key_name in key_names
    )

    try:
        spark.sql(
            f"""
            MERGE INTO {_quote_table(table_fqn)} AS target
            USING {_quote_identifier(temp_view)} AS source
            ON {join_condition}
            WHEN MATCHED THEN UPDATE SET {assignments}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
            """
        )
    finally:
        spark.catalog.dropTempView(temp_view)
