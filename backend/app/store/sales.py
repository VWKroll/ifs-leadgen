"""Sales claims, drafts, and draft message persistence."""
from __future__ import annotations

import uuid
from typing import Any

from pyspark.sql import Row
from pyspark.sql import functions as F

from idc_app.spark import get_spark

from ..pipeline.models import normalize_company_name
from ._core import (
    SALES_CLAIMS_TABLE,
    SALES_DRAFT_MESSAGES_TABLE,
    SALES_DRAFTS_TABLE,
    _clean_value,
    _coerce_row_to_schema,
    _quote_identifier,
    _quote_table,
    _upsert_rows,
    ensure_control_plane_tables,
    utcnow,
)


def _build_claim_record(record: dict[str, Any], now: Any) -> dict[str, Any]:
    return {
        "claim_id": record["claim_id"],
        "cluster_id": record["cluster_id"],
        "sales_item_id": record["sales_item_id"],
        "cluster_entity_id": record.get("cluster_entity_id"),
        "event_subject_company_name": record.get("event_subject_company_name"),
        "event_headline": record.get("event_headline"),
        "subject_company_name": record.get("subject_company_name"),
        "branch_type": record.get("branch_type"),
        "entity_type": record.get("entity_type"),
        "normalized_company_name": record.get("normalized_company_name")
        or normalize_company_name(str(record.get("subject_company_name") or "")),
        "claimed_by_user_id": record.get("claimed_by_user_id"),
        "claimed_by_name": record.get("claimed_by_name"),
        "claimed_by_email": record.get("claimed_by_email"),
        "claimed_at": record.get("claimed_at", now),
        "updated_at": now,
        "status": record.get("status", "claimed"),
        "salesforce_stage": record.get("salesforce_stage", "Claimed"),
        "salesforce_record_type": record.get("salesforce_record_type", "prospect"),
        "salesforce_record_id": record.get("salesforce_record_id"),
        "salesforce_owner_name": record.get("salesforce_owner_name") or record.get("claimed_by_name"),
        "salesforce_owner_id": record.get("salesforce_owner_id"),
        "salesforce_account_name": record.get("salesforce_account_name"),
        "salesforce_account_id": record.get("salesforce_account_id"),
        "salesforce_contact_count": int(record.get("salesforce_contact_count") or 0),
        "salesforce_open_opportunity_count": int(record.get("salesforce_open_opportunity_count") or 0),
        "last_activity_note": record.get("last_activity_note"),
        "last_activity_at": record.get("last_activity_at"),
        "last_pushed_at": record.get("last_pushed_at"),
        "next_step": record.get("next_step"),
        "draft_id": record.get("draft_id"),
    }


def list_sales_claim_records(
    *,
    cluster_ids: list[str] | None = None,
    sales_item_ids: list[str] | None = None,
    normalized_company_name: str | None = None,
    claimed_by_user_id: str | None = None,
    limit: int | None = 200,
) -> list[dict[str, Any]]:
    ensure_control_plane_tables()
    spark = get_spark()
    frame = spark.table(SALES_CLAIMS_TABLE)
    if cluster_ids:
        frame = frame.filter(F.col("cluster_id").isin(cluster_ids))
    if sales_item_ids:
        frame = frame.filter(F.col("sales_item_id").isin(sales_item_ids))
    if normalized_company_name:
        frame = frame.filter(F.col("normalized_company_name") == F.lit(normalized_company_name))
    if claimed_by_user_id:
        frame = frame.filter(F.col("claimed_by_user_id") == F.lit(claimed_by_user_id))
    frame = frame.orderBy(F.col("claimed_at").desc())
    if limit:
        frame = frame.limit(limit)
    pdf = frame.toPandas()
    return [{key: _clean_value(value) for key, value in row.items()} for row in pdf.to_dict(orient="records")]


def get_sales_claim_record(sales_item_id: str) -> dict[str, Any] | None:
    ensure_control_plane_tables()
    spark = get_spark()
    pdf = spark.table(SALES_CLAIMS_TABLE).filter(F.col("sales_item_id") == F.lit(sales_item_id)).limit(1).toPandas()
    if pdf.empty:
        return None
    return {key: _clean_value(value) for key, value in pdf.iloc[0].to_dict().items()}


def save_sales_claim_record(record: dict[str, Any]) -> dict[str, Any]:
    ensure_control_plane_tables()
    next_record = _build_claim_record(record, utcnow())
    _upsert_rows(SALES_CLAIMS_TABLE, ["sales_item_id"], [next_record])
    return next_record


def claim_sales_claim_record(record: dict[str, Any]) -> dict[str, Any]:
    ensure_control_plane_tables()
    spark = get_spark()
    schema = spark.table(SALES_CLAIMS_TABLE).schema
    next_record = _build_claim_record(record, utcnow())
    normalized_row = _coerce_row_to_schema(next_record, schema)
    temp_view = f"idc_claim_{uuid.uuid4().hex}"
    source_df = spark.createDataFrame([Row(**normalized_row)], schema=schema)
    source_df.createOrReplaceTempView(temp_view)

    assignments = ", ".join(
        f"target.{_quote_identifier(field.name)} = source.{_quote_identifier(field.name)}"
        for field in schema.fields
    )
    insert_columns = ", ".join(_quote_identifier(field.name) for field in schema.fields)
    insert_values = ", ".join(f"source.{_quote_identifier(field.name)}" for field in schema.fields)

    try:
        spark.sql(
            f"""
            MERGE INTO {_quote_table(SALES_CLAIMS_TABLE)} AS target
            USING {_quote_identifier(temp_view)} AS source
            ON target.{_quote_identifier("sales_item_id")} <=> source.{_quote_identifier("sales_item_id")}
            WHEN MATCHED AND target.{_quote_identifier("claimed_by_user_id")} <=> source.{_quote_identifier("claimed_by_user_id")}
              THEN UPDATE SET {assignments}
            WHEN NOT MATCHED THEN
              INSERT ({insert_columns}) VALUES ({insert_values})
            """
        )
    finally:
        spark.catalog.dropTempView(temp_view)

    claimed_record = get_sales_claim_record(str(record["sales_item_id"]))
    if claimed_record is None:
        raise RuntimeError("Unable to persist claimed record.")
    return claimed_record


def list_sales_draft_records() -> list[dict[str, Any]]:
    ensure_control_plane_tables()
    spark = get_spark()
    pdf = spark.table(SALES_DRAFTS_TABLE).orderBy(F.col("updated_at").desc()).toPandas()
    return [{key: _clean_value(value) for key, value in row.items()} for row in pdf.to_dict(orient="records")]


def get_sales_draft_record(sales_item_id: str) -> dict[str, Any] | None:
    ensure_control_plane_tables()
    spark = get_spark()
    pdf = spark.table(SALES_DRAFTS_TABLE).filter(F.col("sales_item_id") == F.lit(sales_item_id)).limit(1).toPandas()
    if pdf.empty:
        return None
    return {key: _clean_value(value) for key, value in pdf.iloc[0].to_dict().items()}


def save_sales_draft_record(record: dict[str, Any]) -> dict[str, Any]:
    ensure_control_plane_tables()
    now = utcnow()
    next_record = {
        "draft_id": record["draft_id"],
        "claim_id": record["claim_id"],
        "cluster_id": record["cluster_id"],
        "sales_item_id": record["sales_item_id"],
        "draft_payload_json": record.get("draft_payload_json"),
        "draft_status": record.get("draft_status", "drafting"),
        "last_generated_at": record.get("last_generated_at", now),
        "last_pushed_at": record.get("last_pushed_at"),
        "pushed_by_name": record.get("pushed_by_name"),
        "updated_at": now,
    }
    _upsert_rows(SALES_DRAFTS_TABLE, ["sales_item_id"], [next_record])
    return next_record


def list_sales_draft_message_records(draft_id: str) -> list[dict[str, Any]]:
    ensure_control_plane_tables()
    spark = get_spark()
    pdf = (
        spark.table(SALES_DRAFT_MESSAGES_TABLE)
        .filter(F.col("draft_id") == F.lit(draft_id))
        .orderBy(F.col("created_at").asc())
        .toPandas()
    )
    return [{key: _clean_value(value) for key, value in row.items()} for row in pdf.to_dict(orient="records")]


def save_sales_draft_message_record(record: dict[str, Any]) -> dict[str, Any]:
    ensure_control_plane_tables()
    next_record = {
        "message_id": record["message_id"],
        "draft_id": record["draft_id"],
        "cluster_id": record["cluster_id"],
        "sales_item_id": record["sales_item_id"],
        "role": record.get("role", "assistant"),
        "channel": record.get("channel", "system"),
        "content": record.get("content", ""),
        "created_at": record.get("created_at", utcnow()),
    }
    _upsert_rows(SALES_DRAFT_MESSAGES_TABLE, ["message_id"], [next_record])
    return next_record
