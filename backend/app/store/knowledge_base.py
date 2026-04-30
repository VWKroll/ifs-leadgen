"""Knowledge-base document and chat note persistence."""
from __future__ import annotations

from typing import Any

from pyspark.sql import Row
from pyspark.sql import functions as F

from idc_app.spark import get_spark

from ._core import (
    CHAT_NOTES_TABLE,
    KNOWLEDGE_BASE_DOCUMENTS_TABLE,
    _clean_value,
    _coerce_row_to_schema,
    _quote_table,
    _upsert_rows,
    ensure_control_plane_tables,
    utcnow,
)


def get_kb_document_record(
    cluster_id: str,
    *,
    document_kind: str = "cluster",
    entity_id: str | None = None,
    source_id: str | None = None,
) -> dict[str, Any] | None:
    ensure_control_plane_tables()
    spark = get_spark()
    frame = spark.table(KNOWLEDGE_BASE_DOCUMENTS_TABLE).filter(F.col("cluster_id") == F.lit(cluster_id))
    frame = frame.filter(F.col("document_kind") == F.lit(document_kind))
    if entity_id is None:
        frame = frame.filter(F.col("entity_id").isNull())
    else:
        frame = frame.filter(F.col("entity_id") == F.lit(entity_id))
    if source_id is None:
        frame = frame.filter(F.col("source_id").isNull())
    else:
        frame = frame.filter(F.col("source_id") == F.lit(source_id))
    pdf = frame.limit(1).toPandas()
    if pdf.empty:
        return None
    return {key: _clean_value(value) for key, value in pdf.iloc[0].to_dict().items()}


def list_kb_document_records() -> list[dict[str, Any]]:
    ensure_control_plane_tables()
    spark = get_spark()
    pdf = spark.table(KNOWLEDGE_BASE_DOCUMENTS_TABLE).orderBy("cluster_id").toPandas()
    return [{key: _clean_value(value) for key, value in row.items()} for row in pdf.to_dict(orient="records")]


def save_kb_document_record(record: dict[str, Any]) -> dict[str, Any]:
    ensure_control_plane_tables()
    next_record = {
        "document_id": record.get("document_id"),
        "cluster_id": record["cluster_id"],
        "document_kind": record.get("document_kind", "cluster"),
        "title": record.get("title"),
        "file_path": record.get("file_path"),
        "content_sha": record.get("content_sha"),
        "source_run_id": record.get("source_run_id"),
        "entity_id": record.get("entity_id"),
        "source_id": record.get("source_id"),
        "linked_entity_id": record.get("linked_entity_id"),
        "attributes_json": record.get("attributes_json"),
        "vector_store_id": record.get("vector_store_id"),
        "uploaded_file_id": record.get("uploaded_file_id"),
        "vector_store_file_id": record.get("vector_store_file_id"),
        "sync_status": record.get("sync_status", "pending"),
        "synced_at": record.get("synced_at"),
        "error_message": record.get("error_message"),
        "updated_at": utcnow(),
    }
    _upsert_rows(KNOWLEDGE_BASE_DOCUMENTS_TABLE, ["cluster_id", "document_kind", "entity_id", "source_id"], [next_record])
    return next_record


def replace_kb_document_records(records: list[dict[str, Any]]) -> None:
    ensure_control_plane_tables()
    spark = get_spark()
    schema = spark.table(KNOWLEDGE_BASE_DOCUMENTS_TABLE).schema
    normalized_rows = [_coerce_row_to_schema(record, schema) for record in records]
    if normalized_rows:
        dataframe = spark.createDataFrame([Row(**row) for row in normalized_rows], schema=schema)
        dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(KNOWLEDGE_BASE_DOCUMENTS_TABLE)
        return
    spark.sql(f"TRUNCATE TABLE {_quote_table(KNOWLEDGE_BASE_DOCUMENTS_TABLE)}")


def count_kb_document_records(*, include_manifest: bool = False) -> int:
    ensure_control_plane_tables()
    spark = get_spark()
    frame = spark.table(KNOWLEDGE_BASE_DOCUMENTS_TABLE)
    if not include_manifest:
        frame = frame.filter(F.col("document_kind") != F.lit("manifest"))
    return int(frame.count())


def list_chat_note_records(cluster_id: str | None = None) -> list[dict[str, Any]]:
    ensure_control_plane_tables()
    spark = get_spark()
    frame = spark.table(CHAT_NOTES_TABLE)
    if cluster_id:
        frame = frame.filter(F.col("cluster_id") == F.lit(cluster_id))
    pdf = frame.orderBy(F.col("committed_at").desc()).toPandas()
    return [{key: _clean_value(value) for key, value in row.items()} for row in pdf.to_dict(orient="records")]


def save_chat_note_record(record: dict[str, Any]) -> dict[str, Any]:
    ensure_control_plane_tables()
    now = utcnow()
    next_record = {
        "note_id": record["note_id"],
        "cluster_id": record["cluster_id"],
        "title": record.get("title"),
        "summary_markdown": record.get("summary_markdown"),
        "source_response_id": record.get("source_response_id"),
        "source_message_count": record.get("source_message_count"),
        "committed_at": record.get("committed_at", now),
        "committed_by": record.get("committed_by", "app"),
        "updated_at": now,
    }
    _upsert_rows(CHAT_NOTES_TABLE, ["note_id"], [next_record])
    return next_record
