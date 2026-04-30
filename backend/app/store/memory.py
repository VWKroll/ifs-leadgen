"""User memory persistence."""
from __future__ import annotations

import json
from typing import Any

from pyspark.sql import functions as F

from idc_app.spark import get_spark

from ._core import (
    USER_MEMORY_TABLE,
    _clean_value,
    _upsert_rows,
    ensure_control_plane_tables,
    utcnow,
)


def get_user_memory(user_id: str) -> dict[str, Any]:
    """Return all memory entries for a user as {memory_key: parsed_json_value}."""
    ensure_control_plane_tables()
    spark = get_spark()
    pdf = spark.table(USER_MEMORY_TABLE).filter(F.col("user_id") == F.lit(user_id)).toPandas()
    result: dict[str, Any] = {}
    for row in pdf.to_dict(orient="records"):
        key = str(row.get("memory_key") or "")
        raw = str(row.get("memory_value") or "{}")
        try:
            result[key] = json.loads(raw)
        except Exception:
            result[key] = raw
    return result


def upsert_user_memory(user_id: str, memory_key: str, value: Any) -> dict[str, Any]:
    """Upsert a single memory entry for a user."""
    ensure_control_plane_tables()
    record = {
        "user_id": user_id,
        "memory_key": memory_key,
        "memory_value": json.dumps(value) if not isinstance(value, str) else value,
        "updated_at": utcnow(),
    }
    _upsert_rows(USER_MEMORY_TABLE, ["user_id", "memory_key"], [record])
    return record
