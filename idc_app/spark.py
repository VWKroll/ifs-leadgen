"""Spark session factory for Databricks Apps."""

from __future__ import annotations

import os
from typing import Literal

from pyspark.sql import SparkSession


def _resolved_auth_type() -> Literal["oauth", "pat"]:
    token = os.environ.get("IDC_PAT_TOKEN")
    auth_type = str(os.environ.get("IDC_DB_AUTH_TYPE") or "oauth").strip().lower()
    if auth_type == "auto":
        return "pat" if token else "oauth"
    if auth_type == "pat":
        return "pat"
    return "oauth"


def _session_builder():
    from databricks.connect import DatabricksSession

    builder = DatabricksSession.builder.serverless(True)
    host = os.environ.get("IDC_DB_HOST") or os.environ.get("DATABRICKS_HOST")
    profile = os.environ.get("IDC_DB_PROFILE") or os.environ.get("DATABRICKS_CONFIG_PROFILE")
    token = os.environ.get("IDC_PAT_TOKEN")

    if _resolved_auth_type() == "pat" and token and host:
        # Clear OAuth env vars to avoid auth conflict when PAT auth is explicitly selected.
        for key in ("DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET", "DATABRICKS_TOKEN", "DATABRICKS_HOST"):
            os.environ.pop(key, None)
        return builder.host(host).token(token)

    if profile:
        builder = builder.profile(profile)
    if host:
        builder = builder.host(host)
    return builder


def get_spark() -> SparkSession:
    """Return a SparkSession connected to Databricks via serverless compute."""

    try:
        return _session_builder().getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()
