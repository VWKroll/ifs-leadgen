"""Databricks and Unity Catalog configuration."""

from __future__ import annotations

import os

# Unity Catalog coordinates
CATALOG = os.environ.get("IDC_CATALOG", "ifs_dev")
SCHEMA = os.environ.get("IDC_SCHEMA", "idc_poc")

# Tables
TABLES = {
    "event_clusters": f"{CATALOG}.{SCHEMA}.event_clusters",
    "cluster_entities": f"{CATALOG}.{SCHEMA}.cluster_entities",
    "cluster_role_recommendations": f"{CATALOG}.{SCHEMA}.cluster_role_recommendations",
    "cluster_sources": f"{CATALOG}.{SCHEMA}.cluster_sources",
}
