from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from idc_app.config import TABLES
from idc_app.dedup import deduplicate_events
from idc_app.scoring import score_clusters
from idc_app.spark import get_spark

CLUSTER_SUMMARY_COLUMNS = [
    ("cluster_id", "string"),
    ("cluster_created_at", "timestamp"),
    ("subject_company_name", "string"),
    ("subject_country", "string"),
    ("subject_region", "string"),
    ("subject_state", "string"),
    ("subject_city", "string"),
    ("subject_address", "string"),
    ("subject_latitude", "double"),
    ("subject_longitude", "double"),
    ("trigger_type", "string"),
    ("trigger_subtype", "string"),
    ("event_date", "date"),
    ("event_headline", "string"),
    ("event_summary", "string"),
    ("event_urgency_score", "double"),
    ("cluster_priority_score", "double"),
    ("cluster_confidence_score", "double"),
    ("best_route_to_market", "string"),
    ("propagation_thesis", "string"),
    ("service_hypotheses_json", "string"),
    ("headline_source_url", "string"),
    ("dedupe_fingerprint", "string"),
]

ENTITY_COUNT_COLUMNS = [
    ("cluster_id", "string"),
    ("cluster_entity_id", "string"),
    ("created_at", "timestamp"),
]

ENTITY_COLUMNS = [
    ("cluster_entity_id", "string"),
    ("cluster_id", "string"),
    ("entity_name", "string"),
    ("entity_type", "string"),
    ("entity_country", "string"),
    ("entity_region", "string"),
    ("entity_state", "string"),
    ("entity_city", "string"),
    ("entity_address", "string"),
    ("entity_latitude", "double"),
    ("entity_longitude", "double"),
    ("relationship_to_subject", "string"),
    ("commercial_role", "string"),
    ("branch_type", "string"),
    ("rationale", "string"),
    ("evidence_type", "string"),
    ("source_urls_json", "string"),
    ("source_snippets_json", "string"),
    ("confidence_score", "double"),
    ("priority_score", "double"),
    ("created_at", "timestamp"),
]

SOURCE_COUNT_COLUMNS = [
    ("cluster_id", "string"),
    ("cluster_source_id", "string"),
]

RECOMMENDATION_COLUMNS = [
    ("role_recommendation_id", "string"),
    ("cluster_id", "string"),
    ("cluster_entity_id", "string"),
    ("entity_name", "string"),
    ("entity_type", "string"),
    ("role_track_type", "string"),
    ("recommended_titles_json", "string"),
    ("departments_json", "string"),
    ("seniority_levels_json", "string"),
    ("hypothesized_services_json", "string"),
    ("rationale", "string"),
    ("role_confidence_score", "double"),
]

SOURCE_COLUMNS = [
    ("cluster_source_id", "string"),
    ("cluster_id", "string"),
    ("cluster_entity_id", "string"),
    ("source_url", "string"),
    ("source_type", "string"),
    ("source_title", "string"),
    ("publisher", "string"),
    ("published_at", "string"),
    ("used_for", "string"),
    ("retrieved_at", "timestamp"),
]


def _project_columns(table_fqn: str, columns: list[tuple[str, str]]) -> DataFrame:
    frame = get_spark().table(table_fqn)
    existing = set(frame.columns)
    projected = []
    for column_name, data_type in columns:
        if column_name in existing:
            projected.append(F.col(column_name))
        else:
            projected.append(F.lit(None).cast(data_type).alias(column_name))
    return frame.select(*projected)


def load_cluster_summaries() -> pd.DataFrame:
    clusters = _project_columns(TABLES["event_clusters"], CLUSTER_SUMMARY_COLUMNS)
    entities = _project_columns(TABLES["cluster_entities"], ENTITY_COUNT_COLUMNS)
    sources = _project_columns(TABLES["cluster_sources"], SOURCE_COUNT_COLUMNS)

    clusters = deduplicate_events(clusters, ["dedupe_fingerprint"], order_col="cluster_created_at")
    entities = deduplicate_events(entities, ["cluster_entity_id"], order_col="created_at")

    entity_counts = entities.groupBy("cluster_id").count().withColumnRenamed("count", "entity_count")
    source_counts = sources.groupBy("cluster_id").count().withColumnRenamed("count", "source_count")

    scored = score_clusters(clusters.join(entity_counts, "cluster_id", "left").join(source_counts, "cluster_id", "left"))
    return scored.orderBy(F.col("cluster_created_at").cast("date").desc_nulls_last(), F.col("cluster_id").asc()).toPandas()


def load_cluster_summary(cluster_id: str) -> pd.DataFrame:
    cluster_frame = _project_columns(TABLES["event_clusters"], CLUSTER_SUMMARY_COLUMNS).filter(F.col("cluster_id") == F.lit(cluster_id)).limit(1)
    if not cluster_frame.take(1):
        return pd.DataFrame()

    entity_count = (
        _project_columns(TABLES["cluster_entities"], ENTITY_COUNT_COLUMNS)
        .filter(F.col("cluster_id") == F.lit(cluster_id))
        .select("cluster_entity_id")
        .distinct()
        .count()
    )
    source_count = (
        _project_columns(TABLES["cluster_sources"], SOURCE_COUNT_COLUMNS)
        .filter(F.col("cluster_id") == F.lit(cluster_id))
        .select("cluster_source_id")
        .distinct()
        .count()
    )

    scored = score_clusters(
        cluster_frame.withColumn("entity_count", F.lit(entity_count)).withColumn("source_count", F.lit(source_count))
    )
    return scored.toPandas()


def load_cluster_scores(cluster_ids: list[str]) -> dict[str, float | None]:
    if not cluster_ids:
        return {}

    unique_ids = list(dict.fromkeys(cluster_ids))
    clusters = _project_columns(TABLES["event_clusters"], CLUSTER_SUMMARY_COLUMNS).filter(F.col("cluster_id").isin(unique_ids))
    entities = _project_columns(TABLES["cluster_entities"], ENTITY_COUNT_COLUMNS).filter(F.col("cluster_id").isin(unique_ids))
    sources = _project_columns(TABLES["cluster_sources"], SOURCE_COUNT_COLUMNS).filter(F.col("cluster_id").isin(unique_ids))

    entity_counts = entities.groupBy("cluster_id").count().withColumnRenamed("count", "entity_count")
    source_counts = sources.groupBy("cluster_id").count().withColumnRenamed("count", "source_count")
    scored = score_clusters(clusters.join(entity_counts, "cluster_id", "left").join(source_counts, "cluster_id", "left"))

    pdf = scored.select("cluster_id", "opportunity_score").toPandas()
    results: dict[str, float | None] = {}
    for row in pdf.to_dict(orient="records"):
        score = row.get("opportunity_score")
        results[str(row.get("cluster_id") or "")] = None if pd.isna(score) else float(score)
    return results


def load_entities_for_cluster(cluster_id: str) -> pd.DataFrame:
    return _project_columns(TABLES["cluster_entities"], ENTITY_COLUMNS).filter(F.col("cluster_id") == F.lit(cluster_id)).toPandas()


def _sales_lead_frame() -> DataFrame:
    clusters = _project_columns(TABLES["event_clusters"], CLUSTER_SUMMARY_COLUMNS)
    entities = _project_columns(TABLES["cluster_entities"], ENTITY_COLUMNS)

    clusters = deduplicate_events(clusters, ["dedupe_fingerprint"], order_col="cluster_created_at")
    entities = deduplicate_events(entities, ["cluster_entity_id"], order_col="created_at")

    entity_counts = entities.groupBy("cluster_id").count().withColumnRenamed("count", "entity_count")
    scored_clusters = score_clusters(clusters.join(entity_counts, "cluster_id", "left"))

    # Alias to avoid ambiguous column references after join.
    ent = entities.alias("ent")
    sc = scored_clusters.alias("sc")

    # RIGHT join so clusters with zero entities still appear as a subject-company lead row.
    joined = ent.join(sc, F.col("ent.cluster_id") == F.col("sc.cluster_id"), "right")

    # Resolve cluster_id from the right (scored_clusters) side — always populated.
    joined = joined.withColumn("_cluster_id", F.col("sc.cluster_id"))

    # For clusters that had no matching entities, synthesise a lead row from the cluster itself.
    joined = (
        joined
        .withColumn("cluster_entity_id", F.coalesce(F.col("ent.cluster_entity_id"), F.concat(F.lit("subject:"), F.col("_cluster_id"))))
        .withColumn("entity_name", F.coalesce(F.col("ent.entity_name"), F.col("sc.subject_company_name")))
        .withColumn("entity_type", F.coalesce(F.col("ent.entity_type"), F.lit("subject_company")))
        .withColumn("branch_type", F.coalesce(F.col("ent.branch_type"), F.lit("direct")))
        .withColumn("priority_score", F.coalesce(F.col("ent.priority_score"), F.col("sc.cluster_priority_score")))
        .withColumn("_confidence_score", F.coalesce(F.col("ent.confidence_score"), F.col("sc.cluster_confidence_score")))
    )

    return (
        joined
        .withColumn("sales_item_id", F.col("cluster_entity_id"))
        .withColumn("event_subject_company_name", F.col("sc.subject_company_name"))
        .withColumn("event_date_string", F.col("sc.event_date").cast("string"))
        .withColumn("_subject_company_name", F.col("entity_name"))
        .withColumn("_subject_country", F.coalesce(F.col("ent.entity_country"), F.col("sc.subject_country")))
        .withColumn("_subject_region", F.coalesce(F.col("ent.entity_region"), F.col("sc.subject_region")))
        .withColumn("opportunity_score", F.col("priority_score"))
        .withColumn("event_priority_score", F.col("sc.cluster_priority_score"))
        .withColumn("event_confidence_score", F.col("sc.cluster_confidence_score"))
        .select(
            F.col("_cluster_id").alias("cluster_id"),
            "sales_item_id",
            "cluster_entity_id",
            "event_subject_company_name",
            F.col("sc.event_headline").alias("event_headline"),
            F.col("event_date_string").alias("event_date"),
            F.col("sc.cluster_created_at").cast("date").cast("string").alias("cluster_created_at"),
            F.col("sc.trigger_type").alias("trigger_type"),
            F.col("_subject_company_name").alias("subject_company_name"),
            F.col("_subject_country").alias("subject_country"),
            F.col("_subject_region").alias("subject_region"),
            "entity_type",
            "branch_type",
            F.col("ent.relationship_to_subject").alias("relationship_to_subject"),
            F.col("ent.commercial_role").alias("commercial_role"),
            F.col("ent.rationale").alias("rationale"),
            "opportunity_score",
            F.col("_confidence_score").alias("confidence_score"),
            "event_priority_score",
            "event_confidence_score",
        )
    )


def _sales_lead_order_columns(sort_by: str) -> list:
    if sort_by == "highest_priority":
        return [
            F.col("opportunity_score").desc_nulls_last(),
            F.col("confidence_score").desc_nulls_last(),
            F.col("cluster_created_at").desc_nulls_last(),
            F.col("cluster_id").asc(),
        ]
    if sort_by == "best_confidence":
        return [
            F.col("confidence_score").desc_nulls_last(),
            F.col("opportunity_score").desc_nulls_last(),
            F.col("cluster_created_at").desc_nulls_last(),
            F.col("cluster_id").asc(),
        ]
    return [
        F.col("cluster_created_at").desc_nulls_last(),
        F.col("cluster_id").asc(),
        F.col("opportunity_score").desc_nulls_last(),
        F.col("confidence_score").desc_nulls_last(),
    ]


def load_sales_lead_rows(*, page: int = 1, page_size: int | None = None, sort_by: str = "newest_event") -> tuple[pd.DataFrame, int]:
    frame = _sales_lead_frame()
    total_items = int(frame.count())
    if page_size is None:
        ordered = frame.orderBy(*_sales_lead_order_columns(sort_by))
        return ordered.toPandas(), total_items

    safe_page = max(page, 1)
    safe_page_size = max(min(page_size, 250), 1)
    offset = (safe_page - 1) * safe_page_size
    window = Window.orderBy(*_sales_lead_order_columns(sort_by))
    paged = (
        frame.withColumn("row_num", F.row_number().over(window))
        .filter((F.col("row_num") > F.lit(offset)) & (F.col("row_num") <= F.lit(offset + safe_page_size)))
        .drop("row_num")
    )
    return paged.toPandas(), total_items


def load_recommendations_for_cluster(cluster_id: str) -> pd.DataFrame:
    return _project_columns(TABLES["cluster_role_recommendations"], RECOMMENDATION_COLUMNS).filter(
        F.col("cluster_id") == F.lit(cluster_id)
    ).toPandas()


def load_sources_for_cluster(cluster_id: str) -> pd.DataFrame:
    return _project_columns(TABLES["cluster_sources"], SOURCE_COLUMNS).filter(F.col("cluster_id") == F.lit(cluster_id)).toPandas()
