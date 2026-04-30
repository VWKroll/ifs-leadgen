from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
import json
from typing import Any, Callable, Literal

from pyspark.sql import functions as F

from idc_app.config import TABLES
from idc_app.spark import get_spark

from .services import build_knowledge_graph, get_opportunity_detail, list_opportunities
from .settings import settings
from .store import (
    count_kb_document_records,
    list_chat_note_records,
    get_kb_document_record,
    get_pipeline_settings_record,
    list_kb_document_records,
    replace_kb_document_records,
    save_kb_document_record,
    save_pipeline_settings_record,
    utcnow,
)
from .pipeline.provider import get_azure_capabilities, get_azure_client

MANIFEST_CLUSTER_ID = "__manifest__"
MANIFEST_RELATIVE_PATH = "manifests/latest.md"
FILE_EXPIRY_SECONDS = 60 * 60 * 24 * 365
MAX_REMOTE_ATTRIBUTE_LENGTH = 512
_SYNC_LOCK = Lock()


@dataclass(slots=True)
class KnowledgeDocument:
    document_id: str
    cluster_id: str
    document_kind: Literal["cluster", "entity", "source", "region", "country", "manifest"]
    title: str
    file_path: str
    content: str
    content_sha: str
    source_run_id: str | None
    attributes: dict[str, str | bool | float]
    entity_id: str | None = None
    source_id: str | None = None
    linked_entity_id: str | None = None


@dataclass(slots=True)
class KnowledgeBaseSyncResult:
    status: str
    last_synced_at: datetime | None
    document_count: int
    vector_store_id: str | None
    last_error: str | None = None


def _normalize_storage_root(raw_root: str) -> Path:
    root = raw_root.strip() or "/tmp/idc-event-intelligence/knowledge_base"
    if root.startswith("dbfs:/"):
        root = "/dbfs/" + root.removeprefix("dbfs:/").lstrip("/")
    return Path(root)


def knowledge_base_root() -> Path:
    return _normalize_storage_root(settings.kb_storage_root)


def _cluster_relative_path(cluster_id: str) -> str:
    return f"clusters/{cluster_id}.md"


def _entity_relative_path(cluster_id: str, entity_id: str) -> str:
    return f"clusters/{cluster_id}/entities/{entity_id}.md"


def _source_relative_path(cluster_id: str, source_id: str) -> str:
    return f"clusters/{cluster_id}/sources/{source_id}.md"


def _region_relative_path(region_id: str) -> str:
    slug = region_id.lower().replace(" ", "-")
    return f"geography/regions/{slug}.md"


def _country_relative_path(region_id: str, country_id: str) -> str:
    region_slug = region_id.lower().replace(" ", "-")
    country_slug = country_id.lower().replace(" ", "-")
    return f"geography/regions/{region_slug}/countries/{country_slug}.md"


def _document_id(cluster_id: str, document_kind: str, *, entity_id: str | None = None, source_id: str | None = None) -> str:
    parts = [cluster_id, document_kind]
    if entity_id:
        parts.append(entity_id)
    if source_id:
        parts.append(source_id)
    return ":".join(parts)


def _resolve_storage_path(relative_path: str) -> Path:
    return knowledge_base_root() / relative_path


def _write_markdown(relative_path: str, content: str) -> str:
    path = _resolve_storage_path(relative_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return str(path)


def _sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _display_value(value: Any, default: str = "Unknown") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _as_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ordered_source_urls(entity: dict[str, Any]) -> list[str]:
    urls = entity.get("source_urls_json")
    if isinstance(urls, list):
        return sorted(str(item) for item in urls if item)
    if not isinstance(urls, str):
        return []
    try:
        import json

        parsed = json.loads(urls)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return sorted(str(item) for item in parsed if item)


def _ordered_source_snippets(entity: dict[str, Any]) -> list[str]:
    snippets = entity.get("source_snippets_json")
    if isinstance(snippets, list):
        return [str(item) for item in snippets if item]
    if not isinstance(snippets, str):
        return []
    try:
        import json

        parsed = json.loads(snippets)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if item]


def _string_list(values: list[Any], *, limit: int | None = None) -> list[str]:
    normalized = [str(value).strip() for value in values if str(value).strip()]
    if limit is not None:
        normalized = normalized[:limit]
    return normalized


def _compact_remote_attribute_value(value: str) -> str:
    if len(value) <= MAX_REMOTE_ATTRIBUTE_LENGTH:
        return value

    try:
        parsed = json.loads(value)
    except Exception:
        parsed = None

    if isinstance(parsed, list):
        preview: list[str] = []
        for item in parsed:
            candidate = preview + [str(item)]
            compact = json.dumps(
                {
                    "preview": candidate,
                    "count": len(parsed),
                    "truncated": True,
                },
                separators=(",", ":"),
            )
            if len(compact) > MAX_REMOTE_ATTRIBUTE_LENGTH:
                break
            preview = candidate

        compact = json.dumps(
            {
                "preview": preview,
                "count": len(parsed),
                "truncated": True,
            },
            separators=(",", ":"),
        )
        if len(compact) <= MAX_REMOTE_ATTRIBUTE_LENGTH:
            return compact

    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:16]
    suffix = f"... [truncated len={len(value)} sha={digest}]"
    head_budget = MAX_REMOTE_ATTRIBUTE_LENGTH - len(suffix)
    return f"{value[:max(0, head_budget)]}{suffix}"


def _remote_attributes(attributes: dict[str, str | bool | float]) -> dict[str, str | bool | float]:
    compacted: dict[str, str | bool | float] = {}
    for key, value in attributes.items():
        if isinstance(value, str):
            compacted[key] = _compact_remote_attribute_value(value)
        else:
            compacted[key] = value
    return compacted


def _cluster_document_attributes(detail: Any) -> dict[str, str | bool | float]:
    cluster = detail.cluster
    entity_ids = _string_list([item.get("cluster_entity_id") for item in detail.entities], limit=24)
    source_ids = _string_list([item.get("cluster_source_id") for item in detail.sources], limit=24)
    return {
        "cluster_id": cluster.cluster_id,
        "document_kind": "cluster",
        "subject_company_name": _display_value(cluster.subject_company_name),
        "trigger_type": _display_value(cluster.trigger_type),
        "trigger_subtype": _display_value(cluster.trigger_subtype),
        "subject_country": _display_value(cluster.subject_country),
        "entity_ids": json.dumps(entity_ids),
        "source_ids": json.dumps(source_ids),
    }


def _entity_document_attributes(detail: Any, entity: dict[str, Any]) -> dict[str, str | bool | float]:
    return {
        "cluster_id": detail.cluster.cluster_id,
        "document_kind": "entity",
        "entity_id": str(entity.get("cluster_entity_id") or ""),
        "entity_name": _display_value(entity.get("entity_name")),
        "entity_type": _display_value(entity.get("entity_type")),
        "branch_type": _display_value(entity.get("branch_type")),
        "commercial_role": _display_value(entity.get("commercial_role")),
        "evidence_type": _display_value(entity.get("evidence_type")),
        "subject_company_name": _display_value(detail.cluster.subject_company_name),
        "subject_country": _display_value(detail.cluster.subject_country),
    }


def _source_document_attributes(detail: Any, source: dict[str, Any]) -> dict[str, str | bool | float]:
    return {
        "cluster_id": detail.cluster.cluster_id,
        "document_kind": "source",
        "source_id": str(source.get("cluster_source_id") or ""),
        "linked_entity_id": str(source.get("cluster_entity_id") or ""),
        "source_title": _display_value(source.get("source_title"), "Selected source"),
        "publisher": _display_value(source.get("publisher")),
        "used_for": _display_value(source.get("used_for")),
        "subject_company_name": _display_value(detail.cluster.subject_company_name),
        "subject_country": _display_value(detail.cluster.subject_country),
    }


def _region_document_attributes(region: Any) -> dict[str, str | bool | float]:
    cluster_ids = _string_list(
        [event.cluster_id for country in getattr(region, "countries", []) for event in country.events],
        limit=24,
    )
    return {
        "cluster_id": f"__region__:{region.region_id}",
        "document_kind": "region",
        "region_id": region.region_id,
        "country_count": float(region.country_count),
        "event_count": float(region.event_count),
        "company_count": float(region.company_count),
        "trigger_types": json.dumps([item.trigger_type for item in region.dominant_triggers if item.trigger_type]),
        "cluster_ids": json.dumps(cluster_ids),
    }


def _country_document_attributes(country: Any) -> dict[str, str | bool | float]:
    return {
        "cluster_id": f"__country__:{country.region_id}:{country.country_id}",
        "document_kind": "country",
        "region_id": country.region_id,
        "country_id": country.country_id,
        "event_count": float(country.event_count),
        "company_count": float(country.company_count),
        "trigger_types": json.dumps([item.trigger_type for item in country.dominant_triggers if item.trigger_type]),
        "cluster_ids": json.dumps([event.cluster_id for event in country.events]),
    }


def _cluster_run_metadata(cluster_id: str) -> dict[str, Any]:
    spark = get_spark()
    frame = spark.table(TABLES["event_clusters"]).filter(F.col("cluster_id") == F.lit(cluster_id)).limit(1)
    if not frame.take(1):
        raise KeyError(cluster_id)

    select_columns: list[str] = ["cluster_id"]
    available_columns = set(frame.columns)
    for column in ("run_id", "cluster_created_at"):
        if column in available_columns:
            select_columns.append(column)
    pdf = frame.select(*select_columns).toPandas()
    if pdf.empty:
        return {key: cluster_id if key == "cluster_id" else None for key in select_columns}
    row = pdf.iloc[0].to_dict()
    return {key: row.get(key) for key in select_columns}


def build_cluster_markdown(cluster_id: str, *, detail: Any | None = None) -> KnowledgeDocument:
    detail = detail or get_opportunity_detail(cluster_id)
    cluster = detail.cluster
    metadata = _cluster_run_metadata(cluster_id)
    chat_notes = list_chat_note_records(cluster_id)

    entities = sorted(
        detail.entities,
        key=lambda item: (-float(item.get("priority_score") or 0), str(item.get("entity_name") or "")),
    )
    recommendations = sorted(
        detail.recommendations,
        key=lambda item: (
            str(item.get("entity_name") or ""),
            str(item.get("role_track_type") or ""),
        ),
    )
    sources = sorted(
        detail.sources,
        key=lambda item: (
            str(item.get("used_for") or ""),
            str(item.get("source_url") or ""),
        ),
    )

    scores = [
        f"- Opportunity score: {_display_value(cluster.opportunity_score, '0')}",
        f"- Priority score: {_display_value(cluster.cluster_priority_score, '0')}",
        f"- Confidence score: {_display_value(cluster.cluster_confidence_score, '0')}",
    ]

    geography = [
        f"- Country: {_display_value(cluster.subject_country)}",
        f"- Region: {_display_value(cluster.subject_region)}",
        f"- State: {_display_value(cluster.subject_state)}",
        f"- City: {_display_value(cluster.subject_city)}",
        f"- Address: {_display_value(cluster.subject_address)}",
    ]

    lines = [
        f"# Opportunity Cluster: {_display_value(cluster.subject_company_name)}",
        "",
        "## Metadata",
        f"- Cluster ID: {cluster.cluster_id}",
        f"- Source Run ID: {_display_value(metadata.get('run_id'))}",
        f"- Event Date: {_display_value(cluster.event_date)}",
        f"- Trigger Type: {_display_value(cluster.trigger_type)}",
        f"- Trigger Subtype: {_display_value(cluster.trigger_subtype)}",
        f"- Headline Source: {_display_value(cluster.headline_source_url, 'Unavailable')}",
        "",
        "## Event Summary",
        _display_value(cluster.event_summary, "No event summary is available."),
        "",
        "## Propagation Thesis",
        _display_value(cluster.propagation_thesis, "No propagation thesis is available."),
        "",
        "## Scores",
        *scores,
        "",
        "## Geography",
        *geography,
        "",
        "## Entities",
    ]

    if not entities:
        lines.extend(["No entities are currently associated with this cluster.", ""])
    else:
        for entity in entities:
            lines.extend(
                [
                    f"### {_display_value(entity.get('entity_name'))}",
                    f"- Entity Type: {_display_value(entity.get('entity_type'))}",
                    f"- Branch Type: {_display_value(entity.get('branch_type'))}",
                    f"- Commercial Role: {_display_value(entity.get('commercial_role'))}",
                    f"- Evidence Type: {_display_value(entity.get('evidence_type'))}",
                    f"- Priority Score: {_display_value(entity.get('priority_score'), '0')}",
                    f"- Confidence Score: {_display_value(entity.get('confidence_score'), '0')}",
                    f"- Relationship To Subject: {_display_value(entity.get('relationship_to_subject'), 'Unavailable')}",
                    f"- Rationale: {_display_value(entity.get('rationale'), 'Unavailable')}",
                ]
            )
            source_urls = _ordered_source_urls(entity)
            if source_urls:
                lines.append("- Source URLs:")
                lines.extend(f"  - {url}" for url in source_urls)
            snippets = _ordered_source_snippets(entity)
            if snippets:
                lines.append("- Source Snippets:")
                lines.extend(f"  - {snippet}" for snippet in snippets)
            lines.append("")

    lines.extend(["## Role Recommendations"])
    if not recommendations:
        lines.extend(["No role recommendations are available for this cluster.", ""])
    else:
        for recommendation in recommendations:
            lines.extend(
                [
                    f"### {_display_value(recommendation.get('entity_name'))} · {_display_value(recommendation.get('role_track_type'))}",
                    f"- Entity Type: {_display_value(recommendation.get('entity_type'))}",
                    f"- Confidence Score: {_display_value(recommendation.get('role_confidence_score'), '0')}",
                    f"- Rationale: {_display_value(recommendation.get('rationale'), 'Unavailable')}",
                    f"- Recommended Titles JSON: {_display_value(recommendation.get('recommended_titles_json'), '[]')}",
                    f"- Departments JSON: {_display_value(recommendation.get('departments_json'), '[]')}",
                    f"- Seniority Levels JSON: {_display_value(recommendation.get('seniority_levels_json'), '[]')}",
                    "",
                ]
            )

    lines.extend(["## Sources"])
    if not sources:
        lines.extend(["No source records are available for this cluster.", ""])
    else:
        for source in sources:
            lines.extend(
                [
                    f"- URL: {_display_value(source.get('source_url'), 'Unavailable')}",
                    f"  - Type: {_display_value(source.get('source_type'))}",
                    f"  - Title: {_display_value(source.get('source_title'))}",
                    f"  - Publisher: {_display_value(source.get('publisher'))}",
                    f"  - Used For: {_display_value(source.get('used_for'))}",
                    f"  - Published At: {_display_value(source.get('published_at'))}",
                ]
            )
        lines.append("")

    lines.extend(["## Analyst Notes"])
    if not chat_notes:
        lines.extend(["No committed analyst notes are available for this cluster yet.", ""])
    else:
        for note in chat_notes:
            lines.extend(
                [
                    f"### {_display_value(note.get('title'), 'Committed analyst note')}",
                    f"- Committed At: {_display_value(note.get('committed_at'))}",
                    f"- Committed By: {_display_value(note.get('committed_by'))}",
                    "",
                    _display_value(note.get("summary_markdown"), "No note content is available."),
                    "",
                ]
            )

    content = "\n".join(lines).strip() + "\n"
    return KnowledgeDocument(
        document_id=_document_id(cluster.cluster_id, "cluster"),
        cluster_id=cluster.cluster_id,
        document_kind="cluster",
        title=f"Opportunity Cluster: {_display_value(cluster.subject_company_name)}",
        file_path=_cluster_relative_path(cluster.cluster_id),
        content=content,
        content_sha=_sha256_text(content),
        source_run_id=str(metadata.get("run_id")) if metadata.get("run_id") else None,
        attributes=_cluster_document_attributes(detail),
    )


def build_entity_markdown(cluster_id: str, entity_id: str, *, detail: Any | None = None) -> KnowledgeDocument:
    detail = detail or get_opportunity_detail(cluster_id)
    cluster = detail.cluster
    entity = next((item for item in detail.entities if str(item.get("cluster_entity_id") or "") == entity_id), None)
    if entity is None:
        raise KeyError(entity_id)

    recommendations = [
        item
        for item in detail.recommendations
        if str(item.get("cluster_entity_id") or "") == entity_id
    ]
    related_sources = [
        item
        for item in detail.sources
        if str(item.get("cluster_entity_id") or "") == entity_id
    ]
    source_urls = _ordered_source_urls(entity)
    source_snippets = _ordered_source_snippets(entity)
    lines = [
        f"# Entity Focus: {_display_value(entity.get('entity_name'))}",
        "",
        "## Metadata",
        f"- Cluster ID: {cluster.cluster_id}",
        f"- Entity ID: {entity_id}",
        f"- Subject Company: {_display_value(cluster.subject_company_name)}",
        f"- Trigger Type: {_display_value(cluster.trigger_type)}",
        f"- Event Date: {_display_value(cluster.event_date)}",
        "",
        "## Commercial Context",
        f"- Entity Type: {_display_value(entity.get('entity_type'))}",
        f"- Branch Type: {_display_value(entity.get('branch_type'))}",
        f"- Commercial Role: {_display_value(entity.get('commercial_role'))}",
        f"- Evidence Type: {_display_value(entity.get('evidence_type'))}",
        f"- Priority Score: {_display_value(entity.get('priority_score'), '0')}",
        f"- Confidence Score: {_display_value(entity.get('confidence_score'), '0')}",
        "",
        "## Relationship To Event",
        _display_value(entity.get("relationship_to_subject"), "Unavailable"),
        "",
        "## Rationale",
        _display_value(entity.get("rationale"), "Unavailable"),
    ]
    if source_urls:
        lines.extend(["", "## Source URLs", *[f"- {url}" for url in source_urls]])
    if source_snippets:
        lines.extend(["", "## Source Snippets", *[f"- {snippet}" for snippet in source_snippets]])
    lines.extend(["", "## Role Recommendations"])
    if not recommendations:
        lines.append("No role recommendations are available for this entity.")
    else:
        for recommendation in recommendations:
            lines.extend(
                [
                    f"### {_display_value(recommendation.get('role_track_type'))}",
                    f"- Confidence Score: {_display_value(recommendation.get('role_confidence_score'), '0')}",
                    f"- Rationale: {_display_value(recommendation.get('rationale'), 'Unavailable')}",
                    f"- Recommended Titles JSON: {_display_value(recommendation.get('recommended_titles_json'), '[]')}",
                    f"- Departments JSON: {_display_value(recommendation.get('departments_json'), '[]')}",
                    f"- Seniority Levels JSON: {_display_value(recommendation.get('seniority_levels_json'), '[]')}",
                    "",
                ]
            )
    lines.extend(["## Related Sources"])
    if not related_sources:
        lines.append("No source records are directly linked to this entity.")
    else:
        for source in related_sources:
            lines.extend(
                [
                    f"- {_display_value(source.get('source_title'), 'Source')}",
                    f"  - Source ID: {_display_value(source.get('cluster_source_id'), 'Unavailable')}",
                    f"  - URL: {_display_value(source.get('source_url'), 'Unavailable')}",
                    f"  - Publisher: {_display_value(source.get('publisher'))}",
                    f"  - Used For: {_display_value(source.get('used_for'))}",
                ]
            )
    content = "\n".join(lines).strip() + "\n"
    return KnowledgeDocument(
        document_id=_document_id(cluster_id, "entity", entity_id=entity_id),
        cluster_id=cluster_id,
        document_kind="entity",
        title=f"Entity Focus: {_display_value(entity.get('entity_name'))}",
        file_path=_entity_relative_path(cluster_id, entity_id),
        content=content,
        content_sha=_sha256_text(content),
        source_run_id=None,
        attributes=_entity_document_attributes(detail, entity),
        entity_id=entity_id,
    )


def build_source_markdown(cluster_id: str, source_id: str, *, detail: Any | None = None) -> KnowledgeDocument:
    detail = detail or get_opportunity_detail(cluster_id)
    cluster = detail.cluster
    source = next((item for item in detail.sources if str(item.get("cluster_source_id") or "") == source_id), None)
    if source is None:
        raise KeyError(source_id)

    linked_entity_id = str(source.get("cluster_entity_id") or "")
    linked_entity = next((item for item in detail.entities if str(item.get("cluster_entity_id") or "") == linked_entity_id), None)
    lines = [
        f"# Source Focus: {_display_value(source.get('source_title'), 'Selected source')}",
        "",
        "## Metadata",
        f"- Cluster ID: {cluster.cluster_id}",
        f"- Source ID: {source_id}",
        f"- Subject Company: {_display_value(cluster.subject_company_name)}",
        f"- Trigger Type: {_display_value(cluster.trigger_type)}",
        f"- Used For: {_display_value(source.get('used_for'))}",
        f"- Publisher: {_display_value(source.get('publisher'))}",
        f"- Published At: {_display_value(source.get('published_at'))}",
        f"- URL: {_display_value(source.get('source_url'), 'Unavailable')}",
        "",
        "## Event Summary",
        _display_value(cluster.event_summary, "No event summary is available."),
        "",
        "## Source Assessment Context",
        f"- Linked Entity ID: {_display_value(linked_entity_id, 'Unavailable')}",
        f"- Linked Entity Name: {_display_value(linked_entity.get('entity_name') if linked_entity else None, 'Unavailable')}",
        f"- Linked Entity Branch: {_display_value(linked_entity.get('branch_type') if linked_entity else None, 'Unavailable')}",
        f"- Linked Entity Rationale: {_display_value(linked_entity.get('rationale') if linked_entity else None, 'Unavailable')}",
    ]
    content = "\n".join(lines).strip() + "\n"
    return KnowledgeDocument(
        document_id=_document_id(cluster_id, "source", source_id=source_id),
        cluster_id=cluster_id,
        document_kind="source",
        title=f"Source Focus: {_display_value(source.get('source_title'), 'Selected source')}",
        file_path=_source_relative_path(cluster_id, source_id),
        content=content,
        content_sha=_sha256_text(content),
        source_run_id=None,
        attributes=_source_document_attributes(detail, source),
        source_id=source_id,
        linked_entity_id=linked_entity_id or None,
    )


def build_region_markdown(region: Any) -> KnowledgeDocument:
    lines = [
        f"# Regional Narrative: {_display_value(region.label)}",
        "",
        "## Metadata",
        f"- Region ID: {_display_value(region.region_id)}",
        f"- Event Count: {_display_value(region.event_count, '0')}",
        f"- Country Count: {_display_value(region.country_count, '0')}",
        f"- Company Count: {_display_value(region.company_count, '0')}",
        f"- Average Opportunity: {_display_value(round(float(region.average_opportunity), 2), '0')}",
        f"- Average Confidence: {_display_value(round(float(region.average_confidence), 2), '0')}",
        "",
        "## Regional Insight",
        _display_value(region.narrative, "No regional narrative is available."),
        "",
        "## Dominant Triggers",
    ]
    if not region.dominant_triggers:
        lines.append("No trigger distribution is available.")
    else:
        for trigger in region.dominant_triggers:
            lines.append(f"- {trigger.label}: {trigger.count}")

    lines.extend(["", "## Countries"])
    if not region.countries:
        lines.append("No country summaries are available.")
    else:
        for country in region.countries:
            lines.extend(
                [
                    f"### {_display_value(country.label)}",
                    f"- Event Count: {_display_value(country.event_count, '0')}",
                    f"- Company Count: {_display_value(country.company_count, '0')}",
                    f"- Average Opportunity: {_display_value(round(float(country.average_opportunity), 2), '0')}",
                    f"- Average Confidence: {_display_value(round(float(country.average_confidence), 2), '0')}",
                    f"- Dominant Trigger: {_display_value(country.dominant_triggers[0].label if country.dominant_triggers else None)}",
                    "",
                ]
            )

    content = "\n".join(lines).strip() + "\n"
    region_cluster_id = f"__region__:{region.region_id}"
    return KnowledgeDocument(
        document_id=_document_id(region_cluster_id, "region"),
        cluster_id=region_cluster_id,
        document_kind="region",
        title=f"Regional Narrative: {_display_value(region.label)}",
        file_path=_region_relative_path(region.region_id),
        content=content,
        content_sha=_sha256_text(content),
        source_run_id=None,
        attributes=_region_document_attributes(region),
    )


def build_country_markdown(country: Any) -> KnowledgeDocument:
    lines = [
        f"# Country Narrative: {_display_value(country.label)}",
        "",
        "## Metadata",
        f"- Country ID: {_display_value(country.country_id)}",
        f"- Region ID: {_display_value(country.region_id)}",
        f"- Event Count: {_display_value(country.event_count, '0')}",
        f"- Company Count: {_display_value(country.company_count, '0')}",
        f"- Average Opportunity: {_display_value(round(float(country.average_opportunity), 2), '0')}",
        f"- Average Confidence: {_display_value(round(float(country.average_confidence), 2), '0')}",
        "",
        "## Country Insight",
        _display_value(country.narrative, "No country narrative is available."),
        "",
        "## Dominant Triggers",
    ]
    if not country.dominant_triggers:
        lines.append("No trigger distribution is available.")
    else:
        for trigger in country.dominant_triggers:
            lines.append(f"- {trigger.label}: {trigger.count}")

    lines.extend(["", "## Priority Events"])
    if not country.events:
        lines.append("No clustered events are available.")
    else:
        for event in country.events:
            lines.extend(
                [
                    f"### {_display_value(event.subject_company_name)}",
                    f"- Cluster ID: {_display_value(event.cluster_id)}",
                    f"- Trigger Type: {_display_value(event.trigger_type)}",
                    f"- Event Date: {_display_value(event.event_date)}",
                    f"- Opportunity Score: {_display_value(event.opportunity_score, '0')}",
                    f"- Confidence Score: {_display_value(event.cluster_confidence_score, '0')}",
                    f"- Summary: {_display_value(event.event_summary, 'Unavailable')}",
                    "",
                ]
            )

    content = "\n".join(lines).strip() + "\n"
    country_cluster_id = f"__country__:{country.region_id}:{country.country_id}"
    return KnowledgeDocument(
        document_id=_document_id(country_cluster_id, "country"),
        cluster_id=country_cluster_id,
        document_kind="country",
        title=f"Country Narrative: {_display_value(country.label)}",
        file_path=_country_relative_path(country.region_id, country.country_id),
        content=content,
        content_sha=_sha256_text(content),
        source_run_id=None,
        attributes=_country_document_attributes(country),
    )


def build_manifest_markdown() -> KnowledgeDocument:
    opportunities = list_opportunities()
    lines = [
        "# IDC Event Intelligence Knowledge Base Manifest",
        "",
        f"- Generated At: {utcnow().isoformat()}",
        f"- Cluster Count: {len(opportunities)}",
        "",
        "## Cluster Index",
    ]

    if not opportunities:
        lines.extend(["No opportunity clusters are available.", ""])
    else:
        for item in opportunities:
            lines.extend(
                [
                    f"### {_display_value(item.subject_company_name)}",
                    f"- Cluster ID: {item.cluster_id}",
                    f"- Trigger Type: {_display_value(item.trigger_type)}",
                    f"- Event Date: {_display_value(item.event_date)}",
                    f"- Country: {_display_value(item.subject_country)}",
                    f"- Region: {_display_value(item.subject_region)}",
                    f"- Opportunity Score: {_display_value(item.opportunity_score, '0')}",
                    f"- Priority Score: {_display_value(item.cluster_priority_score, '0')}",
                    f"- Confidence Score: {_display_value(item.cluster_confidence_score, '0')}",
                    f"- Summary: {_display_value(item.event_summary, 'Unavailable')}",
                    "",
                ]
            )

    content = "\n".join(lines).strip() + "\n"
    return KnowledgeDocument(
        document_id=_document_id(MANIFEST_CLUSTER_ID, "manifest"),
        cluster_id=MANIFEST_CLUSTER_ID,
        document_kind="manifest",
        title="IDC Event Intelligence Knowledge Base Manifest",
        file_path=MANIFEST_RELATIVE_PATH,
        content=content,
        content_sha=_sha256_text(content),
        source_run_id=None,
        attributes={"document_kind": "manifest"},
    )


def _ensure_vector_store_id() -> str:
    settings_record = get_pipeline_settings_record()
    vector_store_id = str(settings_record.get("kb_vector_store_id") or settings.kb_vector_store_id or "").strip()
    if vector_store_id:
        if not settings_record.get("kb_vector_store_id"):
            save_pipeline_settings_record({"kb_vector_store_id": vector_store_id}, updated_by="knowledge-base")
        return vector_store_id

    vector_store = get_azure_client().vector_stores.create(
        name="IDC Event Intelligence Knowledge Base",
    )
    save_pipeline_settings_record({"kb_vector_store_id": vector_store.id}, updated_by="knowledge-base")
    return vector_store.id


def _best_effort_delete_remote(record: dict[str, Any], vector_store_id: str) -> None:
    client = get_azure_client()
    vector_store_file_id = record.get("vector_store_file_id")
    uploaded_file_id = record.get("uploaded_file_id")

    if vector_store_file_id:
        try:
            client.vector_stores.files.delete(str(vector_store_file_id), vector_store_id=vector_store_id)
        except Exception:
            pass


def _kb_record_identity(record: dict[str, Any]) -> tuple[str, str, str | None, str | None]:
    return (
        str(record.get("cluster_id") or ""),
        str(record.get("document_kind") or ""),
        str(record.get("entity_id")) if record.get("entity_id") else None,
        str(record.get("source_id")) if record.get("source_id") else None,
    )


def _kb_duplicate_stats(records: list[dict[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    groups: dict[tuple[str, str, str | None, str | None], list[dict[str, Any]]] = {}
    for record in records:
        groups.setdefault(_kb_record_identity(record), []).append(record)
    duplicates = [record for group in groups.values() if len(group) > 1 for record in group[1:]]
    return len(duplicates), duplicates


def _stale_local_file_count(records: list[dict[str, Any]]) -> int:
    root = knowledge_base_root()
    if not root.exists():
        return 0
    valid_paths = {str(Path(str(record.get("file_path")))) for record in records if record.get("file_path")}
    return sum(1 for path in root.rglob("*.md") if str(path) not in valid_paths)


def cleanup_knowledge_base(*, mode: str | None = None) -> dict[str, Any]:
    with _SYNC_LOCK:
        settings_record = get_pipeline_settings_record()
        effective_mode = str(mode or settings_record.get("kb_cleanup_mode") or settings.kb_cleanup_mode).strip() or "off"
        records = list_kb_document_records()
        if effective_mode == "off":
            return get_knowledge_base_status()

        groups: dict[tuple[str, str, str | None, str | None], list[dict[str, Any]]] = {}
        for record in records:
            groups.setdefault(_kb_record_identity(record), []).append(record)

        kept_records: list[dict[str, Any]] = []
        removed_records: list[dict[str, Any]] = []
        for group in groups.values():
            ordered = sorted(
                group,
                key=lambda item: (
                    item.get("sync_status") != "synced",
                    item.get("synced_at") or datetime.min,
                    item.get("updated_at") or datetime.min,
                ),
            )
            ordered.reverse()
            survivor = ordered[0]
            kept_records.append(survivor)
            if effective_mode == "aggressive":
                removed_records.extend(ordered[1:])
            else:
                seen_hashes: set[str] = {str(survivor.get("content_sha") or "")}
                for record in ordered[1:]:
                    content_sha = str(record.get("content_sha") or "")
                    if content_sha in seen_hashes:
                        removed_records.append(record)
                    else:
                        kept_records.append(record)
                        seen_hashes.add(content_sha)

        for record in removed_records:
            vector_store_id = str(record.get("vector_store_id") or "")
            if vector_store_id:
                _best_effort_delete_remote(record, vector_store_id)

        replace_kb_document_records(kept_records)

        valid_paths = {str(Path(str(record.get("file_path")))) for record in kept_records if record.get("file_path")}
        removed_file_count = 0
        root = knowledge_base_root()
        if root.exists():
            for path in root.rglob("*.md"):
                if str(path) not in valid_paths:
                    path.unlink(missing_ok=True)
                    removed_file_count += 1

        save_pipeline_settings_record(
            {
                "kb_last_error": None,
                "kb_status": "ready" if any(record.get("sync_status") == "synced" for record in kept_records) else "fallback",
            },
            updated_by="knowledge-base-cleanup",
        )

        status = get_knowledge_base_status()
        status["last_error"] = status.get("last_error")
        status["cleanup_removed_documents"] = len(removed_records)
        status["cleanup_removed_files"] = removed_file_count
        return status


def _sync_remote_document(document: KnowledgeDocument, existing: dict[str, Any] | None, vector_store_id: str) -> dict[str, Any]:
    if existing:
        _best_effort_delete_remote(existing, vector_store_id)

    path = _resolve_storage_path(document.file_path)
    client = get_azure_client()
    with path.open("rb") as file_handle:
        uploaded_file = client.files.create(
            file=file_handle,
            purpose="assistants",
        )

    remote_attributes = _remote_attributes(document.attributes)
    vector_store_file = client.vector_stores.files.create_and_poll(
        file_id=uploaded_file.id,
        vector_store_id=vector_store_id,
        attributes=remote_attributes,
        poll_interval_ms=1_000,
    )

    status = "synced" if getattr(vector_store_file, "status", "") == "completed" else "failed"
    error_message = None
    last_error = getattr(vector_store_file, "last_error", None)
    if status != "synced":
        error_message = getattr(last_error, "message", None) or "Vector store file processing did not complete."

    return {
        "document_id": document.document_id,
        "cluster_id": document.cluster_id,
        "document_kind": document.document_kind,
        "title": document.title,
        "file_path": str(path),
        "content_sha": document.content_sha,
        "source_run_id": document.source_run_id,
        "entity_id": document.entity_id,
        "source_id": document.source_id,
        "linked_entity_id": document.linked_entity_id,
        "attributes_json": json.dumps(document.attributes, sort_keys=True),
        "vector_store_id": vector_store_id,
        "uploaded_file_id": uploaded_file.id,
        "vector_store_file_id": vector_store_file.id,
        "sync_status": status,
        "synced_at": utcnow(),
        "error_message": error_message,
    }


def _store_local_document(document: KnowledgeDocument, status: str, error_message: str | None) -> dict[str, Any]:
    path = _resolve_storage_path(document.file_path)
    return {
        "document_id": document.document_id,
        "cluster_id": document.cluster_id,
        "document_kind": document.document_kind,
        "title": document.title,
        "file_path": str(path),
        "content_sha": document.content_sha,
        "source_run_id": document.source_run_id,
        "entity_id": document.entity_id,
        "source_id": document.source_id,
        "linked_entity_id": document.linked_entity_id,
        "attributes_json": json.dumps(document.attributes, sort_keys=True),
        "vector_store_id": None,
        "uploaded_file_id": None,
        "vector_store_file_id": None,
        "sync_status": status,
        "synced_at": utcnow(),
        "error_message": error_message,
    }


def _documents_to_sync(cluster_id: str | None, full_refresh: bool) -> list[KnowledgeDocument]:
    cluster_ids: list[str]
    if full_refresh or not cluster_id:
        cluster_ids = [item.cluster_id for item in list_opportunities()]
    else:
        cluster_ids = [cluster_id]

    documents: list[KnowledgeDocument] = []
    for item in cluster_ids:
        detail = get_opportunity_detail(item)
        documents.append(build_cluster_markdown(item, detail=detail))
        for entity in detail.entities:
            entity_id = str(entity.get("cluster_entity_id") or "").strip()
            if entity_id:
                documents.append(build_entity_markdown(item, entity_id, detail=detail))
        for source in detail.sources:
            source_id = str(source.get("cluster_source_id") or "").strip()
            if source_id:
                documents.append(build_source_markdown(item, source_id, detail=detail))
    knowledge_graph = build_knowledge_graph()
    for region in knowledge_graph.regions:
        documents.append(build_region_markdown(region))
        for country in region.countries:
            documents.append(build_country_markdown(country))
    documents.append(build_manifest_markdown())
    return documents


def sync_knowledge_base(
    *,
    cluster_id: str | None = None,
    source_run_id: str | None = None,
    full_refresh: bool = False,
    on_lock_acquired: Callable[[], None] | None = None,
) -> KnowledgeBaseSyncResult:
    with _SYNC_LOCK:
        if on_lock_acquired:
            on_lock_acquired()
        documents = _documents_to_sync(cluster_id, full_refresh)
        capabilities = get_azure_capabilities(force_refresh=True)
        vector_store_id: str | None = None
        last_synced_at: datetime | None = None
        last_error: str | None = None

        if capabilities.file_search_supported:
            try:
                vector_store_id = _ensure_vector_store_id()
            except Exception as exc:
                last_error = str(exc)

        for document in documents:
            _write_markdown(document.file_path, document.content)
            existing = get_kb_document_record(
                document.cluster_id,
                document_kind=document.document_kind,
                entity_id=document.entity_id,
                source_id=document.source_id,
            )
            current_vector_store_id = existing.get("vector_store_id") if existing else None
            needs_remote_sync = bool(
                vector_store_id
                and (
                    existing is None
                    or existing.get("content_sha") != document.content_sha
                    or existing.get("sync_status") != "synced"
                    or current_vector_store_id != vector_store_id
                )
            )

            if vector_store_id and needs_remote_sync:
                record = _sync_remote_document(document, existing, vector_store_id)
            elif vector_store_id and existing:
                record = {
                    **existing,
                    "document_id": document.document_id,
                    "cluster_id": document.cluster_id,
                    "document_kind": document.document_kind,
                    "title": document.title,
                    "file_path": str(_resolve_storage_path(document.file_path)),
                    "content_sha": document.content_sha,
                    "source_run_id": document.source_run_id or existing.get("source_run_id"),
                    "entity_id": document.entity_id,
                    "source_id": document.source_id,
                    "linked_entity_id": document.linked_entity_id,
                    "attributes_json": json.dumps(document.attributes, sort_keys=True),
                    "vector_store_id": vector_store_id,
                    "sync_status": existing.get("sync_status", "synced"),
                    "synced_at": existing.get("synced_at") or utcnow(),
                    "error_message": existing.get("error_message"),
                }
            else:
                fallback_status = "stored_local" if count_kb_document_records(include_manifest=True) or documents else "not_synced"
                error_message = last_error or capabilities.message
                record = _store_local_document(document, fallback_status, error_message)

            save_kb_document_record(record)
            last_synced_at = record.get("synced_at") or last_synced_at

            if record.get("sync_status") == "failed":
                last_error = str(record.get("error_message") or "Knowledge base sync failed.")

        if vector_store_id and not last_error:
            status = "ready"
        elif count_kb_document_records() > 0:
            status = "fallback"
        else:
            status = "not_synced"

        if not settings.azure_openai_api_key or not settings.azure_openai_endpoint:
            status = "not_configured"
            last_error = last_error or "Azure OpenAI endpoint or API key is not configured."

        save_pipeline_settings_record(
            {
                "kb_vector_store_id": vector_store_id or settings.kb_vector_store_id,
                "kb_last_synced_at": last_synced_at,
                "kb_last_error": last_error,
                "kb_status": status,
            },
            updated_by="knowledge-base",
        )

        document_count = count_kb_document_records()
        return KnowledgeBaseSyncResult(
            status=status,
            last_synced_at=last_synced_at,
            document_count=document_count,
            vector_store_id=vector_store_id or settings.kb_vector_store_id,
            last_error=last_error,
        )


def get_knowledge_base_status() -> dict[str, Any]:
    settings_record = get_pipeline_settings_record()
    capabilities = get_azure_capabilities()
    records = list_kb_document_records()
    cluster_records = [record for record in records if record.get("document_kind") != "manifest"]
    cluster_document_count = sum(1 for record in cluster_records if record.get("document_kind") == "cluster")
    entity_document_count = sum(1 for record in cluster_records if record.get("document_kind") == "entity")
    source_document_count = sum(1 for record in cluster_records if record.get("document_kind") == "source")
    region_document_count = sum(1 for record in cluster_records if record.get("document_kind") == "region")
    country_document_count = sum(1 for record in cluster_records if record.get("document_kind") == "country")
    duplicate_candidate_count, _ = _kb_duplicate_stats(cluster_records)
    stale_local_file_count = _stale_local_file_count(cluster_records)

    last_synced_at = None
    if records:
        synced_values = [record.get("synced_at") for record in records if record.get("synced_at") is not None]
        if synced_values:
            last_synced_at = max(synced_values)

    last_error = settings_record.get("kb_last_error")
    if not last_error:
        failed_records = [record for record in records if record.get("sync_status") == "failed" and record.get("error_message")]
        if failed_records:
            last_error = str(failed_records[-1]["error_message"])

    vector_store_id = settings_record.get("kb_vector_store_id") or settings.kb_vector_store_id
    stored_status = str(settings_record.get("kb_status") or "").strip()

    if not settings.azure_openai_endpoint or not settings.azure_openai_api_key:
        status = "not_configured"
    elif capabilities.file_search_supported and vector_store_id and any(record.get("sync_status") == "synced" for record in cluster_records):
        status = "ready"
    elif stored_status == "syncing":
        status = "syncing"
    elif cluster_records:
        status = "fallback"
    else:
        status = stored_status or "not_synced"

    return {
        "status": status,
        "last_synced_at": last_synced_at,
        "document_count": len(cluster_records),
        "cluster_document_count": cluster_document_count,
        "entity_document_count": entity_document_count,
        "source_document_count": source_document_count,
        "region_document_count": region_document_count,
        "country_document_count": country_document_count,
        "duplicate_candidate_count": duplicate_candidate_count,
        "stale_local_file_count": stale_local_file_count,
        "vector_store_id": vector_store_id,
        "last_error": last_error if last_error else None,
    }


def ensure_cluster_markdown(cluster_id: str) -> tuple[str, str]:
    record = get_kb_document_record(cluster_id, document_kind="cluster")
    path = Path(str(record.get("file_path"))) if record and record.get("file_path") else None
    if path and path.exists():
        return path.read_text(encoding="utf-8"), str(path)

    document = build_cluster_markdown(cluster_id)
    file_path = _write_markdown(document.file_path, document.content)
    save_kb_document_record(_store_local_document(document, "stored_local", "Using local markdown fallback."))
    return document.content, file_path
