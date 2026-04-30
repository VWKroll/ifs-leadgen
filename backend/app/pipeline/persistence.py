from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime
from typing import Any

from .models import ClusterEntity, ClusterExpansion, ClusterScoring, EventCandidate, RoleRecommendation, normalize_company_name
from ..store import utcnow


def make_event_fingerprint(subject_company: str, trigger_type: str, event_date: str, headline: str) -> str:
    raw = (
        f"{normalize_company_name(subject_company)}|"
        f"{trigger_type.strip().lower()}|"
        f"{event_date}|"
        f"{headline.strip().lower()}"
    )
    return hashlib.sha256(raw.encode()).hexdigest()


def all_nodes(expansion: ClusterExpansion) -> list[ClusterEntity]:
    return [expansion.direct_node] + expansion.peer_nodes + expansion.ownership_nodes


def build_cluster_header(
    run_id: str,
    event: EventCandidate,
    expansion: ClusterExpansion,
    scoring: ClusterScoring,
) -> dict[str, Any]:
    cluster_id = str(uuid.uuid4())
    fingerprint = make_event_fingerprint(event.subject_company_name, event.trigger_type, event.event_date, event.event_headline)

    event_date_parsed = None
    if len(event.event_date) == 10:
        try:
            event_date_parsed = datetime.strptime(event.event_date, "%Y-%m-%d").date()
        except ValueError:
            event_date_parsed = None

    return {
        "cluster_id": cluster_id,
        "event_id": event.event_id,
        "run_id": run_id,
        "cluster_created_at": utcnow(),
        "subject_company_name": event.subject_company_name,
        "subject_company_normalized": normalize_company_name(event.subject_company_name),
        "subject_country": event.subject_company_country,
        "subject_region": event.subject_company_region,
        "subject_state": event.subject_company_state,
        "subject_city": event.subject_company_city,
        "subject_address": event.subject_company_address,
        "subject_latitude": event.subject_company_latitude,
        "subject_longitude": event.subject_company_longitude,
        "trigger_type": event.trigger_type,
        "trigger_subtype": event.trigger_subtype,
        "event_date": event_date_parsed,
        "event_headline": event.event_headline,
        "headline_source_url": event.headline_source_url,
        "event_summary": event.event_summary,
        "service_hypotheses_json": json.dumps(event.service_hypotheses),
        "event_confidence_score": float(event.event_confidence_score),
        "event_severity_score": float(event.event_severity_score),
        "event_urgency_score": float(event.event_urgency_score),
        "peer_branch_score": float(scoring.peer_branch_score),
        "ownership_branch_score": float(scoring.ownership_branch_score),
        "governance_branch_score": float(scoring.governance_branch_score),
        "cluster_priority_score": float(scoring.cluster_priority_score),
        "cluster_confidence_score": float(scoring.cluster_confidence_score),
        "best_route_to_market": expansion.best_route_to_market,
        "propagation_thesis": expansion.propagation_thesis,
        "dedupe_fingerprint": fingerprint,
    }


def build_entity_rows(cluster_id: str, expansion: ClusterExpansion) -> list[dict[str, Any]]:
    now = utcnow()
    rows: list[dict[str, Any]] = []
    for node in all_nodes(expansion):
        rows.append(
            {
                "cluster_entity_id": str(uuid.uuid4()),
                "cluster_id": cluster_id,
                "entity_name": node.entity_name,
                "entity_name_normalized": normalize_company_name(node.entity_name),
                "entity_type": node.entity_type,
                "entity_country": node.entity_country,
                "entity_region": node.entity_region,
                "entity_state": node.entity_state,
                "entity_city": node.entity_city,
                "entity_address": node.entity_address,
                "entity_latitude": node.entity_latitude,
                "entity_longitude": node.entity_longitude,
                "relationship_to_subject": node.relationship_to_subject,
                "commercial_role": node.commercial_role,
                "branch_type": node.branch_type,
                "rationale": node.rationale,
                "evidence_type": node.evidence_type,
                "source_urls_json": json.dumps(node.source_urls),
                "source_snippets_json": json.dumps(node.source_snippets),
                "confidence_score": float(node.confidence_score),
                "priority_score": float(node.priority_score),
                "created_at": now,
            }
        )
    return rows


def build_role_rows(cluster_id: str, entity_rows: list[dict[str, Any]], role_recs: list[RoleRecommendation]) -> list[dict[str, Any]]:
    by_entity = {(record.entity_name, record.entity_type): record for record in role_recs}
    now = utcnow()
    rows: list[dict[str, Any]] = []
    for entity_row in entity_rows:
        rec = by_entity.get((entity_row["entity_name"], entity_row["entity_type"]))
        if not rec:
            continue
        rows.append(
            {
                "role_recommendation_id": str(uuid.uuid4()),
                "cluster_id": cluster_id,
                "cluster_entity_id": entity_row["cluster_entity_id"],
                "entity_name": entity_row["entity_name"],
                "entity_type": entity_row["entity_type"],
                "role_track_type": rec.role_track_type,
                "recommended_titles_json": json.dumps(rec.recommended_titles),
                "departments_json": json.dumps(rec.departments),
                "seniority_levels_json": json.dumps(rec.seniority_levels),
                "hypothesized_services_json": json.dumps(rec.hypothesized_services),
                "rationale": rec.rationale,
                "role_confidence_score": float(rec.role_confidence_score),
                "created_at": now,
            }
        )
    return rows


def build_source_rows(entity_rows: list[dict[str, Any]], expansion: ClusterExpansion) -> list[dict[str, Any]]:
    now = utcnow()
    entity_map = {(row["entity_name"], row["entity_type"]): row["cluster_entity_id"] for row in entity_rows}
    rows: list[dict[str, Any]] = []

    for node in all_nodes(expansion):
        cluster_entity_id = entity_map.get((node.entity_name, node.entity_type))
        for url in node.source_urls:
            rows.append(
                {
                    "cluster_source_id": str(uuid.uuid4()),
                    "cluster_id": entity_rows[0]["cluster_id"] if entity_rows else None,
                    "cluster_entity_id": cluster_entity_id,
                    "source_url": url,
                    "source_type": "web",
                    "source_title": None,
                    "publisher": None,
                    "published_at": None,
                    "used_for": f"{node.branch_type}_inference",
                    "retrieved_at": now,
                }
            )

    return rows
