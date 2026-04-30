from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Lock
from typing import Any

import pandas as pd
from cachetools import TTLCache

from .opportunity_data import (
    load_cluster_summaries,
    load_cluster_summary,
    load_entities_for_cluster,
    load_recommendations_for_cluster,
    load_sources_for_cluster,
)

from .schemas import (
    GraphEdge,
    GraphNode,
    KnowledgeGraphCountrySummary,
    KnowledgeGraphDistributionItem,
    KnowledgeGraphEventSummary,
    KnowledgeGraphRegionSummary,
    KnowledgeGraphResponse,
    MapMarker,
    OpportunityDetail,
    OpportunitySummary,
)

DETAIL_CACHE = TTLCache(maxsize=128, ttl=300)
DETAIL_CACHE_LOCK = Lock()
CLUSTERS_CACHE = TTLCache(maxsize=1, ttl=60)
CLUSTERS_CACHE_LOCK = Lock()
KNOWLEDGE_GRAPH_CACHE = TTLCache(maxsize=1, ttl=60)
KNOWLEDGE_GRAPH_CACHE_LOCK = Lock()

COUNTRY_REGION_FALLBACK: dict[str, str] = {
    "Argentina": "Latin America",
    "Australia": "APAC",
    "Austria": "EMEA",
    "Belgium": "EMEA",
    "Brazil": "Latin America",
    "Canada": "North America",
    "China": "APAC",
    "France": "EMEA",
    "Germany": "EMEA",
    "India": "APAC",
    "Italy": "EMEA",
    "Japan": "APAC",
    "Luxembourg": "EMEA",
    "Mexico": "North America",
    "Netherlands": "EMEA",
    "Singapore": "APAC",
    "South Africa": "EMEA",
    "Spain": "EMEA",
    "Switzerland": "EMEA",
    "United Arab Emirates": "EMEA",
    "United Kingdom": "EMEA",
    "United States": "North America",
}

TRIGGER_TONES: dict[str, str] = {
    "insolvency": "red",
    "regulatory_action": "blue",
    "regulatory_inquiry": "blue",
    "m_and_a": "purple",
    "financing": "amber",
}


def _clean_value(value: Any) -> Any:
    if isinstance(value, float) and pd.isna(value):
        return None
    if pd.isna(value):
        return None
    return value


def _json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, float) and pd.isna(value):
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(v) for v in parsed]
    except Exception:
        pass
    return []


def _series_to_summary(row: pd.Series) -> OpportunitySummary:
    payload = {key: _clean_value(value) for key, value in row.to_dict().items()}
    if payload.get("event_date") is not None and not isinstance(payload.get("event_date"), str):
        payload["event_date"] = str(payload["event_date"])
    if payload.get("cluster_created_at") is not None and not isinstance(payload.get("cluster_created_at"), str):
        payload["cluster_created_at"] = str(payload["cluster_created_at"])[:10]
    return OpportunitySummary(**payload)


def get_clusters_cached() -> pd.DataFrame:
    with CLUSTERS_CACHE_LOCK:
        cached = CLUSTERS_CACHE.get("clusters")
    if cached is not None:
        return cached
    clusters = load_cluster_summaries()
    with CLUSTERS_CACHE_LOCK:
        existing = CLUSTERS_CACHE.get("clusters")
        if existing is not None:
            return existing
        CLUSTERS_CACHE["clusters"] = clusters
        return clusters


def invalidate_read_caches() -> None:
    with CLUSTERS_CACHE_LOCK:
        CLUSTERS_CACHE.clear()
    with DETAIL_CACHE_LOCK:
        DETAIL_CACHE.clear()
    with KNOWLEDGE_GRAPH_CACHE_LOCK:
        KNOWLEDGE_GRAPH_CACHE.clear()


def list_opportunities() -> list[OpportunitySummary]:
    clusters = get_clusters_cached()
    return [_series_to_summary(row) for _, row in clusters.iterrows()]


def search_opportunities(query: str, limit: int = 10) -> list[OpportunitySummary]:
    """Search opportunities by pushing text filter into the cached DataFrame.

    Uses vectorised pandas string matching instead of a Python loop so the
    hot-path is handled in compiled C rather than per-row iteration.
    """
    if not query:
        return list_opportunities()[:limit]
    clusters = get_clusters_cached()
    if clusters.empty:
        return []
    q = query.lower()
    mask = pd.Series(False, index=clusters.index)
    for col in ("subject_company_name", "event_headline", "trigger_type", "subject_region", "subject_country"):
        if col in clusters.columns:
            mask = mask | clusters[col].fillna("").str.lower().str.contains(q, regex=False)
    matched = clusters[mask]
    if matched.empty:
        return []
    matched = matched.sort_values("opportunity_score", ascending=False, na_position="last")
    return [_series_to_summary(row) for _, row in matched.head(limit).iterrows()]


def normalize_region(region: str | None, country: str | None) -> str:
    raw = (region or "").strip().lower()
    if "emea" in raw or "europe" in raw or "middle east" in raw or "africa" in raw:
        return "EMEA"
    if "apac" in raw or "asia" in raw or "pacific" in raw or "oceania" in raw:
        return "APAC"
    if "north america" in raw:
        return "North America"
    if "latin" in raw or "latam" in raw or "south america" in raw:
        return "Latin America"
    if country and country in COUNTRY_REGION_FALLBACK:
        return COUNTRY_REGION_FALLBACK[country]
    return "Other"


def _tone_for_trigger(trigger_type: str | None) -> str:
    if not trigger_type:
        return "neutral"
    return TRIGGER_TONES.get(trigger_type, "neutral")


def _average_numeric(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _build_distribution(items: list[OpportunitySummary]) -> list[KnowledgeGraphDistributionItem]:
    counts: dict[tuple[str, str | None], int] = {}
    for item in items:
        label = item.trigger_type.replace("_", " ").title() if item.trigger_type else "Other"
        key = (label, item.trigger_type)
        counts[key] = counts.get(key, 0) + 1
    ordered = sorted(counts.items(), key=lambda entry: (-entry[1], entry[0][0]))[:4]
    return [
        KnowledgeGraphDistributionItem(
            label=label,
            count=count,
            trigger_type=trigger_type,
            tone=_tone_for_trigger(trigger_type),
        )
        for (label, trigger_type), count in ordered
    ]


def _event_summary(item: OpportunitySummary) -> KnowledgeGraphEventSummary:
    return KnowledgeGraphEventSummary(
        cluster_id=item.cluster_id,
        subject_company_name=item.subject_company_name,
        subject_country=item.subject_country,
        subject_region=normalize_region(item.subject_region, item.subject_country),
        trigger_type=item.trigger_type,
        event_date=item.event_date,
        event_summary=item.event_summary,
        opportunity_score=item.opportunity_score,
        cluster_confidence_score=item.cluster_confidence_score,
        headline_source_url=item.headline_source_url,
    )


def _region_narrative(region: str, items: list[OpportunitySummary]) -> str:
    distribution = _build_distribution(items)
    dominant = ", ".join(f"{entry.label.lower()} ({entry.count})" for entry in distribution) if distribution else "mixed event types"
    company_count = len({item.subject_company_name for item in items if item.subject_company_name})
    country_count = len({item.subject_country for item in items if item.subject_country})
    return "\n\n".join(
        [
            f"The research picture in {region} is currently led by {dominant}.",
            f"{len(items)} tracked events across {country_count} countries and {company_count} companies suggest where insolvency stress, regulatory friction, and capital structure pressure may be creating the strongest IDC advisory openings.",
            "Use this regional layer to compare country-level heat, determine where restructuring momentum is clustering, and decide which countries deserve deeper local narrative work.",
        ]
    )


def _country_narrative(country: str, region: str, items: list[OpportunitySummary]) -> str:
    distribution = _build_distribution(items)
    dominant = ", ".join(f"{entry.label.lower()} ({entry.count})" for entry in distribution) if distribution else "mixed event types"
    company_count = len({item.subject_company_name for item in items if item.subject_company_name})
    return "\n\n".join(
        [
            f"{country} currently shows a trigger mix led by {dominant}.",
            f"{len(items)} tracked events across {company_count} companies indicate how the local restructuring, regulatory, and economic environment is evolving inside the broader {region} picture.",
            "Use the country layer to tie individual events back to a country-level narrative about restructuring climate, regulatory posture, and near-term IDC opportunity density.",
        ]
    )


def _build_country_summary(region_id: str, country: str, items: list[OpportunitySummary]) -> KnowledgeGraphCountrySummary:
    sorted_items = sorted(items, key=lambda item: float(item.opportunity_score or item.cluster_priority_score or 0), reverse=True)
    opportunity_values = [float(item.opportunity_score or 0) for item in items if item.opportunity_score is not None]
    confidence_values = [float(item.cluster_confidence_score or 0) for item in items if item.cluster_confidence_score is not None]
    top_companies = list(dict.fromkeys(item.subject_company_name for item in sorted_items if item.subject_company_name))[:6]
    return KnowledgeGraphCountrySummary(
        country_id=country,
        label=country,
        region_id=region_id,
        narrative=_country_narrative(country, region_id, items),
        event_count=len(items),
        company_count=len({item.subject_company_name for item in items if item.subject_company_name}),
        average_opportunity=_average_numeric(opportunity_values),
        average_confidence=_average_numeric(confidence_values),
        dominant_triggers=_build_distribution(items),
        top_companies=top_companies,
        events=[_event_summary(item) for item in sorted_items[:12]],
    )


def build_knowledge_graph() -> KnowledgeGraphResponse:
    with KNOWLEDGE_GRAPH_CACHE_LOCK:
        cached = KNOWLEDGE_GRAPH_CACHE.get("graph")
    if cached is not None:
        return cached

    opportunities = list_opportunities()
    grouped_by_region: dict[str, list[OpportunitySummary]] = {}
    for item in opportunities:
        region_id = normalize_region(item.subject_region, item.subject_country)
        grouped_by_region.setdefault(region_id, []).append(item)

    regions: list[KnowledgeGraphRegionSummary] = []
    country_count = 0
    for region_id, region_items in sorted(grouped_by_region.items(), key=lambda entry: (-len(entry[1]), entry[0])):
        country_groups: dict[str, list[OpportunitySummary]] = {}
        for item in region_items:
            country_groups.setdefault(item.subject_country or "Unknown country", []).append(item)
        country_summaries = [
            _build_country_summary(region_id, country, country_items)
            for country, country_items in sorted(country_groups.items(), key=lambda entry: (-len(entry[1]), entry[0]))
        ]
        country_count += len(country_summaries)
        opportunity_values = [float(item.opportunity_score or 0) for item in region_items if item.opportunity_score is not None]
        confidence_values = [float(item.cluster_confidence_score or 0) for item in region_items if item.cluster_confidence_score is not None]
        regions.append(
            KnowledgeGraphRegionSummary(
                region_id=region_id,
                label=region_id,
                narrative=_region_narrative(region_id, region_items),
                event_count=len(region_items),
                country_count=len(country_summaries),
                company_count=len({item.subject_company_name for item in region_items if item.subject_company_name}),
                average_opportunity=_average_numeric(opportunity_values),
                average_confidence=_average_numeric(confidence_values),
                dominant_triggers=_build_distribution(region_items),
                countries=country_summaries,
            )
        )

    graph = KnowledgeGraphResponse(
        generated_at=datetime.now(timezone.utc),
        region_count=len(regions),
        country_count=country_count,
        event_count=len(opportunities),
        regions=regions,
    )
    with KNOWLEDGE_GRAPH_CACHE_LOCK:
        KNOWLEDGE_GRAPH_CACHE["graph"] = graph
    return graph


def get_cluster_summary_row(cluster_id: str) -> pd.Series:
    with CLUSTERS_CACHE_LOCK:
        cached = CLUSTERS_CACHE.get("clusters")
    if cached is not None:
        match = cached[cached["cluster_id"] == cluster_id]
        if not match.empty:
            return match.iloc[0]

    summary = load_cluster_summary(cluster_id)
    if summary.empty:
        raise KeyError(cluster_id)
    return summary.iloc[0]


def _build_graph_payload(
    row: pd.Series,
    entities_df: pd.DataFrame,
    recs_df: pd.DataFrame,
) -> tuple[list[GraphNode], list[GraphEdge]]:
    cluster_id = str(row["cluster_id"])
    cluster_node_id = f"cluster:{cluster_id}"
    nodes: list[GraphNode] = [
        GraphNode(
            id=cluster_node_id,
            type="cluster",
            subtype="opportunity_cluster",
            label=str(row.get("subject_company_name", "Opportunity")),
            score=float(row.get("opportunity_score") or row.get("cluster_priority_score") or 0),
            confidence_score=float(row.get("cluster_confidence_score") or 0),
            priority_score=float(row.get("cluster_priority_score") or 0),
            route_to_market=str(row.get("best_route_to_market", "") or ""),
            detail={
                "headline": str(row.get("event_headline", "") or ""),
                "summary": str(row.get("event_summary", "") or ""),
                "propagation_thesis": str(row.get("propagation_thesis", "") or ""),
                "trigger_type": str(row.get("trigger_type", "") or ""),
                "trigger_subtype": str(row.get("trigger_subtype", "") or ""),
                "country": str(row.get("subject_country", "") or ""),
                "region": str(row.get("subject_region", "") or ""),
                "state": str(row.get("subject_state", "") or ""),
                "city": str(row.get("subject_city", "") or ""),
                "address": str(row.get("subject_address", "") or ""),
                "latitude": float(row.get("subject_latitude") or 0) if row.get("subject_latitude") is not None else None,
                "longitude": float(row.get("subject_longitude") or 0) if row.get("subject_longitude") is not None else None,
            },
        )
    ]
    edges: list[GraphEdge] = []

    recs_by_entity: dict[str, list[dict[str, Any]]] = {}
    for _, rec in recs_df.iterrows():
        recs_by_entity.setdefault(str(rec.get("cluster_entity_id", "")), []).append(rec.to_dict())

    for _, ent in entities_df.iterrows():
        ent_id = str(ent.get("cluster_entity_id", ""))
        node_id = f"entity:{ent_id}"
        node = GraphNode(
            id=node_id,
            type="company",
            subtype=str(ent.get("entity_type", "") or ""),
            label=str(ent.get("entity_name", "") or "Unknown"),
            entity_id=ent_id,
            branch_type=str(ent.get("branch_type", "") or ""),
            score=float(ent.get("priority_score") or 0),
            confidence_score=float(ent.get("confidence_score") or 0),
            priority_score=float(ent.get("priority_score") or 0),
            route_to_market=str(row.get("best_route_to_market", "") or ""),
            detail={
                "relationship_to_subject": str(ent.get("relationship_to_subject", "") or ""),
                "commercial_role": str(ent.get("commercial_role", "") or ""),
                "rationale": str(ent.get("rationale", "") or ""),
                "country": str(ent.get("entity_country", "") or ""),
                "region": str(ent.get("entity_region", "") or ""),
                "state": str(ent.get("entity_state", "") or ""),
                "city": str(ent.get("entity_city", "") or ""),
                "address": str(ent.get("entity_address", "") or ""),
                "latitude": float(ent.get("entity_latitude") or 0) if ent.get("entity_latitude") is not None else None,
                "longitude": float(ent.get("entity_longitude") or 0) if ent.get("entity_longitude") is not None else None,
                "source_urls": _json_list(ent.get("source_urls_json")),
                "source_snippets": _json_list(ent.get("source_snippets_json")),
            },
        )
        nodes.append(node)
        edges.append(
            GraphEdge(
                id=f"edge:{cluster_id}:{ent_id}",
                source=cluster_node_id,
                target=node_id,
                type="has_entity",
                branch_type=str(ent.get("branch_type", "") or ""),
                label=str(ent.get("commercial_role", "") or ""),
                weight=float(ent.get("priority_score") or 0),
                rationale=str(ent.get("rationale", "") or ""),
            )
        )

        for rec in recs_by_entity.get(ent_id, []):
            role_id = str(rec.get("role_recommendation_id", ""))
            role_node_id = f"role:{role_id}"
            nodes.append(
                GraphNode(
                    id=role_node_id,
                    type="role_track",
                    subtype=str(rec.get("role_track_type", "") or ""),
                    label=str(rec.get("role_track_type", "") or "").replace("_", " ").title(),
                    entity_id=ent_id,
                    branch_type=str(ent.get("branch_type", "") or ""),
                    score=float(rec.get("role_confidence_score") or 0),
                    confidence_score=float(rec.get("role_confidence_score") or 0),
                    priority_score=float(ent.get("priority_score") or 0),
                    route_to_market=str(row.get("best_route_to_market", "") or ""),
                    detail={
                        "rationale": str(rec.get("rationale", "") or ""),
                        "recommended_titles": _json_list(rec.get("recommended_titles_json")),
                        "departments": _json_list(rec.get("departments_json")),
                        "seniority_levels": _json_list(rec.get("seniority_levels_json")),
                    },
                )
            )
            edges.append(
                GraphEdge(
                    id=f"edge:role:{role_id}",
                    source=node_id,
                    target=role_node_id,
                    type="recommends_role",
                    branch_type=str(ent.get("branch_type", "") or ""),
                    label=str(rec.get("role_track_type", "") or "").replace("_", " ").title(),
                    weight=float(rec.get("role_confidence_score") or 0),
                    rationale=str(rec.get("rationale", "") or ""),
                )
            )

    return nodes, edges


def get_opportunity_detail(cluster_id: str) -> OpportunityDetail:
    with DETAIL_CACHE_LOCK:
        cached = DETAIL_CACHE.get(cluster_id)
    if cached is not None:
        return cached

    row = get_cluster_summary_row(cluster_id)
    with ThreadPoolExecutor(max_workers=3) as executor:
        entities_future = executor.submit(load_entities_for_cluster, cluster_id)
        recs_future = executor.submit(load_recommendations_for_cluster, cluster_id)
        sources_future = executor.submit(load_sources_for_cluster, cluster_id)
        entities = entities_future.result()
        recs = recs_future.result()
        sources = sources_future.result()

    nodes, edges = _build_graph_payload(row, entities, recs)

    detail = OpportunityDetail(
        cluster=_series_to_summary(row),
        graph_nodes=nodes,
        graph_edges=edges,
        entities=entities.to_dict(orient="records"),
        recommendations=recs.to_dict(orient="records"),
        sources=sources.to_dict(orient="records"),
    )
    with DETAIL_CACHE_LOCK:
        DETAIL_CACHE[cluster_id] = detail
    return detail


def build_map_markers() -> list[MapMarker]:
    clusters = get_clusters_cached()
    return [
        MapMarker(
            cluster_id=str(row.get("cluster_id", "")),
            label=str(row.get("subject_company_name", "") or "Unknown"),
            country=str(row.get("subject_country", "") or ""),
            state=str(row.get("subject_state", "") or ""),
            city=str(row.get("subject_city", "") or ""),
            address=str(row.get("subject_address", "") or ""),
            latitude=float(row.get("subject_latitude")) if row.get("subject_latitude") is not None else None,
            longitude=float(row.get("subject_longitude")) if row.get("subject_longitude") is not None else None,
            region=normalize_region(str(row.get("subject_region", "") or ""), str(row.get("subject_country", "") or "")),
            trigger_type=str(row.get("trigger_type", "") or ""),
            cluster_priority_score=float(row.get("cluster_priority_score") or 0),
            cluster_confidence_score=float(row.get("cluster_confidence_score") or 0),
            opportunity_score=float(row.get("opportunity_score") or row.get("cluster_priority_score") or 0),
        )
        for _, row in clusters.iterrows()
    ]
