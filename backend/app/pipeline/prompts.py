from __future__ import annotations

import json

from .config import DEFAULT_WEIGHTS, PEER_WEIGHTS, TRIGGER_RULES
from .models import ClusterEntity, EventCandidate


def event_discovery_prompts(
    excluded_subjects: list[str],
    allowed_triggers: list[str],
    recency_days: int,
    *,
    target_region: str,
    company_name: str | None = None,
) -> tuple[str, str]:
    normalized_region = target_region.strip() or "Europe"

    if company_name:
        system_prompt = f"""
You are an IDC lead-generation analyst for professional-services sales.

Task:
- Find exactly ONE recent IDC-relevant trigger event for {company_name}.
- Retrieve the event headline and its direct news source URL.
- The event must imply a plausible need for Investigations, Diligence & Compliance services.
- Prefer the strongest evidence-backed event within the last {recency_days} days.
- You MUST use the web_search_preview tool to find and verify headlines and source URLs.

Allowed trigger types: {json.dumps(allowed_triggers)}

Rules:
1. The subject company should be {company_name} or its clearly matched operating entity.
2. Event must be within the past {recency_days} days.
3. Event must be specific and evidence-backed.
4. Separate fact from inference.
5. Scores (confidence, severity, urgency) range 0-100.
6. Return ONLY structured JSON.
7. If you know the company's headquarters location, populate country and any available city, state, address, latitude, and longitude fields.

Source URL requirements:
- headline_source_url must be the direct article URL.
- primary_source_urls must contain at least one verifiable public URL.
- secondary_source_urls should contain corroborating links when available.
- Do not fabricate URLs or use placeholder domains.
"""

        user_prompt = f"""
Find one recent IDC-relevant trigger event for {company_name}.

Do not switch to another company unless you cannot verify that the event belongs to {company_name} or its clearly matched operating entity.
If the company is part of a group, choose the most commercially relevant named operating entity and make that explicit in the structured response.

Return: subject company, event, summary, IDC service hypotheses, location context, headline_source_url, and evidence URLs.
"""
        return system_prompt, user_prompt

    system_prompt = f"""
You are an IDC lead-generation analyst for professional-services sales.

Task:
- Find exactly ONE recent company in {normalized_region} experiencing a material trigger event.
- Retrieve the event headline and its direct news source URL.
- The event must imply a plausible need for Investigations, Diligence & Compliance services.
- Prefer events with propagation potential to peers or sponsors.
- Avoid companies in the exclusion list.
- You MUST use the web_search_preview tool to find and verify headlines and source URLs.

Allowed trigger types: {json.dumps(allowed_triggers)}

Rules:
1. Company must be domiciled in {normalized_region}.
2. Event must be within the past {recency_days} days.
3. Event must be specific and evidence-backed.
4. Separate fact from inference.
5. Scores (confidence, severity, urgency) range 0-100.
6. Return ONLY structured JSON.
7. If you know the company's headquarters location, populate country and any available city, state, address, latitude, and longitude fields.

Source URL requirements:
- headline_source_url must be the direct article URL.
- primary_source_urls must contain at least one verifiable public URL.
- secondary_source_urls should contain corroborating links when available.
- Do not fabricate URLs or use placeholder domains.
"""

    user_prompt = f"""
Find one company in {normalized_region} with a recent IDC-relevant trigger event.

Exclude these subject companies:
{json.dumps(excluded_subjects[:500])}

Return: subject company, event, summary, IDC service hypotheses, location context, headline_source_url, and evidence URLs.
"""
    return system_prompt, user_prompt


def cluster_expansion_prompts(event: EventCandidate, max_peers: int, max_ownership_nodes: int) -> tuple[str, str]:
    rules = TRIGGER_RULES.get(event.trigger_type, DEFAULT_WEIGHTS)

    system_prompt = f"""
You are building an event-centered opportunity cluster for IDC sales.

Given one trigger event, expand into:
1. Direct subject node
2. Up to {max_peers} named peer-company nodes
3. Up to {max_ownership_nodes} ownership / sponsor nodes

V1 scope:
- Prioritize peer extrapolation and sponsor expansion.
- Accounts first, not named people.
- No generic competitor lists without rationale.

Every node must include: relationship_to_subject, commercial_role, rationale, evidence_type, source_urls, confidence_score, priority_score.
If known, include country and any available city, state, address, latitude, and longitude for the entity.

Peer logic: weighted combination using {json.dumps(PEER_WEIGHTS)}.
Only include peers with a strong rationale tied to this exact trigger.

Ownership logic: include current owners, recent sponsors, or deal counterparties when commercially credible.

Trigger-specific weights: {json.dumps(rules)}

evidence_type options: direct_evidence | structured_inference | commercial_hypothesis
best_route_to_market options: direct_subject | sponsor_led | mixed
"""

    user_prompt = f"""
Build a cluster for this event:
{event.model_dump_json(indent=2)}

Return: propagation_thesis, best_route_to_market, direct_node, peer_nodes, ownership_nodes, cluster_confidence_score.
"""
    return system_prompt, user_prompt


def role_prompts(event: EventCandidate, entity: ClusterEntity) -> tuple[str, str]:
    system_prompt = """
You recommend role tracks for IDC sales outreach.
Do NOT name individuals — return titles, departments, seniority levels.

Allowed role_track_type values:
- management_execution
- board_oversight
- sponsor_governance

Guidance:
- Subject companies -> execution + governance roles.
- Peer companies -> preventive management + oversight roles.
- Sponsors / owners -> governance + operating roles.

hypothesized_services:
- Include 2-5 specific Kroll / IDC professional service lines that are most relevant for THIS entity.
- Consider the entity's relationship to the event, its branch type, and the trigger context.
- Examples: "Forensic Investigations", "Compliance Program Advisory", "Cyber Risk", "Valuation Advisory", "Transaction Advisory", "Restructuring", "Regulatory Compliance".

Return structured JSON only.
"""

    user_prompt = f"""
Event:
{event.model_dump_json(indent=2)}

Cluster entity:
{entity.model_dump_json(indent=2)}

Recommend the best role track for outreach to this account.
"""
    return system_prompt, user_prompt
