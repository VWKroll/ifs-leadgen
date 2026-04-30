"""Sherlock AI tool definitions and execution logic — extracted from chat_service."""
from __future__ import annotations

import json

from .services import get_opportunity_detail, search_opportunities


# ---------------------------------------------------------------------------
# Agentic tool definitions for Sherlock AI
# ---------------------------------------------------------------------------

SHERLOCK_TOOLS = [
    {
        "type": "function",
        "name": "search_leads",
        "description": "Search the opportunity/lead universe by company name, event keyword, or sector. Returns top scored leads.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Company name, event keyword, or sector to search for."},
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "get_lead_detail",
        "description": "Get full detail on a specific opportunity cluster by its cluster ID. Returns entities, sources, graph, and recommendations.",
        "parameters": {
            "type": "object",
            "properties": {
                "cluster_id": {"type": "string", "description": "The cluster_id of the opportunity to inspect."},
            },
            "required": ["cluster_id"],
            "additionalProperties": False,
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "save_deduction",
        "description": "Save a key insight or deduction about the analyst or an opportunity to persistent memory. Use for facts worth remembering across sessions.",
        "parameters": {
            "type": "object",
            "properties": {
                "deduction": {"type": "string", "description": "The insight or deduction to save."},
            },
            "required": ["deduction"],
            "additionalProperties": False,
        },
        "strict": True,
    },
    {
        "type": "function",
        "name": "claim_lead",
        "description": (
            "Claim a company (entity) from an opportunity cluster for the current user's sales pipeline. "
            "Use this when the analyst asks you to claim a company or add it to their pipeline. "
            "You need the cluster_id and the entity/company name. The analyst's identity is used automatically."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "cluster_id": {"type": "string", "description": "The cluster_id of the opportunity cluster containing the company."},
                "company_name": {"type": "string", "description": "The entity/company name to claim within the cluster."},
                "notes": {"type": ["string", "null"], "description": "Optional notes or instructions for the claim draft."},
            },
            "required": ["cluster_id", "company_name", "notes"],
            "additionalProperties": False,
        },
        "strict": True,
    },
]


def execute_tool(tool_name: str, arguments: dict, user_id: str | None) -> str:
    """Execute a Sherlock AI tool call and return the result as a string."""
    try:
        if tool_name == "search_leads":
            query = str(arguments.get("query", "")).lower()
            matches = search_opportunities(query, limit=10)
            if not matches:
                return json.dumps({"results": [], "message": f"No leads found matching '{query}'."})
            return json.dumps({
                "results": [
                    {
                        "cluster_id": lead.cluster_id,
                        "company": lead.subject_company_name,
                        "headline": lead.event_headline,
                        "score": lead.opportunity_score,
                        "region": lead.subject_region,
                        "country": lead.subject_country,
                        "trigger": lead.trigger_type,
                    }
                    for lead in matches
                ],
                "total_matches": len(matches),
            })

        elif tool_name == "get_lead_detail":
            cluster_id = str(arguments.get("cluster_id", ""))
            detail = get_opportunity_detail(cluster_id)
            return json.dumps({
                "cluster": detail.cluster.model_dump(mode="json"),
                "entity_count": len(detail.entities),
                "source_count": len(detail.sources),
                "top_entities": [
                    {"name": e.get("entity_name"), "type": e.get("entity_type"), "role": e.get("commercial_role")}
                    for e in detail.entities[:5]
                ],
                "recommendations": [
                    {"text": r.get("recommendation_text", r.get("service_line", ""))}
                    for r in detail.recommendations[:3]
                ],
            })

        elif tool_name == "save_deduction":
            deduction = str(arguments.get("deduction", ""))
            if user_id and deduction:
                from .store import get_user_memory, upsert_user_memory
                memory = get_user_memory(user_id)
                profile = memory.get("profile", {})
                existing = profile.get("key_deductions", []) if isinstance(profile, dict) else []
                existing.append(deduction)
                profile["key_deductions"] = existing[-20:]  # keep last 20
                upsert_user_memory(user_id, "profile", profile)
                return json.dumps({"status": "saved", "deduction": deduction})
            return json.dumps({"status": "no_user_id", "message": "Cannot save without user identity."})

        elif tool_name == "claim_lead":
            if not user_id:
                return json.dumps({"error": "Cannot claim without user identity. Please sign in first."})
            cluster_id = str(arguments.get("cluster_id", ""))
            company_name = str(arguments.get("company_name", "")).strip()
            notes = arguments.get("notes") or None
            if not cluster_id or not company_name:
                return json.dumps({"error": "Both cluster_id and company_name are required."})

            detail = get_opportunity_detail(cluster_id)
            # Find the matching entity by name (case-insensitive)
            entity = None
            for e in detail.entities:
                if str(e.get("entity_name", "")).strip().lower() == company_name.lower():
                    entity = e
                    break
            if entity is None:
                available = [str(e.get("entity_name", "")) for e in detail.entities[:10]]
                return json.dumps({
                    "error": f"No entity named '{company_name}' found in cluster {cluster_id}.",
                    "available_entities": available,
                })

            sales_item_id = str(entity.get("cluster_entity_id", ""))
            # Resolve user display name from memory
            from .store import get_user_memory
            memory = get_user_memory(user_id)
            profile = memory.get("profile", {}) if isinstance(memory, dict) else {}
            user_name = str(profile.get("name") or user_id) if isinstance(profile, dict) else user_id

            from .schemas import ClaimOpportunityRequest
            from .sales_workspace import claim_opportunity
            claim_request = ClaimOpportunityRequest(
                sales_item_id=sales_item_id,
                claimed_by_user_id=user_id,
                claimed_by_name=user_name,
                notes=notes,
            )
            workspace = claim_opportunity(cluster_id, claim_request)
            return json.dumps({
                "status": "claimed",
                "claim_id": workspace.claim_id,
                "company": str(entity.get("entity_name", "")),
                "cluster_id": cluster_id,
                "branch_type": str(entity.get("branch_type", "")),
                "message": f"Successfully claimed {entity.get('entity_name')} for your sales pipeline.",
            })

        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})
    except Exception as exc:
        return json.dumps({"error": str(exc)})
