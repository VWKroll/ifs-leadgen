from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Iterator

from pydantic import BaseModel

from .control_plane import trigger_knowledge_base_sync
from .chat_tools import SHERLOCK_TOOLS, execute_tool as _execute_tool
from .knowledge_base import ensure_cluster_markdown, get_knowledge_base_status
from .schemas import ChatCitation, ChatCommitRequest, ChatCommitResponse, ChatRequest, ChatResponse, ChatMessage
from .services import build_knowledge_graph, get_opportunity_detail
from .settings import settings
from .store import count_kb_document_records, get_user_memory, list_kb_document_records, save_chat_note_record, upsert_user_memory, utcnow
from .pipeline.provider import get_azure_capabilities, get_azure_client, parse_json_output, patch_schema_for_strict

CHAT_SYSTEM_PROMPT = """
You are Sherlock AI — the deductive reasoning engine powering IDC Event Intelligence.

Personality & tone:
- Channel Sherlock Holmes. Be incisive, direct, and analytical.
- Skip pleasantries. Lead with your strongest deduction, then show the evidence chain.
- Use phrases like "The data reveals…", "Observe:", "Elementary —", "Three facts demand attention:".
- When uncertain, state that plainly — never fabricate evidence.
- Be concise; analysts prize speed over verbosity.

Your role:
- Help sales analysts identify, investigate, and advance event-driven opportunities.
- You have access to a knowledge base of news events, entity relationships, and scored leads.
- Proactively surface connections the analyst may have missed ("You may not have noticed, but…").

Rules:
- Ground every claim in retrieved context or cited sources.
- If the knowledge base does not support a claim, say so clearly.
- When the analyst's profile/memory is available, tailor deductions to their sector expertise, region focus, and active pipeline.
- You may call the provided tools (search_leads, get_lead_detail, claim_lead, advance_stage, save_deduction) to take action on behalf of the analyst. Confirm before claiming or advancing leads.
""".strip()

SHERLOCK_ONBOARDING_PROMPT = """
The game is afoot. Before I can apply my methods to your advantage, I require a few data points — consider it calibrating the instrument.

1. **What is your coverage sector?** (e.g., TMT, Industrials, Healthcare, Energy, Financial Services)
2. **Which region do you primarily focus on?** (e.g., North America, EMEA, APAC, Latin America, Global)
3. **What deal stages do you typically own?** (Origination, execution, or both?)
4. **Any active pursuits I should know about?** (Company names or sectors you're currently tracking)

I shall remember everything. You will not need to repeat yourself.
""".strip()

CHAT_COMMIT_SYSTEM_PROMPT = """
You are Sherlock AI preparing a durable knowledge-base note from a working analyst chat.

Remove pleasantries, dead ends, and repetition.
Keep only durable, high-signal information that will help future teams understand the opportunity.
Preserve uncertainty when claims are not verified.
Write concise markdown in a Sherlock-style analytical tone that can live inside an internal wiki.
""".strip()


class ChatCommitSummary(BaseModel):
    title: str
    summary_markdown: str


class ProfileExtract(BaseModel):
    """Extracted profile updates from a conversation."""
    sector: str | None = None
    region: str | None = None
    role: str | None = None
    deal_stages: list[str] | None = None
    active_pursuits: list[str] | None = None
    expertise_areas: list[str] | None = None
    key_deductions: list[str] | None = None


def _build_user_context(user_id: str | None) -> str | None:
    """Load user memory and format it as context for the system prompt."""
    if not user_id:
        return None
    try:
        memory = get_user_memory(user_id)
    except Exception:
        return None
    if not memory:
        return None
    profile = memory.get("profile", {})
    if not isinstance(profile, dict) or not profile:
        return None
    parts = ["Known about this analyst:"]
    if profile.get("name"):
        parts.append(f"- Name: {profile['name']}")
    if profile.get("role"):
        parts.append(f"- Role: {profile['role']}")
    if profile.get("sector"):
        parts.append(f"- Sector focus: {profile['sector']}")
    if profile.get("region"):
        parts.append(f"- Region focus: {profile['region']}")
    if profile.get("deal_stages"):
        parts.append(f"- Deal stages: {', '.join(profile['deal_stages'])}")
    if profile.get("active_pursuits"):
        parts.append(f"- Active pursuits: {', '.join(profile['active_pursuits'][:5])}")
    if profile.get("expertise_areas"):
        parts.append(f"- Expertise: {', '.join(profile['expertise_areas'][:5])}")
    if profile.get("key_deductions"):
        recent = profile["key_deductions"][-5:]
        parts.append("- Recent deductions:")
        for d in recent:
            parts.append(f"  • {d}")
    return "\n".join(parts) if len(parts) > 1 else None


def _sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


def _format_chat_transcript(messages: list[ChatMessage]) -> str:
    lines: list[str] = []
    for message in messages:
        role = "Assistant" if message.role == "assistant" else "User"
        content = message.content.strip()
        if not content:
            continue
        lines.append(f"{role}: {content}")
    return "\n\n".join(lines)


def _parse_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return [value] if value.strip() else []
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item).strip()]
    return []


def _append_focus_citation(citations: list[ChatCitation], citation: ChatCitation | None) -> None:
    if citation is None:
        return
    key = (
        citation.url or "",
        citation.file_path or "",
        citation.label,
        citation.entity_id or "",
        citation.source_id or "",
        citation.graph_node_id or "",
        citation.region_id or "",
        citation.country_id or "",
    )
    if any(
        (
            existing.url or "",
            existing.file_path or "",
            existing.label,
            existing.entity_id or "",
            existing.source_id or "",
            existing.graph_node_id or "",
            existing.region_id or "",
            existing.country_id or "",
        )
        == key
        for existing in citations
    ):
        return
    citations.append(citation)


def _record_attributes(record: dict[str, Any] | None) -> dict[str, Any]:
    if not record:
        return {}
    raw = record.get("attributes_json")
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _find_region_summary(region_id: str):
    graph = build_knowledge_graph()
    return next((region for region in graph.regions if region.region_id == region_id), None)


def _find_country_summary(region_id: str | None, country_id: str):
    graph = build_knowledge_graph()
    for region in graph.regions:
        if region_id and region.region_id != region_id:
            continue
        country = next((item for item in region.countries if item.country_id == country_id), None)
        if country:
            return country
    return None


def _build_focused_context(request: ChatRequest) -> tuple[str | None, list[ChatCitation]]:
    if not any([request.selected_cluster_id, request.region_id, request.country_id]):
        return None, []

    if not any([request.entity_id, request.source_id, request.graph_node_id, request.active_tab, request.region_id, request.country_id]):
        return None, []

    context_lines: list[str] = []
    citations: list[ChatCitation] = []
    detail = get_opportunity_detail(request.selected_cluster_id) if request.selected_cluster_id else None

    if request.active_tab:
        context_lines.append(f"Active UI tab: {request.active_tab}.")

    if request.region_id:
        region = _find_region_summary(request.region_id)
        if region:
            context_lines.extend(
                [
                    "Focused region context:",
                    f"- Region ID: {region.region_id}",
                    f"- Event count: {region.event_count}",
                    f"- Country count: {region.country_count}",
                    f"- Company count: {region.company_count}",
                    f"- Regional narrative: {region.narrative}",
                ]
            )
            _append_focus_citation(
                citations,
                ChatCitation(
                    id=f"focus:region:{region.region_id}",
                    label=region.label,
                    region_id=region.region_id,
                ),
            )

    if request.country_id:
        country = _find_country_summary(request.region_id, request.country_id)
        if country:
            context_lines.extend(
                [
                    "Focused country context:",
                    f"- Country ID: {country.country_id}",
                    f"- Region ID: {country.region_id}",
                    f"- Event count: {country.event_count}",
                    f"- Company count: {country.company_count}",
                    f"- Country narrative: {country.narrative}",
                ]
            )
            _append_focus_citation(
                citations,
                ChatCitation(
                    id=f"focus:country:{country.region_id}:{country.country_id}",
                    label=country.label,
                    region_id=country.region_id,
                    country_id=country.country_id,
                ),
            )

    if request.entity_id and detail:
        entity = next(
            (
                item
                for item in detail.entities
                if str(item.get("cluster_entity_id") or "") == request.entity_id
            ),
            None,
        )
        if entity:
            entity_name = str(entity.get("entity_name") or "Unknown entity")
            source_urls = _parse_string_list(entity.get("source_urls_json"))
            source_snippets = _parse_string_list(entity.get("source_snippets_json"))
            context_lines.extend(
                [
                    "Focused entity context:",
                    f"- Entity ID: {request.entity_id}",
                    f"- Name: {entity_name}",
                    f"- Type: {entity.get('entity_type') or 'Unknown'}",
                    f"- Branch: {entity.get('branch_type') or 'Unknown'}",
                    f"- Commercial role: {entity.get('commercial_role') or 'Unknown'}",
                    f"- Relationship to subject: {entity.get('relationship_to_subject') or 'Unavailable'}",
                    f"- Rationale: {entity.get('rationale') or 'Unavailable'}",
                ]
            )
            if source_snippets:
                context_lines.append(f"- Source snippet: {source_snippets[0]}")
            _append_focus_citation(
                citations,
                ChatCitation(
                    id=f"focus:entity:{request.entity_id}",
                    label=entity_name,
                    cluster_id=request.selected_cluster_id,
                    entity_id=request.entity_id,
                    url=source_urls[0] if source_urls else None,
                ),
            )

    if request.source_id and detail:
        source = next(
            (
                item
                for item in detail.sources
                if str(item.get("cluster_source_id") or "") == request.source_id
            ),
            None,
        )
        if source:
            source_title = str(source.get("source_title") or source.get("source_url") or "Selected source")
            context_lines.extend(
                [
                    "Focused source context:",
                    f"- Source ID: {request.source_id}",
                    f"- Title: {source_title}",
                    f"- URL: {source.get('source_url') or 'Unavailable'}",
                    f"- Publisher: {source.get('publisher') or 'Unknown'}",
                    f"- Used for: {source.get('used_for') or 'Unknown'}",
                    f"- Published at: {source.get('published_at') or 'Unknown'}",
                    f"- Linked entity ID: {source.get('cluster_entity_id') or 'Unavailable'}",
                ]
            )
            _append_focus_citation(
                citations,
                ChatCitation(
                    id=f"focus:source:{request.source_id}",
                    label=source_title,
                    cluster_id=request.selected_cluster_id,
                    source_id=request.source_id,
                    entity_id=str(source.get("cluster_entity_id")) if source.get("cluster_entity_id") else None,
                    url=str(source.get("source_url")) if source.get("source_url") else None,
                ),
            )

    if request.graph_node_id and detail:
        graph_node = next((item for item in detail.graph_nodes if item.id == request.graph_node_id), None)
        if graph_node:
            context_lines.extend(
                [
                    "Focused graph node context:",
                    f"- Node ID: {request.graph_node_id}",
                    f"- Label: {graph_node.label}",
                    f"- Type: {graph_node.type}",
                    f"- Subtype: {graph_node.subtype}",
                    f"- Branch: {graph_node.branch_type or 'Unknown'}",
                ]
            )
            detail_parts: list[str] = []
            for key in ("headline", "summary", "propagation_thesis", "relationship_to_subject", "commercial_role", "rationale"):
                value = graph_node.detail.get(key)
                if value:
                    detail_parts.append(f"{key.replace('_', ' ').title()}: {value}")
            if detail_parts:
                context_lines.append(f"- Graph detail: {' | '.join(detail_parts[:3])}")
            _append_focus_citation(
                citations,
                ChatCitation(
                    id=f"focus:graph:{request.graph_node_id}",
                    label=graph_node.label,
                    cluster_id=request.selected_cluster_id,
                    entity_id=graph_node.entity_id,
                    graph_node_id=request.graph_node_id,
                ),
            )

    context_text = "\n".join(context_lines).strip()
    return (context_text or None), citations


def _build_file_search_filters(request: ChatRequest) -> dict[str, Any] | None:
    if request.scope == "all":
        if request.country_id:
            country_filters: list[dict[str, Any]] = [
                {"type": "eq", "key": "document_kind", "value": "country"},
                {"type": "eq", "key": "country_id", "value": request.country_id},
            ]
            if request.region_id:
                country_filters.append({"type": "eq", "key": "region_id", "value": request.region_id})
            filters: list[dict[str, Any]] = [{"type": "and", "filters": country_filters}]
            if request.region_id:
                filters.append(
                    {
                        "type": "and",
                        "filters": [
                            {"type": "eq", "key": "document_kind", "value": "region"},
                            {"type": "eq", "key": "region_id", "value": request.region_id},
                        ],
                    }
                )
            return {"type": "or", "filters": filters}

        if request.region_id:
            return {
                "type": "or",
                "filters": [
                    {
                        "type": "and",
                        "filters": [
                            {"type": "eq", "key": "document_kind", "value": "region"},
                            {"type": "eq", "key": "region_id", "value": request.region_id},
                        ],
                    },
                    {
                        "type": "and",
                        "filters": [
                            {"type": "eq", "key": "document_kind", "value": "country"},
                            {"type": "eq", "key": "region_id", "value": request.region_id},
                        ],
                    },
                ],
            }

        if request.active_tab == "global_graph":
            return {
                "type": "or",
                "filters": [
                    {"type": "eq", "key": "document_kind", "value": "region"},
                    {"type": "eq", "key": "document_kind", "value": "country"},
                    {"type": "eq", "key": "document_kind", "value": "manifest"},
                ],
            }

    if request.scope != "selected_cluster" or not request.selected_cluster_id:
        return None

    base_cluster_filter = {"type": "eq", "key": "cluster_id", "value": request.selected_cluster_id}

    if request.source_id:
        return {
            "type": "and",
            "filters": [
                base_cluster_filter,
                {"type": "eq", "key": "document_kind", "value": "source"},
                {"type": "eq", "key": "source_id", "value": request.source_id},
            ],
        }

    if request.entity_id:
        return {
            "type": "or",
            "filters": [
                {
                    "type": "and",
                    "filters": [
                        base_cluster_filter,
                        {"type": "eq", "key": "document_kind", "value": "entity"},
                        {"type": "eq", "key": "entity_id", "value": request.entity_id},
                    ],
                },
                {
                    "type": "and",
                    "filters": [
                        base_cluster_filter,
                        {"type": "eq", "key": "document_kind", "value": "source"},
                        {"type": "eq", "key": "linked_entity_id", "value": request.entity_id},
                    ],
                },
            ],
        }

    if request.active_tab == "sources":
        return {
            "type": "and",
            "filters": [
                base_cluster_filter,
                {"type": "eq", "key": "document_kind", "value": "source"},
            ],
        }

    if request.active_tab == "cluster":
        return {
            "type": "or",
            "filters": [
                {
                    "type": "and",
                    "filters": [
                        base_cluster_filter,
                        {"type": "eq", "key": "document_kind", "value": "entity"},
                    ],
                },
                {
                    "type": "and",
                    "filters": [
                        base_cluster_filter,
                        {"type": "eq", "key": "document_kind", "value": "cluster"},
                    ],
                },
            ],
        }

    return base_cluster_filter


def build_chat_payload(
    request: ChatRequest,
    *,
    vector_store_id: str | None,
    fallback_markdown: str | None = None,
    focused_context: str | None = None,
    user_context: str | None = None,
) -> dict[str, Any]:
    scope_label = "selected opportunity cluster" if request.scope == "selected_cluster" else "full knowledge base"
    message_parts = [f"Scope: {scope_label}."]

    if request.selected_cluster_id:
        message_parts.append(f"Selected cluster ID: {request.selected_cluster_id}.")

    if request.active_tab:
        message_parts.append(f"Active tab: {request.active_tab}.")
    if request.entity_id:
        message_parts.append(f"Focused entity ID: {request.entity_id}.")
    if request.source_id:
        message_parts.append(f"Focused source ID: {request.source_id}.")
    if request.graph_node_id:
        message_parts.append(f"Focused graph node ID: {request.graph_node_id}.")
    if request.region_id:
        message_parts.append(f"Focused region ID: {request.region_id}.")
    if request.country_id:
        message_parts.append(f"Focused country ID: {request.country_id}.")

    if focused_context:
        message_parts.extend(
            [
                "Focused opportunity context follows.",
                focused_context,
            ]
        )

    if fallback_markdown:
        message_parts.extend(
            [
                "Attached markdown context follows.",
                "```markdown",
                fallback_markdown,
                "```",
            ]
        )

    message_parts.extend(["User question:", request.message.strip()])

    system_instructions = CHAT_SYSTEM_PROMPT
    if user_context:
        system_instructions = f"{CHAT_SYSTEM_PROMPT}\n\n{user_context}"

    payload: dict[str, Any] = {
        "model": settings.chat_model,
        "instructions": system_instructions,
        "input": [
            {
                "role": "user",
                "content": [{"type": "input_text", "text": "\n\n".join(message_parts)}],
            }
        ],
        "temperature": 0.2,
        "max_output_tokens": 2_000,
        "truncation": "auto",
        "store": True,
    }

    if request.previous_response_id:
        payload["previous_response_id"] = request.previous_response_id

    if vector_store_id:
        tool: dict[str, Any] = {
            "type": "file_search",
            "vector_store_ids": [vector_store_id],
            "max_num_results": settings.kb_max_results,
        }
        filters = _build_file_search_filters(request)
        if filters:
            tool["filters"] = filters
        payload["tools"] = [tool, *SHERLOCK_TOOLS]
    else:
        payload["tools"] = list(SHERLOCK_TOOLS)

    return payload


def _metadata_indexes() -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    records = list_kb_document_records()
    by_uploaded_file_id = {
        str(record["uploaded_file_id"]): record
        for record in records
        if record.get("uploaded_file_id")
    }
    by_filename = {
        Path(str(record["file_path"])).name: record
        for record in records
        if record.get("file_path")
    }
    return by_uploaded_file_id, by_filename


def normalize_chat_citations(
    response: Any,
    *,
    fallback_file_path: str | None = None,
    fallback_cluster_id: str | None = None,
    focused_citations: list[ChatCitation] | None = None,
) -> list[ChatCitation]:
    payload = response.model_dump(exclude_none=True) if hasattr(response, "model_dump") else dict(response)
    by_uploaded_file_id, by_filename = _metadata_indexes()
    citations: list[ChatCitation] = []
    seen: set[tuple[str, str]] = set()

    for item in payload.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") != "output_text":
                continue
            for annotation in content.get("annotations", []):
                annotation_type = str(annotation.get("type") or "")
                if annotation_type == "url_citation":
                    url = str(annotation.get("url") or "")
                    key = ("url", url)
                    if not url or key in seen:
                        continue
                    seen.add(key)
                    citations.append(
                        ChatCitation(
                            id=f"url:{len(citations)}",
                            label=str(annotation.get("title") or url),
                            url=url,
                        )
                    )
                    continue

                if annotation_type not in {"file_citation", "container_file_citation"}:
                    continue

                file_id = str(annotation.get("file_id") or "")
                filename = str(annotation.get("filename") or "")
                record = by_uploaded_file_id.get(file_id) or by_filename.get(filename)
                attributes = _record_attributes(record)
                label = filename or Path(str(record.get("file_path") or "")).name or "Knowledge document"
                key = ("file", file_id or label)
                if key in seen:
                    continue
                seen.add(key)
                citations.append(
                        ChatCitation(
                            id=f"file:{len(citations)}",
                            label=label,
                            file_name=filename or (Path(str(record["file_path"])).name if record and record.get("file_path") else None),
                            file_path=str(record.get("file_path")) if record and record.get("file_path") else None,
                            cluster_id=str(record.get("cluster_id")) if record and record.get("cluster_id") else None,
                            entity_id=str(record.get("entity_id")) if record and record.get("entity_id") else None,
                            source_id=str(record.get("source_id")) if record and record.get("source_id") else None,
                            region_id=str(attributes.get("region_id")) if attributes.get("region_id") else None,
                            country_id=str(attributes.get("country_id")) if attributes.get("country_id") else None,
                        )
                    )

    for citation in focused_citations or []:
        _append_focus_citation(citations, citation)

    if not citations and fallback_file_path:
        citations.append(
            ChatCitation(
                id="fallback:cluster",
                label=Path(fallback_file_path).name,
                file_name=Path(fallback_file_path).name,
                file_path=fallback_file_path,
                cluster_id=fallback_cluster_id,
            )
        )

    return citations


def _stream_once(
    payload: dict[str, Any],
    *,
    fallback_file_path: str | None = None,
    fallback_cluster_id: str | None = None,
    focused_citations: list[ChatCitation] | None = None,
    user_id: str | None = None,
) -> Iterator[str]:
    max_tool_rounds = 5
    current_payload = dict(payload)

    for _round in range(max_tool_rounds + 1):
        with get_azure_client().responses.stream(**current_payload) as stream:
            for event in stream:
                if getattr(event, "type", "") == "response.output_text.delta" and getattr(event, "delta", ""):
                    yield _sse("delta", {"text": event.delta})

            response = stream.get_final_response()

        # Check for function tool calls in output
        tool_calls = [
            item for item in (response.output or [])
            if getattr(item, "type", "") == "function_call"
        ]

        if not tool_calls:
            break  # No tool calls — we're done

        # Execute each tool call and build follow-up input
        tool_results_input = []
        for tc in tool_calls:
            tool_name = getattr(tc, "name", "")
            call_id = getattr(tc, "call_id", "")
            try:
                arguments = json.loads(getattr(tc, "arguments", "{}"))
            except Exception:
                arguments = {}

            yield _sse("delta", {"text": f"\n\n🔍 *Invoking {tool_name}…*\n"})

            result_str = _execute_tool(tool_name, arguments, user_id)

            tool_results_input.append({
                "type": "function_call_output",
                "call_id": call_id,
                "output": result_str,
            })

        # Feed tool results back as a continuation
        current_payload = {
            "model": current_payload["model"],
            "instructions": current_payload.get("instructions", ""),
            "previous_response_id": response.id,
            "input": tool_results_input,
            "tools": current_payload.get("tools", []),
            "temperature": current_payload.get("temperature", 0.2),
            "max_output_tokens": current_payload.get("max_output_tokens", 2_000),
            "truncation": "auto",
            "store": True,
        }

    citations = normalize_chat_citations(
        response,
        fallback_file_path=fallback_file_path,
        fallback_cluster_id=fallback_cluster_id,
        focused_citations=focused_citations,
    )
    chat_response = ChatResponse(
        response_id=str(response.id),
        message=ChatMessage(role="assistant", content=str(getattr(response, "output_text", "") or ""), citations=citations),
    )
    yield _sse("response", chat_response.model_dump(mode="json"))
    yield _sse("done", {"response_id": chat_response.response_id})


def stream_chat_events(request: ChatRequest) -> Iterator[str]:
    message = request.message.strip()
    if not message:
        raise ValueError("A message is required.")

    # --- User memory & onboarding ---
    user_id = request.user_id
    user_context = _build_user_context(user_id)

    # Only onboard if we have a user_id AND the profile lacks any meaningful data
    needs_onboarding = False
    if user_id:
        try:
            existing_memory = get_user_memory(user_id)
            profile = (existing_memory or {}).get("profile") or {}
            already_onboarded = isinstance(profile, dict) and profile.get("onboarded")
            has_profile = isinstance(profile, dict) and any(
                profile.get(k) for k in ("name", "role", "sector", "region")
            )
            needs_onboarding = not already_onboarded and not has_profile
        except Exception:
            needs_onboarding = True

    if needs_onboarding:
        # First conversation ever — send onboarding prompt
        yield _sse("delta", {"text": SHERLOCK_ONBOARDING_PROMPT})
        onboarding_response = ChatResponse(
            response_id="onboarding",
            message=ChatMessage(role="assistant", content=SHERLOCK_ONBOARDING_PROMPT, citations=[]),
        )
        yield _sse("response", onboarding_response.model_dump(mode="json"))
        yield _sse("done", {"response_id": "onboarding"})
        # Mark onboarded=True so the onboarding prompt never fires again
        upsert_user_memory(user_id, "profile", {"onboarded": True})
        return

    effective_scope = request.scope
    if effective_scope == "selected_cluster" and not request.selected_cluster_id:
        effective_scope = "all"
        request = request.model_copy(update={"scope": "all"})

    fallback_markdown = None
    fallback_file_path = None
    focused_context = None
    focused_citations: list[ChatCitation] = []
    records = list_kb_document_records()
    has_documents = count_kb_document_records(include_manifest=True) > 0

    if request.selected_cluster_id:
        existing = any(record.get("cluster_id") == request.selected_cluster_id for record in records)
        if not existing:
            # Don't block chat on a full KB sync; selected-cluster chat can work from fresh markdown.
            fallback_markdown, fallback_file_path = ensure_cluster_markdown(request.selected_cluster_id)
    elif not has_documents:
        raise RuntimeError(
            "Knowledge-base retrieval is not ready for corpus-wide chat yet. Select a cluster or finish the knowledge-base sync first."
        )

    kb_status = get_knowledge_base_status()
    capabilities = get_azure_capabilities()
    vector_store_id = kb_status.get("vector_store_id") if kb_status.get("status") == "ready" else None

    if not vector_store_id:
        if effective_scope != "selected_cluster" or not request.selected_cluster_id:
            raise RuntimeError(
                "Knowledge-base retrieval is not ready for corpus-wide chat yet. Select a cluster or finish the vector-store sync first."
            )
        if fallback_markdown is None or fallback_file_path is None:
            fallback_markdown, fallback_file_path = ensure_cluster_markdown(request.selected_cluster_id)

    if request.selected_cluster_id or request.region_id or request.country_id:
        focused_context, focused_citations = _build_focused_context(request)

    payload = build_chat_payload(
        request,
        vector_store_id=vector_store_id,
        fallback_markdown=fallback_markdown,
        focused_context=focused_context,
        user_context=user_context,
    )
    emitted = False

    try:
        for chunk in _stream_once(
            payload,
            fallback_file_path=fallback_file_path,
            fallback_cluster_id=request.selected_cluster_id,
            focused_citations=focused_citations,
            user_id=user_id,
        ):
            emitted = True
            yield chunk
    except Exception as exc:
        if (
            not emitted
            and capabilities.file_search_supported
            and request.selected_cluster_id
            and effective_scope == "selected_cluster"
        ):
            fallback_markdown, fallback_file_path = ensure_cluster_markdown(request.selected_cluster_id)
            fallback_payload = build_chat_payload(
                request,
                vector_store_id=None,
                fallback_markdown=fallback_markdown,
                focused_context=focused_context,
                user_context=user_context,
            )
            for chunk in _stream_once(
                fallback_payload,
                fallback_file_path=fallback_file_path,
                fallback_cluster_id=request.selected_cluster_id,
                focused_citations=focused_citations,
                user_id=user_id,
            ):
                yield chunk
            return
        raise RuntimeError(str(exc)) from exc


def commit_chat_to_knowledge_base(request: ChatCommitRequest) -> ChatCommitResponse:
    if not request.selected_cluster_id:
        raise ValueError("A selected cluster is required to commit chat to the knowledge base.")

    transcript = _format_chat_transcript(request.messages)
    if not transcript.strip():
        raise ValueError("There is no chat content to commit.")

    company_name = str(request.selected_cluster_name or "Selected cluster")
    event_headline = ""

    prompt = "\n\n".join(
        [
            f"Cluster ID: {request.selected_cluster_id}",
            f"Company: {company_name}",
            f"Headline: {event_headline or 'Unavailable'}",
            "Create a compact markdown note with these sections when relevant:",
            "- What We Learned",
            "- Verified Facts",
            "- Hypotheses And Signals",
            "- Relationship Context",
            "- Suggested Next Steps",
            "- Open Questions",
            "Chat transcript:",
            transcript,
        ]
    )

    response = get_azure_client().responses.create(
        model=settings.chat_model,
        input=[
            {"role": "system", "content": [{"type": "input_text", "text": CHAT_COMMIT_SYSTEM_PROMPT}]},
            {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "ChatCommitSummary",
                "schema": patch_schema_for_strict(ChatCommitSummary.model_json_schema()),
                "strict": True,
            }
        },
    )
    payload = parse_json_output(getattr(response, "output_text", "") or "{}")
    summary = ChatCommitSummary.model_validate(payload)

    committed_at = utcnow()
    note = save_chat_note_record(
        {
            "note_id": uuid.uuid4().hex,
            "cluster_id": request.selected_cluster_id,
            "title": summary.title.strip() or f"Analyst note for {company_name}",
            "summary_markdown": summary.summary_markdown.strip(),
            "source_response_id": request.previous_response_id,
            "source_message_count": len(request.messages),
            "committed_at": committed_at,
            "committed_by": request.committed_by or "app",
        }
    )

    knowledge_base = trigger_knowledge_base_sync(cluster_id=request.selected_cluster_id, full_refresh=False)

    # --- Progressive profiling: extract new facts about the analyst ---
    if request.committed_by and request.committed_by != "app":
        try:
            _extract_profile_updates(request.committed_by, request.messages)
        except Exception:
            pass  # Non-critical — don't block the commit

    return ChatCommitResponse(
        note_id=str(note["note_id"]),
        cluster_id=str(note["cluster_id"]),
        title=str(note["title"]),
        summary_markdown=str(note["summary_markdown"]),
        committed_at=note["committed_at"],
        committed_by=str(note["committed_by"]),
        knowledge_base=knowledge_base,
    )


def _extract_profile_updates(user_id: str, messages: list[ChatMessage]) -> None:
    """Silently extract any new profile facts from the conversation and merge into user memory."""
    transcript = _format_chat_transcript(messages)
    if not transcript.strip():
        return

    prompt = (
        "Extract any new facts about the analyst from this conversation: "
        "sector focus, region, deal preferences, companies they're tracking, "
        "expertise areas, or key deductions. Return JSON matching this schema. "
        "Only include fields where you found new information; use null for others.\n\n"
        f"Transcript:\n{transcript}"
    )

    response = get_azure_client().responses.create(
        model=settings.chat_model,
        input=[
            {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "ProfileExtract",
                "schema": patch_schema_for_strict(ProfileExtract.model_json_schema()),
                "strict": True,
            }
        },
    )
    payload = parse_json_output(getattr(response, "output_text", "") or "{}")
    extract = ProfileExtract.model_validate(payload)

    # Merge with existing profile
    existing_memory = get_user_memory(user_id)
    profile = existing_memory.get("profile", {})
    if not isinstance(profile, dict):
        profile = {}

    if extract.sector and extract.sector.strip():
        profile["sector"] = extract.sector.strip()
    if extract.region and extract.region.strip():
        profile["region"] = extract.region.strip()
    if extract.role and extract.role.strip():
        profile["role"] = extract.role.strip()
    if extract.deal_stages:
        profile["deal_stages"] = list(set(profile.get("deal_stages", []) + extract.deal_stages))
    if extract.active_pursuits:
        existing_pursuits = profile.get("active_pursuits", [])
        profile["active_pursuits"] = list(set(existing_pursuits + extract.active_pursuits))[:20]
    if extract.expertise_areas:
        existing_areas = profile.get("expertise_areas", [])
        profile["expertise_areas"] = list(set(existing_areas + extract.expertise_areas))[:20]
    if extract.key_deductions:
        existing_deductions = profile.get("key_deductions", [])
        profile["key_deductions"] = (existing_deductions + extract.key_deductions)[-20:]

    profile["onboarded"] = True
    upsert_user_memory(user_id, "profile", profile)
