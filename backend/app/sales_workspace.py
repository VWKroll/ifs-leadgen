from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from typing import Any

from pydantic import BaseModel

from .opportunity_data import load_sales_lead_rows
from .pipeline.models import normalize_company_name
from .pipeline.provider import get_provider_client
from .schemas import (
    ClaimOpportunityRequest,
    SalesDashboardItem,
    SalesDashboardMetric,
    SalesDashboardResponse,
    SalesDraftConversationRequest,
    SalesDraftMessage,
    SalesDraftPatchRequest,
    SalesDraftPayload,
    SalesLeadSummary,
    SalesLeadsResponse,
    SalesWorkspaceActorRequest,
    SalesWorkspaceMatchSummary,
    SalesWorkspaceResponse,
    SalesWorkspaceStatusPatchRequest,
)
from .services import get_opportunity_detail
from .settings import settings
from .store import (
    claim_sales_claim_record,
    get_sales_claim_record,
    get_sales_draft_record,
    list_sales_claim_records,
    list_sales_draft_message_records,
    save_sales_claim_record,
    save_sales_draft_message_record,
    save_sales_draft_record,
    utcnow,
)


class SalesDraftGenerationResult(BaseModel):
    assistant_response: str
    draft_payload: SalesDraftPayload


SALES_DRAFT_SYSTEM_PROMPT = """
You are an internal sales workspace assistant preparing a Salesforce-ready prospect draft.

Your job is to turn event intelligence into concise, commercially useful CRM content.
Only include claims that are supported by the supplied opportunity detail, evidence, relationship summary, and user instructions.
Do not fabricate contacts, prior meetings, or closed business history.
Keep notes practical for a salesperson who may push this record into Salesforce immediately after review.

Critical context to use:
- Anchor the draft in the specific trigger event and the entity's relationship to it.
- Use the priority and confidence scores to calibrate urgency and certainty in your language.
- Reference hypothesized services to suggest what advisory angle to lead with.
- Use the outreach angle and engagement rationale to frame the recommended approach.
- For non-direct entities (peer / ownership branches), explain the propagation logic clearly.

Output must be a JSON object matching the SalesDraftGenerationResult schema.
""".strip()

logger = logging.getLogger(__name__)


def _normalize_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if record is None:
        return None
    return {key: value for key, value in record.items()}


def _priority_label(score: float | None) -> str:
    numeric = float(score or 0)
    if numeric >= 80:
        return "High"
    if numeric >= 60:
        return "Medium"
    return "Watch"


def _confidence_label(score: float | None) -> str:
    numeric = float(score or 0)
    if numeric >= 80:
        return "High confidence"
    if numeric >= 60:
        return "Medium confidence"
    return "Needs review"


def _first_text(value: str | None, fallback: str) -> str:
    cleaned = str(value or "").strip()
    return cleaned or fallback


def _json_payload(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_list(value: Any) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except Exception:
        return []
    return [str(item).strip() for item in parsed if str(item).strip()] if isinstance(parsed, list) else []


def _message_records_as_models(records: list[dict[str, Any]]) -> list[SalesDraftMessage]:
    return [
        SalesDraftMessage(
            message_id=str(record.get("message_id") or ""),
            role=str(record.get("role") or "assistant"),
            channel=str(record.get("channel") or "system"),
            content=str(record.get("content") or ""),
            created_at=record.get("created_at") or utcnow(),
        )
        for record in records
    ]


def _entity_for_sales_item(detail: Any, sales_item_id: str) -> dict[str, Any]:
    for entity in list(detail.entities or []):
        if str(entity.get("cluster_entity_id") or "") == sales_item_id:
            return entity
    raise KeyError(sales_item_id)


def _recommendations_for_entity(detail: Any, sales_item_id: str) -> list[dict[str, Any]]:
    return [
        recommendation
        for recommendation in list(detail.recommendations or [])
        if str(recommendation.get("cluster_entity_id") or "") == sales_item_id
    ]


def _build_match_summary(cluster_id: str, sales_item_id: str, company_name: str) -> SalesWorkspaceMatchSummary:
    normalized = normalize_company_name(company_name)
    related_claims = [
        record
        for record in list_sales_claim_records(normalized_company_name=normalized, limit=25)
        if str(record.get("sales_item_id") or "") != sales_item_id
    ]
    if not related_claims:
        return SalesWorkspaceMatchSummary(
            account_name=company_name,
            relationship_summary="No prior local Salesforce push history is recorded for this company yet.",
        )

    last_activity = max(
        (
            record.get("last_activity_at")
            or record.get("last_pushed_at")
            or record.get("claimed_at")
            for record in related_claims
        ),
        default=None,
    )
    active_related = [
        record
        for record in related_claims
        if str(record.get("status") or "") not in {"closed_won", "closed_lost"}
    ]
    first = related_claims[0]
    return SalesWorkspaceMatchSummary(
        account_name=str(first.get("salesforce_account_name") or company_name),
        account_id=first.get("salesforce_account_id"),
        contact_count=max(int(first.get("salesforce_contact_count") or 0), min(len(related_claims) + 1, 6)),
        open_opportunity_count=max(
            int(first.get("salesforce_open_opportunity_count") or 0),
            len(active_related),
        ),
        last_activity_at=last_activity,
        relationship_summary=f"{len(related_claims)} prior claimed or pushed record(s) already exist for this company in the local sales workspace.",
    )


def _build_default_payload(detail: Any, entity: dict[str, Any], owner_name: str, owner_email: str | None) -> SalesDraftPayload:
    cluster = detail.cluster
    sales_item_id = str(entity.get("cluster_entity_id") or "")
    recommendations = _recommendations_for_entity(detail, sales_item_id)
    sources = list(detail.sources or [])

    stakeholder_focus: list[str] = []
    for recommendation in recommendations[:3]:
      for title in _json_list(recommendation.get("recommended_titles_json")):
          if title not in stakeholder_focus:
              stakeholder_focus.append(title)
          if len(stakeholder_focus) >= 6:
              break
      if len(stakeholder_focus) >= 6:
          break

    evidence_bullets = [str(cluster.event_headline or "").strip()] if cluster.event_headline else []
    for value in (
        entity.get("relationship_to_subject"),
        entity.get("rationale"),
        recommendations[0].get("rationale") if recommendations else None,
        cluster.propagation_thesis,
    ):
        text = str(value or "").strip()
        if text and text not in evidence_bullets:
            evidence_bullets.append(text)
        if len(evidence_bullets) >= 5:
            break

    source_urls: list[str] = []
    if cluster.headline_source_url:
        source_urls.append(str(cluster.headline_source_url))
    for source in sources[:4]:
        url = str(source.get("source_url") or "").strip()
        if url and url not in source_urls:
            source_urls.append(url)

    relevant_services = []
    # Include entity-level hypothesized services from recommendations first
    for recommendation in recommendations[:3]:
        for svc in _json_list(recommendation.get("hypothesized_services_json")):
            if svc not in relevant_services:
                relevant_services.append(svc)
    # Fall back to cluster-level service hypotheses
    if not relevant_services:
        for svc in _json_list(getattr(cluster, "service_hypotheses_json", None)):
            if svc not in relevant_services:
                relevant_services.append(svc)
    if cluster.best_route_to_market:
        relevant_services.append(str(cluster.best_route_to_market))
    if cluster.trigger_type:
        relevant_services.append(str(cluster.trigger_type).replace("_", " ").title())
    branch_label = str(entity.get("branch_type") or "").strip()
    if branch_label:
        relevant_services.append(f"{branch_label.title()} opportunity")
    relevant_services = list(dict.fromkeys([item for item in relevant_services if item]))

    company_name = str(entity.get("entity_name") or cluster.subject_company_name)
    role = str(entity.get("commercial_role") or "account team")
    relationship = _first_text(
        entity.get("relationship_to_subject"),
        f"{company_name} is part of the commercial opportunity set surfaced by this event.",
    )
    rationale = _first_text(
        entity.get("rationale"),
        cluster.propagation_thesis or f"The event may create an advisory opening for {company_name}.",
    )

    return SalesDraftPayload(
        company_name=company_name,
        owner_name=owner_name,
        owner_email=owner_email,
        prospect_summary=_first_text(
            entity.get("relationship_to_subject"),
            f"{company_name} surfaced inside the event cluster generated by {cluster.subject_company_name}.",
        ),
        why_now=rationale,
        sales_strategy=f"Anchor outreach on the event involving {cluster.subject_company_name} and explain why {company_name} matters as a {role}. Lead with the most credible advisory angle and tailor the message to the {branch_label or 'current'} relationship path.",
        outreach_angle=f"Reference the trigger affecting {cluster.subject_company_name}, connect it to {company_name}, and open with the strongest evidence-backed reason this company should care now.",
        recommended_next_step=f"Confirm ownership for {company_name}, check whether it is already being worked in CRM, and prepare a first outreach tailored to the {branch_label or 'current'} opportunity path.",
        internal_notes=f"Claimed from the event intelligence workspace for {company_name}. Event context: {cluster.event_headline or 'recent trigger event'}.",
        stakeholder_focus=stakeholder_focus or ["CFO", "General Counsel", "Transformation lead"],
        relevant_services=relevant_services,
        evidence_bullets=evidence_bullets[:5],
        source_urls=source_urls[:5],
        priority_label=_priority_label(entity.get("priority_score") or cluster.opportunity_score or cluster.cluster_priority_score),
        confidence_label=_confidence_label(entity.get("confidence_score") or cluster.cluster_confidence_score),
        salesforce_status="Draft",
    )


def _generation_context(
    detail: Any,
    entity: dict[str, Any],
    match_summary: SalesWorkspaceMatchSummary,
    payload: SalesDraftPayload,
    messages: list[SalesDraftMessage],
) -> str:
    sales_item_id = str(entity.get("cluster_entity_id") or "")
    recommendations = [
        {
            "role_track_type": str(item.get("role_track_type") or ""),
            "rationale": str(item.get("rationale") or ""),
            "recommended_titles": _json_list(item.get("recommended_titles_json")),
            "hypothesized_services": _json_list(item.get("hypothesized_services_json")),
        }
        for item in _recommendations_for_entity(detail, sales_item_id)[:4]
    ]
    sources = [
        {
            "title": str(item.get("source_title") or item.get("source_url") or ""),
            "url": str(item.get("source_url") or ""),
            "publisher": str(item.get("publisher") or ""),
        }
        for item in list(detail.sources or [])[:5]
    ]
    conversation = [
        {
            "role": message.role,
            "channel": message.channel,
            "content": message.content,
        }
        for message in messages[-8:]
    ]
    context = {
        "cluster": detail.cluster.model_dump(mode="json"),
        "focus_entity": {
            **entity,
            "priority_score": float(entity.get("priority_score") or 0),
            "confidence_score": float(entity.get("confidence_score") or 0),
            "relationship_to_subject": str(entity.get("relationship_to_subject") or ""),
            "rationale": str(entity.get("rationale") or ""),
        },
        "recommendations": recommendations,
        "sources": sources,
        "relationship_summary": match_summary.model_dump(mode="json"),
        "current_draft": payload.model_dump(mode="json"),
        "conversation": conversation,
        "engagement_context": {
            "entity_name": str(entity.get("entity_name") or ""),
            "relationship_to_event": str(entity.get("relationship_to_subject") or ""),
            "cluster_summary": str(detail.cluster.event_summary or ""),
            "engagement_rationale": str(entity.get("rationale") or ""),
            "outreach_angle": str(recommendations[0]["rationale"] if recommendations else ""),
            "priority_score": float(entity.get("priority_score") or 0),
            "confidence_score": float(entity.get("confidence_score") or 0),
            "hypothesized_services": recommendations[0].get("hypothesized_services", []) if recommendations else _json_list(detail.cluster.service_hypotheses_json),
        },
    }
    return json.dumps(context, indent=2, default=str)


def _fallback_generation(
    detail: Any,
    entity: dict[str, Any],
    match_summary: SalesWorkspaceMatchSummary,
    payload: SalesDraftPayload,
    messages: list[SalesDraftMessage],
) -> SalesDraftGenerationResult:
    latest_instruction = next((message.content.strip() for message in reversed(messages) if message.role == "user" and message.content.strip()), "")
    next_payload = payload.model_copy(deep=True)
    if latest_instruction:
        next_payload.internal_notes = f"{payload.internal_notes}\n\nLatest seller instruction: {latest_instruction}".strip()
        next_payload.sales_strategy = f"{payload.sales_strategy}\n\nSeller emphasis: {latest_instruction}".strip()
    next_payload.why_now = (
        f"{payload.why_now}\n\nRelationship context: {match_summary.relationship_summary or 'No prior CRM history recorded.'}"
    ).strip()
    next_payload.outreach_angle = (
        f"{payload.outreach_angle}\n\nFocus company: {str(entity.get('entity_name') or payload.company_name)}."
    ).strip()
    next_payload.salesforce_status = "Draft reviewed"
    return SalesDraftGenerationResult(
        assistant_response=(
            "I refreshed the Salesforce draft with the latest instructions. "
            "Review the strategy, why-now framing, and internal notes before pushing."
        ),
        draft_payload=next_payload,
    )


def _generate_draft_payload(
    detail: Any,
    entity: dict[str, Any],
    match_summary: SalesWorkspaceMatchSummary,
    payload: SalesDraftPayload,
    messages: list[SalesDraftMessage],
) -> SalesDraftGenerationResult:
    if not settings.azure_openai_api_key or not settings.azure_openai_endpoint:
        return _fallback_generation(detail, entity, match_summary, payload, messages)

    start_time = time.monotonic()
    try:
        client = get_provider_client()
        result = client.call_json_model(
            system_prompt=SALES_DRAFT_SYSTEM_PROMPT,
            user_prompt=_generation_context(detail, entity, match_summary, payload, messages),
            response_model=SalesDraftGenerationResult,
            model=settings.chat_model,
            temperature=0.2,
            max_retries=2,
        )
        latency_ms = (time.monotonic() - start_time) * 1000
        logger.info(
            "draft_generation entity=%s latency_ms=%.0f response_len=%d success=true",
            entity.get("entity_name", "unknown"),
            latency_ms,
            len(result.assistant_response),
        )
        return result
    except Exception as exc:
        latency_ms = (time.monotonic() - start_time) * 1000
        logger.warning(
            "draft_generation entity=%s latency_ms=%.0f success=false error=%s",
            entity.get("entity_name", "unknown"),
            latency_ms,
            str(exc),
        )
        return _fallback_generation(detail, entity, match_summary, payload, messages)


def _workspace_from_records(sales_item_id: str, claim_record: dict[str, Any], draft_record: dict[str, Any]) -> SalesWorkspaceResponse:
    messages = _message_records_as_models(list_sales_draft_message_records(str(draft_record.get("draft_id") or "")))
    payload_dict = _json_payload(draft_record.get("draft_payload_json"))
    payload = SalesDraftPayload.model_validate(payload_dict)
    match_summary = SalesWorkspaceMatchSummary(
        account_name=claim_record.get("salesforce_account_name"),
        account_id=claim_record.get("salesforce_account_id"),
        contact_count=int(claim_record.get("salesforce_contact_count") or 0),
        open_opportunity_count=int(claim_record.get("salesforce_open_opportunity_count") or 0),
        last_activity_at=claim_record.get("last_activity_at"),
        relationship_summary=claim_record.get("last_activity_note") or None,
    )
    if not match_summary.relationship_summary:
        match_summary = _build_match_summary(
            str(claim_record.get("cluster_id") or ""),
            sales_item_id,
            str(claim_record.get("subject_company_name") or payload.company_name),
        )

    return SalesWorkspaceResponse(
        claim_id=str(claim_record.get("claim_id") or ""),
        cluster_id=str(claim_record.get("cluster_id") or ""),
        sales_item_id=sales_item_id,
        cluster_entity_id=claim_record.get("cluster_entity_id"),
        event_subject_company_name=str(claim_record.get("event_subject_company_name") or ""),
        event_headline=claim_record.get("event_headline"),
        subject_company_name=str(claim_record.get("subject_company_name") or payload.company_name),
        branch_type=claim_record.get("branch_type"),
        entity_type=claim_record.get("entity_type"),
        claimed_by_user_id=str(claim_record.get("claimed_by_user_id") or ""),
        status=str(claim_record.get("status") or "claimed"),
        claimed_by_name=str(claim_record.get("claimed_by_name") or payload.owner_name),
        claimed_by_email=claim_record.get("claimed_by_email"),
        claimed_at=claim_record.get("claimed_at") or utcnow(),
        updated_at=claim_record.get("updated_at") or utcnow(),
        salesforce_stage=str(claim_record.get("salesforce_stage") or "Claimed"),
        salesforce_owner_name=str(claim_record.get("salesforce_owner_name") or payload.owner_name),
        salesforce_owner_id=claim_record.get("salesforce_owner_id"),
        salesforce_record_type=str(claim_record.get("salesforce_record_type") or "prospect"),
        salesforce_record_id=claim_record.get("salesforce_record_id"),
        last_pushed_at=claim_record.get("last_pushed_at"),
        next_step=claim_record.get("next_step"),
        last_activity_note=claim_record.get("last_activity_note"),
        draft_id=str(draft_record.get("draft_id") or ""),
        draft_payload=payload,
        draft_updated_at=draft_record.get("updated_at"),
        draft_status=str(draft_record.get("draft_status") or "drafting"),
        match_summary=match_summary,
        messages=messages,
    )


def get_sales_workspace(cluster_id: str, sales_item_id: str) -> SalesWorkspaceResponse | None:
    claim_record = _normalize_record(get_sales_claim_record(sales_item_id))
    draft_record = _normalize_record(get_sales_draft_record(sales_item_id))
    if claim_record is None or draft_record is None:
        return None
    if str(claim_record.get("cluster_id") or "") != cluster_id:
        return None
    return _workspace_from_records(sales_item_id, claim_record, draft_record)


def _require_workspace_owner(workspace: SalesWorkspaceResponse, actor_user_id: str) -> None:
    if workspace.claimed_by_user_id != actor_user_id:
        raise PermissionError("Only the user who claimed this opportunity can modify it.")


def claim_opportunity(cluster_id: str, request: ClaimOpportunityRequest) -> SalesWorkspaceResponse:
    existing = get_sales_workspace(cluster_id, request.sales_item_id)
    if existing:
        if existing.claimed_by_user_id != request.claimed_by_user_id:
            raise ValueError(f"This opportunity is already claimed by {existing.claimed_by_name}.")
        return existing

    detail = get_opportunity_detail(cluster_id)
    entity = _entity_for_sales_item(detail, request.sales_item_id)
    claim_id = uuid.uuid4().hex
    draft_id = uuid.uuid4().hex
    owner_id = request.claimed_by_email or request.claimed_by_name
    company_name = str(entity.get("entity_name") or detail.cluster.subject_company_name)
    match_summary = _build_match_summary(cluster_id, request.sales_item_id, company_name)
    claimed_record = claim_sales_claim_record(
        {
            "claim_id": claim_id,
            "cluster_id": cluster_id,
            "sales_item_id": request.sales_item_id,
            "cluster_entity_id": request.sales_item_id,
            "event_subject_company_name": detail.cluster.subject_company_name,
            "event_headline": detail.cluster.event_headline,
            "subject_company_name": company_name,
            "branch_type": entity.get("branch_type"),
            "entity_type": entity.get("entity_type"),
            "claimed_by_user_id": request.claimed_by_user_id,
            "claimed_by_name": request.claimed_by_name,
            "claimed_by_email": request.claimed_by_email,
            "status": "drafting",
            "salesforce_stage": "Drafting",
            "salesforce_record_type": "prospect",
            "salesforce_owner_name": request.claimed_by_name,
            "salesforce_owner_id": owner_id,
            "salesforce_account_name": match_summary.account_name,
            "salesforce_account_id": match_summary.account_id,
            "salesforce_contact_count": match_summary.contact_count,
            "salesforce_open_opportunity_count": match_summary.open_opportunity_count,
            "last_activity_note": match_summary.relationship_summary,
            "last_activity_at": match_summary.last_activity_at,
            "draft_id": draft_id,
        }
    )
    if str(claimed_record.get("claimed_by_user_id") or "") != request.claimed_by_user_id:
        raise ValueError(f"This opportunity is already claimed by {claimed_record.get('claimed_by_name') or 'another user'}.")

    base_payload = _build_default_payload(detail, entity, request.claimed_by_name, request.claimed_by_email)

    # ── Save the stub draft immediately so we can return inside the platform
    # ── HTTP timeout (30 s). AI generation runs in a background thread.
    system_message = SalesDraftMessage(
        message_id=uuid.uuid4().hex,
        role="system",
        channel="system",
        content=f"Opportunity claimed for {company_name}. Drafting a Salesforce-ready prospect record.",
        created_at=utcnow(),
    )
    seed_messages: list[SalesDraftMessage] = [system_message]
    if request.notes and request.notes.strip():
        seed_messages.append(
            SalesDraftMessage(
                message_id=uuid.uuid4().hex,
                role="user",
                channel="chat",
                content=request.notes.strip(),
                created_at=utcnow(),
            )
        )

    save_sales_draft_record(
        {
            "draft_id": draft_id,
            "claim_id": claim_id,
            "cluster_id": cluster_id,
            "sales_item_id": request.sales_item_id,
            "draft_payload_json": base_payload.model_dump_json(),
            "draft_status": "generating",
        }
    )
    for msg in seed_messages:
        save_sales_draft_message_record(
            {
                "message_id": msg.message_id,
                "draft_id": draft_id,
                "cluster_id": cluster_id,
                "sales_item_id": request.sales_item_id,
                "role": msg.role,
                "channel": msg.channel,
                "content": msg.content,
                "created_at": msg.created_at,
            }
        )

    # ── Background thread: generate AI draft and update records ──
    _snapshot_claimed = dict(claimed_record)

    def _background_generate() -> None:
        try:
            generated = _generate_draft_payload(detail, entity, match_summary, base_payload, seed_messages)
            save_sales_draft_record(
                {
                    "draft_id": draft_id,
                    "claim_id": claim_id,
                    "cluster_id": cluster_id,
                    "sales_item_id": request.sales_item_id,
                    "draft_payload_json": generated.draft_payload.model_dump_json(),
                    "draft_status": "ready_to_push",
                }
            )
            save_sales_draft_message_record(
                {
                    "message_id": uuid.uuid4().hex,
                    "draft_id": draft_id,
                    "cluster_id": cluster_id,
                    "sales_item_id": request.sales_item_id,
                    "role": "assistant",
                    "channel": "system",
                    "content": generated.assistant_response,
                    "created_at": utcnow(),
                }
            )
            save_sales_claim_record(
                {
                    **_snapshot_claimed,
                    "status": "drafting",
                    "salesforce_stage": "Drafting",
                    "next_step": generated.draft_payload.recommended_next_step,
                }
            )
            logger.info("async_draft_generation completed cluster_id=%s sales_item_id=%s", cluster_id, request.sales_item_id)
        except Exception as exc:
            logger.error("async_draft_generation failed cluster_id=%s sales_item_id=%s error=%s", cluster_id, request.sales_item_id, exc)
            # Flip status to ready_to_push with the base payload so the user can still edit
            try:
                save_sales_draft_record(
                    {
                        "draft_id": draft_id,
                        "claim_id": claim_id,
                        "cluster_id": cluster_id,
                        "sales_item_id": request.sales_item_id,
                        "draft_payload_json": base_payload.model_dump_json(),
                        "draft_status": "ready_to_push",
                    }
                )
            except Exception:
                pass

    threading.Thread(target=_background_generate, daemon=True).start()

    workspace = get_sales_workspace(cluster_id, request.sales_item_id)
    if workspace is None:
        raise RuntimeError("Unable to create sales workspace.")
    return workspace


def update_sales_draft(cluster_id: str, sales_item_id: str, request: SalesDraftPatchRequest) -> SalesWorkspaceResponse:
    workspace = get_sales_workspace(cluster_id, sales_item_id)
    if workspace is None:
        raise KeyError(sales_item_id)
    _require_workspace_owner(workspace, request.actor_user_id)
    save_sales_draft_record(
        {
            "draft_id": workspace.draft_id,
            "claim_id": workspace.claim_id,
            "cluster_id": cluster_id,
            "sales_item_id": sales_item_id,
            "draft_payload_json": request.draft_payload.model_dump_json(),
            "draft_status": "ready_to_push",
        }
    )
    save_sales_claim_record(
        {
            "claim_id": workspace.claim_id,
            "cluster_id": cluster_id,
            "sales_item_id": sales_item_id,
            "cluster_entity_id": workspace.cluster_entity_id,
            "event_subject_company_name": workspace.event_subject_company_name,
            "event_headline": workspace.event_headline,
            "subject_company_name": workspace.subject_company_name,
            "branch_type": workspace.branch_type,
            "entity_type": workspace.entity_type,
            "claimed_by_user_id": workspace.claimed_by_user_id,
            "claimed_by_name": workspace.claimed_by_name,
            "claimed_by_email": workspace.claimed_by_email,
            "claimed_at": workspace.claimed_at,
            "status": "ready_to_push",
            "salesforce_stage": workspace.salesforce_stage,
            "salesforce_record_type": workspace.salesforce_record_type,
            "salesforce_record_id": workspace.salesforce_record_id,
            "salesforce_owner_name": workspace.salesforce_owner_name,
            "salesforce_owner_id": workspace.salesforce_owner_id,
            "salesforce_account_name": workspace.match_summary.account_name,
            "salesforce_account_id": workspace.match_summary.account_id,
            "salesforce_contact_count": workspace.match_summary.contact_count,
            "salesforce_open_opportunity_count": workspace.match_summary.open_opportunity_count,
            "last_activity_note": workspace.last_activity_note,
            "last_activity_at": workspace.match_summary.last_activity_at,
            "draft_id": workspace.draft_id,
            "next_step": request.draft_payload.recommended_next_step,
        }
    )
    return get_sales_workspace(cluster_id, sales_item_id) or workspace


def send_sales_draft_message(cluster_id: str, sales_item_id: str, request: SalesDraftConversationRequest) -> SalesWorkspaceResponse:
    workspace = get_sales_workspace(cluster_id, sales_item_id)
    if workspace is None:
        raise KeyError(sales_item_id)
    _require_workspace_owner(workspace, request.actor_user_id)

    user_message = SalesDraftMessage(
        message_id=uuid.uuid4().hex,
        role="user",
        channel=request.channel,
        content=request.message.strip(),
        created_at=utcnow(),
    )
    save_sales_draft_message_record(
        {
            "message_id": user_message.message_id,
            "draft_id": workspace.draft_id,
            "cluster_id": cluster_id,
            "sales_item_id": sales_item_id,
            "role": user_message.role,
            "channel": user_message.channel,
            "content": user_message.content,
            "created_at": user_message.created_at,
        }
    )

    detail = get_opportunity_detail(cluster_id)
    entity = _entity_for_sales_item(detail, sales_item_id)
    match_summary = workspace.match_summary
    messages = workspace.messages + [user_message]
    generated = _generate_draft_payload(detail, entity, match_summary, workspace.draft_payload, messages)
    assistant_message = SalesDraftMessage(
        message_id=uuid.uuid4().hex,
        role="assistant",
        channel="chat",
        content=generated.assistant_response,
        created_at=utcnow(),
    )
    save_sales_draft_record(
        {
            "draft_id": workspace.draft_id,
            "claim_id": workspace.claim_id,
            "cluster_id": cluster_id,
            "sales_item_id": sales_item_id,
            "draft_payload_json": generated.draft_payload.model_dump_json(),
            "draft_status": "ready_to_push",
        }
    )
    save_sales_draft_message_record(
        {
            "message_id": assistant_message.message_id,
            "draft_id": workspace.draft_id,
            "cluster_id": cluster_id,
            "sales_item_id": sales_item_id,
            "role": assistant_message.role,
            "channel": assistant_message.channel,
            "content": assistant_message.content,
            "created_at": assistant_message.created_at,
        }
    )
    save_sales_claim_record(
        {
            "claim_id": workspace.claim_id,
            "cluster_id": cluster_id,
            "sales_item_id": sales_item_id,
            "cluster_entity_id": workspace.cluster_entity_id,
            "event_subject_company_name": workspace.event_subject_company_name,
            "event_headline": workspace.event_headline,
            "subject_company_name": workspace.subject_company_name,
            "branch_type": workspace.branch_type,
            "entity_type": workspace.entity_type,
            "claimed_by_user_id": workspace.claimed_by_user_id,
            "claimed_by_name": workspace.claimed_by_name,
            "claimed_by_email": workspace.claimed_by_email,
            "claimed_at": workspace.claimed_at,
            "status": "ready_to_push",
            "salesforce_stage": "Drafting",
            "salesforce_record_type": workspace.salesforce_record_type,
            "salesforce_record_id": workspace.salesforce_record_id,
            "salesforce_owner_name": workspace.salesforce_owner_name,
            "salesforce_owner_id": workspace.salesforce_owner_id,
            "salesforce_account_name": workspace.match_summary.account_name,
            "salesforce_account_id": workspace.match_summary.account_id,
            "salesforce_contact_count": workspace.match_summary.contact_count,
            "salesforce_open_opportunity_count": workspace.match_summary.open_opportunity_count,
            "last_activity_note": workspace.last_activity_note,
            "last_activity_at": workspace.match_summary.last_activity_at,
            "draft_id": workspace.draft_id,
            "next_step": generated.draft_payload.recommended_next_step,
        }
    )
    return get_sales_workspace(cluster_id, sales_item_id) or workspace


def push_sales_draft(cluster_id: str, sales_item_id: str, request: SalesWorkspaceActorRequest) -> SalesWorkspaceResponse:
    workspace = get_sales_workspace(cluster_id, sales_item_id)
    if workspace is None:
        raise KeyError(sales_item_id)
    _require_workspace_owner(workspace, request.actor_user_id)

    push_time = utcnow()
    salesforce_record_id = workspace.salesforce_record_id or f"SF-{workspace.claim_id[:8].upper()}"
    save_sales_draft_record(
        {
            "draft_id": workspace.draft_id,
            "claim_id": workspace.claim_id,
            "cluster_id": cluster_id,
            "sales_item_id": sales_item_id,
            "draft_payload_json": workspace.draft_payload.model_copy(update={"salesforce_status": "Pushed"}).model_dump_json(),
            "draft_status": "pushed",
            "last_generated_at": workspace.draft_updated_at or push_time,
            "last_pushed_at": push_time,
            "pushed_by_name": workspace.claimed_by_name,
        }
    )
    save_sales_claim_record(
        {
            "claim_id": workspace.claim_id,
            "cluster_id": cluster_id,
            "sales_item_id": sales_item_id,
            "cluster_entity_id": workspace.cluster_entity_id,
            "event_subject_company_name": workspace.event_subject_company_name,
            "event_headline": workspace.event_headline,
            "subject_company_name": workspace.subject_company_name,
            "branch_type": workspace.branch_type,
            "entity_type": workspace.entity_type,
            "claimed_by_user_id": workspace.claimed_by_user_id,
            "claimed_by_name": workspace.claimed_by_name,
            "claimed_by_email": workspace.claimed_by_email,
            "claimed_at": workspace.claimed_at,
            "status": "pushed_to_salesforce",
            "salesforce_stage": "Pushed to Salesforce",
            "salesforce_record_type": workspace.salesforce_record_type,
            "salesforce_record_id": salesforce_record_id,
            "salesforce_owner_name": workspace.salesforce_owner_name,
            "salesforce_owner_id": workspace.salesforce_owner_id,
            "salesforce_account_name": workspace.match_summary.account_name,
            "salesforce_account_id": workspace.match_summary.account_id,
            "salesforce_contact_count": workspace.match_summary.contact_count,
            "salesforce_open_opportunity_count": workspace.match_summary.open_opportunity_count,
            "last_activity_note": "Draft approved and pushed from the event intelligence workspace.",
            "last_activity_at": push_time,
            "last_pushed_at": push_time,
            "draft_id": workspace.draft_id,
            "next_step": workspace.draft_payload.recommended_next_step,
        }
    )
    save_sales_draft_message_record(
        {
            "message_id": uuid.uuid4().hex,
            "draft_id": workspace.draft_id,
            "cluster_id": cluster_id,
            "sales_item_id": sales_item_id,
            "role": "system",
            "channel": "system",
            "content": f"Draft pushed to Salesforce as {salesforce_record_id}.",
            "created_at": push_time,
        }
    )
    return get_sales_workspace(cluster_id, sales_item_id) or workspace


def update_sales_claim_status(cluster_id: str, sales_item_id: str, request: SalesWorkspaceStatusPatchRequest) -> SalesWorkspaceResponse:
    workspace = get_sales_workspace(cluster_id, sales_item_id)
    if workspace is None:
        raise KeyError(sales_item_id)
    _require_workspace_owner(workspace, request.actor_user_id)

    save_sales_claim_record(
        {
            "claim_id": workspace.claim_id,
            "cluster_id": cluster_id,
            "sales_item_id": sales_item_id,
            "cluster_entity_id": workspace.cluster_entity_id,
            "event_subject_company_name": workspace.event_subject_company_name,
            "event_headline": workspace.event_headline,
            "subject_company_name": workspace.subject_company_name,
            "branch_type": workspace.branch_type,
            "entity_type": workspace.entity_type,
            "claimed_by_user_id": workspace.claimed_by_user_id,
            "claimed_by_name": workspace.claimed_by_name,
            "claimed_by_email": workspace.claimed_by_email,
            "claimed_at": workspace.claimed_at,
            "status": request.status,
            "salesforce_stage": request.salesforce_stage or workspace.salesforce_stage,
            "salesforce_record_type": workspace.salesforce_record_type,
            "salesforce_record_id": workspace.salesforce_record_id,
            "salesforce_owner_name": workspace.salesforce_owner_name,
            "salesforce_owner_id": workspace.salesforce_owner_id,
            "salesforce_account_name": workspace.match_summary.account_name,
            "salesforce_account_id": workspace.match_summary.account_id,
            "salesforce_contact_count": workspace.match_summary.contact_count,
            "salesforce_open_opportunity_count": workspace.match_summary.open_opportunity_count,
            "last_activity_note": request.last_activity_note or workspace.last_activity_note,
            "last_activity_at": utcnow(),
            "last_pushed_at": workspace.last_pushed_at,
            "draft_id": workspace.draft_id,
            "next_step": request.next_step or workspace.next_step,
        }
    )
    save_sales_draft_message_record(
        {
            "message_id": uuid.uuid4().hex,
            "draft_id": workspace.draft_id,
            "cluster_id": cluster_id,
            "sales_item_id": sales_item_id,
            "role": "system",
            "channel": "system",
            "content": f"Status updated to {request.status}.",
            "created_at": utcnow(),
        }
    )
    return get_sales_workspace(cluster_id, sales_item_id) or workspace


def get_sales_dashboard() -> SalesDashboardResponse:
    claims = list_sales_claim_records(limit=500)
    lead_rows, _ = load_sales_lead_rows()
    lead_map = {
        str(row.get("sales_item_id") or ""): row
        for row in lead_rows.to_dict(orient="records")
    }
    items = [
        SalesDashboardItem(
            claim_id=str(record.get("claim_id") or ""),
            cluster_id=str(record.get("cluster_id") or ""),
            sales_item_id=str(record.get("sales_item_id") or ""),
            cluster_entity_id=record.get("cluster_entity_id"),
            event_subject_company_name=str(record.get("event_subject_company_name") or ""),
            event_headline=record.get("event_headline"),
            subject_company_name=str(record.get("subject_company_name") or "Unknown company"),
            branch_type=record.get("branch_type"),
            entity_type=record.get("entity_type"),
            claimed_by_user_id=record.get("claimed_by_user_id"),
            claimed_by_name=str(record.get("claimed_by_name") or "Unknown owner"),
            claimed_at=record.get("claimed_at") or utcnow(),
            updated_at=record.get("updated_at") or utcnow(),
            status=str(record.get("status") or "claimed"),
            salesforce_stage=str(record.get("salesforce_stage") or "Claimed"),
            salesforce_owner_name=str(record.get("salesforce_owner_name") or record.get("claimed_by_name") or "Unknown owner"),
            salesforce_record_id=record.get("salesforce_record_id"),
            last_pushed_at=record.get("last_pushed_at"),
            next_step=record.get("next_step"),
            last_activity_note=record.get("last_activity_note"),
            opportunity_score=lead_map.get(str(record.get("sales_item_id") or ""), {}).get("opportunity_score"),
        )
        for record in claims
    ]
    metrics = [
        SalesDashboardMetric(label="Claimed", value=len(items), tone="blue"),
        SalesDashboardMetric(label="Ready To Push", value=sum(item.status == "ready_to_push" for item in items), tone="amber"),
        SalesDashboardMetric(label="Pushed", value=sum(item.status == "pushed_to_salesforce" for item in items), tone="green"),
        SalesDashboardMetric(label="Open Pipeline", value=sum(item.status in {"working", "qualified", "opportunity_created"} for item in items), tone="blue"),
        SalesDashboardMetric(label="Closed Won", value=sum(item.status == "closed_won" for item in items), tone="green"),
    ]
    return SalesDashboardResponse(metrics=metrics, items=items)


def get_sales_leads(*, page: int = 1, page_size: int = 100, sort_by: str = "newest_event") -> SalesLeadsResponse:
    lead_rows, total_items = load_sales_lead_rows(page=page, page_size=page_size, sort_by=sort_by)
    claims = {
        str(record.get("sales_item_id") or ""): record
        for record in list_sales_claim_records(limit=1000)
    }
    items = []
    for row in lead_rows.to_dict(orient="records"):
        sales_item_id = str(row.get("sales_item_id") or "")
        claim = claims.get(sales_item_id)
        items.append(
            SalesLeadSummary(
                sales_item_id=sales_item_id,
                cluster_id=str(row.get("cluster_id") or ""),
                cluster_entity_id=row.get("cluster_entity_id"),
                event_subject_company_name=str(row.get("event_subject_company_name") or ""),
                event_headline=row.get("event_headline"),
                event_date=row.get("event_date"),
                trigger_type=row.get("trigger_type"),
                subject_company_name=str(row.get("subject_company_name") or "Unknown company"),
                subject_country=row.get("subject_country"),
                subject_region=row.get("subject_region"),
                branch_type=row.get("branch_type"),
                entity_type=row.get("entity_type"),
                relationship_to_subject=row.get("relationship_to_subject"),
                commercial_role=row.get("commercial_role"),
                rationale=row.get("rationale"),
                opportunity_score=row.get("opportunity_score"),
                confidence_score=row.get("confidence_score"),
                event_priority_score=row.get("event_priority_score"),
                event_confidence_score=row.get("event_confidence_score"),
                claim_id=claim.get("claim_id") if claim else None,
                claimed_by_user_id=claim.get("claimed_by_user_id") if claim else None,
                claimed_by_name=claim.get("claimed_by_name") if claim else None,
                status=str(claim.get("status")) if claim and claim.get("status") else None,
                salesforce_stage=str(claim.get("salesforce_stage")) if claim and claim.get("salesforce_stage") else None,
                salesforce_owner_name=claim.get("salesforce_owner_name") if claim else None,
                updated_at=claim.get("updated_at") if claim else None,
            )
        )
    safe_page_size = max(min(page_size, 250), 1)
    safe_page = max(page, 1)
    total_pages = (total_items + safe_page_size - 1) // safe_page_size if total_items else 0
    return SalesLeadsResponse(
        items=items,
        page=safe_page,
        page_size=safe_page_size,
        total_items=total_items,
        total_pages=total_pages,
        sort_by=sort_by,
    )
