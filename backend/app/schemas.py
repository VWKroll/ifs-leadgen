from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class OpportunitySummary(BaseModel):
    cluster_id: str
    subject_company_name: str
    subject_country: str | None = None
    subject_region: str | None = None
    trigger_type: str | None = None
    trigger_subtype: str | None = None
    event_date: str | None = None
    cluster_created_at: str | None = None
    event_headline: str | None = None
    event_summary: str | None = None
    cluster_priority_score: float | None = None
    cluster_confidence_score: float | None = None
    opportunity_score: float | None = None
    entity_count: int | None = None
    source_count: int | None = None
    best_route_to_market: str | None = None
    propagation_thesis: str | None = None
    service_hypotheses_json: str | None = None
    headline_source_url: str | None = None
    subject_state: str | None = None
    subject_city: str | None = None
    subject_address: str | None = None
    subject_latitude: float | None = None
    subject_longitude: float | None = None


class GraphNode(BaseModel):
    id: str
    type: Literal["cluster", "company", "role_track"]
    subtype: str
    label: str
    entity_id: str | None = None
    branch_type: str | None = None
    score: float | None = None
    confidence_score: float | None = None
    priority_score: float | None = None
    route_to_market: str | None = None
    detail: dict[str, Any]


class GraphEdge(BaseModel):
    id: str
    source: str
    target: str
    type: str
    branch_type: str | None = None
    label: str | None = None
    weight: float | None = None
    rationale: str | None = None


class OpportunityDetail(BaseModel):
    cluster: OpportunitySummary
    graph_nodes: list[GraphNode]
    graph_edges: list[GraphEdge]
    entities: list[dict[str, Any]]
    recommendations: list[dict[str, Any]]
    sources: list[dict[str, Any]]


class OpportunitiesResponse(BaseModel):
    items: list[OpportunitySummary]


class MapMarker(BaseModel):
    cluster_id: str
    label: str
    country: str | None = None
    state: str | None = None
    city: str | None = None
    address: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    region: str | None = None
    trigger_type: str | None = None
    cluster_priority_score: float | None = None
    cluster_confidence_score: float | None = None
    opportunity_score: float | None = None


class MapResponse(BaseModel):
    items: list[MapMarker]


class KnowledgeGraphDistributionItem(BaseModel):
    label: str
    count: int
    trigger_type: str | None = None
    tone: str = "neutral"


class KnowledgeGraphEventSummary(BaseModel):
    cluster_id: str
    subject_company_name: str
    subject_country: str | None = None
    subject_region: str | None = None
    trigger_type: str | None = None
    event_date: str | None = None
    event_summary: str | None = None
    opportunity_score: float | None = None
    cluster_confidence_score: float | None = None
    headline_source_url: str | None = None


class KnowledgeGraphCountrySummary(BaseModel):
    country_id: str
    label: str
    region_id: str
    narrative: str
    event_count: int
    company_count: int
    average_opportunity: float
    average_confidence: float
    dominant_triggers: list[KnowledgeGraphDistributionItem]
    top_companies: list[str]
    events: list[KnowledgeGraphEventSummary]


class KnowledgeGraphRegionSummary(BaseModel):
    region_id: str
    label: str
    narrative: str
    event_count: int
    country_count: int
    company_count: int
    average_opportunity: float
    average_confidence: float
    dominant_triggers: list[KnowledgeGraphDistributionItem]
    countries: list[KnowledgeGraphCountrySummary]


class KnowledgeGraphResponse(BaseModel):
    generated_at: datetime
    region_count: int
    country_count: int
    event_count: int
    regions: list[KnowledgeGraphRegionSummary]


class ProviderHealthResponse(BaseModel):
    provider_name: str
    configured: bool
    status: str
    message: str


class RunnerHealthResponse(BaseModel):
    configured: bool
    runner_type: str
    message: str


class KnowledgeBaseStatus(BaseModel):
    status: str
    last_synced_at: datetime | None = None
    document_count: int = 0
    cluster_document_count: int = 0
    entity_document_count: int = 0
    source_document_count: int = 0
    region_document_count: int = 0
    country_document_count: int = 0
    duplicate_candidate_count: int = 0
    stale_local_file_count: int = 0
    cleanup_removed_documents: int = 0
    cleanup_removed_files: int = 0
    vector_store_id: str | None = None
    last_error: str | None = None


class KnowledgeBaseSyncRequest(BaseModel):
    cluster_id: str | None = None
    full_refresh: bool = True


class KnowledgeBaseCleanupRequest(BaseModel):
    mode: Literal["off", "dedupe", "aggressive"] | None = None


class ChatCommitRequest(BaseModel):
    selected_cluster_id: str
    selected_cluster_name: str | None = None
    messages: list["ChatMessage"]
    previous_response_id: str | None = None
    committed_by: str | None = "app"


class ChatCommitResponse(BaseModel):
    note_id: str
    cluster_id: str
    title: str
    summary_markdown: str
    committed_at: datetime
    committed_by: str
    knowledge_base: KnowledgeBaseStatus


class PipelineSettingsResponse(BaseModel):
    settings_id: str
    schedule_enabled: bool
    schedule_interval_hours: int
    target_region: str
    recency_days: int
    dedup_days: int
    max_peers: int
    max_ownership_nodes: int
    generation_runner: str
    databricks_job_id: int | None = None
    openai_model: str
    chat_model: str
    provider_name: str
    kb_max_results: int
    kb_cleanup_mode: Literal["off", "dedupe", "aggressive"]
    kb_cleanup_on_sync: bool
    kb_document_retention_days: int
    updated_at: datetime | None = None
    updated_by: str | None = None
    last_successful_run_id: str | None = None
    last_successful_run_at: datetime | None = None
    next_scheduled_run: datetime | None = None
    provider: ProviderHealthResponse
    job: RunnerHealthResponse
    knowledge_base: KnowledgeBaseStatus


class PipelineSettingsPatchRequest(BaseModel):
    schedule_enabled: bool | None = None
    schedule_interval_hours: int | None = None
    target_region: str | None = None
    recency_days: int | None = None
    dedup_days: int | None = None
    max_peers: int | None = None
    max_ownership_nodes: int | None = None
    generation_runner: Literal["local", "job"] | None = None
    openai_model: str | None = None
    kb_max_results: int | None = None
    kb_cleanup_mode: Literal["off", "dedupe", "aggressive"] | None = None
    kb_cleanup_on_sync: bool | None = None
    kb_document_retention_days: int | None = None


class GenerationRunResponse(BaseModel):
    app_run_id: str
    trigger_source: str
    requested_by: str
    research_mode: Literal["region", "company"] | None = None
    research_target: str | None = None
    target_region: str | None = None
    company_name: str | None = None
    runner_type: str
    status: str
    requested_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    updated_at: datetime | None = None
    created_cluster_id: str | None = None
    duplicate_skipped: bool = False
    error_message: str | None = None
    step_statuses: dict[str, str] = Field(default_factory=dict)
    databricks_job_id: int | None = None
    databricks_run_id: int | None = None
    job_url: str | None = None


class GenerationRunsResponse(BaseModel):
    items: list[GenerationRunResponse]


class CreateGenerationRunRequest(BaseModel):
    requested_by: str | None = "app"
    research_mode: Literal["region", "company"] = "region"
    target_region: str | None = None
    company_name: str | None = None


class ChatCitation(BaseModel):
    id: str
    label: str
    file_name: str | None = None
    file_path: str | None = None
    cluster_id: str | None = None
    url: str | None = None
    entity_id: str | None = None
    source_id: str | None = None
    graph_node_id: str | None = None
    region_id: str | None = None
    country_id: str | None = None


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str
    citations: list[ChatCitation] = Field(default_factory=list)


class ChatResponse(BaseModel):
    response_id: str
    message: ChatMessage


class ChatRequest(BaseModel):
    message: str
    previous_response_id: str | None = None
    selected_cluster_id: str | None = None
    scope: Literal["selected_cluster", "all"] = "selected_cluster"
    active_tab: Literal["event", "cluster", "sources", "map", "graph", "global_graph"] | None = None
    entity_id: str | None = None
    source_id: str | None = None
    graph_node_id: str | None = None
    region_id: str | None = None
    country_id: str | None = None
    user_id: str | None = None


class SalesWorkspaceMatchSummary(BaseModel):
    account_name: str | None = None
    account_id: str | None = None
    contact_count: int = 0
    open_opportunity_count: int = 0
    last_activity_at: datetime | None = None
    relationship_summary: str | None = None


class SalesDraftPayload(BaseModel):
    company_name: str
    owner_name: str
    owner_email: str | None = None
    prospect_summary: str
    why_now: str
    sales_strategy: str
    outreach_angle: str
    recommended_next_step: str
    internal_notes: str
    stakeholder_focus: list[str] = Field(default_factory=list)
    relevant_services: list[str] = Field(default_factory=list)
    evidence_bullets: list[str] = Field(default_factory=list)
    source_urls: list[str] = Field(default_factory=list)
    priority_label: str
    confidence_label: str
    salesforce_status: str


class SalesDraftMessage(BaseModel):
    message_id: str
    role: Literal["user", "assistant", "system"]
    channel: Literal["chat", "voice", "system"]
    content: str
    created_at: datetime


class SalesWorkspaceResponse(BaseModel):
    claim_id: str
    cluster_id: str
    sales_item_id: str
    cluster_entity_id: str | None = None
    event_subject_company_name: str
    event_headline: str | None = None
    subject_company_name: str
    branch_type: str | None = None
    entity_type: str | None = None
    claimed_by_user_id: str
    status: Literal[
        "claimed",
        "drafting",
        "ready_to_push",
        "pushed_to_salesforce",
        "working",
        "qualified",
        "opportunity_created",
        "closed_won",
        "closed_lost",
    ]
    claimed_by_name: str
    claimed_by_email: str | None = None
    claimed_at: datetime
    updated_at: datetime
    salesforce_stage: str
    salesforce_owner_name: str
    salesforce_owner_id: str | None = None
    salesforce_record_type: Literal["lead", "prospect"] = "prospect"
    salesforce_record_id: str | None = None
    last_pushed_at: datetime | None = None
    next_step: str | None = None
    last_activity_note: str | None = None
    draft_id: str
    draft_payload: SalesDraftPayload
    draft_updated_at: datetime | None = None
    draft_status: Literal["drafting", "generating", "ready_to_push", "pushed"] = "drafting"
    match_summary: SalesWorkspaceMatchSummary = Field(default_factory=SalesWorkspaceMatchSummary)
    messages: list[SalesDraftMessage] = Field(default_factory=list)


class ClaimOpportunityRequest(BaseModel):
    sales_item_id: str
    claimed_by_user_id: str
    claimed_by_name: str
    claimed_by_email: str | None = None
    notes: str | None = None


class SalesDraftConversationRequest(BaseModel):
    actor_user_id: str
    message: str
    channel: Literal["chat", "voice"] = "chat"


class SalesDraftPatchRequest(BaseModel):
    actor_user_id: str
    draft_payload: SalesDraftPayload


class SalesWorkspaceStatusPatchRequest(BaseModel):
    actor_user_id: str
    status: Literal[
        "claimed",
        "drafting",
        "ready_to_push",
        "pushed_to_salesforce",
        "working",
        "qualified",
        "opportunity_created",
        "closed_won",
        "closed_lost",
    ]
    salesforce_stage: str | None = None
    next_step: str | None = None
    last_activity_note: str | None = None


class SalesWorkspaceActorRequest(BaseModel):
    actor_user_id: str


class SalesDashboardMetric(BaseModel):
    label: str
    value: int
    tone: Literal["neutral", "blue", "green", "amber", "red"] = "neutral"


class SalesDashboardItem(BaseModel):
    claim_id: str
    cluster_id: str
    sales_item_id: str
    cluster_entity_id: str | None = None
    event_subject_company_name: str
    event_headline: str | None = None
    subject_company_name: str
    branch_type: str | None = None
    entity_type: str | None = None
    claimed_by_user_id: str | None = None
    claimed_by_name: str
    claimed_at: datetime
    updated_at: datetime
    status: str
    salesforce_stage: str
    salesforce_owner_name: str
    salesforce_record_id: str | None = None
    last_pushed_at: datetime | None = None
    next_step: str | None = None
    last_activity_note: str | None = None
    opportunity_score: float | None = None


class SalesDashboardResponse(BaseModel):
    metrics: list[SalesDashboardMetric]
    items: list[SalesDashboardItem]


class SalesLeadSummary(BaseModel):
    sales_item_id: str
    cluster_id: str
    cluster_entity_id: str | None = None
    event_subject_company_name: str
    event_headline: str | None = None
    event_date: str | None = None
    cluster_created_at: str | None = None
    trigger_type: str | None = None
    subject_company_name: str
    subject_country: str | None = None
    subject_region: str | None = None
    branch_type: str | None = None
    entity_type: str | None = None
    relationship_to_subject: str | None = None
    commercial_role: str | None = None
    rationale: str | None = None
    opportunity_score: float | None = None
    confidence_score: float | None = None
    event_priority_score: float | None = None
    event_confidence_score: float | None = None
    claim_id: str | None = None
    claimed_by_user_id: str | None = None
    claimed_by_name: str | None = None
    status: str | None = None
    salesforce_stage: str | None = None
    salesforce_owner_name: str | None = None
    updated_at: datetime | None = None


class SalesLeadsResponse(BaseModel):
    items: list[SalesLeadSummary]
    page: int = 1
    page_size: int = 100
    total_items: int = 0
    total_pages: int = 0
    sort_by: Literal["newest_event", "highest_priority", "best_confidence"] = "newest_event"


# ---------------------------------------------------------------------------
# User memory / Sherlock AI profile
# ---------------------------------------------------------------------------

class UserProfile(BaseModel):
    name: str | None = None
    role: str | None = None
    sector: str | None = None
    region: str | None = None
    deal_stages: list[str] = Field(default_factory=list)
    active_pursuits: list[str] = Field(default_factory=list)
    expertise_areas: list[str] = Field(default_factory=list)
    key_deductions: list[str] = Field(default_factory=list)


class UserMemoryEntry(BaseModel):
    memory_key: str
    memory_value: Any


class UserMemoryResponse(BaseModel):
    user_id: str
    profile: UserProfile = Field(default_factory=UserProfile)
    entries: dict[str, Any] = Field(default_factory=dict)


class UserMemoryUpsertRequest(BaseModel):
    memory_key: str
    memory_value: Any
