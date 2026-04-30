export type OpportunitySummary = {
  cluster_id: string;
  subject_company_name: string;
  subject_country?: string | null;
  subject_region?: string | null;
  subject_state?: string | null;
  subject_city?: string | null;
  subject_address?: string | null;
  subject_latitude?: number | null;
  subject_longitude?: number | null;
  trigger_type?: string | null;
  trigger_subtype?: string | null;
  event_date?: string | null;
  cluster_created_at?: string | null;
  event_headline?: string | null;
  event_summary?: string | null;
  cluster_priority_score?: number | null;
  cluster_confidence_score?: number | null;
  opportunity_score?: number | null;
  entity_count?: number | null;
  source_count?: number | null;
  best_route_to_market?: string | null;
  propagation_thesis?: string | null;
  service_hypotheses_json?: string | null;
  headline_source_url?: string | null;
};

export type GraphNode = {
  id: string;
  type: "cluster" | "company" | "role_track";
  subtype: string;
  label: string;
  entity_id?: string | null;
  branch_type?: string | null;
  score?: number | null;
  confidence_score?: number | null;
  priority_score?: number | null;
  route_to_market?: string | null;
  detail: Record<string, unknown>;
};

export type GraphEdge = {
  id: string;
  source: string;
  target: string;
  type: string;
  branch_type?: string | null;
  label?: string | null;
  weight?: number | null;
  rationale?: string | null;
};

export type OpportunityDetail = {
  cluster: OpportunitySummary;
  graph_nodes: GraphNode[];
  graph_edges: GraphEdge[];
  entities: Record<string, unknown>[];
  recommendations: Record<string, unknown>[];
  sources: Record<string, unknown>[];
};

export type MapMarker = {
  cluster_id: string;
  label: string;
  country?: string | null;
  state?: string | null;
  city?: string | null;
  address?: string | null;
  latitude?: number | null;
  longitude?: number | null;
  region?: string | null;
  trigger_type?: string | null;
  cluster_priority_score?: number | null;
  cluster_confidence_score?: number | null;
  opportunity_score?: number | null;
};

export type KnowledgeGraphDistributionItem = {
  label: string;
  count: number;
  trigger_type?: string | null;
  tone: string;
};

export type KnowledgeGraphEventSummary = {
  cluster_id: string;
  subject_company_name: string;
  subject_country?: string | null;
  subject_region?: string | null;
  trigger_type?: string | null;
  event_date?: string | null;
  event_summary?: string | null;
  opportunity_score?: number | null;
  cluster_confidence_score?: number | null;
  headline_source_url?: string | null;
};

export type KnowledgeGraphCountrySummary = {
  country_id: string;
  label: string;
  region_id: string;
  narrative: string;
  event_count: number;
  company_count: number;
  average_opportunity: number;
  average_confidence: number;
  dominant_triggers: KnowledgeGraphDistributionItem[];
  top_companies: string[];
  events: KnowledgeGraphEventSummary[];
};

export type KnowledgeGraphRegionSummary = {
  region_id: string;
  label: string;
  narrative: string;
  event_count: number;
  country_count: number;
  company_count: number;
  average_opportunity: number;
  average_confidence: number;
  dominant_triggers: KnowledgeGraphDistributionItem[];
  countries: KnowledgeGraphCountrySummary[];
};

export type KnowledgeGraphResponse = {
  generated_at: string;
  region_count: number;
  country_count: number;
  event_count: number;
  regions: KnowledgeGraphRegionSummary[];
};

export type ProviderHealth = {
  provider_name: string;
  configured: boolean;
  status: string;
  message: string;
};

export type RunnerHealth = {
  configured: boolean;
  runner_type: string;
  message: string;
};

export type KnowledgeBaseStatus = {
  status: string;
  last_synced_at?: string | null;
  document_count: number;
  cluster_document_count: number;
  entity_document_count: number;
  source_document_count: number;
  region_document_count: number;
  country_document_count: number;
  duplicate_candidate_count: number;
  stale_local_file_count: number;
  cleanup_removed_documents: number;
  cleanup_removed_files: number;
  vector_store_id?: string | null;
  last_error?: string | null;
};

export type KnowledgeBaseSyncRequest = {
  cluster_id?: string | null;
  full_refresh?: boolean;
};

export type PipelineSettings = {
  settings_id: string;
  schedule_enabled: boolean;
  schedule_interval_hours: number;
  target_region: string;
  recency_days: number;
  dedup_days: number;
  max_peers: number;
  max_ownership_nodes: number;
  generation_runner: "local" | "job";
  databricks_job_id?: number | null;
  openai_model: string;
  chat_model: string;
  provider_name: string;
  kb_max_results: number;
  kb_cleanup_mode: "off" | "dedupe" | "aggressive";
  kb_cleanup_on_sync: boolean;
  kb_document_retention_days: number;
  updated_at?: string | null;
  updated_by?: string | null;
  last_successful_run_id?: string | null;
  last_successful_run_at?: string | null;
  next_scheduled_run?: string | null;
  provider: ProviderHealth;
  job: RunnerHealth;
  knowledge_base: KnowledgeBaseStatus;
};

export type PipelineSettingsPatch = Partial<{
  schedule_enabled: boolean;
  schedule_interval_hours: number;
  target_region: string;
  recency_days: number;
  dedup_days: number;
  max_peers: number;
  max_ownership_nodes: number;
  generation_runner: "local" | "job";
  openai_model: string;
  kb_max_results: number;
  kb_cleanup_mode: "off" | "dedupe" | "aggressive";
  kb_cleanup_on_sync: boolean;
  kb_document_retention_days: number;
}>;

export type GenerationRun = {
  app_run_id: string;
  trigger_source: string;
  requested_by: string;
  research_mode?: "region" | "company" | null;
  research_target?: string | null;
  target_region?: string | null;
  company_name?: string | null;
  runner_type: string;
  status: string;
  requested_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  updated_at?: string | null;
  created_cluster_id?: string | null;
  duplicate_skipped: boolean;
  error_message?: string | null;
  step_statuses: Record<string, string>;
  databricks_job_id?: number | null;
  databricks_run_id?: number | null;
  job_url?: string | null;
};

export type ChatCitation = {
  id: string;
  label: string;
  file_name?: string | null;
  file_path?: string | null;
  cluster_id?: string | null;
  url?: string | null;
  entity_id?: string | null;
  source_id?: string | null;
  graph_node_id?: string | null;
  region_id?: string | null;
  country_id?: string | null;
};

export type ChatMessage = {
  role: "user" | "assistant";
  content: string;
  citations: ChatCitation[];
};

export type ChatResponse = {
  response_id: string;
  message: ChatMessage;
};

export type ChatRequest = {
  message: string;
  previous_response_id?: string | null;
  selected_cluster_id?: string | null;
  scope: "selected_cluster" | "all";
  active_tab?: "event" | "cluster" | "sources" | "map" | "graph" | "global_graph" | null;
  entity_id?: string | null;
  source_id?: string | null;
  graph_node_id?: string | null;
  region_id?: string | null;
  country_id?: string | null;
  user_id?: string | null;
};

export type CreateGenerationRunRequest = {
  requested_by?: string;
  research_mode?: "region" | "company";
  target_region?: string | null;
  company_name?: string | null;
};

export type ChatCommitRequest = {
  selected_cluster_id: string;
  selected_cluster_name?: string | null;
  messages: ChatMessage[];
  previous_response_id?: string | null;
  committed_by?: string | null;
};

export type ChatCommitResponse = {
  note_id: string;
  cluster_id: string;
  title: string;
  summary_markdown: string;
  committed_at: string;
  committed_by: string;
  knowledge_base: KnowledgeBaseStatus;
};

export type SalesWorkspaceMatchSummary = {
  account_name?: string | null;
  account_id?: string | null;
  contact_count: number;
  open_opportunity_count: number;
  last_activity_at?: string | null;
  relationship_summary?: string | null;
};

export type SalesDraftPayload = {
  company_name: string;
  owner_name: string;
  owner_email?: string | null;
  prospect_summary: string;
  why_now: string;
  sales_strategy: string;
  outreach_angle: string;
  recommended_next_step: string;
  internal_notes: string;
  stakeholder_focus: string[];
  relevant_services: string[];
  evidence_bullets: string[];
  source_urls: string[];
  priority_label: string;
  confidence_label: string;
  salesforce_status: string;
};

export type SalesDraftMessage = {
  message_id: string;
  role: "user" | "assistant" | "system";
  channel: "chat" | "voice" | "system";
  content: string;
  created_at: string;
};

export type SalesWorkspace = {
  claim_id: string;
  cluster_id: string;
  sales_item_id: string;
  cluster_entity_id?: string | null;
  event_subject_company_name: string;
  event_headline?: string | null;
  subject_company_name: string;
  branch_type?: string | null;
  entity_type?: string | null;
  claimed_by_user_id: string;
  status:
    | "claimed"
    | "drafting"
    | "ready_to_push"
    | "pushed_to_salesforce"
    | "working"
    | "qualified"
    | "opportunity_created"
    | "closed_won"
    | "closed_lost";
  claimed_by_name: string;
  claimed_by_email?: string | null;
  claimed_at: string;
  updated_at: string;
  salesforce_stage: string;
  salesforce_owner_name: string;
  salesforce_owner_id?: string | null;
  salesforce_record_type: "lead" | "prospect";
  salesforce_record_id?: string | null;
  last_pushed_at?: string | null;
  next_step?: string | null;
  last_activity_note?: string | null;
  draft_id: string;
  draft_payload: SalesDraftPayload;
  draft_updated_at?: string | null;
  draft_status: "generating" | "drafting" | "ready_to_push" | "pushed";
  match_summary: SalesWorkspaceMatchSummary;
  messages: SalesDraftMessage[];
};

export type ClaimOpportunityRequest = {
  sales_item_id: string;
  claimed_by_user_id: string;
  claimed_by_name: string;
  claimed_by_email?: string | null;
  notes?: string | null;
};

export type SalesDraftConversationRequest = {
  actor_user_id: string;
  message: string;
  channel?: "chat" | "voice";
};

export type SalesDraftPatchRequest = {
  actor_user_id: string;
  draft_payload: SalesDraftPayload;
};

export type SalesWorkspaceStatusPatchRequest = {
  actor_user_id: string;
  status: SalesWorkspace["status"];
  salesforce_stage?: string | null;
  next_step?: string | null;
  last_activity_note?: string | null;
};

export type SalesWorkspaceActorRequest = {
  actor_user_id: string;
};

export type SalesDashboardMetric = {
  label: string;
  value: number;
  tone: "neutral" | "blue" | "green" | "amber" | "red";
};

export type SalesDashboardItem = {
  claim_id: string;
  cluster_id: string;
  sales_item_id: string;
  cluster_entity_id?: string | null;
  event_subject_company_name: string;
  event_headline?: string | null;
  subject_company_name: string;
  branch_type?: string | null;
  entity_type?: string | null;
  claimed_by_user_id?: string | null;
  claimed_by_name: string;
  claimed_at: string;
  updated_at: string;
  status: string;
  salesforce_stage: string;
  salesforce_owner_name: string;
  salesforce_record_id?: string | null;
  last_pushed_at?: string | null;
  next_step?: string | null;
  last_activity_note?: string | null;
  opportunity_score?: number | null;
};

export type SalesDashboard = {
  metrics: SalesDashboardMetric[];
  items: SalesDashboardItem[];
};

export type SalesLead = {
  sales_item_id: string;
  cluster_id: string;
  cluster_entity_id?: string | null;
  event_subject_company_name: string;
  event_headline?: string | null;
  event_date?: string | null;
  cluster_created_at?: string | null;
  trigger_type?: string | null;
  subject_company_name: string;
  subject_country?: string | null;
  subject_region?: string | null;
  branch_type?: string | null;
  entity_type?: string | null;
  relationship_to_subject?: string | null;
  commercial_role?: string | null;
  rationale?: string | null;
  opportunity_score?: number | null;
  confidence_score?: number | null;
  event_priority_score?: number | null;
  event_confidence_score?: number | null;
  claim_id?: string | null;
  claimed_by_user_id?: string | null;
  claimed_by_name?: string | null;
  status?: string | null;
  salesforce_stage?: string | null;
  salesforce_owner_name?: string | null;
  updated_at?: string | null;
};

export type SalesLeadCatalog = {
  items: SalesLead[];
  page: number;
  page_size: number;
  total_items: number;
  total_pages: number;
  sort_by: "newest_event" | "highest_priority" | "best_confidence";
};
