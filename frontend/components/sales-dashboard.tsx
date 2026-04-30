"use client";

import { useEffect, useMemo, useState } from "react";
import { List as VirtualList, type RowComponentProps } from "react-window";

import { firstSentence, formatDate, formatLabel, formatScore } from "@/lib/formatters";
import { SalesDashboard, SalesDashboardItem, SalesLead, SalesLeadCatalog, SalesWorkspace, SalesWorkspaceStatusPatchRequest } from "@/types/api";

type Props = {
  dashboard: SalesDashboard | null;
  leadCatalog: SalesLeadCatalog | null;
  actorUserId: string;
  actorName?: string;
  loading: boolean;
  error: string;
  onRefresh: () => void;
  onOpenCluster: (clusterId: string) => void;
  onOpenWorkspace: (lead: SalesLead) => void;
  onLeadPageChange: (page: number) => void;
  onLeadSortChange: (sort: "newest_event" | "highest_priority" | "best_confidence") => void;
  onUpdateStatus: (
    lead: SalesLead,
    request: Omit<SalesWorkspaceStatusPatchRequest, "actor_user_id">,
  ) => Promise<void>;
};

type DraftEdit = {
  status: SalesWorkspace["status"];
  salesforce_stage: string;
  next_step: string;
  last_activity_note: string;
};

type ViewMode = "pipeline" | "all_leads";
type LeadFilter = "all" | "mine";
type BranchFilter = "all" | "direct" | "peer" | "ownership";
type StatusFilter = "all" | "unclaimed" | "claimed" | "active" | "closed";

/* ── visual helpers ─────────────────────────────────────── */

function statusTone(status?: string | null): "blue" | "amber" | "green" | "red" | "neutral" {
  switch (status) {
    case "ready_to_push":
    case "drafting":
      return "amber";
    case "pushed_to_salesforce":
    case "working":
    case "qualified":
    case "opportunity_created":
      return "blue";
    case "closed_won":
      return "green";
    case "closed_lost":
      return "red";
    case "claimed":
      return "neutral";
    default:
      return "neutral";
  }
}

function statusIcon(status?: string | null): string {
  switch (status) {
    case "drafting":
      return "✏️";
    case "ready_to_push":
      return "🚀";
    case "pushed_to_salesforce":
    case "working":
      return "⚡";
    case "qualified":
    case "opportunity_created":
      return "🎯";
    case "closed_won":
      return "✅";
    case "closed_lost":
      return "⛔";
    case "claimed":
      return "📌";
    default:
      return "○";
  }
}

function ownerTone(isMine: boolean, isClaimed: boolean): "green" | "blue" | "neutral" {
  if (isMine) return "green";
  if (isClaimed) return "blue";
  return "neutral";
}

function matchesStatusFilter(lead: SalesLead, statusFilter: StatusFilter): boolean {
  const status = lead.status ?? "";
  if (statusFilter === "all") return true;
  if (statusFilter === "unclaimed") return !lead.claim_id;
  if (statusFilter === "claimed") return ["claimed", "drafting", "ready_to_push"].includes(status);
  if (statusFilter === "active") return ["pushed_to_salesforce", "working", "qualified", "opportunity_created"].includes(status);
  if (statusFilter === "closed") return ["closed_won", "closed_lost"].includes(status);
  return true;
}

function companyInitials(companyName: string): string {
  const parts = companyName
    .split(/\s+/)
    .map((part) => part.trim())
    .filter(Boolean);
  return parts.slice(0, 2).map((part) => part[0]?.toUpperCase() ?? "").join("") || "OP";
}

function priorityTone(score?: number | null): "red" | "amber" | "blue" {
  const numeric = Number(score ?? 0);
  if (numeric >= 75) return "red";
  if (numeric >= 45) return "amber";
  return "blue";
}

function priorityLabel(score?: number | null): string {
  const numeric = Number(score ?? 0);
  if (numeric >= 75) return "High";
  if (numeric >= 45) return "Medium";
  return "Low";
}

function branchIcon(branch?: string | null): string {
  switch (branch) {
    case "direct": return "◆";
    case "peer": return "◇";
    case "ownership": return "△";
    default: return "●";
  }
}

function relativeDateLabel(value?: string | null): string {
  if (!value) return "";
  const now = Date.now();
  const then = new Date(value).getTime();
  if (Number.isNaN(then)) return "";
  const days = Math.floor((now - then) / 86400000);
  if (days === 0) return "Today";
  if (days === 1) return "Yesterday";
  if (days < 7) return `${days}d ago`;
  if (days < 30) return `${Math.floor(days / 7)}w ago`;
  return formatDate(value);
}

/* ── pipeline stage columns ─────────────────────────────── */

const PIPELINE_STAGES: { key: string; label: string; icon: string; statuses: string[] }[] = [
  { key: "new", label: "New claims", icon: "📌", statuses: ["claimed"] },
  { key: "drafting", label: "Drafting", icon: "✏️", statuses: ["drafting", "ready_to_push"] },
  { key: "active", label: "Active pipeline", icon: "⚡", statuses: ["pushed_to_salesforce", "working", "qualified", "opportunity_created"] },
  { key: "closed", label: "Closed", icon: "🏁", statuses: ["closed_won", "closed_lost"] },
];

const STATUS_PROGRESSION: Record<string, { next: string; label: string; icon: string } | null> = {
  claimed: { next: "drafting", label: "Start drafting", icon: "✏️" },
  drafting: { next: "ready_to_push", label: "Mark ready", icon: "🚀" },
  ready_to_push: { next: "pushed_to_salesforce", label: "Push to SF", icon: "⚡" },
  pushed_to_salesforce: { next: "working", label: "Working", icon: "⚙️" },
  working: { next: "qualified", label: "Qualify", icon: "🎯" },
  qualified: { next: "opportunity_created", label: "Create opp", icon: "✨" },
  opportunity_created: { next: "closed_won", label: "Close won", icon: "✅" },
  closed_won: null,
  closed_lost: null,
};

/* ── virtualised kanban row (react-window v2) ───────────── */

type VirtualKanbanRowProps = {
  items: SalesDashboardItem[];
  savingId: string | null;
  setExpandedCard: (id: string) => void;
  leadFromDashboardItem: (item: SalesDashboardItem) => SalesLead | null;
  onOpenWorkspace: (lead: SalesLead) => void;
  handleAdvanceStage: (item: SalesDashboardItem) => Promise<void>;
};

function VirtualKanbanRow({ index, style, items, savingId, setExpandedCard, leadFromDashboardItem, onOpenWorkspace, handleAdvanceStage }: RowComponentProps<VirtualKanbanRowProps>) {
  const item = items[index];
  const pipelineLead = leadFromDashboardItem(item);
  const nextStage = STATUS_PROGRESSION[item.status];
  const isSaving = savingId === item.sales_item_id;
  return (
    <div style={style}>
      <article className="stKanbanCard">
        <div className="stKanbanCardTop" onClick={() => setExpandedCard(item.sales_item_id)}>
          <div className="stKanbanCardAvatar">{companyInitials(item.subject_company_name)}</div>
          <div className="stKanbanCardInfo">
            <strong className="stKanbanCardName">{item.subject_company_name}</strong>
            <span className="stKanbanCardMeta">{branchIcon(item.branch_type)} {formatLabel(item.branch_type)} · {formatScore(item.opportunity_score)}</span>
          </div>
          <span className="stKanbanCardChevron">▸</span>
        </div>
        <div className="stKanbanCardEvent">{item.event_headline ?? "Event detail unavailable"}</div>
        <div className="stKanbanCardChips">
          <span className={`pill pill-${statusTone(item.status)}`}>{statusIcon(item.status)} {formatLabel(item.status)}</span>
          {item.opportunity_score != null && (
            <span className={`pill pill-${priorityTone(item.opportunity_score)}`}>{priorityLabel(item.opportunity_score)}</span>
          )}
        </div>
        <div className="stKanbanCardQuickActions">
          <button className="stBtnGhostSm" disabled={!pipelineLead} onClick={(e) => { e.stopPropagation(); pipelineLead && onOpenWorkspace(pipelineLead); }}>
            Open workspace
          </button>
          {nextStage && (
            <button className="stBtnAdvance" disabled={isSaving} onClick={(e) => { e.stopPropagation(); void handleAdvanceStage(item); }}>
              {isSaving ? "..." : `${nextStage.icon} ${nextStage.label}`}
            </button>
          )}
        </div>
      </article>
    </div>
  );
}

/* ── component ──────────────────────────────────────────── */

export function SalesDashboardPanel({
  dashboard,
  leadCatalog,
  actorUserId,
  actorName,
  loading,
  error,
  onRefresh,
  onOpenCluster,
  onOpenWorkspace,
  onLeadPageChange,
  onLeadSortChange,
  onUpdateStatus,
}: Props) {
  const leads = useMemo(() => leadCatalog?.items ?? [], [leadCatalog]);
  const [draftEdits, setDraftEdits] = useState<Record<string, DraftEdit>>({});
  const [savingId, setSavingId] = useState("");
  const [viewMode, setViewMode] = useState<ViewMode>("pipeline");
  const [leadFilter, setLeadFilter] = useState<LeadFilter>("all");
  const [searchValue, setSearchValue] = useState("");
  const [branchFilter, setBranchFilter] = useState<BranchFilter>("all");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [expandedCard, setExpandedCard] = useState<string | null>(null);

  useEffect(() => {
    if (!dashboard) return;
    const nextState = dashboard.items.reduce<Record<string, DraftEdit>>((accumulator, item) => {
      accumulator[item.sales_item_id] = {
        status: item.status as SalesWorkspace["status"],
        salesforce_stage: item.salesforce_stage,
        next_step: item.next_step ?? "",
        last_activity_note: item.last_activity_note ?? "",
      };
      return accumulator;
    }, {});
    setDraftEdits(nextState);
  }, [dashboard]);

  const normalizedSearch = searchValue.trim().toLowerCase();

  const visibleLeads = useMemo(() => {
    return leads
      .filter((lead) => (leadFilter === "mine" ? lead.claimed_by_user_id === actorUserId : true))
      .filter((lead) => (branchFilter === "all" ? true : lead.branch_type === branchFilter))
      .filter((lead) => matchesStatusFilter(lead, statusFilter))
      .filter((lead) => {
        if (!normalizedSearch) return true;
        const haystack = [
          lead.subject_company_name,
          lead.event_subject_company_name,
          lead.event_headline,
          lead.relationship_to_subject,
          lead.rationale,
          lead.claimed_by_name,
          lead.entity_type,
          lead.branch_type,
        ]
          .filter(Boolean)
          .join(" ")
          .toLowerCase();
        return haystack.includes(normalizedSearch);
      })
      .sort((left, right) => {
        const leftClaimed = left.claim_id ? 1 : 0;
        const rightClaimed = right.claim_id ? 1 : 0;
        if (leftClaimed !== rightClaimed) return rightClaimed - leftClaimed;
        return Number(right.opportunity_score ?? 0) - Number(left.opportunity_score ?? 0);
      });
  }, [actorUserId, branchFilter, leadFilter, leads, normalizedSearch, statusFilter]);

  const myPipeline = useMemo(
    () =>
      (dashboard?.items ?? []).filter(
        (item) =>
          item.claimed_by_user_id === actorUserId ||
          (actorName && item.claimed_by_name.trim().toLowerCase() === actorName.trim().toLowerCase()),
      ),
    [actorName, actorUserId, dashboard?.items],
  );

  const pipelineByStage = useMemo(() => {
    const result: Record<string, SalesDashboardItem[]> = {};
    for (const stage of PIPELINE_STAGES) {
      result[stage.key] = myPipeline.filter((item) => stage.statuses.includes(item.status));
    }
    return result;
  }, [myPipeline]);

  const leadsByEvent = useMemo(() => {
    const groups = new Map<string, { clusterId: string; headline: string; eventCompany: string; eventDate?: string | null; clusterCreatedAt?: string | null; leads: SalesLead[] }>();
    for (const lead of visibleLeads) {
      const key = lead.cluster_id;
      if (!groups.has(key)) {
        groups.set(key, {
          clusterId: lead.cluster_id,
          headline: lead.event_headline ?? "Untitled event",
          eventCompany: lead.event_subject_company_name,
          eventDate: lead.event_date,
          clusterCreatedAt: lead.cluster_created_at,
          leads: [],
        });
      }
      groups.get(key)?.leads.push(lead);
    }
    return Array.from(groups.values())
      .map((group) => ({
        ...group,
        leads: group.leads.sort((left, right) => Number(right.opportunity_score ?? 0) - Number(left.opportunity_score ?? 0)),
      }))
      .sort((left, right) => (right.clusterCreatedAt ?? "").localeCompare(left.clusterCreatedAt ?? ""));
  }, [visibleLeads]);

  const universeCounts = useMemo(
    () => ({
      all: visibleLeads.length,
      claimed: visibleLeads.filter((lead) => lead.claim_id).length,
      active: visibleLeads.filter((lead) =>
        ["pushed_to_salesforce", "working", "qualified", "opportunity_created"].includes(lead.status ?? ""),
      ).length,
      unclaimed: visibleLeads.filter((lead) => !lead.claim_id).length,
    }),
    [visibleLeads],
  );

  const currentPage = leadCatalog?.page ?? 1;
  const currentPageSize = leadCatalog?.page_size ?? 100;
  const totalPages = leadCatalog?.total_pages ?? 1;
  const pageStart = leadCatalog?.items.length ? ((currentPage - 1) * currentPageSize) + 1 : 0;
  const pageEnd = leadCatalog?.items.length ? pageStart + leadCatalog.items.length - 1 : 0;

  async function handleSave(lead: SalesLead) {
    const draft = draftEdits[lead.sales_item_id];
    if (!draft) return;
    setSavingId(lead.sales_item_id);
    try {
      await onUpdateStatus(lead, draft);
    } finally {
      setSavingId("");
    }
  }

  async function handleAdvanceStage(item: SalesDashboardItem) {
    const progression = STATUS_PROGRESSION[item.status];
    if (!progression) return;
    const lead = leadFromDashboardItem(item);
    if (!lead) return;
    setSavingId(item.sales_item_id);
    try {
      await onUpdateStatus(lead, {
        status: progression.next as SalesWorkspace["status"],
        salesforce_stage: item.salesforce_stage,
        next_step: item.next_step ?? "",
        last_activity_note: `Advanced from ${formatLabel(item.status)} to ${formatLabel(progression.next)}`,
      });
    } finally {
      setSavingId("");
    }
  }

  function leadFromDashboardItem(item: SalesDashboardItem): SalesLead | null {
    if (!item.sales_item_id) return null;
    return {
      sales_item_id: item.sales_item_id,
      cluster_id: item.cluster_id,
      cluster_entity_id: item.cluster_entity_id,
      event_subject_company_name: item.event_subject_company_name,
      event_headline: item.event_headline,
      subject_company_name: item.subject_company_name,
      branch_type: item.branch_type,
      entity_type: item.entity_type,
      claim_id: item.claim_id,
      claimed_by_user_id: item.claimed_by_user_id,
      claimed_by_name: item.claimed_by_name,
      status: item.status,
      salesforce_stage: item.salesforce_stage,
      salesforce_owner_name: item.salesforce_owner_name,
      updated_at: item.updated_at,
      rationale: item.last_activity_note,
      opportunity_score: item.opportunity_score,
    };
  }

  return (
    <section className="stLayout">
      {/* ── header ─────────────────────────────────────── */}
      <div className="stHeader">
        <div className="stHeaderLeft">
          <div className="stBreadcrumb">
            <span className="stBreadcrumbItem">Sales</span>
            <span className="stBreadcrumbSep">/</span>
            <span className="stBreadcrumbItem stBreadcrumbActive">
              {viewMode === "pipeline" ? "My Pipeline" : "Opportunity Universe"}
            </span>
          </div>
          <h1 className="stTitle">
            {viewMode === "pipeline" ? "Pipeline Board" : "Opportunity Universe"}
          </h1>
          <p className="stSubtitle">
            {viewMode === "pipeline"
              ? "Your claimed opportunities organized by stage — track, update, and advance deals from claim to close."
              : "Every direct, peer, and ownership opportunity across the event corpus — discover and claim new leads."}
          </p>
        </div>
        <div className="stHeaderRight">
          <button className="stRefreshBtn" onClick={onRefresh} disabled={loading}>
            <span className={`stRefreshIcon ${loading ? "stRefreshSpin" : ""}`}>↻</span>
            {loading ? "Syncing..." : "Refresh"}
          </button>
        </div>
      </div>

      {/* ── metrics ────────────────────────────────────── */}
      {(dashboard?.metrics ?? []).length > 0 && (
        <div className="stMetrics">
          {(dashboard?.metrics ?? []).map((metric) => (
            <div key={metric.label} className={`stMetricCard stMetricCard--${metric.tone}`}>
              <div className="stMetricValue">{metric.value}</div>
              <div className="stMetricLabel">{metric.label}</div>
            </div>
          ))}
          {myPipeline.length > 0 && (
            <div className="stMetricCard stMetricCard--green">
              <div className="stMetricValue">{myPipeline.length}</div>
              <div className="stMetricLabel">My pipeline</div>
            </div>
          )}
        </div>
      )}

      {/* ── view toggle ────────────────────────────────── */}
      <div className="stViewToggle">
        <div className="stTabs">
          <button className={`stTab ${viewMode === "pipeline" ? "stTab--active" : ""}`} onClick={() => setViewMode("pipeline")}>
            <span className="stTabIcon">◫</span>
            My Pipeline
            {myPipeline.length > 0 && <span className="stTabBadge">{myPipeline.length}</span>}
          </button>
          <button className={`stTab ${viewMode === "all_leads" ? "stTab--active" : ""}`} onClick={() => setViewMode("all_leads")}>
            <span className="stTabIcon">◱</span>
            Lead Universe
            <span className="stTabBadge">{leadCatalog?.total_items ?? 0}</span>
          </button>
        </div>
        <div className="stQuickFilters">
          <button className={`stQuickFilter ${leadFilter === "all" ? "stQuickFilter--active" : ""}`} onClick={() => setLeadFilter("all")}>All</button>
          <button className={`stQuickFilter ${leadFilter === "mine" ? "stQuickFilter--active" : ""}`} onClick={() => setLeadFilter("mine")}>Mine</button>
        </div>
      </div>

      {/* ── filter bar ─────────────────────────────────── */}
      <div className="stFilterBar">
        <div className="stSearchWrap">
          <span className="stSearchIcon">⌕</span>
          <input className="stSearchInput" placeholder="Search companies, events, owners..." value={searchValue} onChange={(e) => setSearchValue(e.target.value)} />
          {searchValue && <button className="stSearchClear" onClick={() => setSearchValue("")}>×</button>}
        </div>
        <div className="stFilterControls">
          <select className="stSelect" value={leadCatalog?.sort_by ?? "newest_event"} onChange={(e) => onLeadSortChange(e.target.value as "newest_event" | "highest_priority" | "best_confidence")}>
            <option value="newest_event">↕ Newest added</option>
            <option value="highest_priority">↕ Highest priority</option>
            <option value="best_confidence">↕ Best confidence</option>
          </select>
          <select className="stSelect" value={branchFilter} onChange={(e) => setBranchFilter(e.target.value as BranchFilter)}>
            <option value="all">All branches</option>
            <option value="direct">◆ Direct</option>
            <option value="peer">◇ Peer</option>
            <option value="ownership">△ Ownership</option>
          </select>
          <select className="stSelect" value={statusFilter} onChange={(e) => setStatusFilter(e.target.value as StatusFilter)}>
            <option value="all">All statuses</option>
            <option value="unclaimed">○ Unclaimed</option>
            <option value="claimed">📌 Claim / Draft</option>
            <option value="active">⚡ Active</option>
            <option value="closed">🏁 Closed</option>
          </select>
        </div>
        <div className="stFilterSummary">
          <span className="stFilterCount">{visibleLeads.length} visible</span>
          <span className="stFilterDivider">·</span>
          <span className="stFilterCount">{leadsByEvent.length} events</span>
          <span className="stFilterDivider">·</span>
          <span className="stFilterCount">Page {currentPage}/{Math.max(totalPages, 1)}</span>
        </div>
      </div>

      {/* ── loading / error ────────────────────────────── */}
      {loading && !dashboard && !leadCatalog ? (
        <div className="stSkeletonWrap">
          <div className="stSkeletonMetrics">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="skeleton skeletonMetric" />
            ))}
          </div>
          <div className="stSkeletonKanban">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="stSkeletonColumn">
                <div className="skeleton stSkeletonColHead" />
                <div className="skeleton skeletonCard" />
                <div className="skeleton skeletonCard" />
              </div>
            ))}
          </div>
        </div>
      ) : null}
      {error ? <div className="stError">{error}</div> : null}

      {/* ── pipeline board (kanban) ────────────────────── */}
      {viewMode === "pipeline" ? (
        !myPipeline.length ? (
          <div className="stEmptyState">
            <div className="stEmptyIcon">📋</div>
            <h3 className="stEmptyTitle">No opportunities claimed yet</h3>
            <p className="stEmptyText">Switch to <strong>Lead Universe</strong> to discover and claim opportunities from generated events.</p>
            <button className="stEmptyAction" onClick={() => setViewMode("all_leads")}>Browse Lead Universe →</button>
          </div>
        ) : (
          <div className="stKanban">
            {PIPELINE_STAGES.map((stage) => {
              const items = pipelineByStage[stage.key] ?? [];
              return (
                <div key={stage.key} className="stKanbanColumn">
                  <div className="stKanbanColumnHead">
                    <span className="stKanbanColumnIcon">{stage.icon}</span>
                    <span className="stKanbanColumnLabel">{stage.label}</span>
                    <span className="stKanbanColumnCount">{items.length}</span>
                  </div>
                  <div className="stKanbanColumnBody">
                    {items.length === 0 && <div className="stKanbanEmpty">No items</div>}
                    {items.length > 0 && (expandedCard && items.some(i => i.sales_item_id === expandedCard) || items.length <= 20) ? (
                      items.map((item) => {
                        const pipelineLead = leadFromDashboardItem(item);
                        const edit = item.sales_item_id ? draftEdits[item.sales_item_id] : undefined;
                        const isExpanded = expandedCard === item.sales_item_id;
                        const nextStage = STATUS_PROGRESSION[item.status];
                        const isSaving = savingId === item.sales_item_id;
                        return (
                        <article key={item.claim_id} className={`stKanbanCard ${isExpanded ? "stKanbanCard--expanded" : ""}`}>
                          <div className="stKanbanCardTop" onClick={() => setExpandedCard(isExpanded ? null : item.sales_item_id)}>
                            <div className="stKanbanCardAvatar">{companyInitials(item.subject_company_name)}</div>
                            <div className="stKanbanCardInfo">
                              <strong className="stKanbanCardName">{item.subject_company_name}</strong>
                              <span className="stKanbanCardMeta">{branchIcon(item.branch_type)} {formatLabel(item.branch_type)} · {formatScore(item.opportunity_score)}</span>
                            </div>
                            <span className="stKanbanCardChevron">{isExpanded ? "▾" : "▸"}</span>
                          </div>
                          <div className="stKanbanCardEvent">{item.event_headline ?? "Event detail unavailable"}</div>
                          <div className="stKanbanCardChips">
                            <span className={`pill pill-${statusTone(item.status)}`}>{statusIcon(item.status)} {formatLabel(item.status)}</span>
                            {item.opportunity_score != null && (
                              <span className={`pill pill-${priorityTone(item.opportunity_score)}`}>{priorityLabel(item.opportunity_score)}</span>
                            )}
                          </div>
                          {/* ── quick actions (always visible) ── */}
                          <div className="stKanbanCardQuickActions">
                            <button className="stBtnGhostSm" disabled={!pipelineLead} onClick={(e) => { e.stopPropagation(); pipelineLead && onOpenWorkspace(pipelineLead); }}>
                              Open workspace
                            </button>
                            {nextStage && (
                              <button className="stBtnAdvance" disabled={isSaving} onClick={(e) => { e.stopPropagation(); void handleAdvanceStage(item); }}>
                                {isSaving ? "..." : `${nextStage.icon} ${nextStage.label}`}
                              </button>
                            )}
                          </div>
                          {isExpanded && (
                            <div className="stKanbanCardExpanded">
                              <div className="stKanbanCardMetas">
                                <div className="stKanbanMeta"><span>Score</span><strong>{formatScore(item.opportunity_score)}</strong></div>
                                <div className="stKanbanMeta"><span>Claimed</span><strong>{relativeDateLabel(item.claimed_at)}</strong></div>
                                <div className="stKanbanMeta"><span>Updated</span><strong>{relativeDateLabel(item.updated_at)}</strong></div>
                                <div className="stKanbanMeta"><span>Owner</span><strong>{item.salesforce_owner_name ?? item.claimed_by_name ?? "You"}</strong></div>
                              </div>
                              <div className="stKanbanCardFields">
                                <label className="stFieldLabel">
                                  Status
                                  <select
                                    className="stSelect stSelectCompact"
                                    value={edit?.status ?? (item.status as SalesWorkspace["status"]) ?? "claimed"}
                                    disabled={!pipelineLead}
                                    onChange={(e) => setDraftEdits((cur) => ({
                                      ...cur,
                                      [item.sales_item_id]: {
                                        ...(cur[item.sales_item_id] ?? { salesforce_stage: item.salesforce_stage ?? "Claimed", next_step: "", last_activity_note: "" }),
                                        status: e.target.value as SalesWorkspace["status"],
                                      },
                                    }))}
                                  >
                                    {["claimed","drafting","ready_to_push","pushed_to_salesforce","working","qualified","opportunity_created","closed_won","closed_lost"].map((s) => (
                                      <option key={s} value={s}>{formatLabel(s)}</option>
                                    ))}
                                  </select>
                                </label>
                                <label className="stFieldLabel">
                                  Next step
                                  <input
                                    className="stInput"
                                    placeholder="What happens next?"
                                    value={edit?.next_step ?? item.next_step ?? ""}
                                    disabled={!pipelineLead}
                                    onChange={(e) => setDraftEdits((cur) => ({
                                      ...cur,
                                      [item.sales_item_id]: {
                                        ...(cur[item.sales_item_id] ?? { status: (item.status as SalesWorkspace["status"]) ?? "claimed", salesforce_stage: item.salesforce_stage ?? "Claimed", last_activity_note: "" }),
                                        next_step: e.target.value,
                                      },
                                    }))}
                                  />
                                </label>
                                <label className="stFieldLabel">
                                  Activity note
                                  <input
                                    className="stInput"
                                    placeholder="Log activity or context..."
                                    value={edit?.last_activity_note ?? item.last_activity_note ?? ""}
                                    disabled={!pipelineLead}
                                    onChange={(e) => setDraftEdits((cur) => ({
                                      ...cur,
                                      [item.sales_item_id]: {
                                        ...(cur[item.sales_item_id] ?? { status: (item.status as SalesWorkspace["status"]) ?? "claimed", salesforce_stage: item.salesforce_stage ?? "Claimed", next_step: "" }),
                                        last_activity_note: e.target.value,
                                      },
                                    }))}
                                  />
                                </label>
                              </div>
                              <div className="stKanbanCardActions">
                                <button className="stBtnGhost" onClick={() => onOpenCluster(item.cluster_id)}>View event</button>
                                <button className="stBtnPrimary" disabled={!pipelineLead || isSaving} onClick={() => pipelineLead && void handleSave(pipelineLead)}>
                                  {isSaving ? "Saving..." : "Save changes"}
                                </button>
                              </div>
                            </div>
                          )}
                        </article>
                      );
                    })
                    ) : items.length > 0 ? (
                      <VirtualList
                        style={{ height: 520, width: "100%" }}
                        rowCount={items.length}
                        rowHeight={140}
                        overscanCount={4}
                        rowComponent={VirtualKanbanRow}
                        rowProps={{ items, savingId, setExpandedCard, leadFromDashboardItem, onOpenWorkspace, handleAdvanceStage }}
                      />
                    ) : null}
                  </div>
                </div>
              );
            })}
          </div>
        )
      ) : (
        /* ── lead universe ─────────────────────────────── */
        <div className="stUniverse">
          <div className="stSegments">
            {([
              { key: "all" as StatusFilter, label: "All", count: universeCounts.all },
              { key: "unclaimed" as StatusFilter, label: "Unclaimed", count: universeCounts.unclaimed },
              { key: "claimed" as StatusFilter, label: "Claimed", count: universeCounts.claimed },
              { key: "active" as StatusFilter, label: "In progress", count: universeCounts.active },
            ]).map((seg) => (
              <button key={seg.key} className={`stSegment ${statusFilter === seg.key ? "stSegment--active" : ""}`} onClick={() => setStatusFilter(seg.key)}>
                {seg.label}<span className="stSegmentCount">{seg.count}</span>
              </button>
            ))}
          </div>

          <div className="stTableHead">
            <span>Company</span>
            <span>Branch</span>
            <button type="button" className="stSortHead" onClick={() => onLeadSortChange("newest_event")}>Trigger</button>
            <span>Rationale</span>
            <button type="button" className="stSortHead" onClick={() => onLeadSortChange("highest_priority")}>Priority</button>
            <button type="button" className="stSortHead" onClick={() => onLeadSortChange("best_confidence")}>Confidence</button>
            <span>Status</span>
            <span>Owner</span>
            <span></span>
          </div>

          {!leadsByEvent.length ? (
            <div className="stEmptyState stEmptyStateCompact">
              <div className="stEmptyIcon">🔍</div>
              <h3 className="stEmptyTitle">No leads match your filters</h3>
              <p className="stEmptyText">Try adjusting your search or filter criteria.</p>
            </div>
          ) : (
            leadsByEvent.map((group) => (
              <section key={group.clusterId} className="stEventGroup">
                <div className="stEventStrip">
                  <div className="stEventStripLeft">
                    <span className="stEventStripDot" />
                    <div className="stEventStripInfo">
                      <strong>{group.eventCompany}</strong>
                      <span>{group.headline}</span>
                    </div>
                  </div>
                  <div className="stEventStripRight">
                    {group.eventDate && <span className="stEventStripDate">Event: {formatDate(group.eventDate)}</span>}
                    {group.clusterCreatedAt && <span className="stEventStripDate">Added: {relativeDateLabel(group.clusterCreatedAt)}</span>}
                    <span className="stEventStripCount">{group.leads.length} opp{group.leads.length !== 1 ? "s" : ""}</span>
                    <button className="stBtnGhostSm" onClick={() => onOpenCluster(group.clusterId)}>View event →</button>
                  </div>
                </div>
                <div className="stEventRows">
                  {group.leads.map((lead) => (
                    <article key={lead.sales_item_id} className="stLeadRow" data-owned={lead.claimed_by_user_id === actorUserId ? "mine" : lead.claim_id ? "team" : "unclaimed"}>
                      <div className="stLeadCompany">
                        <div className="stLeadAvatar" data-branch={lead.branch_type ?? "direct"}>{companyInitials(lead.subject_company_name)}</div>
                        <div className="stLeadCompanyText">
                          <strong>{lead.subject_company_name}</strong>
                          <span>{lead.entity_type ? formatLabel(lead.entity_type) : "Opportunity"} · {lead.subject_region ?? lead.subject_country ?? ""}</span>
                        </div>
                      </div>
                      <div className="stLeadBranch">
                        <span className={`stBranchPill stBranchPill--${lead.branch_type ?? "direct"}`}>{branchIcon(lead.branch_type)} {formatLabel(lead.branch_type)}</span>
                      </div>
                      <div className="stLeadTrigger">
                        {lead.trigger_type && <span className="stTriggerTypePill">{formatLabel(lead.trigger_type)}</span>}
                        <span className="stTriggerRelationship">{lead.relationship_to_subject ?? lead.rationale ?? "—"}</span>
                      </div>
                      <div className="stLeadRationale">
                        <span>{firstSentence(lead.rationale ?? lead.relationship_to_subject ?? "Open workspace for details.")}</span>
                      </div>
                      <div className="stLeadScore">
                        <div className="stScoreBar"><div className={`stScoreBarFill stScoreBarFill--${priorityTone(lead.opportunity_score)}`} style={{ width: `${Math.min(Number(lead.opportunity_score ?? 0), 100)}%` }} /></div>
                        <span className="stScoreText">{formatScore(lead.opportunity_score)}</span>
                      </div>
                      <div className="stLeadScore">
                        <div className="stScoreBar"><div className="stScoreBarFill stScoreBarFill--blue" style={{ width: `${Math.min(Number(lead.confidence_score ?? 0) * 100, 100)}%` }} /></div>
                        <span className="stScoreText">{formatScore(lead.confidence_score)}</span>
                      </div>
                      <div className="stLeadStatus">
                        <span className={`pill pill-${statusTone(lead.status)}`}>{statusIcon(lead.status ?? "unclaimed")} {formatLabel(lead.status ?? "unclaimed")}</span>
                      </div>
                      <div className="stLeadOwner">
                        <span className={`stOwnerDot stOwnerDot--${ownerTone(lead.claimed_by_user_id === actorUserId, Boolean(lead.claim_id))}`} />
                        <span>{lead.claimed_by_user_id === actorUserId ? "You" : lead.claimed_by_name ?? "Open"}</span>
                      </div>
                      <div className="stLeadActions">
                        <button className="stBtnGhostSm" onClick={() => onOpenWorkspace(lead)}>{lead.claim_id ? "Open" : "Claim"}</button>
                      </div>
                    </article>
                  ))}
                </div>
              </section>
            ))
          )}

          <div className="stPagination">
            <button className="stBtnGhost" disabled={currentPage <= 1 || loading} onClick={() => onLeadPageChange(Math.max(currentPage - 1, 1))}>← Previous</button>
            <span className="stPaginationText">{pageStart ? `${pageStart}–${pageEnd}` : "0"} of {leadCatalog?.total_items ?? 0}</span>
            <button className="stBtnGhost" disabled={currentPage >= totalPages || loading} onClick={() => onLeadPageChange(Math.min(currentPage + 1, totalPages))}>Next →</button>
          </div>
        </div>
      )}
    </section>
  );
}
