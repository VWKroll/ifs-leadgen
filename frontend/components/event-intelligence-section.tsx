"use client";

import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import { RichTextBlock } from "@/components/rich-text-block";
import { SelectionInfoCard } from "@/components/selection-info-card";
import { firstSentence, formatDate, formatLabel, formatScore, normalizeTextBlock, parseJsonArray, scoreBand, SCORE_TOOLTIPS } from "@/lib/formatters";
import { branchLegendItems, toneForBranch, toneForTrigger } from "@/lib/semantics";
import { MapMarker, OpportunityDetail, OpportunitySummary, SalesLead } from "@/types/api";
import { ChatRequestContext, SharedChatContext, TabKey } from "@/types/app-shell";
import { BranchKey, EntityRecord, InsightCardData, InsightChip, InsightTone } from "@/types/view";

const OpportunityGraph = dynamic(
  () => import("@/components/opportunity-graph").then((module) => module.OpportunityGraph),
  {
    ssr: false,
    loading: () => (
      <div className="panel detailCard">
        <h2 className="panelTitle">Loading graph</h2>
        <p className="bodyText bodyTextMuted">Preparing the relationship view for this opportunity cluster.</p>
      </div>
    ),
  },
);

/* MAP TAB — disabled for this pass
const OpportunityMap = dynamic(
  () => import("@/components/opportunity-map").then((module) => module.OpportunityMap),
  {
    ssr: false,
    loading: () => (
      <div className="panel mapCanvas" style={{ display: "grid", placeItems: "center", color: "var(--muted)" }}>
        Loading map...
      </div>
    ),
  },
);
*/

type Props = {
  detail: OpportunityDetail;
  filteredItems: OpportunitySummary[];
  selectedClusterId: string;
  setSelectedClusterId: (clusterId: string) => void;
  activeTab: TabKey;
  setActiveTab: (tab: TabKey) => void;
  activeBranch: BranchKey;
  setActiveBranch: (branch: BranchKey) => void;
  markers: MapMarker[];
  pendingClusterOpen: { clusterId: string; label: string } | null;
  detailLoading: boolean;
  onChatContextChange: (ctx: SharedChatContext) => void;
  salesLeadById: Record<string, SalesLead>;
  onOpenClaimModal: (lead: SalesLead) => void;
};

/** Build a minimal SalesLead from entity + cluster data when no catalog row exists. */
function syntheticLead(entity: EntityRecord, cluster: OpportunitySummary): SalesLead {
  return {
    sales_item_id: entity.cluster_entity_id,
    cluster_id: cluster.cluster_id,
    cluster_entity_id: entity.cluster_entity_id,
    event_subject_company_name: cluster.subject_company_name,
    event_headline: cluster.event_headline ?? null,
    event_date: cluster.event_date ?? null,
    cluster_created_at: cluster.cluster_created_at ?? null,
    trigger_type: cluster.trigger_type ?? null,
    subject_company_name: entity.entity_name,
    subject_country: cluster.subject_country ?? null,
    subject_region: cluster.subject_region ?? null,
    branch_type: entity.branch_type,
    entity_type: entity.entity_type,
    relationship_to_subject: entity.relationship_to_subject ?? null,
    commercial_role: entity.commercial_role ?? null,
    rationale: entity.rationale ?? null,
    opportunity_score: cluster.opportunity_score ?? null,
    confidence_score: entity.confidence_score ?? null,
    event_priority_score: cluster.cluster_priority_score ?? null,
    event_confidence_score: cluster.cluster_confidence_score ?? null,
  };
}

function chipList(values: string[], tone: InsightTone): InsightChip[] {
  return values.filter(Boolean).map((label) => ({ label, tone }));
}

function stringsFromUnknown(value: unknown): string[] {
  return Array.isArray(value) ? value.map(String).filter(Boolean) : [];
}

function buildClusterInsight(cluster: OpportunitySummary, sourceCount: number, entityCount: number): InsightCardData {
  return {
    eyebrow: "Selected Trigger",
    title: cluster.subject_company_name,
    subtitle: `${formatLabel(cluster.trigger_type)} · ${cluster.subject_country ?? "Unknown country"} · ${cluster.subject_region ?? "Unknown region"}`,
    badges: [
      { label: formatLabel(cluster.trigger_type), tone: toneForTrigger(cluster.trigger_type) },
      { label: cluster.subject_country ?? "Country unknown", tone: "neutral" },
      { label: cluster.best_route_to_market ?? "Mixed", tone: "blue" },
    ],
    metrics: [
      { label: "Priority", value: `${formatScore(cluster.cluster_priority_score)} ${scoreBand(cluster.cluster_priority_score)}`, tone: "amber" },
      { label: "Confidence", value: `${formatScore(cluster.cluster_confidence_score)} ${scoreBand(cluster.cluster_confidence_score)}`, tone: "green" },
      { label: "Opportunity", value: `${formatScore(cluster.opportunity_score)} ${scoreBand(cluster.opportunity_score)}`, tone: "blue" },
      { label: "Sources", value: String(sourceCount), tone: "neutral" },
    ],
    sections: [
      { title: "Event Summary", text: normalizeTextBlock(cluster.event_summary) },
      { title: "Propagation Thesis", text: normalizeTextBlock(cluster.propagation_thesis) },
      {
        title: "Coverage",
        chips: [
          { label: `${entityCount} cluster entities`, tone: "blue" },
          { label: cluster.event_date ? formatDate(cluster.event_date) : "Date unknown", tone: "neutral" },
        ],
        linkHref: cluster.headline_source_url ?? undefined,
        linkLabel: cluster.headline_source_url ? "Open headline source" : undefined,
      },
    ],
  };
}

function buildEntityInsight(entity: EntityRecord): InsightCardData {
  const primaryRecommendation = entity.recommendations[0];
  return {
    eyebrow: "Opportunity Node",
    title: entity.entity_name,
    subtitle: `${formatLabel(entity.entity_type)} · ${formatLabel(entity.branch_type)} · ${formatLabel(entity.commercial_role)}`,
    badges: [
      { label: formatLabel(entity.branch_type), tone: toneForBranch(entity.branch_type) },
      { label: formatLabel(entity.entity_type), tone: "blue" },
      { label: formatLabel(entity.evidence_type), tone: "neutral" },
    ],
    metrics: [
      { label: "Priority", value: `${formatScore(entity.priority_score)} ${scoreBand(entity.priority_score)}`, tone: "amber" },
      { label: "Confidence", value: `${formatScore(entity.confidence_score)} ${scoreBand(entity.confidence_score)}`, tone: "green" },
      { label: "Sources", value: String(entity.source_urls.length), tone: "neutral" },
    ],
    sections: [
      { title: "Relationship To Event", text: entity.relationship_to_subject },
      { title: "Engagement Rationale", text: entity.rationale },
      {
        title: "Recommended Outreach",
        text: primaryRecommendation ? normalizeTextBlock(primaryRecommendation.rationale) : "No outreach recommendation is available yet.",
        chips: primaryRecommendation
          ? [
              { label: formatLabel(primaryRecommendation.role_track_type), tone: "blue" },
              { label: `${formatScore(primaryRecommendation.role_confidence_score)} confidence`, tone: "green" },
            ]
          : undefined,
      },
      { title: "Target Titles", chips: primaryRecommendation ? chipList(primaryRecommendation.recommended_titles.slice(0, 8), "purple") : undefined },
      { title: "Target Departments", chips: primaryRecommendation ? chipList(primaryRecommendation.departments.slice(0, 6), "blue") : undefined },
      { title: "Seniority Levels", chips: primaryRecommendation ? chipList(primaryRecommendation.seniority_levels, "green") : undefined },
    ],
  };
}

function buildGraphInsight(node: OpportunityDetail["graph_nodes"][number], detail: OpportunityDetail, entities: EntityRecord[]): InsightCardData {
  if (node.type === "cluster") {
    return buildClusterInsight(detail.cluster, detail.sources.length, detail.entities.length);
  }

  if (node.type === "company") {
    const entity = entities.find((item) => item.cluster_entity_id === node.entity_id);
    return entity
      ? buildEntityInsight(entity)
      : {
          eyebrow: "Graph Node",
          title: node.label,
          subtitle: formatLabel(node.subtype),
        };
  }

  const detailRecord = node.detail as Record<string, unknown>;
  return {
    eyebrow: "Role Track",
    title: node.label,
    subtitle: `${formatLabel(node.subtype)} · ${formatLabel(node.branch_type)}`,
    badges: [
      { label: formatLabel(node.branch_type), tone: toneForBranch(node.branch_type as BranchKey) },
      { label: formatLabel(node.subtype), tone: "purple" },
    ],
    metrics: [
      { label: "Confidence", value: String(formatScore(node.confidence_score)), tone: "green" },
      { label: "Priority", value: String(formatScore(node.priority_score)), tone: "amber" },
    ],
    sections: [
      { title: "Rationale", text: normalizeTextBlock(String(detailRecord.rationale ?? "")) },
      { title: "Hypothesized Services", chips: chipList(stringsFromUnknown(detailRecord.hypothesized_services), "purple") },
      { title: "Target Titles", chips: chipList(stringsFromUnknown(detailRecord.recommended_titles), "purple") },
      { title: "Target Departments", chips: chipList(stringsFromUnknown(detailRecord.departments), "blue") },
      { title: "Seniority Levels", chips: chipList(stringsFromUnknown(detailRecord.seniority_levels), "green") },
    ],
  };
}

export function EventIntelligenceSection({
  detail,
  filteredItems: _filteredItems, // eslint-disable-line @typescript-eslint/no-unused-vars
  selectedClusterId,
  setSelectedClusterId: _setSelectedClusterId, // eslint-disable-line @typescript-eslint/no-unused-vars
  activeTab,
  setActiveTab,
  activeBranch,
  setActiveBranch,
  markers: _markers, // eslint-disable-line @typescript-eslint/no-unused-vars
  pendingClusterOpen,
  detailLoading,
  onChatContextChange,
  salesLeadById,
  onOpenClaimModal,
}: Props) {
  const [selectedGraphNodeId, setSelectedGraphNodeId] = useState("");
  const [selectedEntityId, setSelectedEntityId] = useState("");
  const [selectedSourceId, setSelectedSourceId] = useState("");
  const [graphExpanded, setGraphExpanded] = useState(false);
  const [graphInsightCard, setGraphInsightCard] = useState<InsightCardData | null>(null);

  const mergedEntities = useMemo<EntityRecord[]>(() => {
    const recsByEntity = new Map<string, EntityRecord["recommendations"]>();
    detail.recommendations.forEach((rec) => {
      const entityId = String(rec.cluster_entity_id ?? "");
      const current = recsByEntity.get(entityId) ?? [];
      current.push({
        role_track_type: String(rec.role_track_type ?? ""),
        role_confidence_score: Number(rec.role_confidence_score ?? 0) * 100,
        rationale: normalizeTextBlock(String(rec.rationale ?? "")),
        recommended_titles: parseJsonArray(rec.recommended_titles_json),
        departments: parseJsonArray(rec.departments_json),
        seniority_levels: parseJsonArray(rec.seniority_levels_json),
        hypothesized_services: parseJsonArray(rec.hypothesized_services_json),
      });
      recsByEntity.set(entityId, current);
    });

    return detail.entities
      .map((entity) => ({
        cluster_entity_id: String(entity.cluster_entity_id ?? ""),
        entity_name: String(entity.entity_name ?? "Unknown entity"),
        entity_type: String(entity.entity_type ?? ""),
        branch_type: String(entity.branch_type ?? "direct") as BranchKey,
        commercial_role: String(entity.commercial_role ?? ""),
        relationship_to_subject: normalizeTextBlock(String(entity.relationship_to_subject ?? "")),
        rationale: normalizeTextBlock(String(entity.rationale ?? "")),
        evidence_type: String(entity.evidence_type ?? ""),
        priority_score: Number(entity.priority_score ?? 0),
        confidence_score: Number(entity.confidence_score ?? 0),
        source_urls: parseJsonArray(entity.source_urls_json),
        source_snippets: parseJsonArray(entity.source_snippets_json),
        recommendations: recsByEntity.get(String(entity.cluster_entity_id ?? "")) ?? [],
      }))
      .sort((left, right) => right.priority_score - left.priority_score);
  }, [detail.entities, detail.recommendations]);

  const visibleEntities = useMemo(
    () => mergedEntities.filter((entity) => entity.branch_type === activeBranch),
    [activeBranch, mergedEntities],
  );

  const branchCounts = useMemo(() => {
    const counts: Record<BranchKey, number> = { direct: 0, peer: 0, ownership: 0 };
    mergedEntities.forEach((e) => { counts[e.branch_type] = (counts[e.branch_type] ?? 0) + 1; });
    return counts;
  }, [mergedEntities]);

  const selectedEntity = useMemo(
    () => visibleEntities.find((entity) => entity.cluster_entity_id === selectedEntityId) ?? visibleEntities[0] ?? null,
    [selectedEntityId, visibleEntities],
  );

  const groupedSources = useMemo(
    () =>
      detail.sources.reduce<Record<string, Record<string, unknown>[]>>((groups, source) => {
        const key = String(source.used_for ?? "supporting_context");
        groups[key] = groups[key] ?? [];
        groups[key].push(source);
        return groups;
      }, {}),
    [detail.sources],
  );

  const selectedClusterName = detail.cluster.subject_company_name;
  const selectedGraphNode = useMemo(
    () => detail.graph_nodes.find((item) => item.id === selectedGraphNodeId) ?? null,
    [detail.graph_nodes, selectedGraphNodeId],
  );
  const selectedSource = useMemo(
    () => detail.sources.find((item) => String(item.cluster_source_id ?? "") === selectedSourceId) ?? detail.sources[0] ?? null,
    [detail.sources, selectedSourceId],
  );
  const clusterInsightCard = useMemo(
    () => buildClusterInsight(detail.cluster, detail.sources.length, detail.entities.length),
    [detail],
  );

  useEffect(() => {
    setSelectedGraphNodeId(`cluster:${detail.cluster.cluster_id}`);
    setGraphInsightCard(clusterInsightCard);
  }, [clusterInsightCard, detail.cluster.cluster_id]);

  useEffect(() => {
    if (!visibleEntities.length) {
      if (selectedEntityId) setSelectedEntityId("");
      return;
    }
    if (!visibleEntities.some((entity) => entity.cluster_entity_id === selectedEntityId)) {
      setSelectedEntityId(visibleEntities[0].cluster_entity_id);
    }
  }, [selectedEntityId, visibleEntities]);

  useEffect(() => {
    if (!detail.sources.length) {
      if (selectedSourceId) setSelectedSourceId("");
      return;
    }
    if (!detail.sources.some((source) => String(source.cluster_source_id ?? "") === selectedSourceId)) {
      setSelectedSourceId(String(detail.sources[0].cluster_source_id ?? ""));
    }
  }, [detail.sources, selectedSourceId]);

  function handleGraphSelect(nodeId: string) {
    const node = detail.graph_nodes.find((item) => item.id === nodeId);
    if (!node) return;
    setSelectedGraphNodeId(nodeId);
    setGraphInsightCard(buildGraphInsight(node, detail, mergedEntities));
  }

  const assistantContext = useMemo(() => {
    const triggerLabel = formatLabel(detail.cluster.trigger_type);
    const branchLabel = formatLabel(activeBranch);
    const dateLabel = detail.cluster.event_date ? formatDate(detail.cluster.event_date) : "Date unavailable";
    const locationLabel = detail.cluster.subject_country ?? detail.cluster.subject_region ?? "Location unavailable";
    const topEntityNames = visibleEntities.slice(0, 3).map((entity) => entity.entity_name).filter(Boolean);
    const sourceHighlights = detail.sources
      .slice(0, 3)
      .map((source) => String(source.source_title ?? source.source_url ?? "Source"))
      .filter(Boolean);
    const sourcePublishers = [...new Set(detail.sources.map((source) => String(source.publisher ?? "")).filter(Boolean))].slice(0, 3);
    const commonPromptPrefix = [
      "You are assisting an analyst inside the Event Intelligence workspace.",
      `Selected opportunity cluster: ${selectedClusterName}.`,
      `Trigger type: ${triggerLabel}.`,
      `Location: ${locationLabel}.`,
      `Event date: ${dateLabel}.`,
      "Ground your response in the supplied opportunity data and clearly separate evidence from inference.",
    ].join("\n");

    if (activeTab === "cluster") {
      return {
        title: "Opportunity Cluster Review",
        description: selectedEntity
          ? `The analyst is reviewing ${selectedEntity.entity_name} inside the ${branchLabel.toLowerCase()} branch and wants help assessing the rationale and outreach angle.`
          : `The analyst is reviewing ${branchLabel.toLowerCase()} opportunity entities, their rationale, and the best outreach angles inside ${selectedClusterName}.`,
        promptPrefix: [
          commonPromptPrefix,
          "Current tab: Opportunity Cluster.",
          `Active branch: ${branchLabel}.`,
          `Visible entities in this branch: ${visibleEntities.length}.`,
          selectedEntity ? `Primary entity in focus: ${selectedEntity.entity_name} (${formatLabel(selectedEntity.entity_type)}).` : "No single entity is currently selected.",
          topEntityNames.length ? `Top visible entities: ${topEntityNames.join(", ")}.` : "No entities are currently visible in this branch.",
          "Help the analyst prioritize accounts, assess the rationale, and recommend next follow-up questions.",
        ].join("\n"),
        suggestedPrompts: [
          "Which entities in this branch look strongest and why?",
          "What outreach angle would you use for the top account?",
          "Where is the evidence thin or contradictory in this cluster?",
        ],
        chips: [branchLabel, selectedEntity?.entity_name ?? `${visibleEntities.length} visible entities`, `${detail.recommendations.length} recommendations`],
        footerMeta: [{ label: "Event date", value: dateLabel }],
      };
    }

    if (activeTab === "sources") {
      return {
        title: "Source Review",
        description: selectedSource
          ? `The analyst is validating ${String(selectedSource.source_title ?? selectedSource.source_url ?? "the selected source")} and wants help understanding what it actually proves.`
          : `The analyst is validating the evidence set for ${selectedClusterName} and wants help understanding what the sources actually prove.`,
        promptPrefix: [
          commonPromptPrefix,
          "Current tab: Sources.",
          `Available sources: ${detail.sources.length}.`,
          selectedSource ? `Selected source: ${String(selectedSource.source_title ?? selectedSource.source_url ?? "Unknown source")}.` : "No single source is currently selected.",
          sourceHighlights.length ? `Highlighted sources: ${sourceHighlights.join(" | ")}.` : "No source titles are currently available.",
          sourcePublishers.length ? `Publishers in view: ${sourcePublishers.join(", ")}.` : "Publisher metadata is limited.",
          "Focus on evidence quality, source credibility, corroboration, and what still needs verification.",
        ].join("\n"),
        suggestedPrompts: [
          "What do these sources actually confirm versus infer?",
          "Which source looks most important to review first?",
          "What follow-up research would strengthen this event assessment?",
        ],
        chips: [`${detail.sources.length} sources`, String(selectedSource?.source_title ?? sourcePublishers[0] ?? "Mixed publishers"), "Evidence review"],
        footerMeta: [{ label: "Event date", value: dateLabel }],
      };
    }

    /* MAP TAB — disabled for this pass
    if (activeTab === "map") {
      return {
        title: "Geographic Context",
        description: `The analyst is using the map to understand where ${selectedClusterName} sits relative to other opportunity clusters and branch activity.`,
        promptPrefix: [
          commonPromptPrefix,
          "Current tab: Map.",
          `Map markers loaded: ${markers.length}.`,
          `Active branch overlay: ${branchLabel}.`,
          "Use geographic context to explain concentration, adjacency, and what nearby clusters may imply for account coverage.",
        ].join("\n"),
        suggestedPrompts: [
          "What does the map suggest about geographic concentration?",
          "How should I interpret nearby clusters around this event?",
          "Which branch view is most useful to inspect next on the map?",
        ],
        chips: [locationLabel, `${markers.length} map markers`, `${branchLabel} overlay`],
        footerMeta: [{ label: "Event date", value: dateLabel }],
      };
    }
    */

    if (activeTab === "graph") {
      const graphFocusLabel = selectedGraphNode ? `${selectedGraphNode.label} (${formatLabel(selectedGraphNode.type)})` : "Cluster overview";
      return {
        title: "Relationship Graph",
        description: `The analyst is inspecting the relationship graph for ${selectedClusterName}${selectedGraphNode ? ` with focus on ${selectedGraphNode.label}` : ""}.`,
        promptPrefix: [
          commonPromptPrefix,
          "Current tab: Graph.",
          `Graph nodes: ${detail.graph_nodes.length}.`,
          `Graph edges: ${detail.graph_edges.length}.`,
          `Current graph focus: ${graphFocusLabel}.`,
          "Explain relationship paths, key nodes, and how the graph changes the commercial interpretation of the opportunity.",
        ].join("\n"),
        suggestedPrompts: [
          "What is the most meaningful relationship path in this graph?",
          "Why does this selected node matter commercially?",
          "What graph pattern should I investigate next?",
        ],
        chips: [`${detail.graph_nodes.length} nodes`, `${detail.graph_edges.length} edges`, graphFocusLabel],
        footerMeta: [{ label: "Event date", value: dateLabel }],
      };
    }

    return {
      title: "Event Brief",
      description: `The analyst is reviewing the event narrative, propagation thesis, and market framing for ${selectedClusterName}.`,
      promptPrefix: [
        commonPromptPrefix,
        "Current tab: Event Intelligence summary.",
        `Event summary: ${normalizeTextBlock(detail.cluster.event_summary ?? "Not available")}`,
        `Propagation thesis: ${normalizeTextBlock(detail.cluster.propagation_thesis ?? "Not available")}`,
        "Help the analyst understand what happened, why it matters commercially, and which questions deserve follow-up.",
      ].join("\n"),
      suggestedPrompts: [
        "What is the real commercial implication of this event?",
        "What are the strongest signals and biggest unknowns here?",
        "What should an analyst investigate next before acting on this cluster?",
      ],
      chips: [triggerLabel, locationLabel],
      footerMeta: [{ label: "Event date", value: dateLabel }],
    };
  }, [activeBranch, activeTab, detail, selectedClusterName, selectedEntity, selectedGraphNode, selectedSource, visibleEntities]);

  const chatRequestContext = useMemo<ChatRequestContext>(
    () => ({
      active_tab: activeTab,
      entity_id:
        activeTab === "cluster"
          ? selectedEntity?.cluster_entity_id
          : activeTab === "graph" && selectedGraphNode?.entity_id
            ? String(selectedGraphNode.entity_id)
            : undefined,
      source_id: activeTab === "sources" ? String(selectedSource?.cluster_source_id ?? "") || undefined : undefined,
      graph_node_id: activeTab === "graph" ? selectedGraphNodeId || undefined : undefined,
    }),
    [activeTab, selectedEntity, selectedGraphNode, selectedGraphNodeId, selectedSource],
  );

  useEffect(() => {
    onChatContextChange({
      selectedClusterId,
      selectedClusterName,
      contextTitle: assistantContext.title,
      contextDescription: assistantContext.description,
      contextPromptPrefix: assistantContext.promptPrefix,
      suggestedPrompts: assistantContext.suggestedPrompts,
      contextChips: assistantContext.chips,
      requestContext: chatRequestContext,
      defaultScope: selectedClusterId ? "selected_cluster" : "all",
      footerMeta: assistantContext.footerMeta,
    });
  }, [assistantContext, chatRequestContext, onChatContextChange, selectedClusterId, selectedClusterName]);

  return (
    <div className="eventPrimary">
        {pendingClusterOpen?.clusterId === selectedClusterId ? (
          <section className="panel detailCard navigationNoticeCard">
            <div className="navigationNoticeHeader">
              <span className="pill pill-blue">Opening event</span>
              {detailLoading ? <span className="chip chip-blue">Loading detail</span> : null}
            </div>
            <p className="bodyText">
              Bringing <strong>{pendingClusterOpen.label}</strong> into the Event Intelligence workspace.
            </p>
          </section>
        ) : null}

        {/* ── Condensed Hero ── */}
        <section className="eiHero">
          <div className="eiHeroMeta">
            <span className={`pill pill-${toneForTrigger(detail.cluster.trigger_type)}`}>{formatLabel(detail.cluster.trigger_type)}</span>
            {detail.cluster.subject_country ? <span className="chip">{detail.cluster.subject_country}</span> : null}
            {detail.cluster.event_date ? <span className="eiDate">{formatDate(detail.cluster.event_date)}</span> : null}
            {detail.cluster.best_route_to_market ? <span className="pill pill-outline">{detail.cluster.best_route_to_market}</span> : null}
          </div>
          <h1 className="eiHeadline">{detail.cluster.event_headline}</h1>
          <div className="eiHeroFooter">
            <span className="eiCompany">{detail.cluster.subject_company_name}</span>
            <div className="eiScoreStrip">
              <span className="eiScore eiScorePriority" title={SCORE_TOOLTIPS.priority}><i>PRI</i>{formatScore(detail.cluster.cluster_priority_score)} <small className="eiScoreBand">{scoreBand(detail.cluster.cluster_priority_score)}</small></span>
              <span className="eiScore eiScoreConfidence" title={SCORE_TOOLTIPS.confidence}><i>CON</i>{formatScore(detail.cluster.cluster_confidence_score)} <small className="eiScoreBand">{scoreBand(detail.cluster.cluster_confidence_score)}</small></span>
              <span className="eiScore eiScoreOpportunity" title={SCORE_TOOLTIPS.opportunity}><i>OPP</i>{formatScore(detail.cluster.opportunity_score)} <small className="eiScoreBand">{scoreBand(detail.cluster.opportunity_score)}</small></span>
              <span className="eiScore eiScoreCoverage"><i>SRC</i>{detail.sources.length}</span>
            </div>
          </div>
        </section>

        {/* ── Tab bar ── */}
        <section className="eiTabBar">
          {[
            { key: "event" as TabKey, label: "Event Brief", tooltip: "The trigger event that created this opportunity cluster — headline, summary, propagation thesis, and hypothesized service lines." },
            { key: "cluster" as TabKey, label: "Cluster", count: detail.entities.length, tooltip: "All companies in the opportunity cluster, grouped by their relationship to the trigger event: Direct subject, Peer competitors, or Ownership chain. Expand any entity to see engagement rationale, outreach angle, and hypothesized services." },
            { key: "sources" as TabKey, label: "Sources", count: detail.sources.length, tooltip: "The news articles, filings, and web sources the pipeline used to identify and validate this event." },
            // { key: "map" as TabKey, label: "Map" }, // MAP TAB — disabled for this pass
            { key: "graph" as TabKey, label: "Graph", tooltip: "Interactive relationship graph showing how the subject company connects to peer and ownership entities in this cluster. Click any node to inspect its context." },
          ].map((tab) => (
            <button key={tab.key} className="eiTab" data-active={activeTab === tab.key} title={tab.tooltip} onClick={() => setActiveTab(tab.key)}>
              {tab.label}
              {tab.count != null ? <span className="eiTabCount">{tab.count}</span> : null}
            </button>
          ))}
        </section>

        {activeTab === "event" ? (
          <section className="tabPanel eiEventPanel">
            <div className="panel eiNarrativeCard">
              <h2 className="eiSectionLabel">Event Summary</h2>
              <RichTextBlock className="bodyText narrativeText" text={normalizeTextBlock(detail.cluster.event_summary)} />
            </div>
            <div className="panel eiNarrativeCard">
              <h2 className="eiSectionLabel">Propagation Thesis</h2>
              <RichTextBlock className="bodyText" text={normalizeTextBlock(detail.cluster.propagation_thesis)} />
            </div>
            {(() => {
              const hypotheses = parseJsonArray(detail.cluster.service_hypotheses_json);
              return hypotheses.length ? (
                <div className="panel eiNarrativeCard">
                  <h2 className="eiSectionLabel">Hypothesized Services</h2>
                  <p className="bodyText bodyTextMuted" style={{ marginBottom: ".5rem" }}>Service lines the pipeline believes may be relevant based on the trigger event and entity landscape.</p>
                  <div className="chipWrap">{hypotheses.map((h) => <span key={h} className="chip chip-purple">{h}</span>)}</div>
                </div>
              ) : null;
            })()}
            <div className="eiContextGrid">
              <div className="eiContextItem">
                <span className="eiContextLabel">Company</span>
                <strong>{detail.cluster.subject_company_name}</strong>
              </div>
              <div className="eiContextItem">
                <span className="eiContextLabel">Country</span>
                <strong>{detail.cluster.subject_country ?? "Unknown"}</strong>
              </div>
              <div className="eiContextItem">
                <span className="eiContextLabel">Region</span>
                <strong>{detail.cluster.subject_region ?? "Unknown"}</strong>
              </div>
              <div className="eiContextItem">
                <span className="eiContextLabel">Trigger</span>
                <strong>{formatLabel(detail.cluster.trigger_type)}</strong>
              </div>
              <div className="eiContextItem">
                <span className="eiContextLabel">Subtype</span>
                <strong>{formatLabel(detail.cluster.trigger_subtype)}</strong>
              </div>
              <div className="eiContextItem">
                <span className="eiContextLabel">Best Route to Market</span>
                <strong>{detail.cluster.best_route_to_market ?? "Mixed"}</strong>
              </div>
              <div className="eiContextItem">
                <span className="eiContextLabel">Cluster Size</span>
                <strong>{detail.entities.length} entities · {detail.sources.length} sources</strong>
              </div>
              <div className="eiContextItem">
                <span className="eiContextLabel">Branch Composition</span>
                <strong>{branchCounts.direct} direct · {branchCounts.peer} peer · {branchCounts.ownership} ownership</strong>
              </div>
            </div>
            {detail.cluster.headline_source_url ? (
              <a className="sourceLink eiSourceLink" href={detail.cluster.headline_source_url} target="_blank" rel="noreferrer">
                Open headline source →
              </a>
            ) : null}
          </section>
        ) : null}

        {activeTab === "cluster" ? (
          <section className="tabPanel">
            {/* Branch switcher */}
            <div className="eiBranchBar">
              {([
                { key: "direct" as BranchKey, tooltip: "Direct — The primary subject of the trigger event (e.g. the company named in the lawsuit, acquisition, or regulatory action). Highest-confidence opportunity; this company is most immediately affected." },
                { key: "peer" as BranchKey, tooltip: "Peer — Competitors or sector peers of the direct subject. They face similar risk exposure or may benefit from the subject's situation, making them secondary but high-value targets." },
                { key: "ownership" as BranchKey, tooltip: "Ownership — Parent companies, subsidiaries, or significant shareholders in the ownership chain. Their exposure is indirect but can be material, especially for compliance, restructuring, or M&A-related events." },
              ]).map(({ key: branch, tooltip }) => (
                <button
                  key={branch}
                  className={`eiBranch eiBranch-${branch}`}
                  data-active={activeBranch === branch}
                  title={tooltip}
                  onClick={() => setActiveBranch(branch)}
                >
                  <span className="eiBranchDot" />
                  {formatLabel(branch)}
                  <span className="eiBranchCount">{branchCounts[branch]}</span>
                </button>
              ))}
            </div>

            {/* Entity accordion list */}
            <div className="eiEntityList">
              {visibleEntities.map((entity) => {
                const isOpen = selectedEntity?.cluster_entity_id === entity.cluster_entity_id;
                const topRec = entity.recommendations[0];
                const lead = salesLeadById[entity.cluster_entity_id] ?? syntheticLead(entity, detail.cluster);
                return (
                  <div key={entity.cluster_entity_id} className="eiEntity" data-open={isOpen}>
                    <button
                      className="eiEntityHeader"
                      onClick={() => setSelectedEntityId(entity.cluster_entity_id)}
                      aria-expanded={isOpen}
                    >
                      <div className="eiEntityHeaderLeft">
                        <strong className="eiEntityName">{entity.entity_name}</strong>
                        <div className="eiEntityTags">
                          <span className={`pill pill-${toneForBranch(entity.branch_type)}`}>{formatLabel(entity.branch_type)}</span>
                          <span className="pill pill-outline">{formatLabel(entity.entity_type)}</span>
                          {entity.evidence_type ? <span className="chip">{formatLabel(entity.evidence_type)}</span> : null}
                        </div>
                      </div>
                      <div className="eiEntityScores">
                        <span className="eiEntityScore" title={SCORE_TOOLTIPS.priority}><i>PRI</i>{formatScore(entity.priority_score)} <small className="eiScoreBand">{scoreBand(entity.priority_score)}</small></span>
                        <span className="eiEntityScore" title={SCORE_TOOLTIPS.confidence}><i>CON</i>{formatScore(entity.confidence_score)} <small className="eiScoreBand">{scoreBand(entity.confidence_score)}</small></span>
                      </div>
                      <button
                        className="eiClaimChip"
                        type="button"
                        onClick={(e) => { e.stopPropagation(); onOpenClaimModal(lead); }}
                      >
                        {lead.claim_id ? "Open" : "Claim"}
                      </button>
                      <span className="eiChevron">{isOpen ? "▾" : "▸"}</span>
                    </button>

                    {isOpen ? (
                      <div className="eiEntityBody">
                        <div className="eiEntityNarrative">
                          <div className="textSection">
                            <h2 className="eiSectionLabel">Relationship To Event</h2>
                            <RichTextBlock className="bodyText" text={entity.relationship_to_subject} />
                          </div>
                          <div className="textSection">
                            <h2 className="eiSectionLabel">Engagement Rationale</h2>
                            <RichTextBlock className="bodyText" text={entity.rationale} />
                          </div>
                          {topRec ? (
                            <div className="textSection eiOutreachAccent">
                              <h2 className="eiSectionLabel">Outreach Angle</h2>
                              <RichTextBlock className="bodyText" text={firstSentence(topRec.rationale)} />
                            </div>
                          ) : null}
                        </div>

                        {(() => {
                          const entityServices = topRec?.hypothesized_services ?? [];
                          const services = entityServices.length ? entityServices : parseJsonArray(detail.cluster.service_hypotheses_json);
                          return (
                            <div className="eiServicesPanel">
                              <div className="eiServicesPanelHeader">
                                <span className="eiServicesPanelLabel">Hypothesized Services</span>
                                <span className={`chip chip-${entity.branch_type === "direct" ? "blue" : entity.branch_type === "peer" ? "amber" : "green"}`}>{entity.branch_type}</span>
                              </div>
                              {services.length ? (
                                <div className="eiServicesList">
                                  {services.map((s) => (
                                    <span key={s} className="eiServiceItem">
                                      <span className="eiServiceDot" />
                                      {s}
                                    </span>
                                  ))}
                                </div>
                              ) : (
                                <p className="bodyText bodyTextMuted" style={{ fontSize: "0.82rem", margin: 0 }}>Run a new research cycle to generate entity-level service hypotheses.</p>
                              )}
                            </div>
                          );
                        })()}

                        {topRec ? (
                          <aside className="eiEntityOutreach">
                            <div className="eiOutreachHeader">
                              <strong>Titles &amp; Departments for Outreach</strong>
                              <span className="eiOutreachScore">{formatScore(topRec.role_confidence_score)}</span>
                            </div>
                            {topRec.recommended_titles.length ? (
                              <div className="eiOutreachGroup">
                                <span className="eiOutreachLabel">Target Titles</span>
                                <div className="chipWrap">{topRec.recommended_titles.map((t) => <span key={t} className="chip chip-purple">{t}</span>)}</div>
                              </div>
                            ) : null}
                            {topRec.departments.length ? (
                              <div className="eiOutreachGroup">
                                <span className="eiOutreachLabel">Target Departments</span>
                                <div className="chipWrap">{topRec.departments.map((d) => <span key={d} className="chip chip-blue">{d}</span>)}</div>
                              </div>
                            ) : null}
                            {topRec.seniority_levels.length ? (
                              <div className="eiOutreachGroup">
                                <span className="eiOutreachLabel">Seniority Levels</span>
                                <div className="chipWrap">{topRec.seniority_levels.map((l) => <span key={l} className="chip chip-green">{l}</span>)}</div>
                              </div>
                            ) : null}
                          </aside>
                        ) : null}

                        {salesLeadById[entity.cluster_entity_id] ? (
                          <div className="eiEntityAction">
                            <button className="secondaryButton" type="button" onClick={() => onOpenClaimModal(salesLeadById[entity.cluster_entity_id])}>
                              {salesLeadById[entity.cluster_entity_id].claim_id ? "Open sales workspace" : "Claim for sales"}
                            </button>
                          </div>
                        ) : (
                          <div className="eiEntityAction">
                            <button className="secondaryButton" type="button" onClick={() => onOpenClaimModal(lead)}>
                              Claim for sales
                            </button>
                          </div>
                        )}
                      </div>
                    ) : null}
                  </div>
                );
              })}
              {!visibleEntities.length ? (
                <div className="panel detailCard">
                  <h3 className="panelTitle panelTitleMedium">No entities in this branch</h3>
                  <p className="bodyText bodyTextMuted">There are no {activeBranch} entities for the selected opportunity cluster.</p>
                </div>
              ) : null}
            </div>
          </section>
        ) : null}

        {activeTab === "sources" ? (
          <section className="tabPanel">
            {Object.entries(groupedSources).map(([usedFor, sources]) => (
              <div key={usedFor} className="eiSourceGroup">
                <h2 className="eiSectionLabel">{formatLabel(usedFor)}</h2>
                <div className="eiSourceList">
                  {sources.map((source, index) => {
                    const sid = String(source.cluster_source_id ?? index);
                    const isActive = sid === String(selectedSource?.cluster_source_id ?? "");
                    const snippet = String(source.text_snippet ?? source.source_snippet ?? "");
                    return (
                      <article
                        key={sid}
                        className="eiSourceCard"
                        data-active={isActive}
                        onClick={() => setSelectedSourceId(sid)}
                        onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setSelectedSourceId(sid); } }}
                        role="button"
                        tabIndex={0}
                      >
                        <div className="eiSourceMeta">
                          <strong className="eiSourceTitle">{String(source.source_title ?? source.source_url ?? "Source")}</strong>
                          <span className="eiSourcePub">{String(source.publisher ?? "Unknown publisher")} · {source.published_at ? formatDate(String(source.published_at)) : "Date unavailable"}</span>
                        </div>
                        {snippet ? <p className="eiSourceSnippet">{snippet}</p> : null}
                        <div className="eiSourceFooter">
                          <span className="chip">{formatLabel(usedFor)}</span>
                          {source.source_url ? <a className="sourceLink" href={String(source.source_url)} target="_blank" rel="noreferrer" onClick={(e) => e.stopPropagation()}>Open source →</a> : null}
                        </div>
                      </article>
                    );
                  })}
                </div>
              </div>
            ))}
          </section>
        ) : null}

        {/* MAP TAB — disabled for this pass
        {activeTab === "map" ? (
          <section className="tabPanel">
            <OpportunityMap
              markers={markers}
              items={filteredItems}
              selectedClusterId={selectedClusterId}
              selectedCluster={detail.cluster}
              focusEntities={mergedEntities}
              onSelectCluster={setSelectedClusterId}
              onSelectBranch={setActiveBranch}
            />
          </section>
        ) : null}
        */}

        {activeTab === "graph" ? (
          <section className="tabPanel">
            <div className="graphToolbar">
              <div className="semanticLegend">
                {branchLegendItems.map((item) => (
                  <span key={item.key}><i style={{ background: item.color }} />{item.label}</span>
                ))}
              </div>
              <button className="graphExpandButton" onClick={() => setGraphExpanded((current) => !current)}>
                {graphExpanded ? "Standard view" : "Expanded view"}
              </button>
            </div>

            <OpportunityGraph nodes={detail.graph_nodes} edges={detail.graph_edges} selectedNodeId={selectedGraphNodeId} expanded={graphExpanded} onSelectNode={handleGraphSelect} />

            <SelectionInfoCard data={graphInsightCard} emptyTitle="Graph Detail" emptyBody="Click a graph node to inspect the direct, peer, ownership, or role-track context." sticky={false} />
          </section>
        ) : null}
      </div>

  );
}
