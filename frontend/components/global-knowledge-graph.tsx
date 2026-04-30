"use client";

import dynamic from "next/dynamic";
import { useCallback, useEffect, useMemo, useState, type CSSProperties } from "react";
import { Background, Controls, Handle, MiniMap, Position, ReactFlowProvider, type Edge, type NodeProps } from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { RichTextBlock } from "@/components/rich-text-block";
import { SelectionInfoCard } from "@/components/selection-info-card";
import { formatDate, formatLabel, formatScore } from "@/lib/formatters";
import type {
  KnowledgeGraphCountrySummary,
  KnowledgeGraphDistributionItem,
  KnowledgeGraphEventSummary,
  KnowledgeGraphRegionSummary,
  KnowledgeGraphResponse,
} from "@/types/api";
import type { SharedChatContext } from "@/types/app-shell";
import type { InsightCardData, InsightTone } from "@/types/view";

function insightTone(tone: string): InsightTone {
  if (tone === "blue" || tone === "green" || tone === "amber" || tone === "purple" || tone === "red" || tone === "neutral") {
    return tone;
  }
  return "neutral";
}

const ReactFlow = dynamic(() => import("@xyflow/react").then((m) => m.ReactFlow), { ssr: false });

/* ─── Scope state ─── */

type GraphScope =
  | { level: "global" }
  | { level: "region"; region: string }
  | { level: "country"; region: string; country: string };

/* ─── Tree layout helper ─── */

const NODE_W = 260;
const NODE_H = 120;
const LEVEL_GAP = 200;

/** Centers children under a parent; returns {id, position}[] */
function treeLayout(
  rootId: string,
  childIds: string[],
  canvasWidth: number,
): { id: string; x: number; y: number }[] {
  const rootX = canvasWidth / 2 - NODE_W / 2;
  const rootY = 60;
  const result = [{ id: rootId, x: rootX, y: rootY }];

  const count = childIds.length;
  if (count === 0) return result;

  const gap = 32;
  const totalW = count * NODE_W + (count - 1) * gap;
  const startX = canvasWidth / 2 - totalW / 2;
  const childY = rootY + NODE_H + LEVEL_GAP;

  childIds.forEach((id, i) => {
    result.push({ id, x: startX + i * (NODE_W + gap), y: childY });
  });

  return result;
}

/* ─── ReactFlow custom node ─── */

const NODE_STYLES: Record<string, { bg: string; border: string; glow: string; accent: string }> = {
  root: { bg: "var(--panel)", border: "var(--accent)", glow: "rgba(63,146,255,0.22)", accent: "var(--accent)" },
  region: { bg: "var(--panel-2)", border: "var(--branch-direct)", glow: "rgba(90,167,255,0.14)", accent: "var(--branch-direct)" },
  country: { bg: "var(--panel-2)", border: "var(--peer)", glow: "rgba(217,166,59,0.14)", accent: "var(--peer)" },
  cluster: { bg: "var(--panel-2)", border: "var(--graph-role-track)", glow: "rgba(105,67,197,0.16)", accent: "var(--graph-role-track)" },
};

function KnowledgeGraphNode({ data }: NodeProps) {
  const d = data as { title: string; subtitle: string; meta: string; kind: string; drillLabel?: string };
  const s = NODE_STYLES[d.kind] ?? NODE_STYLES.region;
  const isRoot = d.kind === "root";

  const style: CSSProperties = {
    background: s.bg,
    border: `2px solid ${s.border}`,
    borderRadius: 16,
    padding: isRoot ? "20px 28px" : "18px 24px",
    width: isRoot ? 300 : NODE_W,
    boxShadow: `0 6px 32px ${s.glow}`,
    cursor: "pointer",
  };

  const handleStyle: CSSProperties = { background: s.border, width: 8, height: 8, border: "none" };

  return (
    <div className="gkgNode" style={style}>
      <Handle type="target" position={Position.Top} style={{ ...handleStyle, opacity: isRoot ? 0 : 1 }} />
      <span className="gkgNodeEyebrow">{d.meta}</span>
      <span className={isRoot ? "gkgNodeTitleLg" : "gkgNodeTitle"}>{d.title}</span>
      <span className="gkgNodeSubtitle">{d.subtitle}</span>
      {d.drillLabel && <span className="gkgNodeDrill">{d.drillLabel}</span>}
      <Handle type="source" position={Position.Bottom} style={handleStyle} />
    </div>
  );
}

/* ─── Insight card builders ─── */

function buildGlobalInsight(graph: KnowledgeGraphResponse): InsightCardData {
  return {
    eyebrow: "Global Intelligence Lens",
    title: "Global Event Signal Map",
    subtitle: `${graph.event_count} events across ${graph.region_count} regions and ${graph.country_count} countries`,
    badges: [
      { label: `${graph.region_count} Regions`, tone: "blue" },
      { label: `${graph.country_count} Countries`, tone: "neutral" },
      { label: `${graph.event_count} Events`, tone: "green" },
    ],
    metrics: [],
    sections: graph.regions.map((region) => ({
      title: region.label,
      text: region.narrative,
      chips: region.dominant_triggers.map((t) => ({ label: `${t.label} (${t.count})`, tone: insightTone(t.tone) })),
    })),
  };
}

function buildRegionInsight(region: KnowledgeGraphRegionSummary): InsightCardData {
  return {
    eyebrow: "Regional Narrative",
    title: region.label,
    subtitle: `${region.event_count} events across ${region.country_count} countries`,
    badges: region.dominant_triggers.map((t) => ({ label: `${t.label} (${t.count})`, tone: insightTone(t.tone) })),
    metrics: [
      { label: "Countries", value: String(region.country_count), tone: "blue" },
      { label: "Companies", value: String(region.company_count), tone: "neutral" },
      { label: "Avg Opportunity", value: String(formatScore(region.average_opportunity)), tone: "green" },
      { label: "Avg Confidence", value: String(formatScore(region.average_confidence)), tone: "blue" },
    ],
    sections: [{ title: "IDC Regional Narrative", text: region.narrative }],
  };
}

function buildCountryInsight(country: KnowledgeGraphCountrySummary): InsightCardData {
  return {
    eyebrow: `${country.region_id} / Country Detail`,
    title: country.label,
    subtitle: `${country.event_count} events · ${country.company_count} companies`,
    badges: country.dominant_triggers.map((t) => ({ label: `${t.label} (${t.count})`, tone: insightTone(t.tone) })),
    metrics: [
      { label: "Events", value: String(country.event_count), tone: "blue" },
      { label: "Companies", value: String(country.company_count), tone: "neutral" },
      { label: "Avg Opportunity", value: String(formatScore(country.average_opportunity)), tone: "green" },
      { label: "Avg Confidence", value: String(formatScore(country.average_confidence)), tone: "blue" },
    ],
    sections: [
      { title: "Country Narrative", text: country.narrative },
      {
        title: "Top Companies",
        chips: country.top_companies.map((name) => ({ label: name, tone: "neutral" as const })),
      },
    ],
  };
}

function buildEventInsight(event: KnowledgeGraphEventSummary, regionId: string, countryLabel: string): InsightCardData {
  return {
    eyebrow: `${regionId} / ${countryLabel} / Event`,
    title: event.subject_company_name,
    subtitle: `${formatLabel(event.trigger_type)} · ${formatDate(event.event_date)}`,
    badges: [
      { label: formatLabel(event.trigger_type), tone: "blue" },
      ...(event.subject_country ? [{ label: event.subject_country, tone: "neutral" as const }] : []),
    ],
    metrics: [
      { label: "Opportunity", value: String(formatScore(event.opportunity_score)), tone: "green" },
      { label: "Confidence", value: String(formatScore(event.cluster_confidence_score)), tone: "blue" },
    ],
    sections: [
      { title: "Event Summary", text: event.event_summary ?? "No summary available yet." },
      ...(event.headline_source_url
        ? [{ title: "Source", linkHref: event.headline_source_url, linkLabel: "Open source article" }]
        : []),
    ],
  };
}

/* ─── Distribution donut helper ─── */

function distributionBackground(triggers: KnowledgeGraphDistributionItem[]): string {
  if (!triggers.length) return "var(--border)";
  const total = triggers.reduce((sum, t) => sum + t.count, 0) || 1;
  const palette: Record<string, string> = {
    positive: "var(--ownership)",
    negative: "var(--accent-2)",
    neutral: "var(--muted)",
    info: "var(--accent)",
    warning: "var(--peer)",
  };
  let angle = 0;
  const stops = triggers.map((t) => {
    const color = palette[t.tone] ?? "var(--muted)";
    const start = angle;
    angle += (t.count / total) * 360;
    return `${color} ${start}deg ${angle}deg`;
  });
  return `conic-gradient(${stops.join(", ")})`;
}

/* ─── Props ─── */

type Props = {
  graph: KnowledgeGraphResponse | null;
  onOpenCluster: (clusterId: string) => void;
  onChatContextChange: (ctx: SharedChatContext) => void;
};

/* ─── Canvas sizing — estimate from typical panel width ─── */
const CANVAS_W = 1200;

/* ─── Component ─── */

export function GlobalKnowledgeGraph({ graph, onOpenCluster, onChatContextChange }: Props) {
  const [scope, setScope] = useState<GraphScope>({ level: "global" });
  const [selectedNodeId, setSelectedNodeId] = useState("");

  const regions = graph?.regions ?? [];

  const selectedRegion = useMemo<KnowledgeGraphRegionSummary | null>(() => {
    if (scope.level === "global") return null;
    return regions.find((r) => r.region_id === scope.region) ?? null;
  }, [regions, scope]);

  const countries = useMemo<KnowledgeGraphCountrySummary[]>(() => selectedRegion?.countries ?? [], [selectedRegion]);

  const selectedCountry = useMemo<KnowledgeGraphCountrySummary | null>(() => {
    if (scope.level !== "country") return null;
    return countries.find((c) => c.country_id === scope.country) ?? null;
  }, [countries, scope]);

  const visibleEvents = useMemo<KnowledgeGraphEventSummary[]>(() => selectedCountry?.events ?? [], [selectedCountry]);

  /* ── ReactFlow nodes with proper tree layout ── */
  const graphNodes = useMemo(() => {
    if (!graph) return [];

    if (scope.level === "global") {
      const childIds = regions.map((r) => `region:${r.region_id}`);
      const positions = treeLayout("global-root", childIds, CANVAS_W);
      const posMap = new Map(positions.map((p) => [p.id, { x: p.x, y: p.y }]));

      return [
        {
          id: "global-root",
          type: "knowledgeNode",
          position: posMap.get("global-root")!,
          data: {
            title: "Global Event Signal Map",
            subtitle: `${graph.event_count} events across ${graph.region_count} regions`,
            meta: "IDC GLOBAL LENS",
            kind: "root",
          },
        },
        ...regions.map((region) => ({
          id: `region:${region.region_id}`,
          type: "knowledgeNode",
          position: posMap.get(`region:${region.region_id}`) ?? { x: 0, y: 0 },
          data: {
            title: region.label,
            subtitle: `${region.event_count} events · ${region.country_count} countries`,
            meta: region.dominant_triggers[0] ? `LEAD: ${region.dominant_triggers[0].label}` : "MIXED SIGNALS",
            kind: "region" as const,
            drillLabel: `Click to explore ${region.country_count} countries →`,
          },
        })),
      ];
    }

    if (scope.level === "region" && selectedRegion) {
      const rootId = `region:${selectedRegion.region_id}`;
      const childIds = countries.map((c) => `country:${c.country_id}`);
      const positions = treeLayout(rootId, childIds, CANVAS_W);
      const posMap = new Map(positions.map((p) => [p.id, { x: p.x, y: p.y }]));

      return [
        {
          id: rootId,
          type: "knowledgeNode",
          position: posMap.get(rootId)!,
          data: {
            title: selectedRegion.label,
            subtitle: `${selectedRegion.country_count} countries · ${selectedRegion.event_count} events`,
            meta: "REGIONAL DRILLDOWN",
            kind: "root",
          },
        },
        ...countries.map((country) => ({
          id: `country:${country.country_id}`,
          type: "knowledgeNode",
          position: posMap.get(`country:${country.country_id}`) ?? { x: 0, y: 0 },
          data: {
            title: country.label,
            subtitle: `${country.event_count} events · ${country.company_count} companies`,
            meta: country.dominant_triggers[0] ? `LEAD: ${country.dominant_triggers[0].label}` : "MIXED SIGNALS",
            kind: "country" as const,
            drillLabel: `Click to explore ${country.event_count} events →`,
          },
        })),
      ];
    }

    if (scope.level === "country" && selectedCountry) {
      const rootId = `country:${selectedCountry.country_id}`;
      const childIds = visibleEvents.map((e) => `cluster:${e.cluster_id}`);
      const positions = treeLayout(rootId, childIds, CANVAS_W);
      const posMap = new Map(positions.map((p) => [p.id, { x: p.x, y: p.y }]));

      return [
        {
          id: rootId,
          type: "knowledgeNode",
          position: posMap.get(rootId)!,
          data: {
            title: selectedCountry.label,
            subtitle: `${selectedCountry.events.length} event clusters`,
            meta: `${selectedCountry.region_id} · COUNTRY VIEW`,
            kind: "root",
          },
        },
        ...visibleEvents.map((event) => ({
          id: `cluster:${event.cluster_id}`,
          type: "knowledgeNode",
          position: posMap.get(`cluster:${event.cluster_id}`) ?? { x: 0, y: 0 },
          data: {
            title: event.subject_company_name,
            subtitle: `${formatLabel(event.trigger_type)} · ${formatDate(event.event_date)}`,
            meta: `SCORE: ${formatScore(event.opportunity_score)}`,
            kind: "cluster" as const,
            drillLabel: "Click to inspect →",
          },
        })),
      ];
    }

    return [];
  }, [countries, graph, regions, scope, selectedCountry, selectedRegion, visibleEvents]);

  /* ── ReactFlow edges ── */
  const graphEdges = useMemo<Edge[]>(() => {
    if (!graph) return [];
    if (scope.level === "global") {
      return regions.map((region) => ({
        id: `edge:global:${region.region_id}`,
        source: "global-root",
        target: `region:${region.region_id}`,
        type: "smoothstep",
        style: { stroke: "#5aa7ff", strokeWidth: 2.5 },
        animated: true,
      }));
    }
    if (scope.level === "region" && selectedRegion) {
      return countries.map((country) => ({
        id: `edge:${selectedRegion.region_id}:${country.country_id}`,
        source: `region:${selectedRegion.region_id}`,
        target: `country:${country.country_id}`,
        type: "smoothstep",
        style: { stroke: "#efc159", strokeWidth: 2.5 },
        animated: true,
      }));
    }
    if (scope.level === "country" && selectedCountry) {
      return visibleEvents.map((event) => ({
        id: `edge:${selectedCountry.country_id}:${event.cluster_id}`,
        source: `country:${selectedCountry.country_id}`,
        target: `cluster:${event.cluster_id}`,
        type: "smoothstep",
        style: { stroke: "#9f7aea", strokeWidth: 2 },
        animated: true,
      }));
    }
    return [];
  }, [countries, graph, regions, scope, selectedCountry, selectedRegion, visibleEvents]);

  /* ── Selected event / insight ── */
  const selectedEvent = useMemo(() => {
    if (!selectedNodeId.startsWith("cluster:")) return null;
    const clusterId = selectedNodeId.replace("cluster:", "");
    return visibleEvents.find((event) => event.cluster_id === clusterId) ?? null;
  }, [selectedNodeId, visibleEvents]);

  const selectedInsight = useMemo<InsightCardData | null>(() => {
    if (!graph) return null;
    if (selectedEvent && selectedCountry) {
      return buildEventInsight(selectedEvent, selectedCountry.region_id, selectedCountry.label);
    }
    if (scope.level === "country" && selectedCountry) return buildCountryInsight(selectedCountry);
    if (scope.level === "region" && selectedRegion) return buildRegionInsight(selectedRegion);
    return buildGlobalInsight(graph);
  }, [graph, scope, selectedCountry, selectedEvent, selectedRegion]);

  /* ── Chat context ── */
  const chatContext = useMemo(() => {
    if (selectedCountry) {
      return {
        title: "Country Research Context",
        description: `Reviewing ${selectedCountry.label} inside ${selectedCountry.region_id}.`,
        promptPrefix: [
          "You are assisting an analyst inside the Global Knowledge Graph workspace.",
          `Focused region: ${selectedCountry.region_id}.`,
          `Focused country: ${selectedCountry.label}.`,
          `Country narrative: ${selectedCountry.narrative}`,
          "Use the geography knowledge-base documents to explain country patterns, compare local events, and identify follow-up questions grounded in the available evidence.",
        ].join("\n"),
        suggestedPrompts: [
          `What does the event mix suggest about ${selectedCountry.label}?`,
          `Which companies in ${selectedCountry.label} look most important?`,
          `How does ${selectedCountry.label} compare with ${selectedCountry.region_id}?`,
        ],
        chips: [selectedCountry.region_id, selectedCountry.label, `${selectedCountry.event_count} events`],
      };
    }

    if (selectedRegion) {
      return {
        title: "Regional Research Context",
        description: `Reviewing ${selectedRegion.label} — comparing countries, triggers, and outlook.`,
        promptPrefix: [
          "You are assisting an analyst inside the Global Knowledge Graph workspace.",
          `Focused region: ${selectedRegion.label}.`,
          `Regional narrative: ${selectedRegion.narrative}`,
          "Use the geography knowledge-base documents to compare countries, identify regional patterns, and explain what the current research suggests for IDC priorities.",
        ].join("\n"),
        suggestedPrompts: [
          `What are the strongest patterns inside ${selectedRegion.label}?`,
          `Which countries in ${selectedRegion.label} should I drill into?`,
          `What does the trigger mix suggest for ${selectedRegion.label}?`,
        ],
        chips: [selectedRegion.label, `${selectedRegion.country_count} countries`, `${selectedRegion.event_count} events`],
      };
    }

    return {
      title: "Global Research Context",
      description: "Reviewing the full geography graph — comparing regional narratives and emerging patterns.",
      promptPrefix: [
        "You are assisting an analyst inside the Global Knowledge Graph workspace.",
        "The analyst is at the global view and wants a geography-first readout across the knowledge base.",
        "Use the regional and country knowledge-base documents to compare regions, highlight emerging patterns, and identify where IDC should focus next.",
      ].join("\n"),
      suggestedPrompts: [
        "Which regions look most active right now and why?",
        "How do the regional narratives differ across the corpus?",
        "Where should I drill down next based on the global picture?",
      ],
      chips: [`${graph?.region_count ?? 0} regions`, `${graph?.country_count ?? 0} countries`, `${graph?.event_count ?? 0} events`],
    };
  }, [graph?.country_count, graph?.event_count, graph?.region_count, selectedCountry, selectedRegion]);

  useEffect(() => {
    onChatContextChange({
      selectedClusterId: selectedEvent?.cluster_id ?? "",
      selectedClusterName: selectedEvent?.subject_company_name ?? "",
      contextTitle: chatContext.title,
      contextDescription: chatContext.description,
      contextPromptPrefix: chatContext.promptPrefix,
      suggestedPrompts: chatContext.suggestedPrompts,
      contextChips: chatContext.chips,
      requestContext: {
        active_tab: "global_graph",
        region_id: selectedCountry?.region_id ?? selectedRegion?.region_id,
        country_id: selectedCountry?.country_id,
      },
      defaultScope: "all",
    });
  }, [chatContext, onChatContextChange, selectedCountry, selectedEvent, selectedRegion]);

  /* ── Drill handler — click a node to drill AND select ── */
  const handleNodeClick = useCallback(
    (_: unknown, node: { id: string }) => {
      const nodeId = node.id;
      setSelectedNodeId(nodeId);

      // Drill into region
      if (nodeId.startsWith("region:")) {
        setScope({ level: "region", region: nodeId.replace("region:", "") });
        return;
      }
      // Drill into country
      if (nodeId.startsWith("country:") && selectedRegion) {
        setScope({ level: "country", region: selectedRegion.region_id, country: nodeId.replace("country:", "") });
        return;
      }
      // Open event in Event Intelligence
      if (nodeId.startsWith("cluster:")) {
        const clusterId = nodeId.replace("cluster:", "");
        const evt = visibleEvents.find((e) => e.cluster_id === clusterId);
        if (evt) {
          onOpenCluster(evt.cluster_id);
        }
      }
    },
    [selectedRegion, visibleEvents, onOpenCluster],
  );

  /* ── Scope label helpers ── */
  const scopeLabel = scope.level === "global" ? "Regions" : scope.level === "region" ? "Countries" : "Events";
  const scopeCount =
    scope.level === "global"
      ? regions.length
      : scope.level === "region"
        ? countries.length
        : visibleEvents.length;

  /* ── Loading ── */
  if (!graph) {
    return (
      <section className="tabPanel">
        <div className="panel gkgLoadingCard">
          <div className="gkgLoadingPulse" />
          <div>
            <h2 className="panelTitle">Building Knowledge Graph</h2>
            <p className="bodyText bodyTextMuted">Generating regional hierarchy and narrative summaries from the current research corpus…</p>
          </div>
        </div>
      </section>
    );
  }

  /* ── Main render ── */
  return (
    <section className="tabPanel">
      {/* ── Header bar ── */}
      <header className="gkgHeader">
        <div className="gkgHeaderLeft">
          <h1 className="gkgTitle">Global Knowledge Graph</h1>
          <div className="gkgHeaderStats">
            <span className="gkgStat">
              <span className="gkgStatValue">{graph.region_count}</span> regions
            </span>
            <span className="gkgStatDivider" />
            <span className="gkgStat">
              <span className="gkgStatValue">{graph.country_count}</span> countries
            </span>
            <span className="gkgStatDivider" />
            <span className="gkgStat">
              <span className="gkgStatValue">{graph.event_count}</span> events
            </span>
          </div>
        </div>
        {scope.level === "country" && selectedEvent && (
          <button className="primaryButton gkgOpenEvent" onClick={() => onOpenCluster(selectedEvent.cluster_id)}>
            Open in Event Intelligence →
          </button>
        )}
      </header>

      {/* ── Breadcrumbs ── */}
      <nav className="gkgBreadcrumbs" aria-label="Graph navigation">
        <button
          className={`gkgBreadcrumbItem ${scope.level === "global" ? "gkgBreadcrumbActive" : ""}`}
          onClick={() => { setScope({ level: "global" }); setSelectedNodeId(""); }}
        >
          ◉ Global
        </button>
        {scope.level !== "global" && selectedRegion && (
          <>
            <span className="gkgBreadcrumbSep">›</span>
            <button
              className={`gkgBreadcrumbItem ${scope.level === "region" ? "gkgBreadcrumbActive" : ""}`}
              onClick={() => { setScope({ level: "region", region: selectedRegion.region_id }); setSelectedNodeId(""); }}
            >
              {selectedRegion.label}
            </button>
          </>
        )}
        {scope.level === "country" && selectedCountry && (
          <>
            <span className="gkgBreadcrumbSep">›</span>
            <span className="gkgBreadcrumbItem gkgBreadcrumbActive">{selectedCountry.label}</span>
          </>
        )}
        <span className="gkgBreadcrumbScope">
          {scopeLabel}: {scopeCount}
        </span>
      </nav>

      <div className="eventPrimary">
          {/* ── Graph canvas ── */}
          <div className="panel gkgCanvasPanel">
            <div className="gkgCanvasHeader">
              <div>
                <h2 className="panelTitle">Hierarchy Graph</h2>
                <p className="bodyText bodyTextMuted">Click any node to drill down into it. Use breadcrumbs to navigate back up.</p>
              </div>
              <span className="gkgViewBadge">
                {scope.level === "global" ? "Region view" : scope.level === "region" ? "Country view" : "Event view"}
              </span>
            </div>
            <div className="gkgCanvas">
              <ReactFlowProvider>
                <ReactFlow
                  key={`${scope.level}-${scope.level !== "global" ? ("region" in scope ? scope.region : "") : ""}`}
                  nodes={graphNodes}
                  edges={graphEdges}
                  nodeTypes={{ knowledgeNode: KnowledgeGraphNode }}
                  fitView
                  fitViewOptions={{ padding: 0.25 }}
                  minZoom={0.3}
                  maxZoom={2.5}
                  nodesConnectable={false}
                  nodesDraggable
                  onNodeClick={handleNodeClick}
                  defaultEdgeOptions={{ type: "smoothstep" }}
                >
                  <MiniMap
                    style={{ background: "#0a1020", borderRadius: 10, border: "1px solid rgba(255,255,255,0.08)" }}
                    maskColor="rgba(0,0,0,0.55)"
                    nodeColor="#5aa7ff"
                  />
                  <Controls
                    style={{ background: "var(--panel)", border: "1px solid var(--border)", borderRadius: 10 }}
                  />
                  <Background color="#1e2d42" gap={32} size={1.5} />
                </ReactFlow>
              </ReactFlowProvider>
            </div>
          </div>

          {/* ── Insight card ── */}
          <SelectionInfoCard
            data={selectedInsight}
            emptyTitle="Narrative Insight"
            emptyBody="Select a region, country, or event to inspect the evidence-backed narrative."
            sticky={false}
          />

          {/* ── Summary grid ── */}
          <div className="gkgSummarySection">
            <div className="gkgSummarySectionHeader">
              <h2 className="panelTitle">{scopeLabel} Overview</h2>
              <span className="bodyText bodyTextMuted">{scopeCount} items in current scope</span>
            </div>
            <div className="gkgSummaryGrid">
              {(scope.level === "global" ? regions : scope.level === "region" ? countries : visibleEvents).map((entry) => {
                if ("cluster_id" in entry) {
                  return (
                    <article key={entry.cluster_id} className="panel gkgSummaryCard gkgSummaryCardEvent">
                      <div className="gkgSummaryCardHeader">
                        <div>
                          <h3 className="gkgSummaryCardTitle">{entry.subject_company_name}</h3>
                          <p className="gkgSummaryCardMeta">
                            {formatLabel(entry.trigger_type)} · {entry.subject_country ?? "Unknown"} · {formatDate(entry.event_date)}
                          </p>
                        </div>
                        <span className="gkgScoreBadge">{formatScore(entry.opportunity_score)}</span>
                      </div>
                      <RichTextBlock className="bodyText bodyTextMuted gkgSummaryNarrative" text={entry.event_summary ?? "No event summary available yet."} />
                      <div className="gkgSummaryCardActions">
                        <button className="secondaryButton" onClick={() => setSelectedNodeId(`cluster:${entry.cluster_id}`)}>
                          Inspect
                        </button>
                        <button className="primaryButton" onClick={() => onOpenCluster(entry.cluster_id)}>
                          Open event →
                        </button>
                      </div>
                    </article>
                  );
                }

                const isRegion = "countries" in entry;
                return (
                  <article key={isRegion ? entry.region_id : entry.country_id} className="panel gkgSummaryCard">
                    <div className="gkgSummaryCardHeader">
                      <div>
                        <h3 className="gkgSummaryCardTitle">{entry.label}</h3>
                        <p className="gkgSummaryCardMeta">
                          {entry.event_count} events · {isRegion ? `${entry.country_count} countries` : `${entry.company_count} companies`}
                        </p>
                      </div>
                      <div className="gkgDonut" style={{ background: distributionBackground(entry.dominant_triggers) }} />
                    </div>
                    <RichTextBlock className="bodyText bodyTextMuted gkgSummaryNarrative" text={entry.narrative} />
                    <div className="chipWrap">
                      {entry.dominant_triggers.map((trigger) => (
                        <span key={`${entry.label}-${trigger.label}`} className={`chip chip-${trigger.tone}`}>
                          {trigger.label} ({trigger.count})
                        </span>
                      ))}
                    </div>
                    <div className="gkgSummaryCardActions">
                      <button
                        className="gkgDrillButton"
                        onClick={() =>
                          isRegion
                            ? setScope({ level: "region", region: entry.region_id })
                            : setScope({ level: "country", region: entry.region_id, country: entry.country_id })
                        }
                      >
                        {isRegion ? "Drill into countries →" : "Drill into events →"}
                      </button>
                    </div>
                  </article>
                );
              })}
            </div>
          </div>
      </div>
    </section>
  );
}
