"use client";

import { useCallback, useDeferredValue, useEffect, useMemo, useState } from "react";

import { cancelGenerationRun, cleanupKnowledgeBase, commitChatToKnowledgeBase, createGenerationRun, getKnowledgeGraph, getMapMarkers, getOpportunities, getOpportunityDetailWithSignal, getPipelineSettings, listGenerationRuns, patchPipelineSettings, syncKnowledgeBase } from "@/lib/api";
import { formatLabel, normalizeTextBlock, parseJsonArray } from "@/lib/formatters";
import { ChatCommitResponse, ChatMessage, GenerationRun, KnowledgeGraphResponse, MapMarker, OpportunityDetail, OpportunitySummary, PipelineSettings, PipelineSettingsPatch } from "@/types/api";
import { AppSection, TabKey } from "@/types/app-shell";
import { BranchKey, EntityRecord } from "@/types/view";

export function useOpportunityWorkspace(activeSection: AppSection) {
  const [items, setItems] = useState<OpportunitySummary[]>([]);
  const [selectedClusterId, setSelectedClusterId] = useState("");
  const [detail, setDetail] = useState<OpportunityDetail | null>(null);
  const [markers, setMarkers] = useState<MapMarker[]>([]);
  const [activeTab, setActiveTab] = useState<TabKey>("event");
  const [searchValue, setSearchValue] = useState("");
  const [eventTypeFilter, setEventTypeFilter] = useState<string[]>([]);
  const [countryFilter, setCountryFilter] = useState<string[]>([]);
  const [activeBranch, setActiveBranch] = useState<BranchKey>("direct");
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState("");
  const [selectedGraphNodeId, setSelectedGraphNodeId] = useState("");
  const [selectedEntityId, setSelectedEntityId] = useState("");
  const [selectedSourceId, setSelectedSourceId] = useState("");
  const [graphExpanded, setGraphExpanded] = useState(false);
  const [pipelineSettings, setPipelineSettings] = useState<PipelineSettings | null>(null);
  const [knowledgeGraph, setKnowledgeGraph] = useState<KnowledgeGraphResponse | null>(null);
  const [knowledgeGraphRequested, setKnowledgeGraphRequested] = useState(false);
  const [generationRuns, setGenerationRuns] = useState<GenerationRun[]>([]);
  const [controlLoading, setControlLoading] = useState(true);
  const [controlError, setControlError] = useState("");
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [regionRunSubmitting, setRegionRunSubmitting] = useState(false);
  const [companyRunSubmitting, setCompanyRunSubmitting] = useState(false);
  const [kbSyncing, setKbSyncing] = useState(false);
  const [kbCleaning, setKbCleaning] = useState(false);
  const [observedDataRunId, setObservedDataRunId] = useState("");
  const [pendingClusterOpen, setPendingClusterOpen] = useState<{ clusterId: string; label: string } | null>(null);
  const [markersRequested, setMarkersRequested] = useState(false);
  const deferredSearchValue = useDeferredValue(searchValue);

  const loadDashboard = useCallback(async () => {
    setLoading(true);
    setLoadError("");
    try {
      const opportunities = await getOpportunities();
      setItems(opportunities);
      if (!opportunities.length) {
        setDetail(null);
      }
      setSelectedClusterId((current) => {
        if (!current) return opportunities[0]?.cluster_id ?? "";
        if (!opportunities.some((item) => item.cluster_id === current)) {
          return opportunities[0]?.cluster_id ?? "";
        }
        return current;
      });
    } catch (error) {
      setItems([]);
      setDetail(null);
      setLoadError(error instanceof Error ? error.message : "Unable to load opportunities.");
    } finally {
      setLoading(false);
    }
  }, []);

  const loadMapMarkers = useCallback(async () => {
    setMarkersRequested(true);
    try {
      const nextMarkers = await getMapMarkers();
      setMarkers(nextMarkers);
    } catch {
      setMarkers([]);
    }
  }, []);

  const loadKnowledgeGraphData = useCallback(async () => {
    setKnowledgeGraphRequested(true);
    try {
      const nextKnowledgeGraph = await getKnowledgeGraph();
      setKnowledgeGraph(nextKnowledgeGraph);
    } catch {
      setKnowledgeGraph(null);
    }
  }, []);

  const loadControlPlane = useCallback(async () => {
    setControlLoading(true);
    setControlError("");
    try {
      const [settingsPayload, runsPayload] = await Promise.all([getPipelineSettings(), listGenerationRuns()]);
      setPipelineSettings(settingsPayload);
      setGenerationRuns(runsPayload);
      const newestDataRun = runsPayload.find(
        (run) => Boolean(run.created_cluster_id) && (run.status === "succeeded" || run.status === "failed"),
      );
      if (newestDataRun && newestDataRun.app_run_id !== observedDataRunId) {
        setObservedDataRunId(newestDataRun.app_run_id);
        void loadDashboard();
        if (activeTab === "map") {
          void loadMapMarkers();
        }
        if (activeSection === "global_knowledge_graph") {
          void loadKnowledgeGraphData();
        }
      }
    } catch (error) {
      setControlError(error instanceof Error ? error.message : "Unable to load pipeline control state.");
    } finally {
      setControlLoading(false);
    }
  }, [activeSection, activeTab, loadDashboard, loadKnowledgeGraphData, loadMapMarkers, observedDataRunId]);

  const refreshCoreData = useCallback(async () => {
    await Promise.all([loadDashboard(), loadControlPlane(), loadMapMarkers(), loadKnowledgeGraphData()]);
  }, [loadControlPlane, loadDashboard, loadKnowledgeGraphData, loadMapMarkers]);

  const focusCluster = useCallback((clusterId: string) => {
    const targetLabel = items.find((item) => item.cluster_id === clusterId)?.subject_company_name ?? "selected event";
    setPendingClusterOpen({ clusterId, label: targetLabel });
    setSearchValue("");
    setEventTypeFilter([]);
    setCountryFilter([]);
    setActiveTab("event");
    setActiveBranch("direct");
    setSelectedGraphNodeId("");
    setSelectedEntityId("");
    setSelectedSourceId("");
    setGraphExpanded(false);
    setSelectedClusterId(clusterId);
  }, [items]);

  useEffect(() => {
    void loadDashboard();
  }, [loadDashboard]);

  useEffect(() => {
    let cancelled = false;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    let idleId: number | null = null;
    const supportsIdleCallback = typeof window.requestIdleCallback === "function";

    const loadInBackground = () => {
      if (!cancelled) {
        void loadControlPlane();
      }
    };

    if (supportsIdleCallback) {
      idleId = window.requestIdleCallback(loadInBackground, { timeout: 1200 });
    } else {
      timeoutId = globalThis.setTimeout(loadInBackground, 250);
    }

    return () => {
      cancelled = true;
      if (idleId !== null && supportsIdleCallback) {
        window.cancelIdleCallback(idleId);
      }
      if (timeoutId !== null) {
        globalThis.clearTimeout(timeoutId);
      }
    };
  }, [loadControlPlane]);

  useEffect(() => {
    if (activeSection === "global_knowledge_graph" && !knowledgeGraphRequested) {
      void loadKnowledgeGraphData();
    }
  }, [activeSection, knowledgeGraphRequested, loadKnowledgeGraphData]);

  useEffect(() => {
    if (activeTab === "map" && !markersRequested) {
      void loadMapMarkers();
    }
  }, [activeTab, loadMapMarkers, markersRequested]);

  useEffect(() => {
    if ((activeSection === "settings" || activeSection === "global_knowledge_graph") && !pipelineSettings) {
      void loadControlPlane();
    }
  }, [activeSection, loadControlPlane, pipelineSettings]);

  useEffect(() => {
    if (!selectedClusterId) return;
    const controller = new AbortController();
    setDetailLoading(true);
    setDetailError("");
    getOpportunityDetailWithSignal(selectedClusterId, controller.signal)
      .then((payload) => {
        if (controller.signal.aborted) return;
        setDetail(payload);
        setActiveBranch("direct");
        setPendingClusterOpen((current) => (current?.clusterId === payload.cluster.cluster_id ? null : current));
      })
      .catch((error) => {
        if (controller.signal.aborted || (error instanceof Error && error.name === "AbortError")) {
          return;
        }
        setDetail(null);
        setDetailError(error instanceof Error ? error.message : "Unable to load opportunity detail.");
        setPendingClusterOpen((current) => (current?.clusterId === selectedClusterId ? null : current));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setDetailLoading(false);
        }
      });
    return () => controller.abort();
  }, [selectedClusterId]);

  const eventTypes = useMemo(
    () => [...new Set(items.map((item) => formatLabel(item.trigger_type)).filter((value) => value !== "Unknown"))].sort(),
    [items],
  );

  const countries = useMemo(
    () => [...new Set(items.map((item) => item.subject_country).filter(Boolean) as string[])].sort(),
    [items],
  );

  const filteredItems = useMemo(() => {
    const query = deferredSearchValue.trim().toLowerCase();
    return items.filter((item) => {
      const matchesSearch =
        !query ||
        item.subject_company_name.toLowerCase().includes(query) ||
        (item.event_headline ?? "").toLowerCase().includes(query) ||
        (item.event_summary ?? "").toLowerCase().includes(query);
      const matchesType = !eventTypeFilter.length || eventTypeFilter.includes(formatLabel(item.trigger_type));
      const matchesCountry = !countryFilter.length || countryFilter.includes(item.subject_country ?? "");
      return matchesSearch && matchesType && matchesCountry;
    });
  }, [countryFilter, deferredSearchValue, eventTypeFilter, items]);

  useEffect(() => {
    if (!filteredItems.length) return;
    if (!filteredItems.some((item) => item.cluster_id === selectedClusterId)) {
      setSelectedClusterId(filteredItems[0].cluster_id);
    }
  }, [filteredItems, selectedClusterId]);

  useEffect(() => {
    if (!generationRuns.some((run) => run.status === "queued" || run.status === "running")) return;
    const timer = window.setInterval(() => {
      void loadControlPlane();
    }, 8000);
    return () => window.clearInterval(timer);
  }, [generationRuns, loadControlPlane]);

  const mergedEntities = useMemo<EntityRecord[]>(() => {
    if (!detail) return [];

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
  }, [detail]);

  const visibleEntities = useMemo(
    () => mergedEntities.filter((entity) => entity.branch_type === activeBranch),
    [activeBranch, mergedEntities],
  );

  const selectedEntity = useMemo(
    () => visibleEntities.find((entity) => entity.cluster_entity_id === selectedEntityId) ?? visibleEntities[0] ?? null,
    [selectedEntityId, visibleEntities],
  );

  const groupedSources = useMemo(() => {
    if (!detail) return {} as Record<string, Record<string, unknown>[]>;
    return detail.sources.reduce<Record<string, Record<string, unknown>[]>>((groups, source) => {
      const key = String(source.used_for ?? "supporting_context");
      groups[key] = groups[key] ?? [];
      groups[key].push(source);
      return groups;
    }, {});
  }, [detail]);

  const selectedClusterName =
    detail?.cluster.subject_company_name ?? items.find((item) => item.cluster_id === selectedClusterId)?.subject_company_name ?? "";

  const selectedGraphNode = useMemo(
    () => detail?.graph_nodes.find((item) => item.id === selectedGraphNodeId) ?? null,
    [detail, selectedGraphNodeId],
  );

  const selectedSource = useMemo(
    () => detail?.sources.find((item) => String(item.cluster_source_id ?? "") === selectedSourceId) ?? detail?.sources[0] ?? null,
    [detail, selectedSourceId],
  );

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
    const sources = detail?.sources ?? [];
    if (!sources.length) {
      if (selectedSourceId) setSelectedSourceId("");
      return;
    }
    if (!sources.some((source) => String(source.cluster_source_id ?? "") === selectedSourceId)) {
      setSelectedSourceId(String(sources[0].cluster_source_id ?? ""));
    }
  }, [detail, selectedSourceId]);

  const handleSaveSettings = useCallback(async (patch: PipelineSettingsPatch) => {
    if (!pipelineSettings) {
      await loadControlPlane();
    }
    setSettingsSaving(true);
    setControlError("");
    try {
      const updated = await patchPipelineSettings(patch);
      setPipelineSettings(updated);
      await loadControlPlane();
    } catch (error) {
      setControlError(error instanceof Error ? error.message : "Unable to save pipeline settings.");
    } finally {
      setSettingsSaving(false);
    }
  }, [loadControlPlane, pipelineSettings]);

  const handleRunRegionSearch = useCallback(async (targetRegion: string) => {
    if (!pipelineSettings) {
      await loadControlPlane();
    }
    setRegionRunSubmitting(true);
    setControlError("");
    try {
      const run = await createGenerationRun({
        requested_by: "app",
        research_mode: "region",
        target_region: targetRegion,
      });
      setGenerationRuns((current) => [run, ...current.filter((item) => item.app_run_id !== run.app_run_id)]);
    } catch (error) {
      setControlError(error instanceof Error ? error.message : "Unable to start the regional research run.");
      throw error;
    } finally {
      setRegionRunSubmitting(false);
    }
  }, [loadControlPlane, pipelineSettings]);

  const handleRunCompanySearch = useCallback(async (companyName: string) => {
    if (!pipelineSettings) {
      await loadControlPlane();
    }
    setCompanyRunSubmitting(true);
    setControlError("");
    try {
      const run = await createGenerationRun({
        requested_by: "app",
        research_mode: "company",
        company_name: companyName,
      });
      setGenerationRuns((current) => [run, ...current.filter((item) => item.app_run_id !== run.app_run_id)]);
    } catch (error) {
      setControlError(error instanceof Error ? error.message : "Unable to start the company research run.");
      throw error;
    } finally {
      setCompanyRunSubmitting(false);
    }
  }, [loadControlPlane, pipelineSettings]);

  const handleCancelRun = useCallback(async (appRunId: string) => {
    setControlError("");
    try {
      await cancelGenerationRun(appRunId);
      await loadControlPlane();
    } catch (error) {
      setControlError(error instanceof Error ? error.message : "Unable to cancel the generation run.");
    }
  }, [loadControlPlane]);

  const handleSyncKnowledgeBase = useCallback(async () => {
    setKbSyncing(true);
    setControlError("");
    try {
      const knowledgeBase = await syncKnowledgeBase({ full_refresh: true });
      setPipelineSettings((current) => (current ? { ...current, knowledge_base: knowledgeBase } : current));
      await loadControlPlane();
    } catch (error) {
      setControlError(error instanceof Error ? error.message : "Unable to sync the knowledge base.");
    } finally {
      setKbSyncing(false);
    }
  }, [loadControlPlane]);

  const handleCleanupKnowledgeBase = useCallback(async () => {
    setKbCleaning(true);
    setControlError("");
    try {
      const knowledgeBase = await cleanupKnowledgeBase(pipelineSettings?.kb_cleanup_mode);
      setPipelineSettings((current) => (current ? { ...current, knowledge_base: knowledgeBase } : current));
      await loadControlPlane();
    } catch (error) {
      setControlError(error instanceof Error ? error.message : "Unable to clean the knowledge base.");
    } finally {
      setKbCleaning(false);
    }
  }, [loadControlPlane, pipelineSettings?.kb_cleanup_mode]);

  const handleCommitChat = useCallback(async (messages: ChatMessage[], previousResponseId: string | null): Promise<ChatCommitResponse> => {
    const response = await commitChatToKnowledgeBase({
      selected_cluster_id: selectedClusterId,
      selected_cluster_name: selectedClusterName,
      messages,
      previous_response_id: previousResponseId,
      committed_by: "app",
    });
    setPipelineSettings((current) => (current ? { ...current, knowledge_base: response.knowledge_base } : current));
    await loadControlPlane();
    return response;
  }, [loadControlPlane, selectedClusterId, selectedClusterName]);

  return {
    items,
    filteredItems,
    selectedClusterId,
    setSelectedClusterId,
    detail,
    markers,
    activeTab,
    setActiveTab,
    searchValue,
    setSearchValue,
    eventTypeFilter,
    setEventTypeFilter,
    countryFilter,
    setCountryFilter,
    activeBranch,
    setActiveBranch,
    loading,
    loadError,
    detailLoading,
    detailError,
    selectedGraphNodeId,
    setSelectedGraphNodeId,
    selectedEntityId,
    setSelectedEntityId,
    selectedSourceId,
    setSelectedSourceId,
    graphExpanded,
    setGraphExpanded,
    pipelineSettings,
    knowledgeGraph,
    generationRuns,
    controlLoading,
    controlError,
    settingsSaving,
    regionRunSubmitting,
    companyRunSubmitting,
    kbSyncing,
    kbCleaning,
    pendingClusterOpen,
    eventTypes,
    countries,
    mergedEntities,
    visibleEntities,
    selectedEntity,
    groupedSources,
    selectedClusterName,
    selectedGraphNode,
    selectedSource,
    loadDashboard,
    loadMapMarkers,
    loadKnowledgeGraphData,
    loadControlPlane,
    refreshCoreData,
    focusCluster,
    handleSaveSettings,
    handleRunRegionSearch,
    handleRunCompanySearch,
    handleCancelRun,
    handleSyncKnowledgeBase,
    handleCleanupKnowledgeBase,
    handleCommitChat,
  };
}
