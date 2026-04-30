"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  ApiError,
  claimOpportunity,
  getCurrentUser,
  getSalesDashboard,
  getSalesLeads,
  getSalesWorkspace,
  pushSalesDraft,
  sendSalesDraftMessage,
  updateSalesDraft,
  updateSalesWorkspaceStatus,
} from "@/lib/api";
import { SalesDashboard, SalesDraftPayload, SalesLead, SalesLeadCatalog, SalesWorkspace, SalesWorkspaceStatusPatchRequest } from "@/types/api";
import { AppSection } from "@/types/app-shell";

const SALES_USER_STORAGE_KEY = "idc-sales-workspace-user-v2";
const SALES_LEADS_PAGE_SIZE = 100;

function createUserId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `sales-user-${Date.now()}`;
}

export function useSalesWorkspace(selectedClusterId: string, activeSection: AppSection) {
  const hasLoadedSalesTrackerRef = useRef(false);
  const [salesWorkspace, setSalesWorkspace] = useState<SalesWorkspace | null>(null);
  const [salesWorkspaceLoading, setSalesWorkspaceLoading] = useState(false);
  const [salesWorkspaceError, setSalesWorkspaceError] = useState("");
  const [salesDashboard, setSalesDashboard] = useState<SalesDashboard | null>(null);
  const [salesDashboardLoading, setSalesDashboardLoading] = useState(false);
  const [salesDashboardError, setSalesDashboardError] = useState("");
  const [salesLeads, setSalesLeads] = useState<SalesLeadCatalog | null>(null);
  const [claimModalOpen, setClaimModalOpen] = useState(false);
  const [selectedSalesLead, setSelectedSalesLead] = useState<SalesLead | null>(null);
  const [salesLeadPage, setSalesLeadPage] = useState(1);
  const [salesLeadSort, setSalesLeadSort] = useState<"newest_event" | "highest_priority" | "best_confidence">("newest_event");
  const [rememberedSalesUserId, setRememberedSalesUserId] = useState(() => createUserId());
  const [rememberedSalesUserName, setRememberedSalesUserName] = useState("");
  const [rememberedSalesUserEmail, setRememberedSalesUserEmail] = useState("");
  // Bumped each time the modal is opened to force the workspace-loading effect
  // to re-run even when selectedSalesLead hasn't changed object reference.
  const [_workspaceSeq, setWorkspaceSeq] = useState(0);

  const actorUserId = useMemo(() => rememberedSalesUserId, [rememberedSalesUserId]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    // Try to fetch the logged-in user identity from Databricks App headers.
    getCurrentUser()
      .then((user) => {
        if (user.user_id || user.email) {
          const userId = user.user_id || user.email || createUserId();
          const name = user.name || "";
          const email = user.email || "";
          setRememberedSalesUserId(userId);
          setRememberedSalesUserName(name);
          setRememberedSalesUserEmail(email);
          window.localStorage.setItem(
            SALES_USER_STORAGE_KEY,
            JSON.stringify({ userId, name, email }),
          );
          return;
        }
        // No identity from headers — fall back to localStorage.
        loadFromLocalStorage();
      })
      .catch(() => {
        // API not reachable (e.g. local dev) — fall back to localStorage.
        loadFromLocalStorage();
      });

    function loadFromLocalStorage() {
      try {
        const stored = window.localStorage.getItem(SALES_USER_STORAGE_KEY);
        if (!stored) {
          const nextUserId = createUserId();
          setRememberedSalesUserId(nextUserId);
          window.localStorage.setItem(SALES_USER_STORAGE_KEY, JSON.stringify({ userId: nextUserId, name: "", email: "" }));
          return;
        }
        const parsed = JSON.parse(stored) as { userId?: string; name?: string; email?: string };
        const nextUserId = parsed.userId ?? createUserId();
        setRememberedSalesUserId(nextUserId);
        setRememberedSalesUserName(parsed.name ?? "");
        setRememberedSalesUserEmail(parsed.email ?? "");
        if (!parsed.userId) {
          window.localStorage.setItem(
            SALES_USER_STORAGE_KEY,
            JSON.stringify({ userId: nextUserId, name: parsed.name ?? "", email: parsed.email ?? "" }),
          );
        }
      } catch {}
    }
  }, []);

  const rememberSalesUser = useCallback((name: string, email: string) => {
    const userId = rememberedSalesUserId || createUserId();
    setRememberedSalesUserId(userId);
    setRememberedSalesUserName(name);
    setRememberedSalesUserEmail(email);
    if (typeof window !== "undefined") {
      window.localStorage.setItem(SALES_USER_STORAGE_KEY, JSON.stringify({ userId, name, email }));
    }
  }, [rememberedSalesUserId]);

  const loadSalesWorkspace = useCallback(async (lead: SalesLead | null, signal?: AbortSignal) => {
    if (!lead) {
      setSalesWorkspace(null);
      return;
    }
    setSalesWorkspaceLoading(true);
    setSalesWorkspaceError("");
    try {
      const workspace = await getSalesWorkspace(lead.cluster_id, lead.sales_item_id, signal);
      setSalesWorkspace(workspace);
    } catch (error) {
      if (signal?.aborted || (error instanceof Error && error.name === "AbortError")) {
        return;
      }
      if (error instanceof ApiError && error.status === 404) {
        setSalesWorkspace(null);
      } else {
        const message = error instanceof Error ? error.message : "Unable to load sales workspace.";
        setSalesWorkspace(null);
        setSalesWorkspaceError(message);
      }
    } finally {
      if (!signal?.aborted) {
        setSalesWorkspaceLoading(false);
      }
    }
  }, []);

  const loadSalesTracker = useCallback(async () => {
    setSalesDashboardLoading(true);
    setSalesDashboardError("");
    try {
      const [dashboard, leads] = await Promise.all([
        getSalesDashboard(),
        getSalesLeads({ page: salesLeadPage, page_size: SALES_LEADS_PAGE_SIZE, sort_by: salesLeadSort }),
      ]);
      hasLoadedSalesTrackerRef.current = true;
      setSalesDashboard(dashboard);
      setSalesLeads(leads);
    } catch (error) {
      setSalesDashboard(null);
      setSalesLeads(null);
      setSalesDashboardError(error instanceof Error ? error.message : "Unable to load the sales tracker.");
    } finally {
      setSalesDashboardLoading(false);
    }
  }, [salesLeadPage, salesLeadSort]);

  useEffect(() => {
    if (!selectedSalesLead) {
      setSalesWorkspace(null);
      return;
    }
    // When the modal is open, always load the workspace – the lead may belong to
    // any cluster (e.g. opened from the Sales Tracker drafting queue, not from
    // the current EI cluster view). Only apply the cluster guard when the modal
    // is closed to avoid spurious fetches while the user browses clusters.
    if (!claimModalOpen && selectedSalesLead.cluster_id !== selectedClusterId) {
      setSalesWorkspace(null);
      return;
    }
    const controller = new AbortController();
    void loadSalesWorkspace(selectedSalesLead, controller.signal);
    return () => controller.abort();
  // _workspaceSeq forces a re-run on every modal open even if selectedSalesLead
  // hasn't changed reference (e.g. reopening the same already-claimed item).
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [_workspaceSeq, claimModalOpen, loadSalesWorkspace, selectedClusterId, selectedSalesLead]);

  useEffect(() => {
    if (!salesDashboard && !salesDashboardLoading) {
      void loadSalesTracker();
    }
  }, [activeSection, loadSalesTracker, salesDashboard, salesDashboardLoading]);

  useEffect(() => {
    if (hasLoadedSalesTrackerRef.current) {
      void loadSalesTracker();
    }
  }, [loadSalesTracker, salesLeadPage, salesLeadSort]);

  const leadClaimsById = useMemo(
    () =>
      Object.fromEntries(
        (salesLeads?.items ?? [])
          .filter((lead) => lead.cluster_entity_id)
          .map((lead) => [lead.cluster_entity_id!, lead]),
      ),
    [salesLeads],
  );

  const openLeadWorkspace = useCallback((lead: SalesLead) => {
    // Clear stale workspace immediately so the modal never opens with a leftover
    // workspace from a previous session. The loading effect will re-fetch fresh data.
    setSalesWorkspace(null);
    setSalesWorkspaceError("");
    setSelectedSalesLead(lead);
    setClaimModalOpen(true);
    setWorkspaceSeq((s) => s + 1);
  }, []);

  // Poll with exponential backoff while the AI draft is being generated.
  const pollIntervalRef = useRef(2000);
  useEffect(() => {
    if (!salesWorkspace || salesWorkspace.draft_status !== "generating" || !selectedSalesLead) {
      pollIntervalRef.current = 2000; // reset on status change
      return;
    }
    let cancelled = false;
    const timer = setTimeout(() => {
      if (!cancelled) {
        void loadSalesWorkspace(selectedSalesLead);
        // Exponential backoff: 2s → 4s → 8s → 16s → cap 30s
        pollIntervalRef.current = Math.min(pollIntervalRef.current * 2, 30000);
      }
    }, pollIntervalRef.current);
    return () => { cancelled = true; clearTimeout(timer); };
  }, [salesWorkspace, selectedSalesLead, loadSalesWorkspace]);

  const claimSelectedOpportunity = useCallback(async (payload: { claimed_by_name: string; claimed_by_email?: string; notes?: string }) => {
    if (!selectedSalesLead) throw new Error("No sales opportunity selected.");
    try {
      const workspace = await claimOpportunity(selectedSalesLead.cluster_id, {
        sales_item_id: selectedSalesLead.sales_item_id,
        claimed_by_user_id: actorUserId,
        claimed_by_name: payload.claimed_by_name,
        claimed_by_email: payload.claimed_by_email,
        notes: payload.notes,
      });
      setSalesWorkspace(workspace);
      setSalesWorkspaceError("");
      await loadSalesTracker();
      return workspace;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to claim the opportunity.";
      setSalesWorkspaceError(message);
      throw new Error(message);
    }
  }, [actorUserId, loadSalesTracker, selectedSalesLead]);

  const saveSalesDraft = useCallback(async (draftPayload: SalesDraftPayload) => {
    if (!selectedSalesLead) throw new Error("No sales opportunity selected.");
    try {
      const workspace = await updateSalesDraft(selectedSalesLead.cluster_id, selectedSalesLead.sales_item_id, {
        actor_user_id: actorUserId,
        draft_payload: draftPayload,
      });
      setSalesWorkspace(workspace);
      setSalesWorkspaceError("");
      await loadSalesTracker();
      return workspace;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to save the sales draft.";
      setSalesWorkspaceError(message);
      throw new Error(message);
    }
  }, [actorUserId, loadSalesTracker, selectedSalesLead]);

  const sendSalesMessage = useCallback(async (message: string, channel: "chat" | "voice") => {
    if (!selectedSalesLead) throw new Error("No sales opportunity selected.");
    try {
      const workspace = await sendSalesDraftMessage(selectedSalesLead.cluster_id, selectedSalesLead.sales_item_id, {
        actor_user_id: actorUserId,
        message,
        channel,
      });
      setSalesWorkspace(workspace);
      setSalesWorkspaceError("");
      return workspace;
    } catch (error) {
      const messageText = error instanceof Error ? error.message : "Unable to update the sales draft.";
      setSalesWorkspaceError(messageText);
      throw new Error(messageText);
    }
  }, [actorUserId, selectedSalesLead]);

  const pushSelectedSalesDraft = useCallback(async () => {
    if (!selectedSalesLead) throw new Error("No sales opportunity selected.");
    try {
      const workspace = await pushSalesDraft(selectedSalesLead.cluster_id, selectedSalesLead.sales_item_id, { actor_user_id: actorUserId });
      setSalesWorkspace(workspace);
      setSalesWorkspaceError("");
      await loadSalesTracker();
      return workspace;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to push the Salesforce draft.";
      setSalesWorkspaceError(message);
      throw new Error(message);
    }
  }, [actorUserId, loadSalesTracker, selectedSalesLead]);

  const updateSalesTrackerStatus = useCallback(async (lead: SalesLead, request: Omit<SalesWorkspaceStatusPatchRequest, "actor_user_id">) => {
    try {
      const workspace = await updateSalesWorkspaceStatus(lead.cluster_id, lead.sales_item_id, {
        actor_user_id: actorUserId,
        ...request,
      });
      if (lead.sales_item_id === selectedSalesLead?.sales_item_id) {
        setSalesWorkspace(workspace);
      }
      setSalesDashboardError("");
      await loadSalesTracker();
      return workspace;
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to update the sales status.";
      setSalesDashboardError(message);
      throw new Error(message);
    }
  }, [actorUserId, loadSalesTracker, selectedSalesLead?.sales_item_id]);

  const changeSalesLeadPage = useCallback((page: number) => {
    setSalesLeadPage(Math.max(page, 1));
  }, []);

  const changeSalesLeadSort = useCallback((sort: "newest_event" | "highest_priority" | "best_confidence") => {
    setSalesLeadSort(sort);
    setSalesLeadPage(1);
  }, []);

  return {
    salesWorkspace,
    salesWorkspaceLoading,
    salesWorkspaceError,
    salesDashboard,
    salesDashboardLoading,
    salesDashboardError,
    salesLeads,
    salesLeadsLoading: salesDashboardLoading,
    leadClaimsById,
    claimModalOpen,
    setClaimModalOpen,
    selectedSalesLead,
    setSelectedSalesLead,
    salesLeadPage,
    setSalesLeadPage: changeSalesLeadPage,
    salesLeadSort,
    setSalesLeadSort: changeSalesLeadSort,
    actorUserId,
    rememberedSalesUserName,
    rememberedSalesUserEmail,
    rememberSalesUser,
    loadSalesWorkspace,
    loadSalesTracker,
    openLeadWorkspace,
    claimSelectedOpportunity,
    saveSalesDraft,
    sendSalesMessage,
    pushSelectedSalesDraft,
    updateSalesTrackerStatus,
  };
}
