"use client";

import dynamic from "next/dynamic";
import { useCallback, useMemo, useState } from "react";

import { ChatPanel } from "@/components/chat-panel";
import { ClaimOpportunityModal } from "@/components/claim-opportunity-modal";
import { EventIntelligenceSection } from "@/components/event-intelligence-section";
import { EventListPanel } from "@/components/event-list-panel";
import { NavRail } from "@/components/nav-rail";
import { SalesDashboardPanel } from "@/components/sales-dashboard";
import { useOpportunityWorkspace } from "@/hooks/use-opportunity-workspace";
import { useSalesWorkspace } from "@/hooks/use-sales-workspace";
import { SalesLead } from "@/types/api";
import { AppSection, SharedChatContext } from "@/types/app-shell";

const GlobalKnowledgeGraph = dynamic(
  () => import("@/components/global-knowledge-graph").then((module) => module.GlobalKnowledgeGraph),
  {
    loading: () => (
      <div className="panel detailCard">
        <h2 className="panelTitle">Loading global knowledge graph</h2>
        <p className="bodyText bodyTextMuted">Preparing the regional drilldown graph and copilot workspace.</p>
      </div>
    ),
  },
);

const ResearchPanel = dynamic(
  () => import("@/components/settings-panel").then((module) => module.ResearchPanel),
  {
    loading: () => (
      <div className="panel detailCard">
        <h2 className="panelTitle">Loading research studio</h2>
        <p className="bodyText bodyTextMuted">Preparing research runs, knowledge-base operations, and run history.</p>
      </div>
    ),
  },
);

function WorkspaceState({
  title,
  body,
  tone = "muted",
}: {
  title: string;
  body: string;
  tone?: "muted" | "error";
}) {
  return (
    <section className="panel detailCard">
      <h2 className="panelTitle">{title}</h2>
      <p className={`bodyText ${tone === "error" ? "" : "bodyTextMuted"}`}>{body}</p>
    </section>
  );
}

export default function DashboardPage() {
  const [activeSection, setActiveSection] = useState<AppSection>("event_intelligence");
  const [chatContext, setChatContext] = useState<SharedChatContext>({
    selectedClusterId: "",
    selectedClusterName: "",
    defaultScope: "all",
  });

  const handleChatContextChange = useCallback((ctx: SharedChatContext) => {
    setChatContext(ctx);
  }, []);

  const opportunityWorkspace = useOpportunityWorkspace(activeSection);
  const salesWorkspace = useSalesWorkspace(opportunityWorkspace.selectedClusterId, activeSection);

  const activeRunCount = useMemo(
    () =>
      opportunityWorkspace.generationRuns.filter((run) => run.status === "queued" || run.status === "running").length,
    [opportunityWorkspace.generationRuns],
  );

  const claimedCount = salesWorkspace.salesDashboard?.items.length ?? 0;

  const handleRefresh = useCallback(async () => {
    await Promise.all([opportunityWorkspace.refreshCoreData(), salesWorkspace.loadSalesTracker()]);
  }, [opportunityWorkspace, salesWorkspace]);

  const openClusterInEventIntelligence = useCallback(
    (clusterId: string) => {
      setActiveSection("event_intelligence");
      opportunityWorkspace.focusCluster(clusterId);
    },
    [opportunityWorkspace],
  );

  const openSalesWorkspace = useCallback(
    (lead: SalesLead) => {
      salesWorkspace.openLeadWorkspace(lead);
    },
    [salesWorkspace],
  );

  const mainContent = (() => {
    if (activeSection === "settings") {
      return (
        <ResearchPanel
          settings={opportunityWorkspace.pipelineSettings}
          runs={opportunityWorkspace.generationRuns}
          loading={opportunityWorkspace.controlLoading}
          error={opportunityWorkspace.controlError}
          saving={opportunityWorkspace.settingsSaving}
          runningRegionResearch={opportunityWorkspace.regionRunSubmitting}
          runningCompanyResearch={opportunityWorkspace.companyRunSubmitting}
          syncingKnowledgeBase={opportunityWorkspace.kbSyncing}
          cleaningKnowledgeBase={opportunityWorkspace.kbCleaning}
          onRefresh={opportunityWorkspace.loadControlPlane}
          onSave={opportunityWorkspace.handleSaveSettings}
          onRunRegionSearch={opportunityWorkspace.handleRunRegionSearch}
          onRunCompanySearch={opportunityWorkspace.handleRunCompanySearch}
          onCancelRun={opportunityWorkspace.handleCancelRun}
          onSyncKnowledgeBase={opportunityWorkspace.handleSyncKnowledgeBase}
          onCleanupKnowledgeBase={opportunityWorkspace.handleCleanupKnowledgeBase}
        />
      );
    }

    if (activeSection === "sales_tracker") {
      return (
        <SalesDashboardPanel
          dashboard={salesWorkspace.salesDashboard}
          leadCatalog={salesWorkspace.salesLeads}
          actorUserId={salesWorkspace.actorUserId}
          actorName={salesWorkspace.rememberedSalesUserName}
          loading={salesWorkspace.salesDashboardLoading}
          error={salesWorkspace.salesDashboardError}
          onRefresh={salesWorkspace.loadSalesTracker}
          onOpenCluster={openClusterInEventIntelligence}
          onOpenWorkspace={openSalesWorkspace}
          onLeadPageChange={salesWorkspace.setSalesLeadPage}
          onLeadSortChange={salesWorkspace.setSalesLeadSort}
          onUpdateStatus={async (lead, request) => {
            await salesWorkspace.updateSalesTrackerStatus(lead, request);
          }}
        />
      );
    }

    if (activeSection === "global_knowledge_graph") {
      return (
        <GlobalKnowledgeGraph
          graph={opportunityWorkspace.knowledgeGraph}
          onOpenCluster={openClusterInEventIntelligence}
          onChatContextChange={handleChatContextChange}
        />
      );
    }

    if (opportunityWorkspace.loading && !opportunityWorkspace.items.length) {
      return <WorkspaceState title="Loading opportunities" body="Pulling the latest event intelligence into the workspace." />;
    }

    if (opportunityWorkspace.loadError) {
      return <WorkspaceState title="Unable to load opportunities" body={opportunityWorkspace.loadError} tone="error" />;
    }

    if (opportunityWorkspace.detailError) {
      return <WorkspaceState title="Unable to load event detail" body={opportunityWorkspace.detailError} tone="error" />;
    }

    if (!opportunityWorkspace.detail) {
      return (
        <WorkspaceState
          title="Select an opportunity"
          body="Choose an event from the list to open the event intelligence workspace and start the sales review flow."
        />
      );
    }

    return (
      <EventIntelligenceSection
        detail={opportunityWorkspace.detail}
        filteredItems={opportunityWorkspace.filteredItems}
        selectedClusterId={opportunityWorkspace.selectedClusterId}
        setSelectedClusterId={openClusterInEventIntelligence}
        activeTab={opportunityWorkspace.activeTab}
        setActiveTab={opportunityWorkspace.setActiveTab}
        activeBranch={opportunityWorkspace.activeBranch}
        setActiveBranch={opportunityWorkspace.setActiveBranch}
        markers={opportunityWorkspace.markers}
        pendingClusterOpen={opportunityWorkspace.pendingClusterOpen}
        detailLoading={opportunityWorkspace.detailLoading}
        onChatContextChange={handleChatContextChange}
        salesLeadById={salesWorkspace.leadClaimsById}
        onOpenClaimModal={(lead) => salesWorkspace.openLeadWorkspace(lead)}
      />
    );
  })();

  const showEventList = activeSection === "event_intelligence";
  const showChatRail = activeSection === "event_intelligence" || activeSection === "global_knowledge_graph";

  return (
    <main className="shell">
      <section className={`layout${showChatRail ? " hasChatRail" : ""}`}>
        <NavRail
          activeSection={activeSection}
          onSectionChange={setActiveSection}
          triggerCount={opportunityWorkspace.filteredItems.length}
          claimedCount={claimedCount}
          activeRunCount={activeRunCount}
          countryCount={opportunityWorkspace.countries.length}
          onRefresh={() => void handleRefresh()}
          userName={salesWorkspace.rememberedSalesUserName ?? undefined}
        />

        {showEventList && (
          <EventListPanel
            items={opportunityWorkspace.filteredItems}
            selectedClusterId={opportunityWorkspace.selectedClusterId}
            onSelect={openClusterInEventIntelligence}
            searchValue={opportunityWorkspace.searchValue}
            onSearchChange={opportunityWorkspace.setSearchValue}
            eventTypeValue={opportunityWorkspace.eventTypeFilter}
            onEventTypeChange={opportunityWorkspace.setEventTypeFilter}
            countryValue={opportunityWorkspace.countryFilter}
            onCountryChange={opportunityWorkspace.setCountryFilter}
            eventTypes={opportunityWorkspace.eventTypes}
            countries={opportunityWorkspace.countries}
          />
        )}

        <section className="content">{mainContent}</section>

        <aside className="persistentChatRail">
          <ChatPanel
            selectedClusterId={chatContext.selectedClusterId}
            selectedClusterName={chatContext.selectedClusterName}
            knowledgeBase={opportunityWorkspace.pipelineSettings?.knowledge_base ?? null}
            onCommitChat={opportunityWorkspace.handleCommitChat}
            mode="embedded"
            assistantTitle="Sherlock AI"
            contextTitle={chatContext.contextTitle}
            contextDescription={chatContext.contextDescription}
            contextPromptPrefix={chatContext.contextPromptPrefix}
            suggestedPrompts={chatContext.suggestedPrompts}
            contextChips={chatContext.contextChips}
            requestContext={chatContext.requestContext}
            defaultScope={chatContext.defaultScope ?? "all"}
            userId={salesWorkspace.actorUserId}
          />
        </aside>
      </section>

      <ClaimOpportunityModal
        open={salesWorkspace.claimModalOpen}
        opportunity={salesWorkspace.selectedSalesLead}
        workspace={salesWorkspace.salesWorkspace}
        loading={salesWorkspace.salesWorkspaceLoading}
        error={salesWorkspace.salesWorkspaceError}
        rememberedName={salesWorkspace.rememberedSalesUserName}
        rememberedEmail={salesWorkspace.rememberedSalesUserEmail}
        onRememberUser={salesWorkspace.rememberSalesUser}
        onClose={() => salesWorkspace.setClaimModalOpen(false)}
        onClaim={async (payload) => {
          await salesWorkspace.claimSelectedOpportunity(payload);
        }}
        onSaveDraft={async (draft) => {
          await salesWorkspace.saveSalesDraft(draft);
        }}
        onSendMessage={async (message, channel) => {
          await salesWorkspace.sendSalesMessage(message, channel);
        }}
        onPush={async () => {
          await salesWorkspace.pushSelectedSalesDraft();
        }}
      />
    </main>
  );
}
