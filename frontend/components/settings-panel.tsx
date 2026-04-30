"use client";

import { useEffect, useMemo, useState } from "react";

import { RichTextBlock } from "@/components/rich-text-block";
import { compactText, formatDate, formatLabel } from "@/lib/formatters";
import { GenerationRun, PipelineSettings, PipelineSettingsPatch } from "@/types/api";

type Props = {
  settings: PipelineSettings | null;
  runs: GenerationRun[];
  loading: boolean;
  error: string;
  saving: boolean;
  runningRegionResearch: boolean;
  runningCompanyResearch: boolean;
  syncingKnowledgeBase: boolean;
  cleaningKnowledgeBase: boolean;
  onRefresh: () => void;
  onSave: (patch: PipelineSettingsPatch) => Promise<void>;
  onRunRegionSearch: (targetRegion: string) => Promise<void>;
  onRunCompanySearch: (companyName: string) => Promise<void>;
  onCancelRun: (appRunId: string) => Promise<void>;
  onSyncKnowledgeBase: () => Promise<void>;
  onCleanupKnowledgeBase: () => Promise<void>;
};

const RUN_STEP_ORDER = [
  "discovery",
  "expansion",
  "scoring",
  "role_recommendation",
  "persistence",
  "knowledge_base_sync",
] as const;

function isActiveStatus(status: string) {
  return status === "queued" || status === "running";
}

function statusTone(status: string | undefined) {
  switch (status) {
    case "succeeded":
      return "chip-green";
    case "failed":
    case "cancelled":
      return "chip-red";
    case "skipped":
      return "chip-amber";
    case "running":
      return "chip-blue";
    case "queued":
      return "chip-neutral";
    default:
      return "chip-neutral";
  }
}

function currentRunStep(run: GenerationRun) {
  for (const key of RUN_STEP_ORDER) {
    const status = run.step_statuses?.[key];
    if (status === "running" || status === "queued" || status === "failed") {
      return key;
    }
  }
  return run.status === "succeeded" ? "knowledge_base_sync" : RUN_STEP_ORDER[0];
}

export function ResearchPanel({
  settings,
  runs,
  loading,
  error,
  saving,
  runningRegionResearch,
  runningCompanyResearch,
  syncingKnowledgeBase,
  cleaningKnowledgeBase,
  onRefresh,
  onSave,
  onRunRegionSearch,
  onRunCompanySearch,
  onCancelRun,
  onSyncKnowledgeBase,
  onCleanupKnowledgeBase,
}: Props) {
  const [knowledgeBaseDraft, setKnowledgeBaseDraft] = useState<PipelineSettingsPatch>({
    kb_max_results: 12,
    kb_cleanup_mode: "dedupe",
    kb_cleanup_on_sync: true,
    kb_document_retention_days: 45,
  });
  const [regionQuery, setRegionQuery] = useState("Europe");
  const [companyQuery, setCompanyQuery] = useState("");

  useEffect(() => {
    if (!settings) return;
    setKnowledgeBaseDraft({
      kb_max_results: settings.kb_max_results,
      kb_cleanup_mode: settings.kb_cleanup_mode,
      kb_cleanup_on_sync: settings.kb_cleanup_on_sync,
      kb_document_retention_days: settings.kb_document_retention_days,
    });
    setRegionQuery(settings.target_region);
  }, [settings]);

  const activeRuns = useMemo(() => runs.filter((run) => isActiveStatus(run.status)), [runs]);
  const isRefreshing = loading && runs.length > 0;

  async function handleSubmit() {
    await onSave(knowledgeBaseDraft);
  }

  async function handleRegionSearch() {
    await onRunRegionSearch(regionQuery.trim());
  }

  async function handleCompanySearch() {
    await onRunCompanySearch(companyQuery.trim());
  }

  return (
    <section className="tabPanel researchStudioLayout">
      <section className="panel featureCard">
        <div className="featureMeta">
          <span className="pill pill-primary">Research</span>
          <span className="chip">{settings?.generation_runner ?? "local"}</span>
          {settings?.provider?.configured ? <span className="chip chip-green">Provider ready</span> : <span className="chip chip-red">Provider missing</span>}
          <span className={`chip ${settings?.knowledge_base?.status === "ready" ? "chip-green" : settings?.knowledge_base?.status === "fallback" ? "chip-amber" : "chip-neutral"}`}>
            KB {formatLabel(settings?.knowledge_base?.status)}
          </span>
          {isRefreshing ? <span className="chip chip-blue">Refreshing live run state</span> : null}
        </div>
        <h1>Research Studio</h1>
        <div className="featureFooter">
          <span className="featureCompany">Launch focused research, manage the knowledge base, and watch live pipeline progress from one place.</span>
        </div>
      </section>

      <section className="metricRow">
        <div className="panel metricCard">
          <h2 className="metricLabel">Runner</h2>
          <strong className="metricValue opportunity">{formatLabel(settings?.generation_runner)}</strong>
        </div>
        <div className="panel metricCard">
          <h2 className="metricLabel">Provider Health</h2>
          <strong className={`metricValue ${settings?.provider?.configured ? "confidence" : "coverage"}`}>
            {settings?.provider?.configured ? "Ready" : "Needs config"}
          </strong>
        </div>
        <div className="panel metricCard">
          <h2 className="metricLabel">Active Runs</h2>
          <strong className="metricValue priority">{activeRuns.length}</strong>
        </div>
        <div className="panel metricCard">
          <h2 className="metricLabel">Chat Model</h2>
          <strong className="metricValue confidence">
            {settings?.chat_model ?? "Unknown"}
          </strong>
        </div>
      </section>

      {error ? (
        <section className="panel detailCard">
          <h2 className="panelTitle">System Error</h2>
          <RichTextBlock className="bodyText bodyTextMuted" text={error} />
        </section>
      ) : null}

      {activeRuns.length ? (
        <section className="panel detailCard researchProgressPanel">
          <div className="settingsHeader">
            <div>
              <h2 className="panelTitle">Research In Progress</h2>
              <p className="bodyText bodyTextMuted">
                Each run moves through the same pipeline so the team can see whether research is discovering, expanding, scoring, persisting, or waiting for KB sync.
              </p>
            </div>
            <span className="chip chip-blue">{activeRuns.length} live run{activeRuns.length === 1 ? "" : "s"}</span>
          </div>

          <div className="researchProgressStack">
            {activeRuns.map((run) => {
              const focusLabel =
                run.research_target ??
                (run.created_cluster_id ? `Cluster ${run.created_cluster_id.slice(0, 8)}` : run.app_run_id.slice(0, 8));
              const currentStep = currentRunStep(run);

              return (
                <article key={run.app_run_id} className="researchProgressCard">
                  <div className="runHistoryTop">
                    <div>
                      <strong>{focusLabel}</strong>
                      <p>
                        {formatLabel(run.research_mode ?? run.trigger_source)} · {formatLabel(run.runner_type)} ·{" "}
                        {run.requested_at ? formatDate(run.requested_at) : "Requested time unavailable"}
                      </p>
                    </div>
                    <div className="chipWrap">
                      <span className={`chip ${statusTone(run.status)}`}>{formatLabel(run.status)}</span>
                      <span className="chip chip-neutral">Current step: {formatLabel(currentStep)}</span>
                    </div>
                  </div>

                  <div className="researchProgressSteps" aria-label={`Progress for ${focusLabel}`}>
                    {RUN_STEP_ORDER.map((step, index) => {
                      const status = run.step_statuses?.[step] ?? "pending";
                      return (
                        <div key={step} className="researchStep" data-status={status}>
                          <div className="researchStepMarker">
                            <span>{index + 1}</span>
                          </div>
                          <div className="researchStepBody">
                            <div className="researchStepHeader">
                              <strong>{formatLabel(step)}</strong>
                              <span className={`chip ${statusTone(status)}`}>{formatLabel(status)}</span>
                            </div>
                            <p className="bodyText bodyTextMuted">
                              {step === "knowledge_base_sync" && status === "queued"
                                ? "Waiting for the knowledge-base worker to pick up this sync."
                                : step === "knowledge_base_sync" && status === "running"
                                  ? "Writing research artifacts into the assistant knowledge base."
                                  : status === "running"
                                    ? "This stage is actively processing now."
                                    : status === "pending"
                                      ? "This stage has not started yet."
                                      : status === "failed"
                                        ? "This stage needs attention before the run can finish cleanly."
                                        : status === "succeeded"
                                          ? "This stage completed successfully."
                                          : "This stage was skipped for this run."}
                            </p>
                          </div>
                        </div>
                      );
                    })}
                  </div>

                  {run.error_message ? <RichTextBlock className="bodyText bodyTextMuted" text={compactText(run.error_message)} /> : null}

                  <div className="runHistoryActions">
                    {run.job_url ? (
                      <a className="secondaryButton linkButton" href={run.job_url} target="_blank" rel="noreferrer">
                        Open Databricks run
                      </a>
                    ) : null}
                    <button className="secondaryButton" onClick={() => onCancelRun(run.app_run_id)}>
                      Cancel
                    </button>
                  </div>
                </article>
              );
            })}
          </div>
        </section>
      ) : null}

      <div className="settingsGrid">
        <section className="panel settingsSection">
          <div className="settingsHeader">
            <div>
              <h2 className="panelTitle">Research Launchpad</h2>
              <p className="bodyText bodyTextMuted">
                Start a one-time research run by region or by company. Recency, deduplication, and expansion depth stay managed in the backend.
              </p>
            </div>
            <button className="secondaryButton" onClick={onRefresh}>
              Refresh
            </button>
          </div>

          <div className="launchpadGrid">
            <section className="panel launchpadCard">
              <div className="launchpadHeader">
                <div>
                  <div className="panelEyebrow">One-Time Search</div>
                  <h3 className="panelTitle panelTitleMedium">Target region research</h3>
                </div>
                <span className="chip chip-blue">Regional scan</span>
              </div>
              <p className="bodyText bodyTextMuted">
                Search for one fresh IDC-relevant event inside a target region, then run the full event extraction, peer expansion, and knowledge-base sync.
              </p>
              <label className="controlCard">
                <span>Target region</span>
                <input value={regionQuery} onChange={(event) => setRegionQuery(event.target.value)} placeholder="EMEA, APAC, North America..." />
              </label>
              <div className="launchpadActions">
                <button className="primaryButton" disabled={runningRegionResearch || loading || !regionQuery.trim()} onClick={handleRegionSearch}>
                  {runningRegionResearch ? "Starting region search..." : "Search target region"}
                </button>
              </div>
            </section>

            <section className="panel launchpadCard">
              <div className="launchpadHeader">
                <div>
                  <div className="panelEyebrow">One-Time Search</div>
                  <h3 className="panelTitle panelTitleMedium">Company event research</h3>
                </div>
                <span className="chip chip-purple">Company-led</span>
              </div>
              <p className="bodyText bodyTextMuted">
                Enter a company name to search for recent events tied to that company, then expand into peers, ownership context, and follow-on opportunities automatically.
              </p>
              <label className="controlCard">
                <span>Company name</span>
                <input value={companyQuery} onChange={(event) => setCompanyQuery(event.target.value)} placeholder="Acme plc" />
              </label>
              <div className="launchpadActions">
                <button className="primaryButton" disabled={runningCompanyResearch || loading || !companyQuery.trim()} onClick={handleCompanySearch}>
                  {runningCompanyResearch ? "Starting company search..." : "Research company events"}
                </button>
              </div>
            </section>
          </div>

          <div className="settingsHeader settingsSubsectionHeader">
            <div>
              <h2 className="panelTitle">Knowledge Base</h2>
              <p className="bodyText bodyTextMuted">Tune retrieval depth, sync hygiene, and cleanup behavior for the assistant’s research corpus.</p>
            </div>
          </div>

          <div className="settingsForm">
            <label className="controlCard">
              <span>KB max results</span>
              <input
                type="number"
                min={1}
                max={50}
                value={knowledgeBaseDraft.kb_max_results ?? 12}
                onChange={(event) => setKnowledgeBaseDraft((current) => ({ ...current, kb_max_results: Number(event.target.value) || 1 }))}
              />
            </label>

            <label className="controlCard">
              <span>Cleanup mode</span>
              <select
                value={knowledgeBaseDraft.kb_cleanup_mode ?? "dedupe"}
                onChange={(event) =>
                  setKnowledgeBaseDraft((current) => ({
                    ...current,
                    kb_cleanup_mode: event.target.value as "off" | "dedupe" | "aggressive",
                  }))
                }
              >
                <option value="off">Off</option>
                <option value="dedupe">Dedupe</option>
                <option value="aggressive">Aggressive scrub</option>
              </select>
            </label>

            <label className="controlCard">
              <span>Cleanup on sync</span>
              <select
                value={knowledgeBaseDraft.kb_cleanup_on_sync ? "enabled" : "disabled"}
                onChange={(event) =>
                  setKnowledgeBaseDraft((current) => ({ ...current, kb_cleanup_on_sync: event.target.value === "enabled" }))
                }
              >
                <option value="enabled">Enabled</option>
                <option value="disabled">Disabled</option>
              </select>
            </label>

            <label className="controlCard">
              <span>Retention window (days)</span>
              <input
                type="number"
                min={1}
                value={knowledgeBaseDraft.kb_document_retention_days ?? 45}
                onChange={(event) =>
                  setKnowledgeBaseDraft((current) => ({ ...current, kb_document_retention_days: Number(event.target.value) || 1 }))
                }
              />
            </label>
          </div>

          <div className="settingsActions">
            <button className="primaryButton" disabled={saving || loading} onClick={handleSubmit}>
              {saving ? "Saving..." : "Save KB settings"}
            </button>
            <button className="secondaryButton" disabled={syncingKnowledgeBase || loading} onClick={onSyncKnowledgeBase}>
              {syncingKnowledgeBase ? "Syncing KB..." : "Sync Knowledge Base"}
            </button>
            <button className="secondaryButton" disabled={cleaningKnowledgeBase || loading} onClick={onCleanupKnowledgeBase}>
              {cleaningKnowledgeBase ? "Cleaning KB..." : "Cleanup Redundant Data"}
            </button>
          </div>

          <section className="panel detailCard">
            <div className="settingsHeader">
              <div>
                <h2 className="panelTitle">Knowledge Base Snapshot</h2>
                <p className="bodyText bodyTextMuted">
                  Review current corpus health, sync status, and cleanup outcomes alongside the controls above.
                </p>
              </div>
              <div className="chipWrap">
                <span
                  className={`chip ${
                    settings?.knowledge_base?.status === "ready"
                      ? "chip-green"
                      : settings?.knowledge_base?.status === "fallback"
                        ? "chip-amber"
                        : "chip-neutral"
                  }`}
                >
                  {formatLabel(settings?.knowledge_base?.status)}
                </span>
                {settings?.knowledge_base?.vector_store_id ? <span className="chip chip-blue">Vector store linked</span> : null}
                <span className="chip chip-neutral">{formatLabel(settings?.kb_cleanup_mode)} cleanup</span>
              </div>
            </div>
            <RichTextBlock
              className="bodyText bodyTextMuted"
              text={
                settings?.knowledge_base?.last_error
                  ? compactText(settings.knowledge_base.last_error)
                  : `${settings?.knowledge_base?.document_count ?? 0} documents are currently available across cluster, entity, source, region, and country context.`
              }
            />
            <p className="bodyText bodyTextMuted">
              {settings?.knowledge_base?.last_synced_at
                ? `Last sync: ${formatDate(settings.knowledge_base.last_synced_at)}`
                : "The knowledge base has not been synced yet."}
            </p>
            <ul className="keyValueList keyValueListCompact">
              <li>
                <span>Cluster docs</span>
                <strong>{settings?.knowledge_base?.cluster_document_count ?? 0}</strong>
              </li>
              <li>
                <span>Entity docs</span>
                <strong>{settings?.knowledge_base?.entity_document_count ?? 0}</strong>
              </li>
              <li>
                <span>Source docs</span>
                <strong>{settings?.knowledge_base?.source_document_count ?? 0}</strong>
              </li>
              <li>
                <span>Region docs</span>
                <strong>{settings?.knowledge_base?.region_document_count ?? 0}</strong>
              </li>
              <li>
                <span>Country docs</span>
                <strong>{settings?.knowledge_base?.country_document_count ?? 0}</strong>
              </li>
              <li>
                <span>Duplicate candidates</span>
                <strong>{settings?.knowledge_base?.duplicate_candidate_count ?? 0}</strong>
              </li>
              <li>
                <span>Stale local files</span>
                <strong>{settings?.knowledge_base?.stale_local_file_count ?? 0}</strong>
              </li>
            </ul>
            {(settings?.knowledge_base?.cleanup_removed_documents || settings?.knowledge_base?.cleanup_removed_files) ? (
              <p className="bodyText bodyTextMuted">
                Last cleanup removed {settings.knowledge_base.cleanup_removed_documents ?? 0} document records and{" "}
                {settings.knowledge_base.cleanup_removed_files ?? 0} stale markdown files.
              </p>
            ) : null}
          </section>
        </section>

        <aside className="settingsSidebar">
          <section className="panel detailCard">
            <div className="settingsHeader">
              <div>
                <h2 className="panelTitle">Research Logs</h2>
                <p className="bodyText bodyTextMuted">
                  Watch completed and active runs, inspect the current step, and review failures without leaving the studio.
                </p>
              </div>
              {isRefreshing ? <span className="chip chip-blue">Refreshing</span> : null}
            </div>
            <div className="runHistoryList">
              {loading && !runs.length ? (
                <div className="runHistoryEmpty">Loading run history...</div>
              ) : runs.length ? (
                runs.map((run) => (
                  <article key={run.app_run_id} className="runHistoryCard">
                    <div className="runHistoryTop">
                      <div>
                        <strong>{run.research_target ?? (run.created_cluster_id ? `Cluster ${run.created_cluster_id.slice(0, 8)}` : run.app_run_id.slice(0, 8))}</strong>
                        <p>
                          {formatLabel(run.research_mode ?? run.trigger_source)} · {formatLabel(run.runner_type)} ·{" "}
                          {run.requested_at ? formatDate(run.requested_at) : "Requested time unavailable"}
                        </p>
                      </div>
                      <div className="chipWrap">
                        <span className={`chip ${run.status === "succeeded" ? "chip-green" : run.status === "failed" ? "chip-red" : run.status === "skipped" ? "chip-amber" : "chip-blue"}`}>
                          {formatLabel(run.status)}
                        </span>
                        {run.duplicate_skipped ? <span className="chip chip-amber">Duplicate skipped</span> : null}
                      </div>
                    </div>

                    <div className="runHistoryBody">
                      {run.target_region || run.company_name ? (
                        <div className="runMetricLine">
                          <span>Research target</span>
                          <strong>{run.company_name ?? run.target_region}</strong>
                        </div>
                      ) : null}
                      <div className="runMetricLine">
                        <span>Requested by</span>
                        <strong>{run.requested_by}</strong>
                      </div>
                      <div className="runMetricLine">
                        <span>Current step</span>
                        <strong>{formatLabel(currentRunStep(run))}</strong>
                      </div>
                      <div className="runHistoryStepGrid">
                        {RUN_STEP_ORDER.map((step) => {
                          const status = run.step_statuses?.[step] ?? "pending";
                          return (
                            <div key={step} className="runHistoryStepChip" data-status={status}>
                              <span>{formatLabel(step)}</span>
                              <strong>{formatLabel(status)}</strong>
                            </div>
                          );
                        })}
                      </div>
                      {run.error_message ? <RichTextBlock className="bodyText bodyTextMuted" text={compactText(run.error_message)} /> : null}
                    </div>

                    <div className="runHistoryActions">
                      {run.job_url ? (
                        <a className="secondaryButton linkButton" href={run.job_url} target="_blank" rel="noreferrer">
                          Open Databricks run
                        </a>
                      ) : null}
                      {isActiveStatus(run.status) ? (
                        <button className="secondaryButton" onClick={() => onCancelRun(run.app_run_id)}>
                          Cancel
                        </button>
                      ) : null}
                    </div>
                  </article>
                ))
              ) : (
                <div className="runHistoryEmpty">No generation runs yet. Start one from this page to populate run history.</div>
              )}
            </div>
          </section>
        </aside>
      </div>
    </section>
  );
}
