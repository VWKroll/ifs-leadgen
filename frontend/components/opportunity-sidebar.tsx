"use client";

import { ChangeEvent } from "react";
import Image from "next/image";

import { formatDate, formatLabel } from "@/lib/formatters";
import { toneForTrigger } from "@/lib/semantics";
import { PipelineSettings, OpportunitySummary } from "@/types/api";
import { AppSection } from "@/types/app-shell";

type Props = {
  activeSection: AppSection;
  onSectionChange: (section: AppSection) => void;
  items: OpportunitySummary[];
  selectedClusterId: string;
  onSelect: (clusterId: string) => void;
  searchValue: string;
  onSearchChange: (value: string) => void;
  eventTypeValue: string;
  onEventTypeChange: (value: string) => void;
  countryValue: string;
  onCountryChange: (value: string) => void;
  eventTypes: string[];
  countries: string[];
  onRefresh: () => void;
  pipelineSettings: PipelineSettings | null;
  activeRunCount: number;
  claimedCount: number;
};

export function OpportunitySidebar({
  activeSection,
  onSectionChange,
  items,
  selectedClusterId,
  onSelect,
  searchValue,
  onSearchChange,
  eventTypeValue,
  onEventTypeChange,
  countryValue,
  onCountryChange,
  eventTypes,
  countries,
  onRefresh,
  pipelineSettings,
  activeRunCount,
  claimedCount,
}: Props) {
  const handleSearch = (event: ChangeEvent<HTMLInputElement>) => onSearchChange(event.target.value);
  const handleEventType = (event: ChangeEvent<HTMLSelectElement>) => onEventTypeChange(event.target.value);
  const handleCountry = (event: ChangeEvent<HTMLSelectElement>) => onCountryChange(event.target.value);

  return (
    <aside className="sidebar">
      <div className="brandBlock">
        <Image
          className="brandLogo"
          src="/kroll-logo.svg"
          alt="Kroll"
          width={300}
          height={76}
          priority
        />
        <div className="brandSubline">IDC Event Intelligence</div>
        <div className="liveBadge">
          <span className="liveDot" />
          Live
        </div>
      </div>

      <button className="refreshButton" onClick={onRefresh}>
        Refresh data
      </button>

      <div className="appNav">
        <button className="appNavButton" data-active={activeSection === "event_intelligence"} onClick={() => onSectionChange("event_intelligence")}>
          <strong>Event Intelligence</strong>
          <span>{items.length} triggers</span>
        </button>
        <button className="appNavButton" data-active={activeSection === "global_knowledge_graph"} onClick={() => onSectionChange("global_knowledge_graph")}>
          <strong>Global Knowledge Graph</strong>
          <span>{countries.length ? `${countries.length} countries in view` : "Regional narrative graph"}</span>
        </button>
        <button className="appNavButton" data-active={activeSection === "sales_tracker"} onClick={() => onSectionChange("sales_tracker")}>
          <strong>Sales Tracker</strong>
          <span>{claimedCount ? `${claimedCount} claimed prospect${claimedCount === 1 ? "" : "s"}` : "Claimed opportunity pipeline"}</span>
        </button>
        <button className="appNavButton" data-active={activeSection === "settings"} onClick={() => onSectionChange("settings")}>
          <strong>Research Studio</strong>
          <span>{activeRunCount ? `${activeRunCount} active run${activeRunCount === 1 ? "" : "s"}` : "Launchpad + KB ops"}</span>
        </button>
      </div>

      {activeSection === "event_intelligence" ? (
        <>
          <div className="filterGroup">
            <input
              className="sidebarInput"
              value={searchValue}
              onChange={handleSearch}
              placeholder="Search events, companies..."
            />
          </div>

          <div className="filterGroup">
            <h2 className="sidebarLabel">Event Type</h2>
            <select className="sidebarSelect" value={eventTypeValue} onChange={handleEventType}>
              <option value="all">All event types</option>
              {eventTypes.map((eventType) => (
                <option key={eventType} value={eventType}>
                  {eventType}
                </option>
              ))}
            </select>
          </div>

          <div className="filterGroup">
            <h2 className="sidebarLabel">Country</h2>
            <select className="sidebarSelect" value={countryValue} onChange={handleCountry}>
              <option value="all">All countries</option>
              {countries.map((country) => (
                <option key={country} value={country}>
                  {country}
                </option>
              ))}
            </select>
          </div>

          <div className="sidebarSectionHeader">
            <h2 className="sidebarLabel">Trigger Events</h2>
            <span className="countBadge">{items.length}</span>
          </div>

          <div className="list">
            {items.map((item) => (
              <button
                key={item.cluster_id}
                className="eventCard"
                data-active={item.cluster_id === selectedClusterId}
                onClick={() => onSelect(item.cluster_id)}
              >
                <div className="eventCardTop">
                  <span className={`pill pill-${toneForTrigger(item.trigger_type)}`}>{formatLabel(item.trigger_type)}</span>
                  {item.subject_country ? <span className="chip">{item.subject_country}</span> : null}
                </div>
                <div className="eventMetaRow">
                  {item.event_date ? <span>Event: {formatDate(item.event_date)}</span> : null}
                  <span>{item.source_count ?? 0} sources</span>
                </div>
                <strong className="eventCardTitle">
                  {item.subject_company_name}
                  {item.entity_count ? ` · ${item.entity_count} opportunities` : ""}
                </strong>
                <p className="eventCardBody">{item.event_headline}</p>
              </button>
            ))}
          </div>
        </>
      ) : activeSection === "sales_tracker" ? (
        <div className="settingsSidebarSummary">
          <div className="sidebarSectionHeader">
            <h2 className="sidebarLabel">Sales Workspace</h2>
          </div>
          <div className="panel sidebarMiniCard">
            <strong>{claimedCount} claimed opportunities</strong>
            <p>Track AI-drafted prospects, Salesforce push readiness, and downstream status in one place.</p>
          </div>
          <div className="panel sidebarMiniCard">
            <strong>Claim flow</strong>
            <p>Open any event, claim it, review the AI-populated draft, then push it to Salesforce when it is ready.</p>
          </div>
        </div>
      ) : (
        <div className="settingsSidebarSummary">
          <div className="sidebarSectionHeader">
            <h2 className="sidebarLabel">Research Status</h2>
          </div>
          <div className="panel sidebarMiniCard">
            <strong>Provider health</strong>
            <p>{pipelineSettings?.provider?.message ?? "Provider status will appear here."}</p>
          </div>
        </div>
      )}
    </aside>
  );
}
