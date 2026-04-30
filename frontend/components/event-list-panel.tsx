"use client";

import { useEffect, useRef, useState } from "react";

import { formatDate, formatLabel } from "@/lib/formatters";
import { toneForTrigger } from "@/lib/semantics";
import { OpportunitySummary } from "@/types/api";

type Props = {
  items: OpportunitySummary[];
  selectedClusterId: string;
  onSelect: (clusterId: string) => void;
  searchValue: string;
  onSearchChange: (value: string) => void;
  eventTypeValue: string[];
  onEventTypeChange: (value: string[]) => void;
  countryValue: string[];
  onCountryChange: (value: string[]) => void;
  eventTypes: string[];
  countries: string[];
};

function toggleItem(current: string[], value: string): string[] {
  return current.includes(value) ? current.filter((v) => v !== value) : [...current, value];
}

type MultiDropdownProps = {
  label: string;
  options: string[];
  selected: string[];
  onChange: (value: string[]) => void;
};

function MultiDropdown({ label, options, selected, onChange }: MultiDropdownProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  const triggerLabel =
    selected.length === 0
      ? `${label} ▾`
      : selected.length === 1
        ? `${selected[0]} ▾`
        : `${selected.length} selected ▾`;

  return (
    <div className="elpDropdown" ref={ref}>
      <button
        type="button"
        className={`elpDropdownTrigger${selected.length > 0 ? " elpDropdownTriggerActive" : ""}`}
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        {triggerLabel}
      </button>
      {open ? (
        <div className="elpDropdownPanel" role="listbox" aria-multiselectable="true">
          {options.map((opt) => {
            const checked = selected.includes(opt);
            return (
              <button
                key={opt}
                type="button"
                role="option"
                aria-selected={checked}
                className={`elpDropdownOption${checked ? " elpDropdownOptionChecked" : ""}`}
                onClick={() => onChange(toggleItem(selected, opt))}
              >
                <span className="elpDropdownCheck">{checked ? "✓" : ""}</span>
                {opt}
              </button>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

export function EventListPanel({
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
}: Props) {
  const hasFilters = eventTypeValue.length > 0 || countryValue.length > 0;

  return (
    <aside className="eventListPanel">
      <div className="elpHeader">
        <h2 className="elpTitle">Trigger Events</h2>
        <span className="elpCount">{items.length}</span>
      </div>

      <div className="elpFilters">
        <input
          className="elpSearch"
          value={searchValue}
          onChange={(e) => onSearchChange(e.target.value)}
          placeholder="Search events, companies..."
        />

        <div className="elpFilterRow">
          <MultiDropdown
            label="Event types"
            options={eventTypes}
            selected={eventTypeValue}
            onChange={onEventTypeChange}
          />
          <MultiDropdown
            label="Countries"
            options={countries}
            selected={countryValue}
            onChange={onCountryChange}
          />
        </div>

        {hasFilters ? (
          <div className="elpActiveFilters">
            {eventTypeValue.map((t) => (
              <span key={`type-${t}`} className="elpActiveTag">
                {t}
                <button type="button" className="elpTagRemove" onClick={() => onEventTypeChange(eventTypeValue.filter((v) => v !== t))}>✕</button>
              </span>
            ))}
            {countryValue.map((c) => (
              <span key={`country-${c}`} className="elpActiveTag elpActiveTagCountry">
                {c}
                <button type="button" className="elpTagRemove" onClick={() => onCountryChange(countryValue.filter((v) => v !== c))}>✕</button>
              </span>
            ))}
            <button
              type="button"
              className="elpClearAll"
              onClick={() => { onEventTypeChange([]); onCountryChange([]); }}
            >
              Clear all
            </button>
          </div>
        ) : null}
      </div>

      <div className="elpList">
        {items.length === 0 ? (
          <div className="elpSkeleton">
            {Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="skeleton elpSkeletonCard" />
            ))}
          </div>
        ) : (
          items.map((item) => (
          <button
            key={item.cluster_id}
            className="elpCard"
            data-active={item.cluster_id === selectedClusterId}
            onClick={() => onSelect(item.cluster_id)}
          >
            <div className="elpCardTags">
              <span className={`pill pill-${toneForTrigger(item.trigger_type)}`}>
                {formatLabel(item.trigger_type)}
              </span>
              {item.subject_country ? <span className="chip">{item.subject_country}</span> : null}
            </div>
            <strong className="elpCardTitle">{item.subject_company_name}</strong>
            <p className="elpCardBody">{item.event_headline}</p>
            <div className="elpCardMeta">
              {item.event_date ? <span>Event: {formatDate(item.event_date)}</span> : null}
              {item.cluster_created_at ? <span>Added: {formatDate(item.cluster_created_at)}</span> : null}
              <span>{item.source_count ?? 0} sources</span>
              {item.entity_count ? <span>{item.entity_count} opps</span> : null}
            </div>
          </button>
        ))
        )}
      </div>
    </aside>
  );
}
