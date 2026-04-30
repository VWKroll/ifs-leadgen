import { BranchKey, InsightTone } from "@/types/view";

type LegendItem = {
  key: string;
  label: string;
  tone: InsightTone;
  color: string;
};

const TRIGGER_META: Record<string, LegendItem> = {
  insolvency: {
    key: "insolvency",
    label: "Insolvency",
    tone: "red",
    color: "var(--trigger-insolvency)",
  },
  regulatory_inquiry: {
    key: "regulatory_inquiry",
    label: "Regulatory Inquiry",
    tone: "blue",
    color: "var(--trigger-regulatory)",
  },
  regulatory_action: {
    key: "regulatory_action",
    label: "Regulatory Action",
    tone: "blue",
    color: "var(--trigger-regulatory)",
  },
  m_and_a: {
    key: "m_and_a",
    label: "M&A",
    tone: "purple",
    color: "var(--trigger-ma)",
  },
  financing: {
    key: "financing",
    label: "Financing",
    tone: "amber",
    color: "var(--trigger-financing)",
  },
};

const DEFAULT_TRIGGER_META: LegendItem = {
  key: "other",
  label: "Other",
  tone: "neutral",
  color: "var(--trigger-other)",
};

const BRANCH_META: Record<BranchKey, LegendItem> = {
  direct: {
    key: "direct",
    label: "Direct",
    tone: "blue",
    color: "var(--branch-direct)",
  },
  peer: {
    key: "peer",
    label: "Peer",
    tone: "amber",
    color: "var(--branch-peer)",
  },
  ownership: {
    key: "ownership",
    label: "Ownership",
    tone: "green",
    color: "var(--branch-ownership)",
  },
};

export const triggerLegendItems = Object.values(TRIGGER_META);
export const branchLegendItems = Object.values(BRANCH_META);

export function metaForTrigger(trigger?: string | null): LegendItem {
  if (!trigger) return DEFAULT_TRIGGER_META;
  return TRIGGER_META[trigger] ?? DEFAULT_TRIGGER_META;
}

export function toneForTrigger(trigger?: string | null): InsightTone {
  return metaForTrigger(trigger).tone;
}

export function colorForTrigger(trigger?: string | null): string {
  return metaForTrigger(trigger).color;
}

export function metaForBranch(branch?: BranchKey | string | null): LegendItem {
  if (!branch) return BRANCH_META.direct;
  if (branch === "peer" || branch === "ownership" || branch === "direct") {
    return BRANCH_META[branch];
  }
  return BRANCH_META.direct;
}

export function toneForBranch(branch?: BranchKey | string | null): InsightTone {
  return metaForBranch(branch).tone;
}

export function colorForBranch(branch?: BranchKey | string | null): string {
  return metaForBranch(branch).color;
}
