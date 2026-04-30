export type BranchKey = "direct" | "peer" | "ownership";

export type RecommendationRecord = {
  role_track_type: string;
  role_confidence_score: number;
  rationale: string;
  recommended_titles: string[];
  departments: string[];
  seniority_levels: string[];
  hypothesized_services: string[];
};

export type EntityRecord = {
  cluster_entity_id: string;
  entity_name: string;
  entity_type: string;
  branch_type: BranchKey;
  commercial_role: string;
  relationship_to_subject: string;
  rationale: string;
  evidence_type: string;
  priority_score: number;
  confidence_score: number;
  source_urls: string[];
  source_snippets: string[];
  recommendations: RecommendationRecord[];
};

export type InsightTone = "blue" | "green" | "amber" | "purple" | "neutral" | "red";

export type InsightChip = {
  label: string;
  tone?: InsightTone;
};

export type InsightMetric = {
  label: string;
  value: string;
  tone?: InsightTone;
};

export type InsightSection = {
  title: string;
  text?: string;
  chips?: InsightChip[];
  linkLabel?: string;
  linkHref?: string;
};

export type InsightCardData = {
  eyebrow?: string;
  title: string;
  subtitle?: string;
  badges?: InsightChip[];
  metrics?: InsightMetric[];
  sections?: InsightSection[];
};
