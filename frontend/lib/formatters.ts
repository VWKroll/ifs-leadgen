export function formatLabel(value?: string | null): string {
  if (!value) return "Unknown";
  return value.replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

export function formatDate(value?: string | null): string {
  if (!value) return "Unknown date";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  }).format(date);
}

export function formatScore(value?: number | null, scale = 1): number {
  return Math.round((value ?? 0) * scale);
}

export function scoreBand(value?: number | null): string {
  const score = Math.round(value ?? 0);
  if (score >= 80) return "High";
  if (score >= 60) return "Medium";
  if (score >= 40) return "Low";
  return "Watch";
}

export const SCORE_TOOLTIPS = {
  priority: "Represents how actionable or urgent this opportunity is based on scoring rules",
  confidence: "Represents the reliability of the data and supporting evidence",
  opportunity: "Combined score derived from priority and confidence to rank opportunities",
} as const;

export function parseJsonArray(value: unknown): string[] {
  if (!value) return [];
  if (Array.isArray(value)) return value.map(String).filter(Boolean);
  if (typeof value !== "string") return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.map(String).filter(Boolean) : [];
  } catch {
    return [];
  }
}

export function compactText(value?: string | null): string {
  if (!value) return "No supporting text is available yet.";
  return value.replace(/\s+/g, " ").trim();
}

export function normalizeTextBlock(value?: string | null): string {
  if (!value) return "No supporting text is available yet.";

  const lines = value
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line) => line.replace(/\s+/g, " ").trim());

  const normalized: string[] = [];
  let previousBlank = false;

  for (const line of lines) {
    if (!line) {
      if (!previousBlank && normalized.length) {
        normalized.push("");
      }
      previousBlank = true;
      continue;
    }

    normalized.push(line);
    previousBlank = false;
  }

  return normalized.join("\n").trim();
}

export function firstSentence(value?: string | null): string {
  const text = compactText(value);
  const match = text.match(/.*?[.!?](\s|$)/);
  return match ? match[0].trim() : text;
}
