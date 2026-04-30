import { describe, expect, it } from "vitest";
import {
  compactText,
  formatDate,
  formatLabel,
  formatScore,
  normalizeTextBlock,
  parseJsonArray,
  scoreBand,
} from "@/lib/formatters";

describe("formatLabel", () => {
  it("replaces underscores and capitalizes", () => {
    expect(formatLabel("closed_won")).toBe("Closed Won");
  });

  it("returns Unknown for falsy input", () => {
    expect(formatLabel(null)).toBe("Unknown");
    expect(formatLabel("")).toBe("Unknown");
  });
});

describe("formatDate", () => {
  it("formats valid ISO date", () => {
    const result = formatDate("2026-04-15T12:00:00Z");
    expect(result).toContain("Apr");
    expect(result).toContain("2026");
  });

  it("returns fallback for null", () => {
    expect(formatDate(null)).toBe("Unknown date");
  });
});

describe("formatScore", () => {
  it("rounds to nearest integer", () => {
    expect(formatScore(88.7)).toBe(89);
  });

  it("defaults to 0 for null", () => {
    expect(formatScore(null)).toBe(0);
  });
});

describe("scoreBand", () => {
  it("returns High for >= 80", () => {
    expect(scoreBand(85)).toBe("High");
  });

  it("returns Medium for >= 60", () => {
    expect(scoreBand(65)).toBe("Medium");
  });

  it("returns Low for >= 40", () => {
    expect(scoreBand(45)).toBe("Low");
  });

  it("returns Watch for < 40", () => {
    expect(scoreBand(20)).toBe("Watch");
  });
});

describe("parseJsonArray", () => {
  it("parses JSON string array", () => {
    expect(parseJsonArray('["a","b"]')).toEqual(["a", "b"]);
  });

  it("returns empty for falsy", () => {
    expect(parseJsonArray(null)).toEqual([]);
  });

  it("passes through array values", () => {
    expect(parseJsonArray(["x", "y"])).toEqual(["x", "y"]);
  });
});

describe("compactText", () => {
  it("collapses whitespace", () => {
    expect(compactText("  hello   world  ")).toBe("hello world");
  });

  it("returns placeholder for null", () => {
    expect(compactText(null)).toBe("No supporting text is available yet.");
  });
});

describe("normalizeTextBlock", () => {
  it("collapses multiple blank lines", () => {
    const result = normalizeTextBlock("line1\n\n\n\nline2");
    expect(result).toBe("line1\n\nline2");
  });
});
