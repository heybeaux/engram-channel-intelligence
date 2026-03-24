/**
 * Google Ads CSV parser.
 * Handles real Google Ads export format quirks:
 * - Line 1: Report title (e.g., "Ad group performance")
 * - Line 2: Date range in quotes (e.g., "February 7, 2026 - March 6, 2026")
 * - Line 3: Column headers
 * - Line 4+: Data rows
 * - Numbers with commas in quotes: "16,772"
 * - "--" for empty/NA values
 * - Percentages with % suffix
 * - Currency with $ prefix
 */

import { parse } from "csv-parse/sync";

export interface ParsedCSV {
  reportTitle: string;
  dateRange: { start: string; end: string } | null;
  headers: string[];
  rows: Record<string, string>[];
}

/** Parse the date range line: "February 7, 2026 - March 6, 2026" */
function parseDateRange(line: string): { start: string; end: string } | null {
  const cleaned = line.replace(/^"|"$/g, "").trim();
  const match = cleaned.match(/^(.+?)\s*-\s*(.+)$/);
  if (!match) return null;

  try {
    const start = new Date(match[1].trim());
    const end = new Date(match[2].trim());
    if (isNaN(start.getTime()) || isNaN(end.getTime())) return null;
    return {
      start: start.toISOString().split("T")[0],
      end: end.toISOString().split("T")[0],
    };
  } catch {
    return null;
  }
}

export function parseGoogleAdsCSV(content: string): ParsedCSV {
  const lines = content.split("\n");

  // Detect format: some exports have 2 header lines (title + date range),
  // others start directly with column headers.
  // Heuristic: if line 2 looks like a date range (starts with "), skip 2 lines.
  // Otherwise, line 1 might be a title with no date range, or it might be
  // the column headers directly.

  let reportTitle = "Unknown";
  let dateRange: { start: string; end: string } | null = null;
  let csvStartLine = 0;

  const line1 = lines[0]?.trim() || "";
  const line2 = lines[1]?.trim() || "";

  // Check if line 2 is a date range (quoted date pattern)
  const line2DateRange = parseDateRange(line2);
  if (line2DateRange) {
    // Format A: title on line 1, date range on line 2, headers on line 3
    reportTitle = line1;
    dateRange = line2DateRange;
    csvStartLine = 2;
  } else if (line1.includes(",") && !line1.startsWith('"')) {
    // Looks like CSV headers directly — no title/date header lines
    reportTitle = detectTitleFromHeaders(line1);
    csvStartLine = 0;
  } else {
    // Line 1 is a title but no date range on line 2
    reportTitle = line1;
    csvStartLine = 1;
  }

  const csvContent = lines.slice(csvStartLine).join("\n");

  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true,
  }) as Record<string, string>[];

  const headers = records.length > 0 ? Object.keys(records[0]) : [];

  return { reportTitle, dateRange, headers, rows: records };
}

/** Infer a report title from the CSV header columns */
function detectTitleFromHeaders(headerLine: string): string {
  const h = headerLine.toLowerCase();
  if (h.includes("search keyword")) return "Search keywords";
  if (h.startsWith("search,")) return "Searches search";
  if (h.includes("word") && h.includes("top containing")) return "Searches word";
  if (h.includes("network")) return "Networks";
  if (h.includes("advertiser name")) return "Auction insights";
  if (h.startsWith("device,")) return "Device performance";
  if (h.startsWith("gender,age")) return "Demographics gender age";
  if (h.startsWith("gender,")) return "Demographics gender";
  if (h.includes("campaign name") && h.includes("comparison")) return "Biggest changes";
  if (h.includes("audience segment")) return "Audience performance";
  if (h.includes("hour of day")) return "Hourly performance";
  if (h.includes("day of week")) return "Day of week performance";
  if (h.includes("ad group")) return "Ad group performance";
  if (h.includes("campaign")) return "Campaign performance";
  return "Unknown";
}

// ---- Value Parsers ----

/** Parse currency: "$642.42" → 642.42, "$1,347.42" → 1347.42 */
export function parseCurrency(val: string): number | null {
  if (!val || val === "--" || val === "") return null;
  const cleaned = val.replace(/[$,]/g, "").trim();
  const num = parseFloat(cleaned);
  return isNaN(num) ? null : num;
}

/** Parse percentage: "34.35%" → 0.3435, "0.60%" → 0.006 */
export function parsePct(val: string): number | null {
  if (!val || val === "--" || val === "" || val === "0") return null;
  const cleaned = val.replace("%", "").trim();
  const num = parseFloat(cleaned);
  return isNaN(num) ? null : num / 100;
}

/** Parse integer: "16,772" → 16772, "556" → 556 */
export function parseNum(val: string): number | null {
  if (!val || val === "--" || val === "") return null;
  const cleaned = val.replace(/,/g, "").replace(/"/g, "").trim();
  const num = parseFloat(cleaned);
  return isNaN(num) ? null : num;
}

/** Parse a value that could be "--" (NA) */
export function parseStr(val: string): string | null {
  if (!val || val.trim() === "--" || val.trim() === "") return null;
  return val.trim();
}

/** Detect report type from the title line */
export function detectReportType(title: string): string {
  const t = title.toLowerCase();
  if (t.includes("search keyword") || t.includes("search keywords")) return "keyword";
  if (t.includes("searches search")) return "search-query";
  if (t.includes("searches word") || t.includes("search word")) return "search-term";
  if (t.includes("demographics gender age")) return "audience-gender-age";
  if (t.includes("demographics gender")) return "audience-gender";
  if (t.includes("biggest change")) return "period-comparison";
  if (t.includes("network")) return "network";
  if (t.includes("auction")) return "competitor";
  if (t.includes("ad group")) return "ad-group";
  if (t.includes("device")) return "device";
  if (t.includes("campaign")) return "campaign";
  if (t.includes("audience") || t.includes("demographic")) return "audience";
  if (t.includes("hour")) return "hourly";
  if (t.includes("day of week") || t.includes("day-of-week")) return "dow";
  if (t.includes("competitor")) return "competitor";
  return "unknown";
}
