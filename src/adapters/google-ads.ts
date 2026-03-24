/**
 * Google Ads CSV adapter.
 * Converts raw CSV rows into NormalizedRecords.
 */

import { readFileSync } from "fs";
import { NormalizedRecord, Channel } from "../types.js";
import {
  parseGoogleAdsCSV,
  parseCurrency,
  parsePct,
  parseNum,
  parseStr,
  detectReportType,
} from "./csv-parser.js";

// Column name normalization map — handles variations across export types
const COLUMN_MAP: Record<string, string> = {
  // Identity
  "Campaign": "campaignName",
  "Campaign type": "campaignType",
  "Campaign state": "campaignState",
  "Campaign status": "campaignState",
  "Campaign subtype": "campaignSubtype",
  "Ad group": "adGroupName",
  "Ad group state": "adGroupState",
  "Ad group bid strategy": "bidStrategy",
  "Ad group bid strategy type": "bidStrategyType",
  "Keyword": "keyword",
  "Match type": "matchType",

  // Performance
  "Impr.": "impressions",
  "Impressions": "impressions",
  "Clicks": "clicks",
  "CTR": "ctr",
  "Avg. CPC": "cpc",
  "Cost": "cost",
  "Conversions": "conversions",
  "Conv. rate": "convRate",
  "Cost / conv.": "costPerConversion",
  "View-through conv.": "viewThroughConv",
  "Impr. (Abs. Top) %": "absTopImprShare",
  "Impr. (Top) %": "topImprShare",

  // Segmentation
  "Device": "device",
  "Audience segment": "audienceSegment",
  "Hour of day": "hourOfDay",
  "Day of week": "dayOfWeek",

  // Competitive
  "Display URL domain": "competitorDomain",
  "Impression share": "impressionShare",
  "Overlap rate": "overlapRate",
  "Position above rate": "positionAboveRate",
  "Top of page rate": "topOfPageRate",

  // Keywords & Search Terms
  "Search Keyword": "keyword",
  "Criterion Status": "criterionStatus",
  "Campaign Status": "campaignState",
  "Ad Group Status": "adGroupState",
  "Word": "searchWord",
  "Search": "searchQuery",
  "Top Containing Queries": "topQueries",

  // Demographics
  "Gender": "gender",
  "Age Range": "ageRange",
  "Percent of known total": "pctOfTotal",

  // Period Comparison (Biggest Changes)
  "Campaign Name": "campaignName",
  "Cost (Comparison)": "costComparison",
  "Clicks (Comparison)": "clicksComparison",
  "Interactions": "interactions",
  "Interactions (Comparison)": "interactionsComparison",

  // Networks
  "Network": "network",

  // Competitor / Auction Insights
  "Advertiser Name": "advertiserName",
  "Impression share (Comparison)": "impressionShareComp",
  "Outranking share": "outrankingShare",
  "Outranking share (Comparison)": "outrankingShareComp",
  "Overlap rate (Comparison)": "overlapRateComp",
  "Top of page rate (Comparison)": "topOfPageRateComp",
  "Position above rate (Comparison)": "positionAboveRateComp",

  // Other
  "Currency code": "currency",
  "Labels on Ad group": "labels",
  "Budget": "budget",
  "Ads: active": "activeAds",
  "Ads: disapproved": "disapprovedAds",
  "Keywords: active": "activeKeywords",
  "Keywords: disapproved": "disapprovedKeywords",
  "Ad group desktop bid adj.": "desktopBidAdj",
  "Ad group mobile bid adj.": "mobileBidAdj",
  "Ad group tablet bid adj.": "tabletBidAdj",
};

// Fields that should be parsed as currency
const CURRENCY_FIELDS = new Set(["cost", "cpc", "costPerConversion", "budget", "costComparison"]);

// Fields that should be parsed as percentages
const PCT_FIELDS = new Set([
  "ctr", "convRate", "impressionShare", "impressionShareComp",
  "overlapRate", "overlapRateComp", "outrankingShare", "outrankingShareComp",
  "positionAboveRate", "positionAboveRateComp",
  "topOfPageRate", "topOfPageRateComp",
  "absTopImprShare", "topImprShare", "pctOfTotal",
]);

// Fields that should be parsed as numbers
const NUM_FIELDS = new Set([
  "impressions", "clicks", "conversions", "viewThroughConv",
  "hourOfDay", "activeAds", "disapprovedAds", "activeKeywords", "disapprovedKeywords",
  "clicksComparison", "interactions", "interactionsComparison",
]);

function normalizeRow(
  row: Record<string, string>,
  recordType: string,
): Record<string, number | string | boolean | null> {
  const parsed: Record<string, number | string | boolean | null> = {};

  for (const [rawKey, rawVal] of Object.entries(row)) {
    const key = COLUMN_MAP[rawKey] || rawKey;

    if (CURRENCY_FIELDS.has(key)) {
      parsed[key] = parseCurrency(rawVal);
    } else if (PCT_FIELDS.has(key)) {
      parsed[key] = parsePct(rawVal);
    } else if (NUM_FIELDS.has(key)) {
      parsed[key] = parseNum(rawVal);
    } else {
      parsed[key] = parseStr(rawVal);
    }
  }

  parsed.recordType = recordType;
  return parsed;
}

function hasActivity(parsed: Record<string, number | string | boolean | null>, recordType: string): boolean {
  // These record types are always relevant
  if (["competitor", "network", "search-term", "search-query", "period-comparison", "audience-gender", "audience-gender-age"].includes(recordType)) return true;

  const clicks = parsed.clicks as number | null;
  const impressions = parsed.impressions as number | null;
  const cost = parsed.cost as number | null;
  return (clicks != null && clicks > 0) ||
    (impressions != null && impressions > 0) ||
    (cost != null && cost > 0);
}

export function ingestGoogleAdsFile(
  filePath: string,
  clientId: string,
  dateStartOverride?: string,
  dateEndOverride?: string,
): NormalizedRecord[] {
  const content = readFileSync(filePath, "utf8");
  const csv = parseGoogleAdsCSV(content);

  const recordType = detectReportType(csv.reportTitle);
  const dateStart = dateStartOverride || csv.dateRange?.start || "unknown";
  const dateEnd = dateEndOverride || csv.dateRange?.end || "unknown";

  const records: NormalizedRecord[] = [];

  for (const row of csv.rows) {
    const parsed = normalizeRow(row, recordType);

    // Skip rows with zero activity (paused campaigns with no data)
    if (!hasActivity(parsed, recordType)) continue;

    records.push({
      clientId,
      channel: "google-ads" as Channel,
      recordType,
      sourceFile: filePath,
      rawRow: row,
      parsed,
      dateStart,
      dateEnd,
    });
  }

  return records;
}
