/**
 * Format B enrichment.
 * Takes normalized records and generates:
 * - Natural language content summaries
 * - Insight tags
 * - Full tag arrays
 * - Metadata objects
 */

import { NormalizedRecord, EnrichedMemory, MemoryMetadata } from "../types.js";
import { createHash } from "crypto";

// ---- Content Generation ----

function fmtPct(val: number | null | undefined): string {
  if (val == null) return "N/A";
  return `${(val * 100).toFixed(1)}%`;
}

function fmtCurrency(val: number | null | undefined): string {
  if (val == null) return "N/A";
  return `$${val.toFixed(2)}`;
}

function fmtNum(val: number | null | undefined): string {
  if (val == null) return "N/A";
  return val.toLocaleString("en-US");
}

function generateAdGroupContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const adGroup = p.adGroupName || "Unknown";
  const campaign = p.campaignName || "Unknown";
  const campaignType = p.campaignType || "Unknown";
  const state = p.campaignState || "Unknown";

  const parts = [
    `${record.clientId} ad group "${adGroup}" in campaign "${campaign}" (${campaignType}, ${state}).`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.impressions != null) parts.push(`${fmtNum(p.impressions as number)} impressions.`);
  if (p.clicks != null) parts.push(`${fmtNum(p.clicks as number)} clicks.`);
  if (p.ctr != null) parts.push(`CTR: ${fmtPct(p.ctr as number)}.`);
  if (p.cost != null) parts.push(`Spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.cpc != null) parts.push(`Avg CPC: ${fmtCurrency(p.cpc as number)}.`);
  if (p.conversions != null) parts.push(`${fmtNum(p.conversions as number)} conversions.`);
  if (p.convRate != null) parts.push(`Conv rate: ${fmtPct(p.convRate as number)}.`);
  if (p.costPerConversion != null) parts.push(`Cost/conv: ${fmtCurrency(p.costPerConversion as number)}.`);
  if (p.absTopImprShare != null) parts.push(`Abs top impression share: ${fmtPct(p.absTopImprShare as number)}.`);
  if (p.topImprShare != null) parts.push(`Top impression share: ${fmtPct(p.topImprShare as number)}.`);
  if (p.bidStrategyType) parts.push(`Bid strategy: ${p.bidStrategyType}.`);

  return parts.join(" ");
}

function generateCampaignContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const campaign = p.campaignName || "Unknown";
  const campaignType = p.campaignType || "Unknown";
  const state = p.campaignState || "Unknown";

  const parts = [
    `${record.clientId} campaign "${campaign}" (${campaignType}, ${state}).`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.impressions != null) parts.push(`${fmtNum(p.impressions as number)} impressions.`);
  if (p.clicks != null) parts.push(`${fmtNum(p.clicks as number)} clicks.`);
  if (p.ctr != null) parts.push(`CTR: ${fmtPct(p.ctr as number)}.`);
  if (p.cost != null) parts.push(`Spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.cpc != null) parts.push(`Avg CPC: ${fmtCurrency(p.cpc as number)}.`);
  if (p.conversions != null) parts.push(`${fmtNum(p.conversions as number)} conversions.`);
  if (p.convRate != null) parts.push(`Conv rate: ${fmtPct(p.convRate as number)}.`);
  if (p.costPerConversion != null) parts.push(`Cost/conv: ${fmtCurrency(p.costPerConversion as number)}.`);

  return parts.join(" ");
}

function generateDeviceContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const device = p.device || "Unknown";
  const campaign = p.campaignName || "all campaigns";

  const parts = [
    `${record.clientId} device breakdown: ${device} for ${campaign}.`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.impressions != null) parts.push(`${fmtNum(p.impressions as number)} impressions.`);
  if (p.clicks != null) parts.push(`${fmtNum(p.clicks as number)} clicks.`);
  if (p.cost != null) parts.push(`Spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.conversions != null) parts.push(`${fmtNum(p.conversions as number)} conversions.`);

  return parts.join(" ");
}

function generateAudienceContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const segment = p.audienceSegment || "Unknown";
  const campaign = p.campaignName || "all campaigns";

  const parts = [
    `${record.clientId} audience segment: ${segment} for ${campaign}.`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.impressions != null) parts.push(`${fmtNum(p.impressions as number)} impressions.`);
  if (p.clicks != null) parts.push(`${fmtNum(p.clicks as number)} clicks.`);
  if (p.cost != null) parts.push(`Spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.conversions != null) parts.push(`${fmtNum(p.conversions as number)} conversions.`);

  return parts.join(" ");
}

function generateCompetitorContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const domain = p.competitorDomain || p.advertiserName || "Unknown";

  const parts = [
    `${record.clientId} competitor: ${domain}.`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.impressionShare != null) parts.push(`Impression share: ${fmtPct(p.impressionShare as number)}.`);
  if (p.overlapRate != null) parts.push(`Overlap rate: ${fmtPct(p.overlapRate as number)}.`);
  if (p.positionAboveRate != null) parts.push(`Position above rate: ${fmtPct(p.positionAboveRate as number)}.`);
  if (p.topOfPageRate != null) parts.push(`Top of page rate: ${fmtPct(p.topOfPageRate as number)}.`);

  return parts.join(" ");
}

function generateHourlyContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const hour = p.hourOfDay ?? "Unknown";

  const parts = [
    `${record.clientId} hourly performance: Hour ${hour}.`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.impressions != null) parts.push(`${fmtNum(p.impressions as number)} impressions.`);
  if (p.clicks != null) parts.push(`${fmtNum(p.clicks as number)} clicks.`);
  if (p.cost != null) parts.push(`Spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.conversions != null) parts.push(`${fmtNum(p.conversions as number)} conversions.`);

  return parts.join(" ");
}

function generateKeywordContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const keyword = p.keyword || "Unknown";
  const matchType = p.matchType || "Unknown";

  const parts = [
    `${record.clientId} keyword "${keyword}" (${matchType}).`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.clicks != null) parts.push(`${fmtNum(p.clicks as number)} clicks.`);
  if (p.cost != null) parts.push(`Spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.ctr != null) parts.push(`CTR: ${fmtPct(p.ctr as number)}.`);
  if (p.impressions != null) parts.push(`${fmtNum(p.impressions as number)} impressions.`);
  if (p.conversions != null) parts.push(`${fmtNum(p.conversions as number)} conversions.`);

  return parts.join(" ");
}

function generateNetworkContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const network = p.network || "Unknown";

  const parts = [
    `${record.clientId} network: ${network}.`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.clicks != null) parts.push(`${fmtNum(p.clicks as number)} clicks.`);
  if (p.cost != null) parts.push(`Spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.cpc != null) parts.push(`Avg CPC: ${fmtCurrency(p.cpc as number)}.`);

  return parts.join(" ");
}

function generateSearchTermContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const word = p.searchWord || "Unknown";

  const parts = [
    `${record.clientId} search term word: "${word}".`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.clicks != null) parts.push(`${fmtNum(p.clicks as number)} clicks.`);
  if (p.cost != null) parts.push(`Spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.impressions != null) parts.push(`${fmtNum(p.impressions as number)} impressions.`);
  if (p.conversions != null) parts.push(`${fmtNum(p.conversions as number)} conversions.`);
  if (p.topQueries) parts.push(`Top queries: ${p.topQueries}.`);

  return parts.join(" ");
}

function generateDemographicContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const gender = p.gender || "Unknown";
  const ageRange = p.ageRange as string | null;

  const label = ageRange ? `${gender}, ${ageRange}` : gender;

  const parts = [
    `${record.clientId} demographic: ${label}.`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.impressions != null) parts.push(`${fmtNum(p.impressions as number)} impressions.`);
  if (p.pctOfTotal != null) parts.push(`${fmtPct(p.pctOfTotal as number)} of known total.`);
  if (p.clicks != null) parts.push(`${fmtNum(p.clicks as number)} clicks.`);
  if (p.cost != null) parts.push(`Spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.conversions != null) parts.push(`${fmtNum(p.conversions as number)} conversions.`);

  return parts.join(" ");
}

function generateSearchQueryContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const query = p.searchQuery || "Unknown";

  const parts = [
    `${record.clientId} search query: "${query}".`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.clicks != null) parts.push(`${fmtNum(p.clicks as number)} clicks.`);
  if (p.cost != null) parts.push(`Spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.impressions != null) parts.push(`${fmtNum(p.impressions as number)} impressions.`);
  if (p.conversions != null) parts.push(`${fmtNum(p.conversions as number)} conversions.`);

  return parts.join(" ");
}

function generatePeriodComparisonContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const campaign = p.campaignName || "Unknown";

  const parts = [
    `${record.clientId} period comparison for campaign "${campaign}".`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.cost != null) parts.push(`Current spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.costComparison != null) parts.push(`Previous spend: ${fmtCurrency(p.costComparison as number)}.`);
  if (p.clicks != null) parts.push(`Current clicks: ${fmtNum(p.clicks as number)}.`);
  if (p.clicksComparison != null) parts.push(`Previous clicks: ${fmtNum(p.clicksComparison as number)}.`);
  if (p.interactions != null) parts.push(`Current interactions: ${fmtNum(p.interactions as number)}.`);
  if (p.interactionsComparison != null) parts.push(`Previous interactions: ${fmtNum(p.interactionsComparison as number)}.`);

  // Compute change direction
  if (typeof p.cost === "number" && typeof p.costComparison === "number" && p.costComparison > 0) {
    const change = ((p.cost - p.costComparison) / p.costComparison) * 100;
    parts.push(`Spend change: ${change > 0 ? "+" : ""}${change.toFixed(0)}%.`);
  }

  return parts.join(" ");
}

function generateGenericContent(record: NormalizedRecord): string {
  const p = record.parsed;
  const parts = [
    `${record.clientId} ${record.recordType} data.`,
    `Period: ${record.dateStart} to ${record.dateEnd}.`,
  ];

  if (p.impressions != null) parts.push(`${fmtNum(p.impressions as number)} impressions.`);
  if (p.clicks != null) parts.push(`${fmtNum(p.clicks as number)} clicks.`);
  if (p.cost != null) parts.push(`Spend: ${fmtCurrency(p.cost as number)}.`);
  if (p.conversions != null) parts.push(`${fmtNum(p.conversions as number)} conversions.`);

  return parts.join(" ");
}

function generateContent(record: NormalizedRecord): string {
  switch (record.recordType) {
    case "ad-group": return generateAdGroupContent(record);
    case "campaign": return generateCampaignContent(record);
    case "device": return generateDeviceContent(record);
    case "audience": return generateAudienceContent(record);
    case "competitor": return generateCompetitorContent(record);
    case "hourly": return generateHourlyContent(record);
    case "keyword": return generateKeywordContent(record);
    case "network": return generateNetworkContent(record);
    case "search-term": return generateSearchTermContent(record);
    case "search-query": return generateSearchQueryContent(record);
    case "period-comparison": return generatePeriodComparisonContent(record);
    case "audience-gender": return generateDemographicContent(record);
    case "audience-gender-age": return generateDemographicContent(record);
    default: return generateGenericContent(record);
  }
}

// ---- Insight Computation ----

function computeInsights(record: NormalizedRecord): string[] {
  const insights: string[] = [];
  const p = record.parsed;

  // High conversion rate (>50%)
  if (typeof p.convRate === "number" && p.convRate > 0.5) {
    insights.push("high-conv-rate");
  }
  // Low conversion rate (<5%)
  if (typeof p.convRate === "number" && p.convRate > 0 && p.convRate < 0.05) {
    insights.push("low-conv-rate");
  }

  // High CTR (>5%)
  if (typeof p.ctr === "number" && p.ctr > 0.05) {
    insights.push("high-ctr");
  }

  // Low CPC (<$2)
  if (typeof p.cpc === "number" && p.cpc > 0 && p.cpc < 2) {
    insights.push("low-cpc");
  }
  // High CPC (>$10)
  if (typeof p.cpc === "number" && p.cpc > 10) {
    insights.push("high-cpc");
  }

  // Paused campaign
  if (p.campaignState === "Paused") {
    insights.push("paused");
  }

  // High spend (>$500)
  if (typeof p.cost === "number" && p.cost > 500) {
    insights.push("high-spend");
  }

  return insights;
}

// ---- Tag Building ----

function buildTags(record: NormalizedRecord, insights: string[]): string[] {
  const tags: string[] = [
    `client:${record.clientId}`,
    `channel:${record.channel}`,
    `record-type:${record.recordType}`,
  ];

  const p = record.parsed;

  if (p.campaignName) tags.push(`campaign:${p.campaignName}`);
  if (p.campaignType) tags.push(`campaign-type:${String(p.campaignType).toLowerCase()}`);
  if (p.device) tags.push(`device:${String(p.device).toLowerCase()}`);
  if (p.dayOfWeek) tags.push(`send-day:${String(p.dayOfWeek).toLowerCase()}`);
  if (p.campaignState) tags.push(`status:${String(p.campaignState).toLowerCase()}`);
  if (p.keyword) tags.push(`keyword:${String(p.keyword).toLowerCase()}`);
  if (p.network) tags.push(`network:${String(p.network).toLowerCase()}`);
  if (p.searchWord) tags.push(`search-word:${String(p.searchWord).toLowerCase()}`);
  if (p.advertiserName) tags.push(`advertiser:${String(p.advertiserName).toLowerCase()}`);
  if (p.matchType) tags.push(`match-type:${String(p.matchType).toLowerCase()}`);
  if (p.gender) tags.push(`gender:${String(p.gender).toLowerCase()}`);
  if (p.ageRange) tags.push(`age:${String(p.ageRange).toLowerCase()}`);

  // Period tags
  if (record.dateStart && record.dateStart !== "unknown") {
    const d = new Date(record.dateStart);
    const month = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
    const q = Math.ceil((d.getMonth() + 1) / 3);
    tags.push(`period:${month}`);
    tags.push(`period:${d.getFullYear()}-q${q}`);
  }

  // Insight tags
  for (const insight of insights) {
    tags.push(`insight:${insight}`);
  }

  return tags;
}

// ---- Metadata Building ----

function buildMetadata(record: NormalizedRecord): MemoryMetadata {
  const p = record.parsed;
  const d = record.dateStart !== "unknown" ? new Date(record.dateStart) : null;

  return {
    clientId: record.clientId,
    channel: record.channel,
    recordType: record.recordType,
    dateStart: record.dateStart,
    dateEnd: record.dateEnd,
    reportedAt: new Date().toISOString(),
    campaignName: p.campaignName as string | undefined,
    campaignType: p.campaignType as string | undefined,
    device: p.device as string | undefined,
    dayOfWeek: p.dayOfWeek as string | undefined,
    hourOfDay: p.hourOfDay as number | undefined,
    quarter: d ? `${d.getFullYear()}-Q${Math.ceil((d.getMonth() + 1) / 3)}` : undefined,
    month: d ? `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}` : undefined,
  };
}

// ---- Dedup Key ----

function buildDedupeKey(record: NormalizedRecord): string {
  const p = record.parsed;
  const parts = [
    record.clientId,
    record.channel,
    record.recordType,
    record.dateStart,
    record.dateEnd,
    p.campaignName || "",
    p.adGroupName || "",
    p.device || "",
    p.audienceSegment || "",
    p.hourOfDay ?? "",
    p.competitorDomain || "",
    p.keyword || "",
    p.network || "",
    p.searchWord || "",
    p.searchQuery || "",
    p.advertiserName || "",
    p.gender || "",
    p.ageRange || "",
  ].join(":");

  return createHash("sha256").update(parts).digest("hex").slice(0, 16);
}

// ---- Main Enrichment Function ----

export function enrichRecord(record: NormalizedRecord): EnrichedMemory {
  const content = generateContent(record);
  const insights = computeInsights(record);
  const tags = buildTags(record, insights);
  const metadata = buildMetadata(record);
  const dedupeKey = buildDedupeKey(record);

  return { content, tags, metadata, dedupeKey };
}
