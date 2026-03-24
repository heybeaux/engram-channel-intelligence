// ---- Core types ----

export type Channel = "google-ads" | "email" | "sms";

export type GoogleAdsRecordType =
  | "campaign"
  | "ad-group"
  | "keyword"
  | "device"
  | "audience"
  | "hourly"
  | "dow"
  | "competitor";

export interface NormalizedRecord {
  clientId: string;
  channel: Channel;
  recordType: string;
  sourceFile: string;
  rawRow: Record<string, string>;
  parsed: Record<string, number | string | boolean | null>;
  dateStart: string;
  dateEnd: string;
}

export interface EnrichedMemory {
  content: string;
  tags: string[];
  metadata: MemoryMetadata;
  dedupeKey: string;
}

export interface MemoryMetadata {
  clientId: string;
  channel: Channel;
  recordType: string;
  dateStart: string;
  dateEnd: string;
  reportedAt: string;
  campaignName?: string;
  campaignType?: string;
  device?: string;
  dayOfWeek?: string;
  hourOfDay?: number;
  quarter?: string;
  month?: string;
  [key: string]: string | number | boolean | undefined;
}

export interface ClientConfig {
  slug: string;
  name: string;
  channels: Channel[];
}

export interface IngestOptions {
  clientId: string;
  channel: Channel;
  file: string;
  dateStart: string;
  dateEnd: string;
  dryRun?: boolean;
  engramUrl?: string;
  engramApiKey?: string;
  userId?: string;
}

export interface IngestResult {
  file: string;
  recordType: string;
  totalRows: number;
  ingested: number;
  skipped: number;
  errors: string[];
  memories: EnrichedMemory[];
}
