// Public API
export { ingestGoogleAdsFile } from "./adapters/google-ads.js";
export { enrichRecord } from "./enrichment/format-b.js";
export { buildPoolName, writeMemory, writeMemories, recallMemories } from "./pool-writer.js";
export type { NormalizedRecord, EnrichedMemory, MemoryMetadata, IngestOptions, IngestResult } from "./types.js";
