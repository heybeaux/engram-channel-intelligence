#!/usr/bin/env node

/**
 * engram-ci CLI
 * Usage:
 *   engram-ci ingest google-ads --client <slug> --file <path> [options]
 *   engram-ci ingest google-ads --client <slug> --dir <path> --batch [options]
 */

import { ingestGoogleAdsFile } from "./adapters/google-ads.js";
import { enrichRecord } from "./enrichment/format-b.js";
import { buildPoolName, writeMemories, PoolWriterOptions } from "./pool-writer.js";
import { IngestResult } from "./types.js";
import { readdirSync, statSync } from "fs";
import { join } from "path";

function usage() {
  console.log(`
engram-ci — Channel Intelligence Ingestion CLI

Usage:
  engram-ci ingest google-ads --client <slug> --file <path> [options]
  engram-ci ingest google-ads --client <slug> --dir <path> --batch [options]

Options:
  --client <slug>       Client slug (e.g., "map-international")
  --file <path>         Single CSV file to ingest
  --dir <path>          Directory of CSV files (use with --batch)
  --batch               Batch ingest all CSVs in directory
  --date-start <date>   Override date start (ISO 8601)
  --date-end <date>     Override date end (ISO 8601)
  --dry-run             Parse and enrich but don't write to Engram
  --engram-url <url>    Engram API URL (default: http://localhost:3002)
  --engram-key <key>    Engram API key
  --user-id <id>        Engram user ID (default: "beaux")
  --json                Output enriched memories as JSON (for dry-run inspection)
`);
}

function parseArgs(args: string[]): Record<string, string | boolean> {
  const parsed: Record<string, string | boolean> = {};
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith("--")) {
      const key = arg.slice(2);
      const next = args[i + 1];
      if (next && !next.startsWith("--")) {
        parsed[key] = next;
        i++;
      } else {
        parsed[key] = true;
      }
    } else if (!parsed._command) {
      parsed._command = arg;
    } else if (!parsed._subcommand) {
      parsed._subcommand = arg;
    }
  }
  return parsed;
}

function findCSVFiles(dir: string): string[] {
  const files: string[] = [];
  const entries = readdirSync(dir);
  for (const entry of entries) {
    const fullPath = join(dir, entry);
    const stat = statSync(fullPath);
    if (stat.isFile() && entry.toLowerCase().endsWith(".csv")) {
      files.push(fullPath);
    } else if (stat.isDirectory()) {
      files.push(...findCSVFiles(fullPath));
    }
  }
  return files;
}

async function ingestGoogleAds(opts: Record<string, string | boolean>): Promise<void> {
  const clientId = opts.client as string;
  if (!clientId) {
    console.error("Error: --client is required");
    process.exit(1);
  }

  const dryRun = !!opts["dry-run"];
  const jsonOutput = !!opts.json;
  const dateStart = opts["date-start"] as string | undefined;
  const dateEnd = opts["date-end"] as string | undefined;

  // Collect files
  const files: string[] = [];
  if (opts.file) {
    files.push(opts.file as string);
  } else if (opts.dir && opts.batch) {
    files.push(...findCSVFiles(opts.dir as string));
  } else {
    console.error("Error: --file or --dir with --batch required");
    process.exit(1);
  }

  if (files.length === 0) {
    console.error("No CSV files found");
    process.exit(1);
  }

  console.log(`\n📊 Ingesting ${files.length} file(s) for client: ${clientId}`);
  console.log(`   Channel: google-ads`);
  console.log(`   Mode: ${dryRun ? "DRY RUN" : "LIVE"}\n`);

  const allResults: IngestResult[] = [];

  for (const file of files) {
    console.log(`📄 Processing: ${file}`);

    try {
      const records = ingestGoogleAdsFile(file, clientId, dateStart, dateEnd);
      console.log(`   Parsed ${records.length} active records`);

      const memories = records.map(enrichRecord);
      console.log(`   Enriched ${memories.length} memories`);

      const result: IngestResult = {
        file,
        recordType: records[0]?.recordType || "unknown",
        totalRows: records.length,
        ingested: 0,
        skipped: 0,
        errors: [],
        memories,
      };

      if (jsonOutput) {
        for (const m of memories) {
          console.log(JSON.stringify(m, null, 2));
        }
      }

      if (!dryRun) {
        const pool = buildPoolName(clientId, "google-ads");
        const engramUrl = (opts["engram-url"] as string) || process.env.ENGRAM_API_URL || "http://localhost:3002";
        const engramKey = (opts["engram-key"] as string) || process.env.ENGRAM_API_KEY || "";
        const userId = (opts["user-id"] as string) || process.env.ENGRAM_USER_ID || "beaux";

        if (!engramKey) {
          console.error("   ⚠️  No Engram API key. Use --engram-key or ENGRAM_API_KEY env var.");
          result.errors.push("Missing API key");
        } else {
          const writerOpts: PoolWriterOptions = { engramUrl, engramApiKey: engramKey, userId };
          console.log(`   Writing to pool: ${pool}`);
          const writeResult = await writeMemories(memories, pool, writerOpts);
          result.ingested = writeResult.written;
          result.errors = writeResult.errors;
          console.log(`   ✅ Written: ${writeResult.written}, Errors: ${writeResult.errors.length}`);
        }
      } else {
        result.ingested = memories.length;
        console.log(`   ✅ Would write ${memories.length} memories (dry run)`);
      }

      allResults.push(result);
    } catch (err: any) {
      console.error(`   ❌ Error: ${err.message}`);
      allResults.push({
        file,
        recordType: "unknown",
        totalRows: 0,
        ingested: 0,
        skipped: 0,
        errors: [err.message],
        memories: [],
      });
    }
  }

  // Summary
  console.log(`\n${"=".repeat(50)}`);
  console.log(`📋 Ingestion Summary`);
  console.log(`${"=".repeat(50)}`);
  const totalIngested = allResults.reduce((s, r) => s + r.ingested, 0);
  const totalErrors = allResults.reduce((s, r) => s + r.errors.length, 0);
  console.log(`   Files: ${allResults.length}`);
  console.log(`   Memories: ${totalIngested}`);
  console.log(`   Errors: ${totalErrors}`);
  if (totalErrors > 0) {
    for (const r of allResults) {
      for (const e of r.errors) {
        console.log(`   ⚠️  ${r.file}: ${e}`);
      }
    }
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (!args._command || args._command === "help") {
    usage();
    process.exit(0);
  }

  if (args._command === "ingest" && args._subcommand === "google-ads") {
    await ingestGoogleAds(args);
  } else {
    console.error(`Unknown command: ${args._command} ${args._subcommand || ""}`);
    usage();
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
