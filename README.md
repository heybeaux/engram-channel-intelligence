# @openengram/channel-intelligence

Channel intelligence ingestion tooling for Engram.

This package provides the `engram-ci` CLI and a small library for parsing channel exports, enriching them into Engram memory records, and writing them to Engram memory pools. The first supported adapter is Google Ads CSV exports.

## Install

```bash
npm install -g @openengram/channel-intelligence
```

## CLI

```bash
engram-ci ingest google-ads --client map-international --file ./campaigns.csv --dry-run
engram-ci ingest google-ads --client map-international --dir ./exports --batch --engram-key eng_xxx
```

Options:

- `--client <slug>` — client slug, for example `map-international`
- `--file <path>` — single CSV file to ingest
- `--dir <path> --batch` — recursively ingest CSV files from a directory
- `--date-start <date>` / `--date-end <date>` — override reporting dates
- `--dry-run` — parse/enrich without writing to Engram
- `--engram-url <url>` — Engram API URL; defaults to `ENGRAM_API_URL` or local development
- `--engram-key <key>` — Engram API key; defaults to `ENGRAM_API_KEY`
- `--user-id <id>` — Engram user ID; defaults to `ENGRAM_USER_ID` or `beaux`
- `--json` — emit enriched memories as JSON for inspection

## Library

```ts
import { ingestGoogleAdsFile, enrichRecord, writeMemories } from '@openengram/channel-intelligence';

const records = ingestGoogleAdsFile('./campaigns.csv', 'map-international');
const memories = records.map(enrichRecord);
await writeMemories(memories, 'pool:map-international:google-ads', {
  engramUrl: 'https://api.openengram.ai',
  engramApiKey: process.env.ENGRAM_API_KEY!,
  userId: 'beaux',
});
```

## License

ISC
