/**
 * Pool writer — writes enriched memories to Engram via HTTP API.
 * Uses pool naming convention: pool:{client}:{channel}
 */

import { EnrichedMemory } from "./types.js";

export interface PoolWriterOptions {
  engramUrl: string;
  engramApiKey: string;
  userId: string;
}

export function buildPoolName(clientId: string, channel: string): string {
  return `pool:${clientId}:${channel}`;
}

export async function writeMemory(
  memory: EnrichedMemory,
  pool: string,
  opts: PoolWriterOptions,
): Promise<{ ok: boolean; id?: string; error?: string }> {
  try {
    const res = await fetch(`${opts.engramUrl}/v1/memories`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-AM-API-Key": opts.engramApiKey,
        "X-AM-User-ID": opts.userId,
      },
      body: JSON.stringify({
        content: memory.content,
        type: "observation",
        layer: "PROJECT",
        source: "AGENT_OBSERVATION",
        agentId: "engram-ci",
        namespace: pool,
        tags: memory.tags,
        metadata: {
          ...memory.metadata,
          dedupeKey: memory.dedupeKey,
        },
      }),
    });

    if (!res.ok) {
      const body = await res.text();
      return { ok: false, error: `HTTP ${res.status}: ${body.slice(0, 200)}` };
    }

    const data = await res.json() as { id?: string };
    return { ok: true, id: data.id };
  } catch (err: any) {
    return { ok: false, error: err.message };
  }
}

export async function writeMemories(
  memories: EnrichedMemory[],
  pool: string,
  opts: PoolWriterOptions,
  concurrency: number = 3,
): Promise<{ written: number; errors: string[] }> {
  let written = 0;
  const errors: string[] = [];

  // Simple concurrency limiter
  const queue = [...memories];
  const workers = Array.from({ length: concurrency }, async () => {
    while (queue.length > 0) {
      const mem = queue.shift();
      if (!mem) break;
      const result = await writeMemory(mem, pool, opts);
      if (result.ok) {
        written++;
      } else {
        errors.push(`${mem.dedupeKey}: ${result.error}`);
      }
    }
  });

  await Promise.all(workers);
  return { written, errors };
}
