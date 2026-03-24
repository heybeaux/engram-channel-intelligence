/**
 * Pool writer — writes enriched memories to Engram via HTTP API.
 * Uses pool naming convention: pool:{client}:{channel}
 *
 * Fix: pools must be created first (to get a UUID), then memories
 * written with poolId: <uuid>. Using namespace: string alone does
 * not add memories to pool membership records.
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

/**
 * Get or create a pool by name. Returns the pool UUID.
 */
export async function getOrCreatePool(
  poolName: string,
  opts: PoolWriterOptions,
): Promise<{ ok: boolean; poolId?: string; error?: string }> {
  try {
    // Try to find existing pool by name
    const listRes = await fetch(
      `${opts.engramUrl}/v1/pools?userId=${encodeURIComponent(opts.userId)}`,
      {
        headers: {
          "X-AM-API-Key": opts.engramApiKey,
          "X-AM-User-ID": opts.userId,
        },
      },
    );

    if (listRes.ok) {
      const pools = await listRes.json() as Array<{ id: string; name: string }>;
      const existing = pools.find((p) => p.name === poolName);
      if (existing) {
        return { ok: true, poolId: existing.id };
      }
    }

    // Create new pool
    const createRes = await fetch(`${opts.engramUrl}/v1/pools`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-AM-API-Key": opts.engramApiKey,
        "X-AM-User-ID": opts.userId,
      },
      body: JSON.stringify({
        name: poolName,
        userId: opts.userId,
        visibility: "PRIVATE",
        description: `Channel intelligence pool: ${poolName}`,
        createdBy: "engram-ci",
      }),
    });

    if (!createRes.ok) {
      const body = await createRes.text();
      return { ok: false, error: `Failed to create pool: HTTP ${createRes.status}: ${body.slice(0, 200)}` };
    }

    const pool = await createRes.json() as { id: string };
    return { ok: true, poolId: pool.id };
  } catch (err: any) {
    return { ok: false, error: err.message };
  }
}

export async function writeMemory(
  memory: EnrichedMemory,
  poolId: string,
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
        poolId,                    // <-- pool UUID, not name string
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
  poolName: string,
  opts: PoolWriterOptions,
  concurrency: number = 3,
): Promise<{ written: number; errors: string[] }> {
  // Get or create pool first — get UUID
  const poolResult = await getOrCreatePool(poolName, opts);
  if (!poolResult.ok || !poolResult.poolId) {
    return { written: 0, errors: [`Pool creation failed: ${poolResult.error}`] };
  }

  const poolId = poolResult.poolId;
  console.log(`   Pool UUID: ${poolId}`);

  let written = 0;
  const errors: string[] = [];

  // Simple concurrency limiter
  const queue = [...memories];
  const workers = Array.from({ length: concurrency }, async () => {
    while (queue.length > 0) {
      const mem = queue.shift();
      if (!mem) break;
      const result = await writeMemory(mem, poolId, opts);
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
