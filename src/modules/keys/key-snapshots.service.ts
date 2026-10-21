// src/modules/keys/key-snapshots.service.ts
//
// Read side of GET /keys/:key_id/snapshots (#1112). Serves the daily metric
// points indexed from SnapshotTaken events, oldest first, for the historical
// performance charts on the frontend.
//
// The response is cached per (canonical key id, range) for five minutes. The
// indexer invalidates all of a key's ranges on every new SnapshotTaken event,
// so a chart refreshes as soon as fresh on-chain data has been indexed.

import { prisma } from '../../utils/prisma.utils';
import { cacheGetJson, cacheSetJson } from '../../utils/redis.utils';
import {
   SNAPSHOT_CACHE_TTL_SECONDS,
   SNAPSHOT_RANGE_DAYS,
   keySnapshotsCacheKey,
   type SnapshotRange,
} from '../../constants/redis.constants';

export interface KeySnapshotPoint {
   /** UTC calendar day, `YYYY-MM-DD`. */
   date: string;
   /** Price in stroops, as a string to avoid JS precision loss. */
   price: string;
   /** Daily trade volume in stroops, as a string. */
   volume: string;
   holder_count: number;
   /** Market capitalisation in stroops, as a string. */
   market_cap: string;
}

export class KeySnapshotKeyNotFoundError extends Error {
   constructor(keyId: string) {
      super(`Key not found: ${keyId}`);
      this.name = 'KeySnapshotKeyNotFoundError';
   }
}

const DAY_MS = 24 * 60 * 60 * 1000;

/** UTC midnight of the day `now` falls on. */
function utcMidnight(now: Date): Date {
   return new Date(
      Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())
   );
}

/**
 * First UTC day included by a range. Bounded ranges include today plus the
 * preceding days (7d covers today and the six days before it). `all` has no
 * lower bound and returns every indexed snapshot.
 */
export function snapshotRangeStart(
   range: SnapshotRange,
   now: Date = new Date()
): Date | null {
   if (range === 'all') return null;
   return new Date(
      utcMidnight(now).getTime() - (SNAPSHOT_RANGE_DAYS[range] - 1) * DAY_MS
   );
}

function toPoint(row: {
   snapshotDate: Date;
   price: bigint;
   volume: bigint;
   holderCount: number;
   marketCap: bigint;
}): KeySnapshotPoint {
   return {
      date: row.snapshotDate.toISOString().slice(0, 10),
      price: row.price.toString(),
      volume: row.volume.toString(),
      holder_count: row.holderCount,
      market_cap: row.marketCap.toString(),
   };
}

/**
 * Daily snapshots for a key within the requested range, ordered oldest first
 * so the chart can plot the series directly. Returns `[]` for a key that has
 * no snapshots yet (cached as an empty array).
 *
 * `keyIdOrHandle` may be either the canonical CreatorProfile id or its
 * handle; the cache is always keyed by the canonical id so indexer
 * invalidation matches regardless of how the client addressed the key.
 */
export async function getKeySnapshots(
   keyIdOrHandle: string,
   range: SnapshotRange = '30d',
   now: Date = new Date()
): Promise<KeySnapshotPoint[]> {
   const creator = await prisma.creatorProfile.findFirst({
      where: { OR: [{ id: keyIdOrHandle }, { handle: keyIdOrHandle }] },
      select: { id: true },
   });
   if (!creator) {
      throw new KeySnapshotKeyNotFoundError(keyIdOrHandle);
   }

   const cacheKey = keySnapshotsCacheKey(creator.id, range);
   const cached = await cacheGetJson<KeySnapshotPoint[]>(cacheKey);
   if (cached !== null) {
      return cached;
   }

   const from = snapshotRangeStart(range, now);
   const rows = await prisma.keySnapshot.findMany({
      where: {
         creatorId: creator.id,
         ...(from ? { snapshotDate: { gte: from } } : {}),
      },
      orderBy: { snapshotDate: 'asc' },
      select: {
         snapshotDate: true,
         price: true,
         volume: true,
         holderCount: true,
         marketCap: true,
      },
   });

   const points = rows.map(toPoint);
   await cacheSetJson(cacheKey, points, SNAPSHOT_CACHE_TTL_SECONDS);
   return points;
}
