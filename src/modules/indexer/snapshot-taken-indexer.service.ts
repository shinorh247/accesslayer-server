// src/modules/indexer/snapshot-taken-indexer.service.ts
//
// Indexes SnapshotTaken contract events into the `key_snapshots` table
// (#1112), which powers GET /keys/:key_id/snapshots and the historical
// performance charts on the frontend.
//
// Guarantees:
// - One row per (creatorId, UTC day). A later snapshot for the same day
//   replaces the earlier one; replaying an older event never rolls the day
//   back to stale metrics.
// - Nothing is silently dropped: a malformed event is written to the indexer
//   DLQ so it is visible and replayable. Database failures rethrow so the
//   dispatcher does not advance the ledger cursor past the batch.
// - After every event, including stale replays, the key's cached snapshot
//   responses are invalidated. Doing it on replays too covers a crash that
//   happened between the write and the invalidation.
//
// Cursor advancement belongs to the dispatcher, not this service.

import { prisma } from '../../utils/prisma.utils';
import { logger } from '../../utils/logger.utils';
import {
   processIndexerChainEvents,
   IndexerChainEvent,
} from '../../utils/indexer-event-processor.utils';
import { moveToDLQ } from '../../utils/indexer-dlq.utils';
import { bigIntReplacer } from '../../utils/bigint-serializer.utils';
import { cacheInvalidate } from '../../utils/redis.utils';
import {
   SNAPSHOT_RANGES,
   keySnapshotsCacheKey,
} from '../../constants/redis.constants';

/** Event discriminators. Both spellings are accepted, as for TWAPUpdated. */
export const SNAPSHOT_TAKEN_EVENT_TYPES: ReadonlySet<string> = new Set([
   'SNAPSHOT_TAKEN',
   'SnapshotTaken',
]);

export const SNAPSHOT_INDEXER_DLQ_JOB_TYPE = 'SNAPSHOT_TAKEN';

/**
 * Decoded SnapshotTaken event as delivered by the external indexing worker.
 *
 * The worker has historically used both camelCase and snake_case payloads,
 * and identifies the key as either `creatorId` or `keyId`, so all spellings
 * are accepted. Metrics are in stroops and may arrive as strings, numbers or
 * bigints.
 */
export interface SnapshotTakenEvent extends IndexerChainEvent {
   creatorId?: string;
   keyId?: string;
   key_id?: string;
   /** UTC day or full event timestamp: ISO string, epoch seconds or ms. */
   snapshotDate?: string | number | Date;
   date?: string | number | Date;
   timestamp?: string | number | Date;
   price?: string | number | bigint;
   volume?: string | number | bigint;
   holderCount?: number;
   holder_count?: number;
   marketCap?: string | number | bigint;
   market_cap?: string | number | bigint;
}

export function isSnapshotTakenEvent(
   event: IndexerChainEvent
): event is SnapshotTakenEvent {
   return SNAPSHOT_TAKEN_EVENT_TYPES.has(event.eventType);
}

export interface ParsedSnapshotTaken {
   creatorId: string;
   /** UTC midnight of the calendar day the snapshot describes. */
   snapshotDate: Date;
   /** Timestamp carried by the event, before day normalisation. */
   snapshotAt: Date;
   price: bigint;
   volume: bigint;
   holderCount: number;
   marketCap: bigint;
   ledger: number;
   txHash: string;
   eventIndex: number;
}

export type SnapshotParseResult =
   | { ok: true; value: ParsedSnapshotTaken }
   | { ok: false; reason: string };

/**
 * Parses an event timestamp into a Date. Accepts ISO strings, epoch seconds
 * (values below 1e12, as used by Soroban) and epoch milliseconds.
 */
function parseEventDate(value: unknown): Date | null {
   if (value === undefined || value === null || value === '') return null;
   if (value instanceof Date) {
      return Number.isNaN(value.getTime()) ? null : value;
   }
   if (typeof value === 'number') {
      if (!Number.isFinite(value)) return null;
      const date = new Date(value < 1e12 ? value * 1000 : value);
      return Number.isNaN(date.getTime()) ? null : date;
   }
   if (typeof value === 'string') {
      const trimmed = value.trim();
      if (trimmed === '') return null;
      if (/^\d+$/.test(trimmed)) {
         const n = Number(trimmed);
         const date = new Date(n < 1e12 ? n * 1000 : n);
         return Number.isNaN(date.getTime()) ? null : date;
      }
      const date = new Date(trimmed);
      return Number.isNaN(date.getTime()) ? null : date;
   }
   return null;
}

/** Truncates a timestamp to UTC midnight (the stored daily bucket). */
export function toUtcDay(date: Date): Date {
   return new Date(
      Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate())
   );
}

function parseNonNegativeBigInt(value: unknown): bigint | null {
   try {
      if (typeof value === 'number' && !Number.isInteger(value)) return null;
      if (typeof value === 'string' && value.trim() === '') return null;
      if (
         typeof value !== 'string' &&
         typeof value !== 'number' &&
         typeof value !== 'bigint'
      ) {
         return null;
      }
      const parsed = BigInt(
         typeof value === 'string' ? value.trim() : (value as string | number)
      );
      return parsed >= 0n ? parsed : null;
   } catch {
      return null;
   }
}

function firstDefined(raw: Record<string, unknown>, keys: string[]): unknown {
   for (const key of keys) {
      const value = raw[key];
      if (value !== undefined && value !== null && value !== '') {
         return value;
      }
   }
   return undefined;
}

/** Pure validation of a raw event into a typed, persistable snapshot. */
export function parseSnapshotTakenEvent(
   event: IndexerChainEvent
): SnapshotParseResult {
   const raw = event as Record<string, unknown>;

   const creatorIdRaw = firstDefined(raw, ['creatorId', 'keyId', 'key_id']);
   if (typeof creatorIdRaw !== 'string' || creatorIdRaw.trim() === '') {
      return { ok: false, reason: 'missing or invalid creatorId/keyId' };
   }

   const dateRaw = firstDefined(raw, [
      'snapshotDate',
      'date',
      'timestamp',
      'snapshotAt',
   ]);
   const parsedDate = parseEventDate(dateRaw);
   if (parsedDate === null) {
      return { ok: false, reason: 'missing or invalid snapshot date' };
   }

   const price = parseNonNegativeBigInt(firstDefined(raw, ['price']));
   if (price === null) {
      return { ok: false, reason: 'missing or invalid price' };
   }

   const volume = parseNonNegativeBigInt(firstDefined(raw, ['volume']));
   if (volume === null) {
      return { ok: false, reason: 'missing or invalid volume' };
   }

   const holderCountRaw = firstDefined(raw, ['holderCount', 'holder_count']);
   const holderCount = Number(holderCountRaw);
   if (
      holderCountRaw === undefined ||
      !Number.isInteger(holderCount) ||
      holderCount < 0
   ) {
      return { ok: false, reason: 'missing or invalid holderCount' };
   }

   const marketCap = parseNonNegativeBigInt(
      firstDefined(raw, ['marketCap', 'market_cap'])
   );
   if (marketCap === null) {
      return { ok: false, reason: 'missing or invalid marketCap' };
   }

   const ledger = Number(raw.ledger);
   if (
      raw.ledger === undefined ||
      raw.ledger === null ||
      !Number.isInteger(ledger) ||
      ledger < 0
   ) {
      return { ok: false, reason: 'missing or invalid ledger' };
   }

   const eventIndex = Number(event.eventIndex);
   if (
      typeof event.txHash !== 'string' ||
      event.txHash === '' ||
      !Number.isInteger(eventIndex) ||
      eventIndex < 0
   ) {
      return { ok: false, reason: 'missing or invalid txHash/eventIndex' };
   }

   return {
      ok: true,
      value: {
         creatorId: creatorIdRaw.trim(),
         snapshotDate: toUtcDay(parsedDate),
         snapshotAt: parsedDate,
         price,
         volume,
         holderCount,
         marketCap,
         ledger,
         txHash: event.txHash,
         eventIndex,
      },
   };
}

/**
 * Drops every cached range for a key. Four literal DELs (no SCAN) keep the
 * invalidation cheap and independent of how many ranges exist.
 */
export async function invalidateKeySnapshotsCache(
   creatorId: string
): Promise<void> {
   await cacheInvalidate(
      ...SNAPSHOT_RANGES.map(range => keySnapshotsCacheKey(creatorId, range))
   );
}

/**
 * Processes one SnapshotTaken event.
 *
 * Returns `true` when a row was created or replaced, `false` when the event
 * was a duplicate/stale replay or was rejected as malformed (and sent to the
 * DLQ). Database errors are rethrown on purpose so the cursor cannot advance
 * past an unprocessed event.
 */
export async function processSnapshotTakenEvent(
   event: IndexerChainEvent
): Promise<boolean> {
   const eventId = `${event.txHash}:${event.eventIndex}`;
   const parsed = parseSnapshotTakenEvent(event);

   if (!parsed.ok) {
      logger.warn(
         {
            type: 'snapshot_taken_event_rejected',
            eventId,
            reason: parsed.reason,
         },
         'Malformed SnapshotTaken event moved to DLQ'
      );
      await moveToDLQ({
         jobType: SNAPSHOT_INDEXER_DLQ_JOB_TYPE,
         payload: JSON.parse(JSON.stringify(event, bigIntReplacer)),
         retryCount: 0,
         failureReason: parsed.reason,
      });
      return false;
   }

   const snapshot = parsed.value;
   const where = {
      creatorId_snapshotDate: {
         creatorId: snapshot.creatorId,
         snapshotDate: snapshot.snapshotDate,
      },
   };

   let written = false;
   try {
      const existing = await prisma.keySnapshot.findUnique({
         where,
         select: { ledger: true, eventIndex: true },
      });

      if (!existing) {
         try {
            await prisma.keySnapshot.create({
               data: {
                  creatorId: snapshot.creatorId,
                  snapshotDate: snapshot.snapshotDate,
                  price: snapshot.price,
                  volume: snapshot.volume,
                  holderCount: snapshot.holderCount,
                  marketCap: snapshot.marketCap,
                  ledger: snapshot.ledger,
                  snapshotAt: snapshot.snapshotAt,
                  txHash: snapshot.txHash,
                  eventIndex: snapshot.eventIndex,
               },
            });
            written = true;
         } catch (error) {
            // Lost a race with a concurrent event for the same day; the other
            // writer's row is authoritative and nothing more is needed.
            if ((error as { code?: string })?.code !== 'P2002') throw error;
            logger.debug(
               { eventId, creator_id: snapshot.creatorId },
               'SnapshotTaken event inserted concurrently; duplicate skipped'
            );
         }
      } else if (
         snapshot.ledger > existing.ledger ||
         (snapshot.ledger === existing.ledger &&
            snapshot.eventIndex > existing.eventIndex)
      ) {
         await prisma.keySnapshot.update({
            where,
            data: {
               price: snapshot.price,
               volume: snapshot.volume,
               holderCount: snapshot.holderCount,
               marketCap: snapshot.marketCap,
               ledger: snapshot.ledger,
               snapshotAt: snapshot.snapshotAt,
               txHash: snapshot.txHash,
               eventIndex: snapshot.eventIndex,
            },
         });
         written = true;
      } else {
         logger.debug(
            {
               eventId,
               creator_id: snapshot.creatorId,
               snapshot_date: snapshot.snapshotDate.toISOString().slice(0, 10),
               existing_ledger: existing.ledger,
               incoming_ledger: snapshot.ledger,
            },
            'SnapshotTaken event is stale for its day; skipping write'
         );
      }
   } catch (error) {
      logger.error(
         { err: error, creatorId: snapshot.creatorId, eventId },
         'snapshot-taken: failed to persist snapshot'
      );
      throw error;
   }

   // Invalidate after the write is durable so a reader that repopulates the
   // cache immediately afterwards sees the new day's metrics.
   await invalidateKeySnapshotsCache(snapshot.creatorId);

   if (written) {
      logger.info(
         {
            type: 'key_snapshot_indexed',
            eventId,
            creator_id: snapshot.creatorId,
            snapshot_date: snapshot.snapshotDate.toISOString().slice(0, 10),
            price: snapshot.price.toString(),
            volume: snapshot.volume.toString(),
            holder_count: snapshot.holderCount,
            market_cap: snapshot.marketCap.toString(),
            ledger: snapshot.ledger,
         },
         'Key snapshot indexed'
      );
   }

   return written;
}

/**
 * Processes a batch of SnapshotTaken events. Non-snapshot events are ignored.
 * Does not advance the ledger cursor; the dispatcher does that once per batch.
 */
export async function processSnapshotTakenEvents(
   events: IndexerChainEvent[]
): Promise<void> {
   await processIndexerChainEvents(events, async event => {
      if (!isSnapshotTakenEvent(event)) {
         return;
      }
      await processSnapshotTakenEvent(event);
   });
}
