// Acceptance tests for GET /keys/:key_id/snapshots (#1112).

jest.mock('../../../utils/redis.utils', () => {
   const store = new Map<string, string>();
   return {
      __store: store,
      cacheGetJson: jest.fn(async (key: string) => {
         const value = store.get(key);
         return value ? JSON.parse(value) : null;
      }),
      cacheSetJson: jest.fn(
         async (key: string, value: unknown, _ttl: number) => {
            store.set(key, JSON.stringify(value));
         }
      ),
      cacheInvalidate: jest.fn(async (...keys: string[]) => {
         for (const key of keys) store.delete(key);
      }),
   };
});

interface SnapshotRow {
   id: string;
   creatorId: string;
   snapshotDate: Date;
   price: bigint;
   volume: bigint;
   holderCount: number;
   marketCap: bigint;
   ledger: number;
   snapshotAt: Date;
   txHash: string;
   eventIndex: number;
}

const db = {
   snapshots: [] as SnapshotRow[],
   findManyCalls: [] as Array<Record<string, unknown>>,
};

const KEYS: Record<string, string> = {
   'key-1': 'key-1',
   'key-2': 'key-2',
   alice: 'key-1',
};

function matches(row: SnapshotRow, where: any): boolean {
   if (where.creatorId && row.creatorId !== where.creatorId) return false;
   if (where.snapshotDate?.gte) {
      return row.snapshotDate.getTime() >= where.snapshotDate.gte.getTime();
   }
   return true;
}

jest.mock('../../../utils/prisma.utils', () => ({
   prisma: {
      creatorProfile: {
         findFirst: jest.fn(async ({ where }: any) => {
            const wanted = where.OR.flatMap((clause: any) =>
               Object.values(clause)
            );
            const hit = wanted.find((id: string) => KEYS[id]);
            return hit ? { id: KEYS[hit] } : null;
         }),
      },
      keySnapshot: {
         findMany: jest.fn(async ({ where, orderBy }: any) => {
            db.findManyCalls.push(where);
            const rows = db.snapshots.filter(row => matches(row, where));
            return rows.sort((a, b) => {
               const diff = a.snapshotDate.getTime() - b.snapshotDate.getTime();
               return orderBy?.snapshotDate === 'desc' ? -diff : diff;
            });
         }),
         findUnique: jest.fn(async ({ where }: any) => {
            const key = where.creatorId_snapshotDate;
            return (
               db.snapshots.find(
                  row =>
                     row.creatorId === key.creatorId &&
                     row.snapshotDate.getTime() === key.snapshotDate.getTime()
               ) ?? null
            );
         }),
         create: jest.fn(async ({ data }: any) => {
            const row: SnapshotRow = {
               id: `snap-${db.snapshots.length + 1}`,
               ...data,
            };
            db.snapshots.push(row);
            return row;
         }),
         update: jest.fn(async ({ where, data }: any) => {
            const key = where.creatorId_snapshotDate;
            const row = db.snapshots.find(
               r =>
                  r.creatorId === key.creatorId &&
                  r.snapshotDate.getTime() === key.snapshotDate.getTime()
            )!;
            Object.assign(row, data);
            return row;
         }),
      },
   },
}));

jest.mock('../../../utils/logger.utils', () => ({
   logger: {
      info: jest.fn(),
      warn: jest.fn(),
      debug: jest.fn(),
      error: jest.fn(),
      isLevelEnabled: jest.fn().mockReturnValue(false),
   },
}));
jest.mock('../../../utils/indexer-dlq.utils', () => ({
   moveToDLQ: jest.fn().mockResolvedValue({}),
}));

import request from 'supertest';
import express from 'express';
import keysRouter from '../keys.routes';
import { processSnapshotTakenEvent } from '../../indexer/snapshot-taken-indexer.service';
import { SNAPSHOT_CACHE_TTL_SECONDS } from '../../../constants/redis.constants';
import * as redisUtils from '../../../utils/redis.utils';

const cacheSetJson = redisUtils.cacheSetJson as jest.Mock;
const mockCache = (redisUtils as unknown as { __store: Map<string, string> })
   .__store;

const app = express();
app.use(express.json());
app.use('/api/v1/keys', keysRouter);

const DAY_MS = 24 * 60 * 60 * 1000;
const serverToday = () => {
   const now = new Date();
   return new Date(
      Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())
   );
};
const daysAgo = (days: number) =>
   new Date(serverToday().getTime() - days * DAY_MS);
const isoDay = (date: Date) => date.toISOString().slice(0, 10);

function seed(creatorId: string, daysBefore: number, price: number) {
   const snapshotDate = daysAgo(daysBefore);
   db.snapshots.push({
      id: `seed-${creatorId}-${daysBefore}`,
      creatorId,
      snapshotDate,
      price: BigInt(price),
      volume: BigInt(price * 4),
      holderCount: 10 + daysBefore,
      marketCap: BigInt(price * 1000),
      ledger: 1000 + daysBefore,
      snapshotAt: new Date(snapshotDate.getTime() + 12 * 60 * 60 * 1000),
      txHash: `tx-${creatorId}-${daysBefore}`,
      eventIndex: 0,
   });
}

const get = (key: string, query = '') =>
   request(app).get(`/api/v1/keys/${key}/snapshots${query}`);

function callsFor(creatorId: string) {
   return db.findManyCalls.filter(where => where.creatorId === creatorId)
      .length;
}

describe('GET /api/v1/keys/:keyId/snapshots (#1112)', () => {
   beforeEach(() => {
      db.snapshots = [];
      db.findManyCalls = [];
      mockCache.clear();
      cacheSetJson.mockClear();

      // Today, plus boundary days for every supported range.
      for (const days of [0, 6, 7, 29, 30, 89, 90, 200]) {
         seed('key-1', days, 1_000_000 + days);
      }
   });

   it('returns every supported range with the correct number of daily points', async () => {
      const expected: Record<string, number> = {
         '7d': 2,
         '30d': 4,
         '90d': 6,
         all: 8,
      };

      for (const [range, count] of Object.entries(expected)) {
         const res = await get('key-1', `?range=${range}`);
         expect(res.status).toBe(200);
         expect(res.body.success).toBe(true);
         expect(res.body.data).toHaveLength(count);
      }
   });

   it('filters backwards from the current UTC date, inclusive of today', async () => {
      const res = await get('key-1', '?range=7d');
      expect(res.body.data.map((p: any) => p.date)).toEqual([
         isoDay(daysAgo(6)),
         isoDay(daysAgo(0)),
      ]);
   });

   it('orders snapshots oldest first', async () => {
      const res = await get('key-1', '?range=all');
      const dates = res.body.data.map((p: any) => p.date);
      expect(dates).toEqual([...dates].sort());
   });

   it('returns all five documented fields with stroop precision as strings', async () => {
      const res = await get('key-1', '?range=7d');
      expect(res.body.data[0]).toEqual({
         date: isoDay(daysAgo(6)),
         price: (1_000_000 + 6).toString(),
         volume: ((1_000_000 + 6) * 4).toString(),
         holder_count: 16,
         market_cap: ((1_000_000 + 6) * 1000).toString(),
      });
   });

   it('defaults to the 30d range', async () => {
      const res = await get('key-1');
      expect(res.body.data).toHaveLength(4);
   });

   it('returns an empty array for a key with no snapshots yet', async () => {
      const res = await get('key-2', '?range=30d');
      expect(res.status).toBe(200);
      expect(res.body.success).toBe(true);
      expect(res.body.data).toEqual([]);
   });

   it('returns 404 for an unknown key', async () => {
      const res = await get('nope');
      expect(res.status).toBe(404);
      expect(res.body.success).toBe(false);
   });

   it.each(['1y', 'week', ''])(
      'returns 400 for an invalid range (%s)',
      async range => {
         const res = await get('key-1', `?range=${range}`);
         expect(res.status).toBe(400);
         expect(res.body.success).toBe(false);
         expect(db.findManyCalls).toHaveLength(0);
      }
   );

   it('resolves a handle to the canonical key id', async () => {
      const res = await get('alice', '?range=all');
      expect(res.status).toBe(200);
      expect(db.findManyCalls[0].creatorId).toBe('key-1');
   });

   it('caches each range for five minutes', async () => {
      await get('key-1', '?range=30d');
      await get('key-1', '?range=30d');
      await get('key-1', '?range=30d');

      expect(db.findManyCalls).toHaveLength(1);
      expect(cacheSetJson).toHaveBeenCalledWith(
         'key:snapshots:key-1:30d',
         expect.any(Array),
         SNAPSHOT_CACHE_TTL_SECONDS
      );
      expect(SNAPSHOT_CACHE_TTL_SECONDS).toBe(300);
   });

   it('caches ranges independently and under the canonical id for handles', async () => {
      await get('alice', '?range=7d');
      await get('key-1', '?range=7d');
      expect(db.findManyCalls).toHaveLength(1);

      await get('key-1', '?range=90d');
      expect(db.findManyCalls).toHaveLength(2);
   });

   it('serves cached requests well under 200ms', async () => {
      await get('key-1', '?range=all'); // warm

      const samples: number[] = [];
      for (let i = 0; i < 20; i++) {
         const start = process.hrtime.bigint();
         const res = await get('key-1', '?range=all');
         samples.push(Number(process.hrtime.bigint() - start) / 1e6);
         expect(res.status).toBe(200);
      }
      samples.sort((a, b) => a - b);
      const p95 = samples[Math.floor(samples.length * 0.95) - 1];
      expect(p95).toBeLessThan(200);
      expect(db.findManyCalls).toHaveLength(1);
   });

   describe('cache invalidation on a new SnapshotTaken event', () => {
      const event = (overrides: Record<string, unknown> = {}) => ({
         txHash: 'tx-new',
         eventIndex: 0,
         eventType: 'SNAPSHOT_TAKEN',
         ledger: 5000,
         creatorId: 'key-1',
         snapshotDate: new Date().toISOString(),
         price: '99000000',
         volume: '123000000',
         holderCount: 77,
         marketCap: '7800000000',
         ...overrides,
      });

      it('invalidates every cached range for the key', async () => {
         for (const range of ['7d', '30d', '90d', 'all']) {
            await get('key-1', `?range=${range}`);
         }
         expect(db.findManyCalls).toHaveLength(4);

         await processSnapshotTakenEvent(event());

         for (const range of ['7d', '30d', '90d', 'all']) {
            await get('key-1', `?range=${range}`);
         }
         expect(db.findManyCalls).toHaveLength(8);
      });

      it('serves the new metrics on the very next request', async () => {
         const before = await get('key-1', '?range=7d');
         const todayBefore = before.body.data[before.body.data.length - 1];
         expect(todayBefore.price).toBe((1_000_000 + 0).toString());

         await processSnapshotTakenEvent(event());

         const after = await get('key-1', '?range=7d');
         expect(after.body.data[after.body.data.length - 1]).toEqual({
            date: isoDay(daysAgo(0)),
            price: '99000000',
            volume: '123000000',
            holder_count: 77,
            market_cap: '7800000000',
         });
      });

      it('does not invalidate other keys', async () => {
         seed('key-2', 0, 5_000_000);
         await get('key-1', '?range=30d');
         await get('key-2', '?range=30d');
         expect(callsFor('key-1')).toBe(1);
         expect(callsFor('key-2')).toBe(1);

         await processSnapshotTakenEvent(event({ creatorId: 'key-1' }));

         await get('key-2', '?range=30d');
         expect(callsFor('key-2')).toBe(1);
         await get('key-1', '?range=30d');
         expect(callsFor('key-1')).toBe(2);
      });

      it('adds a new day to the series without dropping earlier days', async () => {
         const before = await get('key-1', '?range=all');
         const countBefore = before.body.data.length;

         await processSnapshotTakenEvent(
            event({ snapshotDate: isoDay(daysAgo(1)), txHash: 'tx-yesterday' })
         );

         const after = await get('key-1', '?range=all');
         expect(after.body.data).toHaveLength(countBefore + 1);
         expect(
            after.body.data.some((p: any) => p.date === isoDay(daysAgo(1)))
         ).toBe(true);
      });
   });
});
