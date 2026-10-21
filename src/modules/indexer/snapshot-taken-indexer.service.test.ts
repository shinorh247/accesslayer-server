jest.mock('../../utils/prisma.utils', () => ({
   prisma: {
      keySnapshot: {
         findUnique: jest.fn(),
         create: jest.fn(),
         update: jest.fn(),
      },
   },
}));

jest.mock('../../utils/redis.utils', () => ({
   cacheInvalidate: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../../utils/indexer-dlq.utils', () => ({
   moveToDLQ: jest.fn().mockResolvedValue({}),
}));

jest.mock('../../utils/logger.utils', () => ({
   logger: {
      warn: jest.fn(),
      info: jest.fn(),
      debug: jest.fn(),
      error: jest.fn(),
      isLevelEnabled: jest.fn().mockReturnValue(false),
   },
}));

import {
   isSnapshotTakenEvent,
   parseSnapshotTakenEvent,
   processSnapshotTakenEvent,
   processSnapshotTakenEvents,
   SNAPSHOT_INDEXER_DLQ_JOB_TYPE,
   toUtcDay,
} from './snapshot-taken-indexer.service';
import { prisma } from '../../utils/prisma.utils';
import { cacheInvalidate } from '../../utils/redis.utils';
import { moveToDLQ } from '../../utils/indexer-dlq.utils';
import { IndexerChainEvent } from '../../utils/indexer-event-processor.utils';

const mockFindUnique = (prisma as any).keySnapshot.findUnique as jest.Mock;
const mockCreate = (prisma as any).keySnapshot.create as jest.Mock;
const mockUpdate = (prisma as any).keySnapshot.update as jest.Mock;
const mockInvalidate = cacheInvalidate as jest.Mock;
const mockDlq = moveToDLQ as jest.Mock;

function snapshotEvent(
   overrides: Record<string, unknown> = {}
): IndexerChainEvent {
   return {
      txHash: 'tx-1',
      eventIndex: 0,
      eventType: 'SNAPSHOT_TAKEN',
      ledger: 1000,
      creatorId: 'key-1',
      snapshotDate: '2026-10-20T14:32:00.000Z',
      price: '12500000',
      volume: '50000000',
      holderCount: 42,
      marketCap: '900000000',
      ...overrides,
   };
}

describe('parseSnapshotTakenEvent', () => {
   it('parses a well-formed event with all four metrics', () => {
      expect(parseSnapshotTakenEvent(snapshotEvent())).toEqual({
         ok: true,
         value: {
            creatorId: 'key-1',
            snapshotDate: new Date('2026-10-20T00:00:00.000Z'),
            snapshotAt: new Date('2026-10-20T14:32:00.000Z'),
            price: 12500000n,
            volume: 50000000n,
            holderCount: 42,
            marketCap: 900000000n,
            ledger: 1000,
            txHash: 'tx-1',
            eventIndex: 0,
         },
      });
   });

   it('accepts snake_case and keyId aliases from the worker', () => {
      const result = parseSnapshotTakenEvent(
         snapshotEvent({
            creatorId: undefined,
            keyId: 'key-2',
            snapshotDate: undefined,
            date: '2026-10-19',
            holderCount: undefined,
            holder_count: 7,
            marketCap: undefined,
            market_cap: '100',
         })
      );

      expect(result.ok).toBe(true);
      if (!result.ok) return;
      expect(result.value.creatorId).toBe('key-2');
      expect(result.value.holderCount).toBe(7);
      expect(result.value.marketCap).toBe(100n);
      expect(result.value.snapshotDate.toISOString()).toBe(
         '2026-10-19T00:00:00.000Z'
      );
   });

   it('reads the date as ISO, epoch seconds or epoch milliseconds', () => {
      const iso = '2026-10-20T00:00:00.000Z';
      const ms = Date.parse(iso);
      for (const value of [iso, ms, ms / 1000, String(ms / 1000)]) {
         const result = parseSnapshotTakenEvent(
            snapshotEvent({ snapshotDate: value })
         );
         expect(result.ok && result.value.snapshotDate.toISOString()).toBe(iso);
      }
   });

   it('normalises any timestamp within a day to UTC midnight', () => {
      const result = parseSnapshotTakenEvent(
         snapshotEvent({ snapshotDate: '2026-10-20T23:59:59.000Z' })
      );
      expect(result.ok && result.value.snapshotDate.toISOString()).toBe(
         '2026-10-20T00:00:00.000Z'
      );
      expect(toUtcDay(new Date('2026-10-20T23:59:59.000Z')).toISOString()).toBe(
         '2026-10-20T00:00:00.000Z'
      );
   });

   it('keeps stroop precision beyond Number.MAX_SAFE_INTEGER', () => {
      const big = '9007199254740993';
      const result = parseSnapshotTakenEvent(
         snapshotEvent({
            price: big,
            volume: BigInt(big),
            marketCap: big,
         })
      );
      expect(result.ok).toBe(true);
      if (!result.ok) return;
      expect(result.value.price).toBe(BigInt(big));
      expect(result.value.volume).toBe(BigInt(big));
      expect(result.value.marketCap).toBe(BigInt(big));
   });

   it.each([
      ['missing creatorId', { creatorId: undefined }],
      ['empty creatorId', { creatorId: '  ' }],
      ['missing date', { snapshotDate: undefined }],
      ['bad date', { snapshotDate: 'not-a-date' }],
      ['missing price', { price: undefined }],
      ['negative price', { price: '-1' }],
      ['fractional price', { price: 1.5 }],
      ['missing volume', { volume: undefined }],
      ['negative volume', { volume: '-5' }],
      ['missing holderCount', { holderCount: undefined }],
      ['negative holderCount', { holderCount: -1 }],
      ['fractional holderCount', { holderCount: 1.5 }],
      ['missing marketCap', { marketCap: undefined }],
      ['negative marketCap', { marketCap: '-1' }],
      ['missing ledger', { ledger: undefined }],
   ])('rejects %s', (_label, overrides) => {
      expect(parseSnapshotTakenEvent(snapshotEvent(overrides)).ok).toBe(false);
   });

   it('accepts both SnapshotTaken spellings and rejects other event types', () => {
      expect(
         isSnapshotTakenEvent(snapshotEvent({ eventType: 'SNAPSHOT_TAKEN' }))
      ).toBe(true);
      expect(
         isSnapshotTakenEvent(snapshotEvent({ eventType: 'SnapshotTaken' }))
      ).toBe(true);
      expect(
         isSnapshotTakenEvent(snapshotEvent({ eventType: 'KEY_BOUGHT' }))
      ).toBe(false);
   });
});

describe('processSnapshotTakenEvent', () => {
   beforeEach(() => {
      jest.clearAllMocks();
      mockFindUnique.mockResolvedValue(null);
      mockCreate.mockResolvedValue({});
      mockUpdate.mockResolvedValue({});
   });

   it('creates a row containing all four metrics', async () => {
      await expect(processSnapshotTakenEvent(snapshotEvent())).resolves.toBe(
         true
      );

      expect(mockCreate).toHaveBeenCalledTimes(1);
      expect(mockCreate).toHaveBeenCalledWith({
         data: {
            creatorId: 'key-1',
            snapshotDate: new Date('2026-10-20T00:00:00.000Z'),
            price: 12500000n,
            volume: 50000000n,
            holderCount: 42,
            marketCap: 900000000n,
            ledger: 1000,
            snapshotAt: new Date('2026-10-20T14:32:00.000Z'),
            txHash: 'tx-1',
            eventIndex: 0,
         },
      });
   });

   it('replaces the day when a newer snapshot arrives', async () => {
      mockFindUnique.mockResolvedValue({ ledger: 1000, eventIndex: 0 });

      await expect(
         processSnapshotTakenEvent(
            snapshotEvent({
               ledger: 1200,
               txHash: 'tx-2',
               price: '20000000',
               volume: '60000000',
               holderCount: 50,
               marketCap: '1000000000',
            })
         )
      ).resolves.toBe(true);

      expect(mockCreate).not.toHaveBeenCalled();
      expect(mockUpdate).toHaveBeenCalledWith({
         where: {
            creatorId_snapshotDate: {
               creatorId: 'key-1',
               snapshotDate: new Date('2026-10-20T00:00:00.000Z'),
            },
         },
         data: expect.objectContaining({
            price: 20000000n,
            volume: 60000000n,
            holderCount: 50,
            marketCap: 1000000000n,
            ledger: 1200,
         }),
      });
   });

   it('does not roll a day back when an older event is replayed', async () => {
      mockFindUnique.mockResolvedValue({ ledger: 1200, eventIndex: 0 });

      await expect(
         processSnapshotTakenEvent(snapshotEvent({ ledger: 900 }))
      ).resolves.toBe(false);

      expect(mockCreate).not.toHaveBeenCalled();
      expect(mockUpdate).not.toHaveBeenCalled();
   });

   it('treats the exact same event replayed as a no-op but still invalidates', async () => {
      mockFindUnique.mockResolvedValue({ ledger: 1000, eventIndex: 0 });

      await expect(processSnapshotTakenEvent(snapshotEvent())).resolves.toBe(
         false
      );
      expect(mockUpdate).not.toHaveBeenCalled();
      // Covers a crash between the original write and its invalidation.
      expect(mockInvalidate).toHaveBeenCalledTimes(1);
   });

   it('breaks same-ledger ties with eventIndex', async () => {
      mockFindUnique.mockResolvedValue({ ledger: 1000, eventIndex: 1 });

      await expect(
         processSnapshotTakenEvent(snapshotEvent({ eventIndex: 1 }))
      ).resolves.toBe(false);
      await expect(
         processSnapshotTakenEvent(snapshotEvent({ eventIndex: 2 }))
      ).resolves.toBe(true);
   });

   it('invalidates all four cached ranges for the affected key only', async () => {
      await processSnapshotTakenEvent(snapshotEvent());

      expect(mockInvalidate).toHaveBeenCalledTimes(1);
      expect(mockInvalidate).toHaveBeenCalledWith(
         'key:snapshots:key-1:7d',
         'key:snapshots:key-1:30d',
         'key:snapshots:key-1:90d',
         'key:snapshots:key-1:all'
      );
   });

   it('invalidates after the row is written, not before', async () => {
      const order: string[] = [];
      mockCreate.mockImplementation(async () => {
         order.push('insert');
      });
      mockInvalidate.mockImplementation(async () => {
         order.push('invalidate');
      });

      await processSnapshotTakenEvent(snapshotEvent());
      expect(order).toEqual(['insert', 'invalidate']);
   });

   it('treats a concurrent P2002 insert as a duplicate', async () => {
      mockCreate.mockRejectedValue(
         Object.assign(new Error('dup'), { code: 'P2002' })
      );

      await expect(processSnapshotTakenEvent(snapshotEvent())).resolves.toBe(
         false
      );
      expect(mockDlq).not.toHaveBeenCalled();
      expect(mockInvalidate).toHaveBeenCalledTimes(1);
   });

   it('rethrows database errors so the cursor cannot advance', async () => {
      mockFindUnique.mockRejectedValue(new Error('connection lost'));

      await expect(processSnapshotTakenEvent(snapshotEvent())).rejects.toThrow(
         'connection lost'
      );
      expect(mockInvalidate).not.toHaveBeenCalled();
   });

   it('sends malformed events to the DLQ instead of dropping them', async () => {
      await expect(
         processSnapshotTakenEvent(snapshotEvent({ holderCount: -3 }))
      ).resolves.toBe(false);

      expect(mockFindUnique).not.toHaveBeenCalled();
      expect(mockCreate).not.toHaveBeenCalled();
      expect(mockInvalidate).not.toHaveBeenCalled();
      expect(mockDlq).toHaveBeenCalledTimes(1);
      const arg = mockDlq.mock.calls[0][0];
      expect(arg.jobType).toBe(SNAPSHOT_INDEXER_DLQ_JOB_TYPE);
      expect(arg.failureReason).toMatch(/holderCount/);
   });

   it('serialises bigint payloads in the DLQ record', async () => {
      await processSnapshotTakenEvent(
         snapshotEvent({ holderCount: -1, price: 5n, volume: 6n })
      );
      const arg = mockDlq.mock.calls[0][0];
      expect(arg.payload.price).toBe('5');
      expect(arg.payload.volume).toBe('6');
   });

   it('fails the batch if the DLQ write itself fails', async () => {
      mockDlq.mockRejectedValueOnce(new Error('dlq down'));
      await expect(
         processSnapshotTakenEvent(snapshotEvent({ price: 'nope' }))
      ).rejects.toThrow('dlq down');
   });
});

describe('processSnapshotTakenEvents (batch)', () => {
   beforeEach(() => {
      jest.clearAllMocks();
      mockFindUnique.mockResolvedValue(null);
      mockCreate.mockResolvedValue({});
   });

   it('stores every distinct event and collapses duplicates', async () => {
      await processSnapshotTakenEvents([
         snapshotEvent({ txHash: 'a', snapshotDate: '2026-10-18' }),
         snapshotEvent({ txHash: 'b', snapshotDate: '2026-10-19' }),
         snapshotEvent({ txHash: 'a', snapshotDate: '2026-10-18' }),
      ]);

      expect(mockCreate).toHaveBeenCalledTimes(2);
   });

   it('accepts both spellings and ignores unrelated event types', async () => {
      await processSnapshotTakenEvents([
         snapshotEvent({ txHash: 'a', eventType: 'SNAPSHOT_TAKEN' }),
         snapshotEvent({ txHash: 'b', eventType: 'SnapshotTaken' }),
         snapshotEvent({ txHash: 'c', eventType: 'KEY_BOUGHT' }),
      ]);

      expect(mockCreate).toHaveBeenCalledTimes(2);
   });
});
