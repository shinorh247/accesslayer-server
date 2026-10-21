// Verifies the pipeline dispatcher routes SnapshotTaken events to the
// snapshot indexer and advances the cursor only after the write succeeds.

jest.mock('../ownership/ownership.service', () => ({
   updateOwnership: jest.fn(),
   recordKeyPurchase: jest.fn(),
   recordKeySale: jest.fn(),
}));
jest.mock('./price-snapshot.service', () => ({
   upsertPriceSnapshot: jest.fn(),
}));
jest.mock('./persist-circulating-supply.service', () => ({
   persistCirculatingSupply: jest.fn().mockResolvedValue(0),
}));
jest.mock('../creators/creator-leaderboard-volume.service', () => ({
   invalidateVolumeLeaderboardCache: jest.fn(),
}));
jest.mock('../referrals/referrals.service', () => ({
   recordFirstTradeReferralReward: jest.fn(),
}));
jest.mock('../creators/creator-portfolio.service', () => ({
   invalidateCreatorPortfolioStatsCache: jest.fn(),
}));
jest.mock('../activity/activity-feed.service', () => ({
   invalidateActivityFeedCache: jest.fn(),
}));
jest.mock('./social-metadata-indexer.service', () => ({
   processSocialMetadataEvents: jest.fn(),
   SOCIAL_METADATA_UPDATED_EVENT_TYPE: 'SOCIAL_METADATA_UPDATED',
}));
jest.mock('../../utils/redis.utils', () => ({
   cacheInvalidate: jest.fn().mockResolvedValue(undefined),
}));
jest.mock('../../utils/indexer-dlq.utils', () => ({
   moveToDLQ: jest.fn().mockResolvedValue({}),
}));
jest.mock('../../utils/prisma.utils', () => ({
   prisma: {
      activity: { create: jest.fn() },
      indexedLedger: { upsert: jest.fn() },
      keySnapshot: {
         findUnique: jest.fn(),
         create: jest.fn(),
         update: jest.fn(),
      },
   },
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

import { processIndexerEvents } from './indexer-pipeline.service';
import { prisma } from '../../utils/prisma.utils';
import { cacheInvalidate } from '../../utils/redis.utils';

const mockPrisma = prisma as unknown as {
   indexedLedger: { upsert: jest.Mock };
   keySnapshot: {
      findUnique: jest.Mock;
      create: jest.Mock;
      update: jest.Mock;
   };
};

const snapshot = (txHash: string, ledger: number) => ({
   txHash,
   eventIndex: 0,
   eventType: 'SnapshotTaken',
   ledger,
   creatorId: 'key-1',
   snapshotDate: '2026-10-20T10:00:00.000Z',
   price: '100',
   volume: '500',
   holderCount: 3,
   marketCap: '900',
});

describe('processIndexerEvents with SnapshotTaken', () => {
   beforeEach(() => {
      jest.clearAllMocks();
      mockPrisma.indexedLedger.upsert.mockResolvedValue({});
      mockPrisma.keySnapshot.findUnique.mockResolvedValue(null);
      mockPrisma.keySnapshot.create.mockResolvedValue({});
   });

   it('indexes snapshots, invalidates the key cache and advances the cursor', async () => {
      await processIndexerEvents([snapshot('a', 100), snapshot('b', 105)]);

      expect(mockPrisma.keySnapshot.create).toHaveBeenCalledTimes(2);
      expect(cacheInvalidate).toHaveBeenCalledWith(
         'key:snapshots:key-1:7d',
         'key:snapshots:key-1:30d',
         'key:snapshots:key-1:90d',
         'key:snapshots:key-1:all'
      );
      expect(mockPrisma.indexedLedger.upsert).toHaveBeenCalledTimes(1);
      expect(mockPrisma.indexedLedger.upsert).toHaveBeenCalledWith(
         expect.objectContaining({
            update: expect.objectContaining({ ledger: 105 }),
         })
      );
   });

   it('does NOT advance the cursor when indexing fails', async () => {
      mockPrisma.keySnapshot.findUnique.mockRejectedValue(new Error('db down'));

      await expect(
         processIndexerEvents([snapshot('a', 100), snapshot('b', 105)])
      ).rejects.toThrow('db down');

      expect(mockPrisma.indexedLedger.upsert).not.toHaveBeenCalled();
   });

   it('advances the cursor for a batch containing only SnapshotTaken events', async () => {
      await processIndexerEvents([snapshot('a', 300)]);
      expect(mockPrisma.indexedLedger.upsert).toHaveBeenCalledTimes(1);
   });
});
