-- CreateTable
-- Daily historical metric snapshots indexed from SnapshotTaken events (#1112).
-- One row per (creatorId, snapshotDate); metrics are in stroops.
CREATE TABLE "key_snapshots" (
    "id" TEXT NOT NULL,
    "creatorId" TEXT NOT NULL,
    "snapshotDate" DATE NOT NULL,
    "price" BIGINT NOT NULL,
    "volume" BIGINT NOT NULL DEFAULT 0,
    "holderCount" INTEGER NOT NULL DEFAULT 0,
    "marketCap" BIGINT NOT NULL DEFAULT 0,
    "ledger" INTEGER NOT NULL,
    "snapshotAt" TIMESTAMP(3) NOT NULL,
    "txHash" TEXT NOT NULL,
    "eventIndex" INTEGER NOT NULL,
    "indexedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "key_snapshots_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "key_snapshots_price_check" CHECK ("price" >= 0),
    CONSTRAINT "key_snapshots_volume_check" CHECK ("volume" >= 0),
    CONSTRAINT "key_snapshots_holderCount_check" CHECK ("holderCount" >= 0),
    CONSTRAINT "key_snapshots_marketCap_check" CHECK ("marketCap" >= 0)
);

-- CreateIndex
CREATE UNIQUE INDEX "key_snapshots_creatorId_snapshotDate_key" ON "key_snapshots"("creatorId", "snapshotDate");
