/**
 * backfill-range-validation.test.ts
 *
 * Issue #924 — Add a Test for Empty Backfill Ranges
 *
 * Ensures that runBackfill rejects empty or inverted ledger ranges
 * BEFORE contacting RPC or the database, with a clear error message
 * identifying which argument is invalid.
 *
 * Acceptance criteria:
 *  • Invalid range input is rejected before contacting RPC or the DB.
 *  • Valid boundary ranges retain current inclusive semantics.
 *  • The failure message indicates which argument is invalid.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

// ── Suppress dotenv ───────────────────────────────────────────────────────────
vi.mock('dotenv', () => ({ default: { config: vi.fn() } }));

// ── Mock metrics ──────────────────────────────────────────────────────────────
vi.mock('../metrics.js', () => ({
  rpcRetryExhaustedCounter:    { inc: vi.fn() },
  backfillJobsTotal:           { inc: vi.fn() },
  backfillDurationSeconds:     { startTimer: vi.fn(() => vi.fn()) },
  backfillBatchLedgers:        { observe: vi.fn() },
  backfillBatchInserted:       { observe: vi.fn() },
  backfillLockContentions:     { inc: vi.fn() },
  gapsCreatedTotal:            { inc: vi.fn() },
  openGapsGauge:               { set: vi.fn() },
  openGapLedgersTotalGauge:    { set: vi.fn() },
  latestLedgerProcessedGauge:  { set: vi.fn() },
  networkLatestLedgerGauge:    { set: vi.fn() },
  syncLatencyGauge:            { set: vi.fn() },
  decodeErrorsCounter:         { inc: vi.fn() },
  duplicateEventsCounter:      { inc: vi.fn() },
  keeperActionsTotal:          { inc: vi.fn() },
  keeperFeesSpentStroops:      { inc: vi.fn() },
  keeperBudgetExhaustedTotal:  { inc: vi.fn() },
  keeperBudgetExhaustedGauge:  { set: vi.fn() },
  keeperSimulationFailuresTotal: { inc: vi.fn() },
  keeperCycleDurationSeconds:  { startTimer: vi.fn(() => vi.fn()) },
  keeperCandidatesDiscovered:  { set: vi.fn() },
  keeperFeeBumpsTotal:         { inc: vi.fn() },
}));

// ── Mock Prisma — should NOT be called for invalid ranges ─────────────────────
const mockPrisma = vi.hoisted(() => ({
  backfillJob:  { create: vi.fn(), findUnique: vi.fn(), update: vi.fn(), findMany: vi.fn() },
  syncState:    { findUnique: vi.fn(), upsert: vi.fn() },
  ledgerGap:    { upsert: vi.fn(), findMany: vi.fn() },
  $queryRaw:    vi.fn(),
  $transaction: vi.fn(),
}));

vi.mock('../db.js',           () => ({ default: mockPrisma }));
vi.mock('../prisma-write.js', () => ({ default: mockPrisma }));

// ── Mock event-sync — should NOT be called for invalid ranges ─────────────────
vi.mock('../event-sync.js', () => ({
  collectMarketplaceEvents: vi.fn().mockResolvedValue([]),
  MAX_LEDGER_WINDOW: 17_280,
}));

// ── Mock poller ───────────────────────────────────────────────────────────────
vi.mock('../poller.js', () => ({
  applyDecodedEvents:       vi.fn().mockResolvedValue([]),
  buildSyncStateLedgerData: vi.fn((l: number, h: string | null) => ({
    lastLedger: l,
    ...(h ? { lastLedgerHash: h } : {}),
  })),
  persistLedgerGap:         vi.fn().mockResolvedValue(undefined),
  MAX_REORG_DEPTH:          100,
}));

// ── Mock reconciler ───────────────────────────────────────────────────────────
vi.mock('../reconciler.js', () => ({
  runReconciliation: vi.fn().mockResolvedValue({ discrepancies: [] }),
}));

import { runBackfill } from '../backfill.js';
import { collectMarketplaceEvents } from '../event-sync.js';

// ── Fake rpc.Server that reports a known chain tip ────────────────────────────
function makeFakeServer(chainTip: number) {
  return {
    getLatestLedger: vi.fn().mockResolvedValue({ sequence: chainTip }),
    getLedgers:      vi.fn().mockResolvedValue({ ledgers: [{ hash: 'abc123' }] }),
  } as any;
}

const ENV_VARS = {
  MARKETPLACE_CONTRACT_ID: 'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF',
  LAUNCHPAD_CONTRACT_ID:   '',
  STELLAR_RPC_URL:         'https://soroban-testnet.stellar.org',
};

// ─────────────────────────────────────────────────────────────────────────────
// 1. Argument-validation tests (no RPC / DB contact expected)
// ─────────────────────────────────────────────────────────────────────────────

describe('backfill range validation — inverted range (start > end)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    Object.assign(process.env, ENV_VARS);
  });

  it('rejects when startLedger > endLedger', async () => {
    const server = makeFakeServer(5_000);

    await expect(
      runBackfill({
        rpcServer:   server,
        startLedger: 1_000,
        endLedger:   500, // inverted
        rpcUrl:      'https://test.rpc',
        silent:      true,
      }),
    ).rejects.toThrow(/--start.*must be.*--end|invalid range/i);
  });

  it('error message identifies the conflicting arguments', async () => {
    const server = makeFakeServer(5_000);

    let caught: Error | null = null;
    try {
      await runBackfill({
        rpcServer:   server,
        startLedger: 3_000,
        endLedger:   2_000,
        rpcUrl:      'https://test.rpc',
        silent:      true,
      });
    } catch (err) {
      caught = err as Error;
    }

    expect(caught).not.toBeNull();
    // Message must reference the invalid arguments so operators know what to fix
    expect(caught!.message.toLowerCase()).toMatch(/start|end/);
  });

  it('does NOT call the database when the range is inverted', async () => {
    const server = makeFakeServer(5_000);

    await runBackfill({
      rpcServer:   server,
      startLedger: 2_000,
      endLedger:   1_000,
      rpcUrl:      'https://test.rpc',
      silent:      true,
    }).catch(() => { /* expected */ });

    // createBackfillJob (backfillJob.create) must never have been called
    expect(mockPrisma.backfillJob.create).not.toHaveBeenCalled();
  });

  it('does NOT call collectMarketplaceEvents when the range is inverted', async () => {
    const server = makeFakeServer(5_000);

    await runBackfill({
      rpcServer:   server,
      startLedger: 2_000,
      endLedger:   1_000,
      rpcUrl:      'https://test.rpc',
      silent:      true,
    }).catch(() => { /* expected */ });

    expect(collectMarketplaceEvents).not.toHaveBeenCalled();
  });
});

describe('backfill range validation — end exceeds chain tip', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    Object.assign(process.env, ENV_VARS);
  });

  it('rejects when endLedger exceeds the chain tip', async () => {
    const CHAIN_TIP = 5_000;
    const server = makeFakeServer(CHAIN_TIP);

    await expect(
      runBackfill({
        rpcServer:   server,
        startLedger: 4_000,
        endLedger:   6_000, // beyond tip
        rpcUrl:      'https://test.rpc',
        silent:      true,
      }),
    ).rejects.toThrow(/chain tip|exceeds/i);
  });

  it('error message includes both the requested end and the actual chain tip', async () => {
    const CHAIN_TIP = 5_000;
    const server = makeFakeServer(CHAIN_TIP);

    let caught: Error | null = null;
    try {
      await runBackfill({
        rpcServer:   server,
        startLedger: 1,
        endLedger:   9_999,
        rpcUrl:      'https://test.rpc',
        silent:      true,
      });
    } catch (err) {
      caught = err as Error;
    }

    expect(caught).not.toBeNull();
    expect(caught!.message).toContain(String(CHAIN_TIP));
  });
});

describe('backfill range validation — valid boundary ranges', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    Object.assign(process.env, ENV_VARS);

    // Minimal Prisma stubs so a valid range can proceed to job creation
    mockPrisma.$queryRaw.mockResolvedValue([{ acquired: true }]);
    mockPrisma.backfillJob.create.mockResolvedValue({
      id: 1, startLedger: 100, endLedger: 100,
      checkpointLedger: 0, status: 'Pending',
      rpcUrl: 'https://test.rpc', totalInserted: 0,
      gapId: null, error: null,
      createdAt: new Date(), updatedAt: new Date(),
    });
    mockPrisma.backfillJob.update.mockResolvedValue({
      id: 1, status: 'Running', checkpointLedger: 0,
    });
    mockPrisma.syncState.findUnique.mockResolvedValue({ id: 1, lastLedger: 0 });
    mockPrisma.syncState.upsert.mockResolvedValue({});
    mockPrisma.$transaction.mockImplementation((fn: (tx: any) => Promise<any>) =>
      fn({
        backfillJob:    { update: vi.fn().mockResolvedValue({}) },
        syncState:      { upsert: vi.fn().mockResolvedValue({}) },
        marketplaceEvent: { findUnique: vi.fn().mockResolvedValue(null), createMany: vi.fn().mockResolvedValue({}) },
        listing:        { upsert: vi.fn().mockResolvedValue({}) },
        collection:     { upsert: vi.fn().mockResolvedValue({}) },
      }),
    );
  });

  it('accepts startLedger === endLedger (single-ledger inclusive range)', async () => {
    const server = makeFakeServer(5_000);
    // Should not throw — a single-ledger range is valid
    await expect(
      runBackfill({
        rpcServer:   server,
        startLedger: 100,
        endLedger:   100,
        rpcUrl:      'https://test.rpc',
        silent:      true,
      }),
    ).resolves.not.toBeUndefined();
  });

  it('accepts startLedger === 0 (genesis-relative backfill)', async () => {
    mockPrisma.backfillJob.create.mockResolvedValue({
      id: 2, startLedger: 0, endLedger: 0,
      checkpointLedger: 0, status: 'Pending',
      rpcUrl: 'https://test.rpc', totalInserted: 0,
      gapId: null, error: null,
      createdAt: new Date(), updatedAt: new Date(),
    });

    const server = makeFakeServer(5_000);
    await expect(
      runBackfill({
        rpcServer:   server,
        startLedger: 0,
        endLedger:   0,
        rpcUrl:      'https://test.rpc',
        silent:      true,
      }),
    ).resolves.not.toBeUndefined();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. parseLedger helper — argument parsing unit tests
// ─────────────────────────────────────────────────────────────────────────────

describe('parseLedger — CLI argument validation', () => {
  // Access parseLedger indirectly via the error messages runBackfill throws
  // when required flags are missing.  The helper is not exported so we drive
  // it through the public surface.

  beforeEach(() => {
    vi.clearAllMocks();
    Object.assign(process.env, ENV_VARS);
  });

  it('runBackfill throws when neither startLedger nor --start flag is supplied', async () => {
    // process.argv is already clean in tests (no --start flag), so omitting
    // opts.startLedger triggers the missing-flag path inside parseLedger.
    const server = makeFakeServer(5_000);

    await expect(
      runBackfill({
        rpcServer: server,
        // no startLedger
        endLedger: 1_000,
        rpcUrl:    'https://test.rpc',
        silent:    true,
      }),
    ).rejects.toThrow(/--start|start ledger/i);
  });
});
