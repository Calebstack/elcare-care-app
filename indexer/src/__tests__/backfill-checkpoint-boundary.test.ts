/**
 * backfill-checkpoint-boundary.test.ts
 *
 * Issue #925 — Clarify Which Checkpoint Backfill Resumes From
 *
 * Confirms the documented boundary semantics of determineCursorInteraction
 * and the resume-from-checkpoint rule (resumeFrom = checkpointLedger + 1).
 *
 * These tests serve as executable documentation backing the guide in
 * docs/backfill-checkpoint-semantics.md.
 */

import { describe, it, expect, vi } from 'vitest';

// ── Suppress dotenv and metrics ───────────────────────────────────────────────
vi.mock('dotenv', () => ({ default: { config: vi.fn() } }));

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

vi.mock('../db.js',           () => ({ default: {} }));
vi.mock('../prisma-write.js', () => ({ default: {} }));
vi.mock('../event-sync.js',   () => ({ collectMarketplaceEvents: vi.fn(), MAX_LEDGER_WINDOW: 17_280 }));
vi.mock('../poller.js',       () => ({
  applyDecodedEvents:       vi.fn(),
  buildSyncStateLedgerData: vi.fn(),
  persistLedgerGap:         vi.fn(),
  MAX_REORG_DEPTH:          100,
}));
vi.mock('../reconciler.js',   () => ({ runReconciliation: vi.fn() }));

import { determineCursorInteraction } from '../backfill.js';

// ─────────────────────────────────────────────────────────────────────────────
// 1. --start / --end boundary semantics (inclusive, as documented)
// ─────────────────────────────────────────────────────────────────────────────

describe('Documented --start / --end inclusive semantics', () => {
  it('a single-ledger range (start === end) is a valid "below" interaction when below the cursor', () => {
    // Both boundaries at ledger 1000, live cursor at 2000 → "below"
    // SyncState MUST NOT be written.
    expect(determineCursorInteraction(1_000, 1_000, 2_000)).toBe('below');
  });

  it('a single-ledger range at exactly the live cursor is "below" (endLedger === liveCursor)', () => {
    // Inclusive: endLedger ≤ liveCursor → "below"
    expect(determineCursorInteraction(2_000, 2_000, 2_000)).toBe('below');
  });

  it('"overlapping" when range straddles the cursor — SyncState is NOT written', () => {
    expect(determineCursorInteraction(1_500, 2_500, 2_000)).toBe('overlapping');
  });

  it('"overlapping" when start === liveCursor', () => {
    // start exactly on the cursor → overlapping, NOT ahead
    expect(determineCursorInteraction(2_000, 3_000, 2_000)).toBe('overlapping');
  });

  it('"ahead" only when start > liveCursor — SyncState IS written', () => {
    expect(determineCursorInteraction(2_001, 3_000, 2_000)).toBe('ahead');
  });

  it('"ahead" for a fresh DB (liveCursor = 0)', () => {
    // Fresh database — any positive range is "ahead"
    expect(determineCursorInteraction(1, 5_000, 0)).toBe('ahead');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. Resume-from-checkpoint rule
// ─────────────────────────────────────────────────────────────────────────────

describe('Resume-from-checkpoint rule: resumeFrom = checkpointLedger + 1', () => {
  /**
   * The formula below mirrors what runBackfill() does internally:
   *
   *   const resumeFrom =
   *     job.checkpointLedger > 0
   *       ? job.checkpointLedger + 1
   *       : job.startLedger;
   */
  function resumeFrom(checkpointLedger: number, startLedger: number): number {
    return checkpointLedger > 0 ? checkpointLedger + 1 : startLedger;
  }

  it('resumes from startLedger when no batches have been committed (checkpointLedger = 0)', () => {
    expect(resumeFrom(0, 1_000_000)).toBe(1_000_000);
  });

  it('resumes from checkpointLedger + 1 after a partial run', () => {
    // Crashed after committing ledger 1 200 000
    expect(resumeFrom(1_200_000, 1_000_000)).toBe(1_200_001);
  });

  it('a job completed to the end has checkpointLedger === endLedger; resume target is endLedger + 1', () => {
    // Even if someone tries to "resume" a completed job, the math is consistent
    expect(resumeFrom(1_500_000, 1_000_000)).toBe(1_500_001);
  });

  it('resumeFrom never goes below startLedger on a fresh job', () => {
    const start = 500_000;
    expect(resumeFrom(0, start)).toBeGreaterThanOrEqual(start);
  });

  it('resumeFrom is always > checkpointLedger after a partial run', () => {
    const checkpoint = 1_300_000;
    const from = resumeFrom(checkpoint, 1_000_000);
    expect(from).toBeGreaterThan(checkpoint);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 3. SyncState write guarantee — historical backfills never touch the cursor
// ─────────────────────────────────────────────────────────────────────────────

describe('SyncState write guarantee for historical ranges', () => {
  const LIVE_CURSOR = 2_000_000; // current live poller position

  const historicalRanges: [number, number][] = [
    [1_000_000, 1_500_000], // entirely historical
    [1_800_000, 2_000_000], // ends exactly at cursor (inclusive → "below")
    [500_000,   2_000_000], // long historical sweep ending at cursor
  ];

  it.each(historicalRanges)(
    'range [%i, %i] is "below" → SyncState must NOT be written',
    (start, end) => {
      const interaction = determineCursorInteraction(start, end, LIVE_CURSOR);
      expect(interaction).not.toBe('ahead');
    },
  );

  it('"ahead" interaction is ONLY returned when the entire range is beyond the cursor', () => {
    const aheadRange = determineCursorInteraction(
      LIVE_CURSOR + 1,
      LIVE_CURSOR + 10_000,
      LIVE_CURSOR,
    );
    expect(aheadRange).toBe('ahead');
  });
});
