/**
 * last-poll-timestamp.test.ts
 *
 * Issue #926 — Expose the Last Successful Poll Timestamp
 *
 * Verifies that:
 *  • The health/status surface exposes a `lastSuccessfulPoll` field.
 *  • The value is null before any poll has completed (fresh process).
 *  • After recordLastSuccessfulPoll() is called, the value is a valid UTC
 *    ISO-8601 timestamp.
 *  • The format is stable (zero-padded, always UTC "Z" suffix).
 *  • Existing ledger checkpoint fields keep their current meaning.
 *  • The field appears in the JSON body served by the /health route.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import express from 'express';
import request from 'supertest';

// ── Mocks ─────────────────────────────────────────────────────────────────────

const mockPrisma = vi.hoisted(() => ({
  $queryRaw:       vi.fn(),
  syncState:       { findUnique: vi.fn() },
  trackedContract: { findMany: vi.fn() },
  ledgerCheckpoint: {
    groupBy:   vi.fn().mockResolvedValue([]),
    findFirst: vi.fn().mockResolvedValue(null),
  },
}));

const mockRedis = vi.hoisted(() => ({
  isReady: true,
  ping:    vi.fn().mockResolvedValue('PONG'),
}));

const mockRpcServer = vi.hoisted(() => ({
  getLatestLedger: vi.fn().mockResolvedValue({ sequence: 5_000 }),
}));

vi.mock('../db.js',      () => ({ default: mockPrisma }));
vi.mock('../redis.js',   () => ({ default: mockRedis }));
vi.mock('@stellar/stellar-sdk', async (importOriginal) => {
  const actual = await importOriginal() as any;
  return {
    ...actual,
    rpc: {
      ...actual.rpc,
      Server: vi.fn().mockImplementation(() => mockRpcServer),
    },
  };
});

// Mock modules health.ts depends on indirectly
vi.mock('../coordination/lease.js', () => ({
  getLeaseStatus: vi.fn().mockReturnValue({ held: false, role: null, expiresAt: null }),
  acquireLease:   vi.fn(),
  releaseLease:   vi.fn(),
  renewLease:     vi.fn(),
}));
vi.mock('../fenced-lease.js', () => ({
  getCurrentFencedLease: vi.fn().mockReturnValue(null),
}));
vi.mock('../config.js', () => ({
  VERSION: {
    app: '1.0.0', api: '1', eventSchema: '1',
    dbMigration: 'abc', gitSha: 'def', buildTime: '2026-01-01T00:00:00Z',
  },
}));
vi.mock('../snapshot.js',  () => ({ maybeWriteSnapshot: vi.fn() }));
vi.mock('../metrics.js',   () => ({
  snapshotsWrittenTotal:       { inc: vi.fn() },
  rpcRetryExhaustedCounter:    { inc: vi.fn() },
  latestLedgerProcessedGauge:  { set: vi.fn() },
  networkLatestLedgerGauge:    { set: vi.fn() },
  syncLatencyGauge:            { set: vi.fn() },
  decodeErrorsCounter:         { inc: vi.fn() },
}));
vi.mock('../checkpoint.js', () => ({
  getCheckpointHealthSummary: vi.fn().mockResolvedValue({
    total: 0, committed: 0, failed: 0, incomplete: 0,
    oldestIncompleteWindowStart: null,
  }),
  findIncompleteCheckpoints: vi.fn().mockResolvedValue([]),
  resetApplyingCheckpoint:   vi.fn(),
  openCheckpoint:            vi.fn(),
  markApplying:              vi.fn(),
  commitCheckpoint:          vi.fn(),
  failCheckpoint:            vi.fn(),
}));
vi.mock('../reorg.js', () => ({
  getConfirmationHealthSummary: vi.fn().mockResolvedValue({
    confirmationDepth: 10,
    pendingConfirmationCount: 0,
    oldestProvisionalLedger: null,
  }),
}));

import {
  recordLastSuccessfulPoll,
  getLastSuccessfulPollIso,
  _resetLastSuccessfulPoll,
  runAllChecks,
  AggregateHealth,
} from '../health.js';

// ── Reset timestamp before every test ────────────────────────────────────────

beforeEach(() => {
  vi.clearAllMocks();
  _resetLastSuccessfulPoll();

  // Healthy defaults
  mockPrisma.$queryRaw.mockResolvedValue([1]);
  mockRedis.isReady = true;
  mockRedis.ping.mockResolvedValue('PONG');
  mockRpcServer.getLatestLedger.mockResolvedValue({ sequence: 5_010 });
  mockPrisma.syncState.findUnique.mockResolvedValue({ id: 1, lastLedger: 5_005 });
});

// ─────────────────────────────────────────────────────────────────────────────
// 1. recordLastSuccessfulPoll / getLastSuccessfulPollIso
// ─────────────────────────────────────────────────────────────────────────────

describe('getLastSuccessfulPollIso()', () => {
  it('returns null before any poll has been recorded', () => {
    expect(getLastSuccessfulPollIso()).toBeNull();
  });

  it('returns an ISO-8601 UTC string after recordLastSuccessfulPoll()', () => {
    recordLastSuccessfulPoll();
    const ts = getLastSuccessfulPollIso();
    expect(ts).not.toBeNull();
    // Must end with "Z" — always UTC
    expect(ts!.endsWith('Z')).toBe(true);
    // Must parse without NaN
    expect(Number.isNaN(new Date(ts!).getTime())).toBe(false);
  });

  it('timestamp is close to the current wall-clock time', () => {
    const before = Date.now();
    recordLastSuccessfulPoll();
    const after = Date.now();
    const ts = getLastSuccessfulPollIso()!;
    const parsed = new Date(ts).getTime();
    expect(parsed).toBeGreaterThanOrEqual(before);
    expect(parsed).toBeLessThanOrEqual(after + 1); // 1 ms grace
  });

  it('format is stable ISO-8601 with milliseconds (YYYY-MM-DDTHH:mm:ss.sssZ)', () => {
    recordLastSuccessfulPoll();
    const ts = getLastSuccessfulPollIso()!;
    // e.g. "2026-10-20T14:35:00.000Z"
    expect(ts).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
  });

  it('updates on each successive call (monotonically advancing)', async () => {
    recordLastSuccessfulPoll();
    const first = getLastSuccessfulPollIso();

    // Small delay so the timestamps differ
    await new Promise((r) => setTimeout(r, 5));

    recordLastSuccessfulPoll();
    const second = getLastSuccessfulPollIso();

    expect(new Date(second!).getTime()).toBeGreaterThanOrEqual(new Date(first!).getTime());
  });

  it('_resetLastSuccessfulPoll() brings the value back to null', () => {
    recordLastSuccessfulPoll();
    expect(getLastSuccessfulPollIso()).not.toBeNull();
    _resetLastSuccessfulPoll();
    expect(getLastSuccessfulPollIso()).toBeNull();
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. runAllChecks() includes lastSuccessfulPoll in the aggregate response
// ─────────────────────────────────────────────────────────────────────────────

describe('runAllChecks() — lastSuccessfulPoll field', () => {
  it('includes lastSuccessfulPoll as null when no poll has run', async () => {
    const result = await runAllChecks();
    expect(result).toHaveProperty('lastSuccessfulPoll');
    expect(result.lastSuccessfulPoll).toBeNull();
  });

  it('includes lastSuccessfulPoll as ISO string after a poll', async () => {
    recordLastSuccessfulPoll();
    const result = await runAllChecks();
    expect(result.lastSuccessfulPoll).not.toBeNull();
    expect(result.lastSuccessfulPoll!.endsWith('Z')).toBe(true);
  });

  it('does not affect the overall status field', async () => {
    recordLastSuccessfulPoll();
    const result = await runAllChecks();
    expect(result.status).toBe('ok');
  });

  it('existing checkpoint fields retain their meaning', async () => {
    const result = await runAllChecks();
    // The sync_lag check should still report lagLedgers
    const syncLag = result.checks.sync_lag as any;
    expect(syncLag).toHaveProperty('status');
    expect(syncLag).toHaveProperty('lagLedgers');
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 3. HTTP /health route exposes lastSuccessfulPoll in JSON body
// ─────────────────────────────────────────────────────────────────────────────

describe('GET /health — lastSuccessfulPoll in response body', () => {
  function buildApp() {
    const app = express();
    app.get('/health', async (_req, res) => {
      const result: AggregateHealth = await runAllChecks();
      const httpStatus = result.status === 'down' ? 503 : 200;
      res.status(httpStatus).json(result);
    });
    return app;
  }

  it('response body includes lastSuccessfulPoll key', async () => {
    const res = await request(buildApp()).get('/health');
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty('lastSuccessfulPoll');
  });

  it('lastSuccessfulPoll is null in a fresh process (no polling)', async () => {
    const res = await request(buildApp()).get('/health');
    expect(res.body.lastSuccessfulPoll).toBeNull();
  });

  it('lastSuccessfulPoll is a UTC ISO string after a recorded poll', async () => {
    recordLastSuccessfulPoll();
    const res = await request(buildApp()).get('/health');
    expect(res.body.lastSuccessfulPoll).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
  });

  it('response still has all pre-existing fields alongside lastSuccessfulPoll', async () => {
    const res = await request(buildApp()).get('/health');
    expect(res.body).toHaveProperty('status');
    expect(res.body).toHaveProperty('checks');
    expect(res.body).toHaveProperty('timestamp');
    expect(res.body).toHaveProperty('version');
    expect(res.body).toHaveProperty('lastSuccessfulPoll');
  });
});
