/**
 * health.ts — Dependency health checks for /health, /readyz, and /health/details.
 *
 * Each check function returns a HealthCheckResult with:
 *   status  : "ok" | "degraded" | "down"
 *   latencyMs: round-trip time in milliseconds
 *   message  : optional human-readable detail
 */

import { rpc } from '@stellar/stellar-sdk';
import prisma from './db.js';
import redis from './redis.js';
import { logger } from './logger.js';
import { VERSION } from './config.js';
import { getLeaseStatus } from './coordination/lease.js';
import { getCurrentFencedLease } from './fenced-lease.js';

// ── Last-poll timestamp (Issue #926) ─────────────────────────────────────────
//
// The poller calls `recordLastSuccessfulPoll()` after every successful window
// commit.  `getLastSuccessfulPollIso()` surfaces it through the health/status
// endpoint so operators can tell when the indexer last made progress without
// having to cross-reference logs.
//
// The value is process-local (in-memory).  On a fresh restart it is null until
// the first successful poll completes, which is the correct and honest answer.

let _lastSuccessfulPollMs: number | null = null;

/**
 * Record the current wall-clock time as the last successful poll instant.
 * Called by the polling loop after each committed window.
 */
export function recordLastSuccessfulPoll(): void {
  _lastSuccessfulPollMs = Date.now();
}

/**
 * Return the UTC ISO-8601 string of the last successful poll, or null if no
 * poll has completed since the process started.
 *
 * Format: "2026-10-20T14:35:00.000Z"  (stable, zero-padded, always UTC).
 */
export function getLastSuccessfulPollIso(): string | null {
  return _lastSuccessfulPollMs !== null
    ? new Date(_lastSuccessfulPollMs).toISOString()
    : null;
}

/**
 * Exposed for unit tests only — resets the in-memory timestamp.
 * @internal
 */
export function _resetLastSuccessfulPoll(): void {
  _lastSuccessfulPollMs = null;
}

// ── Types ─────────────────────────────────────────────────────────────────────

export type CheckStatus = 'ok' | 'degraded' | 'down';

export interface HealthCheckResult {
  status: CheckStatus;
  latencyMs: number;
  message?: string;
}

export interface AggregateHealth {
  /** Overall system status — worst of all individual checks. */
  status: CheckStatus;
  checks: Record<string, HealthCheckResult>;
  /** Unix timestamp (ms) when this snapshot was taken. */
  timestamp: number;
  /**
   * UTC ISO-8601 timestamp of the last successful poll commit (Issue #926).
   * Null when no poll has completed since the process started.
   * Format: "2026-10-20T14:35:00.000Z"
   */
  lastSuccessfulPoll: string | null;
  /** Component version metadata for runtime identification. */
  version: {
    app: string;
    api: string;
    eventSchema: string;
    dbMigration: string;
    gitSha: string;
    buildTime: string;
  };
}

// ── Thresholds ────────────────────────────────────────────────────────────────

const STELLAR_RPC_TIMEOUT_MS = 5_000;
const SYNC_LAG_DEGRADED = 100;  // ledgers behind → degraded
const SYNC_LAG_DOWN     = 1_000; // ledgers behind → down

// ── Individual checks ─────────────────────────────────────────────────────────

/** Verify the PostgreSQL connection is alive by running SELECT 1. */
export async function checkDatabase(): Promise<HealthCheckResult> {
  const start = Date.now();
  try {
    await prisma.$queryRaw`SELECT 1`;
    return { status: 'ok', latencyMs: Date.now() - start };
  } catch (err) {
    return {
      status: 'down',
      latencyMs: Date.now() - start,
      message: err instanceof Error ? err.message : String(err),
    };
  }
}

/** Verify the Redis connection is alive by running PING. */
export async function checkRedis(): Promise<HealthCheckResult> {
  const start = Date.now();
  try {
    const client = redis as any;
    // isReady / isOpen guard — skip if client not connected
    const ready =
      typeof client.isReady === 'boolean' ? client.isReady :
      typeof client.status === 'string'   ? client.status === 'ready' :
      Boolean(client.isOpen);

    if (!ready) {
      return { status: 'down', latencyMs: Date.now() - start, message: 'Redis client not connected' };
    }

    const reply = await client.ping();
    if (reply !== 'PONG') {
      return { status: 'degraded', latencyMs: Date.now() - start, message: `Unexpected PING reply: ${reply}` };
    }
    return { status: 'ok', latencyMs: Date.now() - start };
  } catch (err) {
    return {
      status: 'down',
      latencyMs: Date.now() - start,
      message: err instanceof Error ? err.message : String(err),
    };
  }
}

/** Call getLatestLedger on the Stellar RPC with a 5-second timeout. */
export async function checkStellarRpc(): Promise<HealthCheckResult> {
  const start = Date.now();
  const rpcUrl = process.env.STELLAR_RPC_URL || 'https://soroban-testnet.stellar.org';
  try {
    const server = new rpc.Server(rpcUrl);

    const timeoutPromise = new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error('Stellar RPC timeout')), STELLAR_RPC_TIMEOUT_MS)
    );

    await Promise.race([server.getLatestLedger(), timeoutPromise]);
    return { status: 'ok', latencyMs: Date.now() - start };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const isTimeout = msg.includes('timeout');
    return {
      status: 'down',
      latencyMs: Date.now() - start,
      message: msg,
      ...(isTimeout && { message: `Stellar RPC did not respond within ${STELLAR_RPC_TIMEOUT_MS}ms` }),
    };
  }
}

/**
 * Measure how far behind the network tip the indexer is.
 * Returns degraded when lag > 100 ledgers, down when lag > 1000 ledgers.
 */
export async function checkSyncLag(): Promise<HealthCheckResult & { lagLedgers: number }> {
  const start = Date.now();
  try {
    const rpcUrl = process.env.STELLAR_RPC_URL || 'https://soroban-testnet.stellar.org';
    const server = new rpc.Server(rpcUrl);

    const [networkLedger, syncState] = await Promise.all([
      server.getLatestLedger().catch(() => null),
      prisma.syncState.findUnique({ where: { id: 1 } }).catch(() => null),
    ]);

    if (!networkLedger || !syncState) {
      return {
        status: 'degraded',
        latencyMs: Date.now() - start,
        lagLedgers: -1,
        message: 'Could not determine sync lag — missing data',
      };
    }

    const lag = Math.max(0, networkLedger.sequence - syncState.lastLedger);
    let status: CheckStatus = 'ok';
    if (lag > SYNC_LAG_DOWN)     status = 'down';
    else if (lag > SYNC_LAG_DEGRADED) status = 'degraded';

    return {
      status,
      latencyMs: Date.now() - start,
      lagLedgers: lag,
      message: lag > 0 ? `${lag} ledgers behind tip` : undefined,
    };
  } catch (err) {
    return {
      status: 'down',
      latencyMs: Date.now() - start,
      lagLedgers: -1,
      message: err instanceof Error ? err.message : String(err),
    };
  }
}

/**
 * Returns the current distributed lease and fencing-token state.
 * Useful for operators to see which worker holds the active lease
 * and whether fencing tokens are advancing normally.
 */
export function checkLeaseState(): HealthCheckResult & {
  coordinationLease: ReturnType<typeof getLeaseStatus>;
  fencedLease: { held: boolean; token: string | null; role: string | null };
} {
  const coordination = getLeaseStatus();
  const fenced = getCurrentFencedLease();
  return {
    status: 'ok',
    latencyMs: 0,
    coordinationLease: coordination,
    fencedLease: {
      held: fenced !== null,
      token: fenced ? fenced.token.toString() : null,
      role: fenced?.role ?? null,
    },
  };
}

/**
 * Issue #286: Report confirmation depth configuration and pending-confirmation
 * event count. This tells operators how many events are still provisional and
 * the configured depth threshold.
 */
export async function checkConfirmationDepth(): Promise<HealthCheckResult & {
  confirmationDepth: number;
  pendingConfirmationCount: number;
  oldestProvisionalLedger: number | null;
}> {
  const start = Date.now();
  const confirmationDepth = parseInt(process.env.CONFIRMATION_DEPTH || '10', 10);
  try {
    const summary = await getConfirmationHealthSummary(confirmationDepth);
    return {
      status: 'ok',
      latencyMs: Date.now() - start,
      confirmationDepth: summary.confirmationDepth,
      pendingConfirmationCount: summary.pendingConfirmationCount,
      oldestProvisionalLedger: summary.oldestProvisionalLedger,
      message:
        summary.pendingConfirmationCount > 0
          ? `${summary.pendingConfirmationCount} event(s) pending confirmation (depth=${summary.confirmationDepth})`
          : undefined,
    };
  } catch (err) {
    return {
      status: 'degraded',
      latencyMs: Date.now() - start,
      confirmationDepth,
      pendingConfirmationCount: -1,
      oldestProvisionalLedger: null,
      message: err instanceof Error ? err.message : String(err),
    };
  }
}

// ── Aggregate health ──────────────────────────────────────────────────────────

/**
 * Run all checks in parallel and aggregate into a single result.
 * Overall status is:
 *   "ok"       — all checks ok
 *   "degraded" — at least one check degraded, none down
 *   "down"     — at least one check down
 */
export async function runAllChecks(): Promise<AggregateHealth> {
  const [db, redisCheck, stellarRpc, syncLag, confirmationDepth] = await Promise.all([
    checkDatabase(),
    checkRedis(),
    checkStellarRpc(),
    checkSyncLag(),
    checkConfirmationDepth(),
  ]);

  const leaseState = checkLeaseState();

  const checks: Record<string, HealthCheckResult> = {
    database: db,
    redis: redisCheck,
    stellar_rpc: stellarRpc,
    sync_lag: syncLag,
    confirmation_depth: confirmationDepth,
    lease_state: leaseState,
  };

  const statuses = Object.values(checks).map((c) => c.status);
  const overall: CheckStatus =
    statuses.includes('down')     ? 'down' :
    statuses.includes('degraded') ? 'degraded' : 'ok';

  return {
    status: overall,
    checks,
    timestamp: Date.now(),
    lastSuccessfulPoll: getLastSuccessfulPollIso(),
    version: {
      app: VERSION.app,
      api: VERSION.api,
      eventSchema: VERSION.eventSchema,
      dbMigration: VERSION.dbMigration,
      gitSha: VERSION.gitSha,
      buildTime: VERSION.buildTime,
    },
  };
}

/**
 * Lightweight readiness check — only database + sync lag.
 * Returns 503 if the DB is down or sync lag is "down".
 */
export async function runReadinessChecks(): Promise<{
  ready: boolean;
  checks: Record<string, HealthCheckResult>;
}> {
  const [db, syncLag] = await Promise.all([checkDatabase(), checkSyncLag()]);
  const checks: Record<string, HealthCheckResult> = { database: db, sync_lag: syncLag };
  const ready = db.status !== 'down' && syncLag.status !== 'down';
  return { ready, checks };
}
