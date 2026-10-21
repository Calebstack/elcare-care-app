import { rpc, Contract, TransactionBuilder, BASE_FEE, nativeToScVal, scValToNative } from '@stellar/stellar-sdk';
import { fetchRawListingFromChain, fetchRawAuctionFromChain } from './chain-state.js';
import prisma from './prisma-write.js';
import { emitSSEEvent } from './api/routes.js';
import dotenv from 'dotenv';
import {
  latestLedgerProcessedGauge,
  networkLatestLedgerGauge,
  syncLatencyGauge,
  gapsCreatedTotal,
  openGapsGauge,
  openGapLedgersTotalGauge,
  duplicateEventsCounter,
  listingsCreatedTotal,
  salesTotalCounter,
  auctionFinalizationsTotal,
  offersMadeTotal,
  offersAcceptedTotal,
  activeListingsGauge,
  activeAuctionsGauge,
  syncLagLedgersGauge,
  eventProcessingDurationHistogram,
} from './metrics.js';
import {
  recordProgress,
  recordDbWrite,
  recordRpcFailure,
  registerPollerLifecycle,
  startWatchdog,
} from './stall.js';
import { collectMarketplaceEvents, MAX_LEDGER_WINDOW } from './event-sync.js';
import { withRpcRetry } from './retry.js';
import { logger } from './logger.js';
import redis from './redis.js';
import { applyInvalidation, invalidateListing, invalidateAuction, invalidateOffer, invalidateCollection, invalidateWalletActivity, invalidateStats, invalidateConfig } from './cache-invalidation.js';
import { loadConfig, parseTrackedContracts } from './config.js';
import { enqueueIpfsFetch } from './ipfs-cache.js';
import { contractRegistry } from './contract-registry.js';
import { promoteConfirmedEvents, rollbackReorg } from './reorg.js';
import { acquireLease, releaseLease, renewLease, type LeaseRole } from './coordination/lease.js';
import { recoveryFSM } from './recovery-state-machine.js';
import {
  reorgRollbackDurationSeconds,
  gapRepairDurationSeconds,
  gapLengthLedgers,
} from './recovery-metrics.js';
import { upsertEvents } from './event-idempotency.js';
import {
  withShutdownTimeout,
  TIMEOUT_BUDGETS,
  TimeoutError,
  CancellationError,
} from './timeout.js';
import { recordLastSuccessfulPoll } from './health.js';

dotenv.config();

// ── Re-org SSE event types ────────────────────────────────────────────────────

/**
 * Emitted on a normal (shallow) re-org that was successfully rolled back.
 * Frontends should show a brief notification and trigger a data refresh.
 */
export interface ReorgEvent {
  type: 'REORG';
  from_ledger: number;
  to_ledger: number;
  timestamp: string;
  depth: number;
}

/**
 * Emitted when a re-org depth exceeds MAX_ROLLBACK_DEPTH.
 * The poller halts; a human operator must call POST /admin/reorg-recovery
 * to trigger manual recovery.
 */
export interface CriticalReorgEvent {
  type: 'CRITICAL_REORG';
  from_ledger: number;
  to_ledger: number;
  timestamp: string;
  depth: number;
  message: string;
}

// ── Poller halt state ─────────────────────────────────────────────────────────

let _pollerHalted = false;
let _haltReason: string | null = null;

/** Returns true when the poller has been halted due to a critical re-org. */
export function isPollerHalted(): boolean {
  return _pollerHalted;
}

/** Returns the halt reason, or null if not halted. */
export function getHaltReason(): string | null {
  return _haltReason;
}

/**
 * Resumes the poller after a critical re-org.  Called by the admin recovery
 * endpoint (POST /admin/reorg-recovery) once an operator has verified the
 * chain state and performed any necessary manual rollback.
 */
export function resumePoller(): void {
  _pollerHalted = false;
  _haltReason = null;
  logger.info('poller: resumed by operator after critical re-org');
}

function emitReorgEvent(fromLedger: number, toLedger: number, depth: number): void {
  const event: ReorgEvent = {
    type: 'REORG',
    from_ledger: fromLedger,
    to_ledger: toLedger,
    timestamp: new Date().toISOString(),
    depth,
  };
  emitSSEEvent(event);
}

function emitCriticalReorgEvent(fromLedger: number, toLedger: number, depth: number): void {
  const msg = `Re-org depth ${depth} exceeds MAX_ROLLBACK_DEPTH — poller halted. ` +
    `Call POST /admin/reorg-recovery to resume after manual verification.`;
  const event: CriticalReorgEvent = {
    type: 'CRITICAL_REORG',
    from_ledger: fromLedger,
    to_ledger: toLedger,
    timestamp: new Date().toISOString(),
    depth,
    message: msg,
  };
  emitSSEEvent(event);
}

export const MAX_REORG_DEPTH = 100;

const RPC_URL = process.env.STELLAR_RPC_URL || 'https://soroban-testnet.stellar.org';

export type LedgerGapSource = 'rpc_window_skip' | 'reorg' | 'manual';

/**
 * Upsert a LedgerGap row for a skipped ledger range.
 *
 * Uses a unique index on (fromLedger, toLedger, source) so repeated calls for
 * the same range are idempotent — the poller may re-enter the same code path
 * after a restart before the gap is repaired.
 *
 * Also refreshes the open-gap gauge so Prometheus always reflects current state.
 */
export async function persistLedgerGap(
  from: number,
  to: number,
  source: LedgerGapSource,
): Promise<void> {
  try {
    await prisma.ledgerGap.upsert({
      where: {
        fromLedger_toLedger_source: { fromLedger: from, toLedger: to, source },
      },
      create: { fromLedger: from, toLedger: to, source, status: 'Open' },
      update: {}, // already exists — leave status/error untouched
    });

    gapsCreatedTotal.inc({ source });

    // Refresh open-gap gauges asynchronously (best-effort, non-blocking)
    prisma.ledgerGap
      .findMany({ where: { status: 'Open' }, select: { fromLedger: true, toLedger: true } })
      .then((gaps) => {
        openGapsGauge.set(gaps.length);
        const total = gaps.reduce((acc, g) => acc + (g.toLedger - g.fromLedger + 1), 0);
        openGapLedgersTotalGauge.set(total);
      })
      .catch(() => {/* non-fatal */});

    logger.info('poller: persisted ledger gap', { from, to, source });
  } catch (err) {
    // Non-fatal: gap persistence must never crash the poller
    logger.error('poller: failed to persist ledger gap', {
      from, to, source,
      err: err instanceof Error ? err.message : String(err),
    });
  }
}

// Retry back-off base in ms; doubles on each consecutive failure up to MAX_BACKOFF_MS.
const BASE_BACKOFF_MS = 2_000;
const MAX_BACKOFF_MS = 60_000;

let consecutiveErrors = 0;

// Graceful shutdown coordination
let shuttingDown = false;
let shutdownStarted = false;
const shutdownHooks: Array<() => Promise<void>> = [];

/** Register an async cleanup function to run during graceful shutdown. */
export function registerShutdownHook(fn: () => Promise<void>): void {
  shutdownHooks.push(fn);
}

/**
 * Signal the polling loop(s) to stop after completing their current batch.
 * Safe to call multiple times — subsequent calls are no-ops.
 */
export function stopPoller(): void {
  if (!shuttingDown) {
    shuttingDown = true;
    logger.info('poller: stop requested — will exit after current batch');
  }
}

/**
 * Reset the shutdown flag so startPolling() can be called again after a
 * watchdog-triggered restart.  Called internally by startPolling().
 */
function resetPollerShutdownFlag(): void {
  shuttingDown = false;
  shutdownStarted = false;
}
function getContractIds(): string[] {
  return parseTrackedContracts().map((c) => c.id).filter(Boolean);
}

function updateSyncMetrics(processedLedger: number, networkLatestLedger: number) {
  latestLedgerProcessedGauge.set(processedLedger);
  networkLatestLedgerGauge.set(networkLatestLedger);
  syncLatencyGauge.set(Math.max(0, networkLatestLedger - processedLedger));
  // Update normalized autoscaling-friendly metrics
  ledgerLagGauge.set(Math.max(0, networkLatestLedger - processedLedger));
}

function setupSignalHandlers() {
  const onSignal = (sig: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info('Shutdown signal received', { signal: sig });
    // Start async cleanup; don't await here since signals may be re-delivered
    gracefulShutdown().catch((err) => {
      logger.error('Graceful shutdown failed', { err });
      process.exit(1);
    });
  };
  process.on('SIGTERM', () => onSignal('SIGTERM'));
  process.on('SIGINT', () => onSignal('SIGINT'));
}

export async function gracefulShutdown(): Promise<void> {
  if (shutdownStarted) return;
  shutdownStarted = true;

  console.log('[Shutdown] Closing resources: Prisma + Redis + registered hooks');
  
  // Use timeout budget for shutdown with abort signal support
  try {
    await withShutdownTimeout(
      async (signal) => {
        // Check if shutdown was cancelled
        if (signal.aborted) {
          throw new CancellationError('shutdown cancelled by signal');
        }

        const cleanup = Promise.allSettled([
          prisma.$disconnect(),
          (redis && typeof redis.disconnect === 'function') ? redis.disconnect() : Promise.resolve(),
          ...shutdownHooks.map((fn) => fn()),
        ]);

        await cleanup;
      },
      'graceful_shutdown',
      TIMEOUT_BUDGETS.shutdown
    );
    
    logger.info('Shutdown: cleanup complete');
    process.exit(0);
  } catch (err) {
    if (err instanceof TimeoutError) {
      logger.error('Shutdown: cleanup timed out', { 
        timeoutMs: TIMEOUT_BUDGETS.shutdown.totalBudgetMs,
        err: err.message 
      });
    } else if (err instanceof CancellationError) {
      logger.warn('Shutdown: cancelled', { reason: err.message });
    } else {
      logger.error('Shutdown: cleanup failed', { err });
    }
    process.exit(1);
  }
}

// Register handlers immediately so any external SIGTERM/SIGINT will be caught
setupSignalHandlers();

const server = new rpc.Server(RPC_URL);

/**
 * Rolls the database back to `safeAtLedger` by deleting all events and
 * listings that were written past that ledger, then resets SyncState.
 * Called when a chain re-org is detected.
 */
export async function revertLedgers(safeAtLedger: number): Promise<void> {
  logger.warn('Reorg: rolling back', { safeAtLedger });
  const rollbackStart = Date.now();
  await prisma.$transaction(async (tx) => {
    // Remove events that occurred after the safe checkpoint
    await tx.marketplaceEvent.deleteMany({
      where: { ledgerSequence: { gt: safeAtLedger } },
    });

    // Remove per-event history rows written past the safe checkpoint
    if (typeof (tx as any).bid?.deleteMany === 'function') {
      await (tx as any).bid.deleteMany({
        where: { ledgerSequence: { gt: safeAtLedger } },
      });
    }
    if (typeof (tx as any).priceHistory?.deleteMany === 'function') {
      await (tx as any).priceHistory.deleteMany({
        where: { ledgerSequence: { gt: safeAtLedger } },
      });
    }
    if (typeof (tx as any).protocolFee?.deleteMany === 'function') {
      await (tx as any).protocolFee.deleteMany({
        where: { ledgerSequence: { gt: safeAtLedger } },
      });
    }

    // ── Royalty payments written past safe point ───────────────────────────
    // RoyaltyPayment rows are created by ROYALTY_PAID events; delete any that
    // arrived from ledgers that no longer exist on the canonical chain.
    if (typeof (tx as any).royaltyPayment?.deleteMany === 'function') {
      await (tx as any).royaltyPayment.deleteMany({
        where: { ledgerSequence: { gt: safeAtLedger } },
      });
    }

    // ── Deployment fees recorded past safe point ───────────────────────────
    // DeploymentFee rows are appended on DEPLOY_* events via the launchpad.
    // Remove any that fell in the reorg window.
    if (typeof (tx as any).deploymentFee?.deleteMany === 'function') {
      await (tx as any).deploymentFee.deleteMany({
        where: { ledgerSequence: { gt: safeAtLedger } },
      });
    }

    // ── Remove listings that were first created after the safe checkpoint ──
    await tx.listing.deleteMany({
      where: { createdAtLedger: { gt: safeAtLedger } },
    });

    // Revert listings whose status changed after the safe checkpoint back to Active
    await tx.listing.updateMany({
      where: { updatedAtLedger: { gt: safeAtLedger } },
      data: { status: 'Active' as const, updatedAtLedger: safeAtLedger },
    });

    // ── Auctions created past the safe point ──────────────────────────────
    // Auctions whose createdAtLedger > safeAtLedger never existed on the
    // canonical chain — delete them entirely.
    if (typeof (tx as any).auction?.deleteMany === 'function') {
      await (tx as any).auction.deleteMany({
        where: { createdAtLedger: { gt: safeAtLedger } },
      });
    }

    // Auctions whose status changed after the safe checkpoint revert to Active
    // (e.g. Finalized/Cancelled due to events in the reorg window).
    if (typeof (tx as any).auction?.updateMany === 'function') {
      await (tx as any).auction.updateMany({
        where: { updatedAtLedger: { gt: safeAtLedger } },
        data: { status: 'Active' as const, updatedAtLedger: safeAtLedger },
      });
    }

    // ── WhitelistedToken: undo removals that happened in the reorg window ──
    // TOKEN_REMOVED events that fall past safeAtLedger must be unwound:
    // restore active=true and clear removedAtLedger / removedBy.
    if (typeof (tx as any).whitelistedToken?.updateMany === 'function') {
      await (tx as any).whitelistedToken.updateMany({
        where: { active: false, removedAtLedger: { gt: safeAtLedger } },
        data: { active: true, removedAtLedger: null, removedBy: null },
      });
      // TOKEN_WHITELISTED events in the reorg window: remove rows that were
      // first added after the safe ledger (addedAtLedger > safeAtLedger).
      await (tx as any).whitelistedToken.deleteMany({
        where: { addedAtLedger: { gt: safeAtLedger } },
      });
    }

    // Reset collections deployed after the safe checkpoint
    await tx.collection.deleteMany({
      where: { deployedAtLedger: { gt: safeAtLedger } },
    });

    // Reset the sync cursor
    await tx.syncState.update({
      where: { id: 1 },
      data: { lastLedger: safeAtLedger, lastLedgerHash: null },
    });

    // Issue #286: also rollback offer and bid state inside the transaction
    // Guard: rollbackReorg may not be available in all test environments
    if (typeof rollbackReorg === 'function') {
      try {
        await rollbackReorg(safeAtLedger, tx);
      } catch {
        // Non-fatal: if rollbackReorg fails (e.g. missing table in test env),
        // the primary rollback already completed above.
      }
    }

    // Revert staged (pending) offers from the rolled-back ledger range.
    // These are OFFER_MADE events whose parent listing had not yet arrived —
    // if the listing was also in the reorg window, the staged offer must go too.
    try {
      await (tx as any).$executeRawUnsafe(
        `SELECT revert_pending_offers($1)`,
        safeAtLedger,
      );
    } catch {
      // Non-fatal: the function may not exist in test/older environments.
    }
  });
  const rollbackDurationSec = (Date.now() - rollbackStart) / 1000;
  reorgRollbackDurationSeconds.observe(rollbackDurationSec);
  logger.info('Reorg: rollback complete', {
    resumeFromLedger: safeAtLedger + 1,
    durationSeconds: rollbackDurationSec.toFixed(3),
  });
  recoveryFSM.reorgRollbackComplete(safeAtLedger);
}

/** SyncState fields for a ledger advance; omits hash when fetch failed so we keep the prior checkpoint. */
export function buildSyncStateLedgerData(
  lastLedger: number,
  ledgerHash: string | null
): { lastLedger: number; lastLedgerHash?: string } {
  if (ledgerHash !== null) {
    return { lastLedger, lastLedgerHash: ledgerHash };
  }
  return { lastLedger };
}

/**
 * Build the TrackedContract update payload for a contract cursor advance.
 *
 * Issue #441: per-contract variant of buildSyncStateLedgerData that makes it
 * explicit that each contract has its own cursor — caller supplies the contract
 * id so the update is scoped correctly.  The shape is identical to the shared
 * version, but the intent is clearer at each call site.
 *
 * @param lastLedger  The ledger sequence being committed.
 * @param ledgerHash  Hash of that ledger, or null when the RPC hash fetch failed.
 *                    Omitting the hash preserves the prior checkpoint so reorg
 *                    detection on the next cycle is not accidentally disabled.
 */
export function buildContractCursorData(
  _contractId: string,
  lastLedger: number,
  ledgerHash: string | null,
): { lastLedger: number; lastLedgerHash?: string } {
  return buildSyncStateLedgerData(lastLedger, ledgerHash);
}

/**
 * Walks back from `divergedAt` up to MAX_REORG_DEPTH ledgers to find the
 * deepest ledger still accessible on the network's canonical chain.
 * Returns that ledger's sequence number as the safe revert point.
 */
export async function findReorgSafePoint(
  divergedAt: number,
  rpcServer: rpc.Server
): Promise<number> {
  for (let depth = 1; depth <= MAX_REORG_DEPTH; depth++) {
    const candidate = divergedAt - depth;
    if (candidate <= 0) return 0;
    try {
      const res = await rpcServer.getLedgers({
        startLedger: candidate,
        pagination: { limit: 1 },
      });
      if (res.ledgers && res.ledgers.length > 0) {
        return candidate;
      }
    } catch {
      // Ledger not accessible at this depth; keep walking back
    }
  }
  return Math.max(0, divergedAt - MAX_REORG_DEPTH);
}

export async function validateHashContinuity(
  syncState: { lastLedger: number; lastLedgerHash: string | null },
  rpcServer: rpc.Server,
  maxRollbackDepth = 100,
  reorgHaltOnDeep = true
): Promise<boolean> {
  // No stored hash (initial sync or prior hash fetch failure) — cannot detect re-org.
  if (syncState.lastLedger > 0 && syncState.lastLedgerHash) {
    try {
      const ledgersRes = await rpcServer.getLedgers({
        startLedger: syncState.lastLedger,
        pagination: { limit: 1 }
      });
      if (ledgersRes.ledgers && ledgersRes.ledgers.length > 0) {
        const networkLedger = ledgersRes.ledgers[0];
        if (networkLedger.hash !== syncState.lastLedgerHash) {
          logger.warn('Chain re-org detected', {
            ledger: syncState.lastLedger,
            dbHash: syncState.lastLedgerHash,
            networkHash: networkLedger.hash,
          });

          const safeLedger = await findReorgSafePoint(syncState.lastLedger, rpcServer);
          const rollbackDepth = syncState.lastLedger - safeLedger;

          // Guard: deep re-orgs halt the poller instead of executing a destructive rollback.
          if (reorgHaltOnDeep && rollbackDepth > maxRollbackDepth) {
            _pollerHalted = true;
            _haltReason =
              `Re-org depth ${rollbackDepth} exceeds MAX_ROLLBACK_DEPTH (${maxRollbackDepth}). ` +
              `Manual operator recovery required via POST /admin/reorg-recovery.`;
            logger.error('CRITICAL: Re-org depth exceeds MAX_ROLLBACK_DEPTH — halting poller', {
              rollbackDepth,
              maxRollbackDepth,
              fromLedger: syncState.lastLedger,
              toLedger: safeLedger,
            });
            emitCriticalReorgEvent(syncState.lastLedger, safeLedger, rollbackDepth);
            return false;
          }

          await revertLedgers(safeLedger);
          emitReorgEvent(syncState.lastLedger, safeLedger, rollbackDepth);
          return false;
        }
      }
    } catch (err) {
      logger.error('Hash continuity check failed', { ledger: syncState.lastLedger, err });
    }
  }
  return true;
}

/**
 * Seed TrackedContract rows from TRACKED_CONTRACTS (or legacy env vars) into
 * the database. Uses upsert on contractId so re-runs are idempotent.
 * After the DB sync, loads every active contract into the runtime contract
 * registry so the formal per-contract health model is populated.
 * Returns the full list of active contracts from the DB after seeding.
 */
export async function seedTrackedContracts() {
  const fromEnv = parseTrackedContracts();
  for (const c of fromEnv) {
    await prisma.trackedContract.upsert({
      where: { contractId: c.id },
      create: {
        contractId: c.id,
        type: c.type,
        label: c.label,
        startLedger: c.startLedger,
        lastLedger: c.startLedger,
        active: true,
      },
      // Only update label/type — don't reset lastLedger for existing contracts
      update: { label: c.label, type: c.type, active: true },
    });
  }

  // ── Populate the runtime contract registry ────────────────────────────────
  // loadFromDb() is idempotent — safe to call on every startup and reconfiguration.
  await contractRegistry.loadFromDb();

  return prisma.trackedContract.findMany({ where: { active: true } });
}

/**
 * Poll a single tracked contract indefinitely.
 * Each contract maintains its own lastLedger / lastLedgerHash in TrackedContract
 * and in the runtime ContractRegistry entry.
 *
 * Checkpoint protocol (Issue #285):
 *   1. openCheckpoint()  — record window in DB as "fetched" (RPC data retrieved)
 *   2. markApplying()    — record window as "applying" before opening DB transaction
 *   3. commitCheckpoint() inside the domain transaction — atomically advances cursor
 *      and marks "committed"; if the process crashes the checkpoint stays "applying"
 *      and startup recovery replays the window idempotently.
 *
 * Registry integration (Issue #441):
 *   - recordProgress() / recordError() / recordGap() / recordStall() are called
 *     on every iteration so the formal per-contract health model stays current.
 *   - A contract that exceeds MAX_CONTRACT_ERRORS is auto-disabled; its loop exits.
 *   - A stalled or gapped contract does NOT affect any sibling contract's cursor.
 */
async function pollContract(
  contractRow: { id: number; contractId: string; lastLedger: number; lastLedgerHash: string | null },
  config: ReturnType<typeof loadConfig>
): Promise<void> {
  let localErrors = 0;

  // ── Startup recovery: replay any incomplete checkpoints ───────────────────
  const stale = await findIncompleteCheckpoints(contractRow.contractId);
  for (const cp of stale) {
    if (cp.status === 'applying') {
      await resetApplyingCheckpoint(cp);
    }
    logger.info('pollContract: replaying incomplete checkpoint on startup', {
      contractId: contractRow.contractId,
      checkpointId: cp.id,
      windowStart: cp.windowStart,
      windowEnd: cp.windowEnd,
      status: cp.status,
    });
  }

  while (!shuttingDown) {
    // If the poller has been halted due to a critical re-org, pause and wait for
    // an operator to call POST /admin/reorg-recovery to clear the halt flag.
    if (_pollerHalted) {
      logger.warn('pollContract: poller halted due to critical re-org — waiting for operator recovery', {
        contractId: contractRow.contractId,
        reason: _haltReason,
      });
      await new Promise((resolve) => setTimeout(resolve, 10_000));
      continue;
    }

    try {
      const contract = await prisma.trackedContract.findUnique({
        where: { id: contractRow.id },
      });

      if (!contract || !contract.active) {
        logger.info('pollContract: contract deactivated, stopping loop', {
          contractId: contractRow.contractId,
        });
        return;
      }

      // Hash continuity check for this contract
      if (contract.lastLedger > 0 && contract.lastLedgerHash) {
        try {
          const ledgersRes = await server.getLedgers({
            startLedger: contract.lastLedger,
            pagination: { limit: 1 },
          });
          if (ledgersRes.ledgers && ledgersRes.ledgers.length > 0) {
            const networkHash = ledgersRes.ledgers[0].hash;
            if (networkHash !== contract.lastLedgerHash) {
              logger.warn('pollContract: reorg detected', {
                contractId: contract.contractId,
                ledger: contract.lastLedger,
              });
              const safeLedger = await findReorgSafePoint(contract.lastLedger, server);
              const rollbackDepth = contract.lastLedger - safeLedger;

              // Guard: deep re-orgs halt the poller.
              if (config.reorgHaltOnDeep && rollbackDepth > config.maxRollbackDepth) {
                _pollerHalted = true;
                _haltReason =
                  `Re-org depth ${rollbackDepth} exceeds MAX_ROLLBACK_DEPTH (${config.maxRollbackDepth}). ` +
                  `Manual operator recovery required via POST /admin/reorg-recovery.`;
                logger.error('CRITICAL: Re-org depth exceeds MAX_ROLLBACK_DEPTH — halting poller', {
                  contractId: contract.contractId,
                  rollbackDepth,
                  maxRollbackDepth: config.maxRollbackDepth,
                  fromLedger: contract.lastLedger,
                  toLedger: safeLedger,
                });
                recoveryFSM.toHalted(
                  `Re-org depth ${rollbackDepth} exceeds MAX_ROLLBACK_DEPTH (${config.maxRollbackDepth})`
                );
                emitCriticalReorgEvent(contract.lastLedger, safeLedger, rollbackDepth);
                continue; // loop back, where _pollerHalted will block
              }

              recoveryFSM.toReorgRollback(contract.lastLedger, safeLedger, rollbackDepth);
              await revertLedgers(safeLedger);
              await prisma.trackedContract.update({
                where: { id: contract.id },
                data: { lastLedger: safeLedger, lastLedgerHash: null },
              });
              emitReorgEvent(contract.lastLedger, safeLedger, rollbackDepth);
              continue;
            }
          }
        } catch (err) {
          logger.error('pollContract: hash check failed', {
            contractId: contract.contractId,
            err: err instanceof Error ? err.message : String(err),
          });
        }
      }

      const networkLatestLedger: number = await withRpcRetry(
        () => server.getLatestLedger().then((r) => r.sequence),
        { operation: 'getLatestLedger', maxAttempts: 5, baseDelayMs: 1_000 }
      );
      // Successful RPC response — the consecutive-failure counter is reset by
      // recordProgress() on every successful ledger advance.  We do NOT reset
      // it here explicitly so that a run of empty batches (no new events) does
      // not mask a lingering RPC instability signal.

      networkLatestLedgerGauge.set(networkLatestLedger);

      if (contract.lastLedger > 0 && networkLatestLedger < contract.lastLedger) {
        await persistLedgerGap(networkLatestLedger + 1, contract.lastLedger, 'reorg');
        await revertLedgers(networkLatestLedger);
        await prisma.trackedContract.update({
          where: { id: contract.id },
          data: { lastLedger: networkLatestLedger, lastLedgerHash: null },
        });
        // ── Per-contract gap tracking (Issue #441) ────────────────────────
        try {
          contractRegistry.recordGap(contract.contractId, networkLatestLedger + 1, contract.lastLedger);
        } catch { /* non-fatal */ }
        continue;
      }

      const windowFloor = networkLatestLedger - MAX_LEDGER_WINDOW;
      let startLedger = contract.lastLedger + 1;

      if (startLedger < windowFloor) {
        const skippedRange = { from: startLedger, to: windowFloor - 1 };
        logger.warn('pollContract: skipping ledger gap outside RPC window', {
          contractId: contract.contractId,
          ...skippedRange,
          windowFloor,
        });
        await persistLedgerGap(skippedRange.from, skippedRange.to, 'rpc_window_skip');
        await prisma.trackedContract.update({
          where: { id: contract.id },
          data: { lastLedger: windowFloor - 1, lastLedgerHash: null },
        });
        // ── Per-contract gap tracking (Issue #441) ────────────────────────
        try {
          contractRegistry.recordGap(contract.contractId, skippedRange.from, skippedRange.to);
        } catch { /* non-fatal */ }
        startLedger = windowFloor;
      }

      const batchEndLedger = Math.min(
        networkLatestLedger,
        startLedger + config.maxLedgersPerCycle - 1
      );

      // ── Step 1: Fetch events from RPC ─────────────────────────────────────
      const decodedEvents = await collectMarketplaceEvents(
        server,
        [contract.contractId],
        startLedger,
        batchEndLedger
      );

      // ── Step 2: Determine the window end and its hash ─────────────────────
      const advanceTo =
        decodedEvents.length > 0
          ? Math.max(...decodedEvents.map((e) => e.ledgerSequence))
          : batchEndLedger > contract.lastLedger
          ? batchEndLedger
          : null;

      if (advanceTo !== null) {
        let latestHash: string | null = null;
        try {
          const ledgersRes = await server.getLedgers({
            startLedger: advanceTo,
            pagination: { limit: 1 },
          });
          if (ledgersRes.ledgers && ledgersRes.ledgers.length > 0) {
            latestHash = ledgersRes.ledgers[0].hash;
          }
        } catch (err) {
          logger.error('pollContract: failed to fetch ledger hash', {
            contractId: contract.contractId,
            ledger: advanceTo,
            err,
          });
        }

        // ── Step 3: Open checkpoint (fetched) ─────────────────────────────
        const checkpoint = await openCheckpoint(
          contract.contractId,
          startLedger,
          advanceTo,
        );

        // ── Step 4: Mark applying before opening the DB transaction ───────
        checkpoint = await markApplying(checkpoint);

        // ── Step 5: Commit domain writes + cursor in ONE transaction ───────
        let newEvents: any[] = [];
        try {
          newEvents = await prisma.$transaction(async (tx) => {
            const inserted = decodedEvents.length > 0
              ? await applyDecodedEvents(decodedEvents, tx)
              : [];

            // Atomically advance cursor + mark checkpoint committed.
            await commitCheckpoint(
              checkpoint,
              inserted.length,
              latestHash,
              contract.id,
              tx,
            );

            return inserted;
          });
          recordDbWrite();
          for (const ev of newEvents) emitSSEEvent(ev);
        } catch (txErr) {
          logger.error('pollContract: domain write transaction failed', {
            contractId: contract.contractId,
            err: txErr instanceof Error ? txErr.message : String(txErr),
          });
          throw txErr;
        }

        const syncData = buildSyncStateLedgerData(advanceTo, latestHash);
        await prisma.trackedContract.update({
          where: { id: contract.id },
          data: {
            lastLedger: syncData.lastLedger,
            ...(syncData.lastLedgerHash ? { lastLedgerHash: syncData.lastLedgerHash } : {}),
          },
        });
        recordDbWrite();

        latestLedgerProcessedGauge.set(advanceTo);
        syncLatencyGauge.set(Math.max(0, networkLatestLedger - advanceTo));
        recordProgress();
        recordLastSuccessfulPoll();

        // ── Per-contract registry progress update (Issue #441) ───────────
        try {
          contractRegistry.recordProgress(contract.contractId, advanceTo, latestHash);
          contractRegistry.updateLagMetrics(networkLatestLedger);
        } catch {
          // non-fatal — registry update must never crash the poller
        }

        // Issue #286: promote events that are now CONFIRMATION_DEPTH ledgers old.
        // Non-fatal — confirmation promotion failures must never crash the poller.
        promoteConfirmedEvents(networkLatestLedger, config.confirmationDepth).catch((err) => {
          logger.error('pollContract: failed to promote confirmed events', {
            contractId: contract.contractId,
            err: err instanceof Error ? err.message : String(err),
          });
        });
      } else {
        latestLedgerProcessedGauge.set(contract.lastLedger);
        syncLatencyGauge.set(Math.max(0, networkLatestLedger - contract.lastLedger));
      }

      localErrors = 0;
      recoveryFSM.toSync();
    } catch (error) {
      localErrors += 1;
      recordRpcFailure();
      recoveryFSM.toRetry(error instanceof Error ? error.message : String(error));

      // ── Per-contract error tracking (Issue #441) ──────────────────────────
      // Record the error on this contract's registry entry. If MAX_CONTRACT_ERRORS
      // is reached the registry auto-disables the contract and we exit cleanly.
      try {
        contractRegistry.recordError(
          contractRow.contractId,
          error instanceof Error ? error.message : String(error),
        );
        if (!contractRegistry.isActive(contractRow.contractId)) {
          logger.error('pollContract: contract auto-disabled after too many errors — stopping loop', {
            contractId: contractRow.contractId,
          });
          return;
        }
      } catch {
        // registry lookup failure is non-fatal
      }

      const backoff = Math.min(BASE_BACKOFF_MS * Math.pow(2, localErrors - 1), MAX_BACKOFF_MS);
      logger.error('pollContract: error in loop', {
        contractId: contractRow.contractId,
        localErrors,
        backoffMs: backoff,
        err: error instanceof Error ? error.message : String(error),
      });
      await new Promise((resolve) => setTimeout(resolve, backoff));
      continue;
    }

    localErrors = 0;
    await new Promise((resolve) => setTimeout(resolve, config.pollIntervalMs));
  }
}

export async function startPolling() {
  const config = loadConfig();

  resetPollerShutdownFlag();

  const activeContracts = await seedTrackedContracts();
  if (activeContracts.length === 0) {
    throw new Error('No active tracked contracts found. Set TRACKED_CONTRACTS or MARKETPLACE_CONTRACT_ID.');
  }

  logger.info('startPolling: launching per-contract pollers', {
    contracts: activeContracts.map((c) => ({
      contractId: c.contractId,
      label: c.label,
      type: c.type,
      lastLedger: c.lastLedger,
      health: contractRegistry.get(c.contractId)?.health ?? 'idle',
    })),
    pollIntervalMs: config.pollIntervalMs,
    maxLedgersPerCycle: config.maxLedgersPerCycle,
  });

  registerPollerLifecycle({
    stopPoller,
    startPoller: startPolling,
  });

  startWatchdog();

  // Issue #294: acquire a distributed lease before advancing the cursor.
  const lease = await acquireLease('poller');
  if (!lease) {
    logger.warn('startPolling: another worker holds the lease — exiting');
    return;
  }

  try {
    await Promise.all(
      activeContracts.map((contract) =>
        pollContract(
          {
            id: contract.id,
            contractId: contract.contractId,
            lastLedger: contract.lastLedger,
            lastLedgerHash: contract.lastLedgerHash,
          },
          config
        )
      )
    );
  } finally {
    releaseLease('poller');
  }

  if (shuttingDown) {
    await gracefulShutdown();
  }
}


async function fetchListingFromChain(listingId: bigint, contractId?: string): Promise<any | null> {
  const cid = contractId || process.env.MARKETPLACE_CONTRACT_ID || '';
  if (!cid) return null;
  return fetchRawListingFromChain(server, cid, listingId);
}

/**
 * Attempt to read already-fetched IPFS metadata for `cid` from the
 * IpfsMetadata cache and write the human-readable fields (title, description,
 * artistName) back onto the Listing row so the PostgreSQL tsvector trigger
 * keeps the search index current.
 *
 * This runs fire-and-forget in the background immediately after a
 * LISTING_CREATED event is processed.  If the IPFS metadata has not been
 * fetched yet (cold start / first-ever listing for this CID) the function
 * exits silently; the IpfsQueue worker will call this path again once the
 * metadata lands.
 *
 * The function is idempotent — re-running it when the fields are already
 * populated is a no-op thanks to the conditional check.
 */
export async function backfillListingMetadata(listingId: bigint, cid: string): Promise<void> {
  if (!cid) return;

  const metadata = await prisma.ipfsMetadata.findUnique({ where: { cid } });
  if (!metadata) return; // not yet cached — queue worker will handle it

  // Only update if at least one metadata field is present
  const title       = typeof metadata.title       === 'string' ? metadata.title       : null;
  const description = typeof metadata.description === 'string' ? metadata.description : null;
  // artistName is not stored directly in IpfsMetadata; it may appear under
  // common IPFS metadata keys like "artist", "creator", or "by".
  const raw = metadata.raw as Record<string, unknown> | null;
  const artistName: string | null =
    (typeof raw?.artist  === 'string' ? raw.artist  : null) ??
    (typeof raw?.creator === 'string' ? raw.creator : null) ??
    (typeof raw?.by      === 'string' ? raw.by      : null) ??
    null;

  if (!title && !description && !artistName) return; // nothing to update

  await prisma.listing.updateMany({
    where: {
      listingId,
      // Only overwrite nulls — avoid clobbering manually-set values.
      OR: [
        { title:       null },
        { description: null },
        { artistName:  null },
      ],
    },
    data: {
      ...(title       !== null ? { title }       : {}),
      ...(description !== null ? { description } : {}),
      ...(artistName  !== null ? { artistName }  : {}),
    },
  });

  logger.debug('[backfillListingMetadata] Updated listing search fields', {
    listingId: listingId.toString(),
    cid,
    title,
    artistName,
  });
}

async function fetchAuctionFromChain(auctionId: bigint, contractId?: string): Promise<any | null> {
  const cid = contractId || process.env.MARKETPLACE_CONTRACT_ID || '';
  if (!cid) return null;
  return fetchRawAuctionFromChain(server, cid, auctionId);
}

/** Resolves the globally unique identity of a decoded event (RPC id preferred). */
function resolveEventId(event: any): string {
  return event.eventId || event.eventHash || '';
}

export async function applyDecodedEvents(decodedEvents: any[], tx: any) {
  if (decodedEvents.length === 0) return [];

  // Use the idempotent batch writer — DB-level unique constraints ensure that
  // concurrent workers or replayed windows cannot create duplicate rows.
  // Any duplicate is counted as a benign replay (not an error).
  const { newEvents } = await upsertEvents(decodedEvents, tx);

  for (const event of newEvents) {
    await processEvent(event, tx, true);
  }

  return newEvents;
}

export async function processEvent(event: any, tx?: any, skipInsert = false) {
  const { eventType, listingId, actor, ledgerSequence, data, txHash } = event;
  const db = tx ?? prisma;

  // ── Measure per-event-type processing time ────────────────────────────────
  const eventStart = process.hrtime();
  const recordEventDuration = () => {
    const [s, ns] = process.hrtime(eventStart);
    eventProcessingDurationHistogram.labels(eventType).observe(s + ns / 1e9);
  };

  if (!skipInsert) {
    const { skipped } = await (upsertEvents as any)([event], db);
    if (skipped > 0) {
      // This is a duplicate — all domain state is already correct from the prior write.
      // Return early rather than double-applying business logic.
      return;
    }
  }

  // Handle deploy events (no listingId — collection deployments)
  if (eventType === 'DEPLOY_NORMAL_721' || eventType === 'DEPLOY_NORMAL_1155' ||
      eventType === 'DEPLOY_LAZY_721' || eventType === 'DEPLOY_LAZY_1155') {
    const kindMap: Record<string, string> = {
      DEPLOY_NORMAL_721:  'normal_721',
      DEPLOY_NORMAL_1155: 'normal_1155',
      DEPLOY_LAZY_721:    'lazy_721',
      DEPLOY_LAZY_1155:   'lazy_1155',
    };
    const rawData = Array.isArray(data) ? data : [];
    const creatorAddr  = rawData[0]?.toString() || actor;
    const contractAddr = rawData[1]?.toString() || '';
    if (contractAddr) {
      await db.collection.upsert({
        where: { contractAddress: contractAddr },
        create: {
          contractAddress: contractAddr,
          kind: kindMap[eventType],
          creator: creatorAddr,
          deployedAtLedger: ledgerSequence,
          // Issue #476: collections deployed via the updated launchpad have
          // passed the shared metadata validation rules.
          metadataStatus: 'valid',
        },
        update: {
          creator: creatorAddr,
          deployedAtLedger: ledgerSequence,
          metadataStatus: 'valid',
        },
      });
      // Invalidate collections cache
      invalidateCollection(contractAddr).catch(() => {});
    }
    recordEventDuration();
    return;
  }

  // Update Listing/Collection state based on event type.
  // Collection fee events carry no listingId — route them first, then guard
  // the rest of the switch which requires a listingId.
  if (eventType === 'COLLECTION_FEE_SET' || eventType === 'COLLECTION_FEE_CLEARED') {
    const collectionAddr: string = data.collection?.toString() ?? '';
    if (collectionAddr) {
      if (eventType === 'COLLECTION_FEE_SET') {
        const bps: number = Number(data.bps ?? 0);
        await db.collection.updateMany({
          where: { contractAddress: collectionAddr },
          data: { feeBpsOverride: bps },
        });
      } else {
        await db.collection.updateMany({
          where: { contractAddress: collectionAddr },
          data: { feeBpsOverride: null },
        });
      }
      invalidateCollection(contractAddr).catch(() => {});
    }
    recordEventDuration();
    if (!tx) emitSSEEvent(event);
    return;
  }

  if (!listingId) { recordEventDuration(); return; }

  switch (eventType) {
    case 'LISTING_CREATED': {
      let chainListing = await fetchListingFromChain(listingId, event.contractId);
      if (chainListing && !chainListing.artist) chainListing = null;

      const artist     = chainListing ? chainListing.artist.toString()      : data.artist;
      const price      = chainListing ? chainListing.price.toString()       : data.price;
      const currency   = chainListing ? chainListing.currency.toString()    : data.currency;
      const collection = chainListing ? chainListing.collection.toString()  : data.collection;
      const nftTokenId = chainListing ? BigInt(chainListing.token_id)       : BigInt(data.token_id);
      const token      = chainListing ? chainListing.token.toString()       : (data.token || '');
      const recipients = chainListing
        ? chainListing.recipients.map((r: any) => ({ address: r.address.toString(), percentage: Number(r.percentage) }))
        : [];

      // Resolve token metadata version for snapshotting — fire-and-forget
      // safe because the result is used for the new column only.
      let listingTokenMetadataVersion: number | null = null;
      if (token) {
        try {
          const { resolveTokenMetadata } = await import('./token-metadata.js');
          const meta = await resolveTokenMetadata(token);
          listingTokenMetadataVersion = meta.metadataVersion;
        } catch {
          // Non-fatal: version snapshot is best-effort
        }
      }

      // Ensure the row exists, then apply data only if this event is not
      // stale — a late-arriving LISTING_CREATED must not reset a listing
      // that has since been sold or cancelled back to Active.
      await db.listing.upsert({
        where: { listingId },
        create: {
          listingId, artist, owner: null, price, currency, collection,
          nftTokenId, token, status: 'Active' as const, recipients,
          createdAtLedger: ledgerSequence, updatedAtLedger: ledgerSequence,
          ...(listingTokenMetadataVersion !== null
            ? { tokenMetadataVersion: listingTokenMetadataVersion }
            : {}),
        },
        update: {
          artist, price, collection, nftTokenId,
          status: 'Active' as const, recipients, updatedAtLedger: ledgerSequence,
          ...(listingTokenMetadataVersion !== null
            ? { tokenMetadataVersion: listingTokenMetadataVersion }
            : {}),
        },
      });

      // ── Metrics & cache invalidation ──────────────────────────────────────
      listingsCreatedTotal.labels(collection ?? 'unknown').inc();
      invalidateListing(listingId.toString()).catch(() => {});

      // Refresh active-listing gauge asynchronously (best-effort)
      prisma.listing.count({ where: { status: 'Active' } })
        .then((n) => activeListingsGauge.set(n)).catch(() => {});

      if (token) {
        // Enqueue background IPFS fetch for the artwork CID.  When it
        // completes, backfillListingMetadata() will update the Listing row's
        // title/description/artistName columns so that full-text search picks
        // them up on the next tsvector trigger update.
        enqueueIpfsFetch(token).catch((err) => {
          logger.warn('[processEvent] Failed to enqueue IPFS fetch', {
            eventType, listingId: listingId?.toString(), ledger: ledgerSequence, token, err: err instanceof Error ? err.message : String(err),
          });
        });

        // Attempt an immediate metadata population in the background so that
        // listings indexed on a cold start get their search fields filled
        // without waiting for the queue worker's next cycle.
        backfillListingMetadata(listingId, token).catch((err) => {
          logger.debug('[processEvent] Background metadata backfill failed (will retry via queue)', {
            listingId: listingId?.toString(), token, err: err instanceof Error ? err.message : String(err),
          });
        });
      }
      break;
    }

    case 'LISTING_UPDATED': {
      const { count } = await db.listing.updateMany({
        where: { listingId, updatedAtLedger: { lte: ledgerSequence } },
        data: {
          price: data.new_price,
          collection: data.collection,
          nftTokenId: BigInt(data.token_id || 0),
          updatedAtLedger: ledgerSequence,
        },
      });
      if (count === 0) logger.warn('LISTING_UPDATED: listing not found', { listingId: listingId?.toString(), ledger: ledgerSequence });
      invalidateListing(listingId.toString()).catch(() => {});

      if (count === 0) logger.warn('LISTING_UPDATED: listing not found', { eventType, listingId: listingId?.toString(), ledger: ledgerSequence });
      invalidatePattern('cache:*/listings*').catch(() => {});
      invalidateKey(`cache:/listings/${listingId.toString()}`).catch(() => {});
      break;
    }

    case 'LISTING_PRICE_UPDATED': {
      // Persist a price-history row so the indexer maintains a full audit trail
      // of every price change for this listing (Issue #213).
      await db.priceHistory.create({
        data: {
          listingId,
          oldPrice: String(data.old_price ?? '0'),
          newPrice: String(data.new_price ?? '0'),
          changedBy: String(data.updated_by ?? actor ?? ''),
          changedAtLedger: ledgerSequence,
        },
      });
      // Also keep the live Listing.price in sync when changed via update_listing_price
      await db.listing.updateMany({
        where: { listingId },
        data: { price: String(data.new_price ?? '0'), updatedAtLedger: ledgerSequence },
      });
      invalidateListing(listingId.toString()).catch(() => {});


      break;
    }

    case 'LISTING_EXPIRED': {
      invalidateListing(listingId.toString()).catch(() => {});

      break;
    }

    case 'ARTWORK_SOLD': {
      const { count } = await db.listing.updateMany({
        where: { listingId },
        data: { status: 'Sold' as const, owner: data.buyer, updatedAtLedger: ledgerSequence },
      });
      if (count === 0) logger.error('ARTWORK_SOLD: listing not found — sale not recorded', { eventType, listingId: listingId?.toString(), ledger: ledgerSequence });

      // ── Metrics & cache invalidation ──────────────────────────────────────
      salesTotalCounter.labels(data.token ?? 'unknown').inc();
      invalidateListing(listingId.toString()).catch(() => {});

      prisma.listing.count({ where: { status: 'Active' } })
        .then((n) => activeListingsGauge.set(n)).catch(() => {});
      break;
    }

    case 'ROYALTY_PAID': {
      // Royalty audit trail (Issue #201): one RoyaltyPayment row per
      // breakdown entry, written in a single atomic createMany. Field values
      // arrive as strings via convertBigInts.
      const isListing = data.listing_id !== undefined && data.listing_id !== null;
      const isAuction = data.auction_id !== undefined && data.auction_id !== null;
      const entries: Array<{ address: string; amount: string }> = Array.isArray(data.recipients)
        ? data.recipients
        : [];
      if (entries.length > 0) {
        await db.royaltyPayment.createMany({
          data: entries.map((r) => ({
            listingId: isListing ? BigInt(data.listing_id) : null,
            auctionId: isAuction ? BigInt(data.auction_id) : null,
            recipient: String(r.address),
            amount: String(r.amount),
            salePrice: String(data.sale_price),
            ledgerSequence,
          })),
        });
      }
      for (const r of entries) {
        invalidateWalletActivity(r.address).catch(() => {});
      }
      break;
    }

    case 'LISTING_CANCELLED': {
      const { count } = await db.listing.updateMany({
        where: { listingId },
        data: { status: 'Cancelled' as const, updatedAtLedger: ledgerSequence },
      });
      if (count === 0) logger.warn('LISTING_CANCELLED: listing not found', { listingId: listingId?.toString(), ledger: ledgerSequence });
      invalidateListing(listingId.toString()).catch(() => {});

      if (count === 0) logger.warn('LISTING_CANCELLED: listing not found', { eventType, listingId: listingId?.toString(), ledger: ledgerSequence });
      invalidatePattern('cache:*/listings*').catch(() => {});
      invalidateKey(`cache:/listings/${listingId.toString()}`).catch(() => {});
      prisma.listing.count({ where: { status: 'Active' } })
        .then((n) => activeListingsGauge.set(n)).catch(() => {});
      break;
    }

    // ── Issue #214: cascade Cancelled on all artist listings + auctions ──────
    case 'ARTIST_REVOKED': {
      const revokedArtist = String(data.artist ?? actor ?? '');
      if (revokedArtist) {
        const [listingResult, auctionResult] = await Promise.all([
          db.listing.updateMany({
            where: { artist: revokedArtist, status: 'Active' },
            data: { status: 'Cancelled' as const, updatedAtLedger: ledgerSequence },
          }),
          db.auction.updateMany({
            where: { creator: revokedArtist, status: 'Active' },
            data: { status: 'Cancelled' as const, updatedAtLedger: ledgerSequence },
          }),
        ]);
        logger.info('ARTIST_REVOKED: cascaded cancellations', {
          artist: revokedArtist,
          listingsCancelled: listingResult.count,
          auctionsCancelled: auctionResult.count,
          ledger: ledgerSequence,
        });
        invalidateListing(listingId.toString()).catch(() => {});
        invalidateAuction(listingId.toString()).catch(() => {});
        prisma.listing.count({ where: { status: 'Active' } })
          .then((n) => activeListingsGauge.set(n)).catch(() => {});
        prisma.auction.count({ where: { status: 'Active' } })
          .then((n) => activeAuctionsGauge.set(n)).catch(() => {});
      }
      break;
    }

    case 'AUCTION_CREATED': {
      let chainAuction = await fetchAuctionFromChain(listingId, event.contractId);
      if (chainAuction && !chainAuction.creator) chainAuction = null;

      const creator      = chainAuction ? chainAuction.creator.toString()         : data.creator;
      const reservePrice = chainAuction ? chainAuction.reserve_price.toString()   : (data.reserve_price || '0');
      const token        = chainAuction ? chainAuction.token.toString()           : (data.token || '');
      const endTime      = chainAuction ? BigInt(chainAuction.end_time)           : BigInt(data.end_time || 0);
      const collection   = chainAuction ? chainAuction.collection.toString()      : data.collection;
      const nftTokenId   = chainAuction ? BigInt(chainAuction.token_id)           : BigInt(data.token_id || 0);
      const recipients   = chainAuction
        ? chainAuction.recipients.map((r: any) => ({ address: r.address.toString(), percentage: Number(r.percentage) }))
        : [];

      const maxExtensions = chainAuction?.max_extensions != null
        ? Number(chainAuction.max_extensions)
        : 0;

      await db.auction.upsert({
        where: { auctionId: listingId },
        create: {
          auctionId: listingId, creator, collection, nftTokenId, token, reservePrice,
          highestBid: '0', highestBidder: null, endTime, status: 'Active' as const,
          recipients, createdAtLedger: ledgerSequence, updatedAtLedger: ledgerSequence,
          extensionCount: 0, originalEndTime: endTime, maxExtensions,
        },
        update: {
          creator, collection, nftTokenId, token, reservePrice, endTime,
          status: 'Active' as const, recipients, updatedAtLedger: ledgerSequence,
          maxExtensions,
        },
      });

      invalidateAuction(listingId.toString()).catch(() => {});
      prisma.auction.count({ where: { status: 'Active' } })
        .then((n) => activeAuctionsGauge.set(n)).catch(() => {});
      break;
    }

    case 'BID_PLACED': {
      // Bid history — one row per (auction, ledger, bidder); replays hit the
      // unique constraint and become a no-op update.
      await db.bid.upsert({
        where: {
          auctionId_ledgerSequence_bidder: {
            auctionId: listingId,
            ledgerSequence,
            bidder: data.bidder,
          },
        },
        create: {
          auctionId: listingId,
          bidder: data.bidder,
          amount: data.bid_amount,
          ledgerSequence,
        },
        update: { amount: data.bid_amount },
      });

      // Monotonic guard: bids strictly increase on-chain, so an out-of-order
      // or replayed BID_PLACED can never lower the recorded highest bid.
      const { count } = await db.auction.updateMany({
        where: { auctionId: listingId },
        data: { highestBid: data.bid_amount, highestBidder: data.bidder, updatedAtLedger: ledgerSequence },
      });
      if (count === 0) logger.warn('BID_PLACED: auction not found', { auctionId: listingId?.toString(), ledger: ledgerSequence });
      invalidateAuction(listingId.toString()).catch(() => {});

      if (count === 0) logger.warn('BID_PLACED: auction not found', { eventType, auctionId: listingId?.toString(), ledger: ledgerSequence });
      invalidatePattern('cache:*/auctions*').catch(() => {});
      invalidateKey(`cache:/auctions/${listingId.toString()}`).catch(() => {});
      break;
    }

    case 'AUCTION_RESOLVED': {
      const winner: string | null = data.winner || null;
      const { count } = await db.auction.updateMany({
        where: { auctionId: listingId, updatedAtLedger: { lte: ledgerSequence } },
        data: {
          status: 'Finalized' as const,
          highestBid: data.amount,
          highestBidder: winner,
          updatedAtLedger: ledgerSequence,
        },
      });
      if (count === 0) logger.error('AUCTION_RESOLVED: auction not found — resolution not recorded', { eventType, auctionId: listingId?.toString(), ledger: ledgerSequence });

      // Mark all losing bids as Refundable (Issue #466). The winner's bid stays None
      // because their funds went to the seller; only genuinely losing bids need recovery.
      if (listingId != null) {
        await db.bid.updateMany({
          where: {
            auctionId: listingId,
            ...(winner ? { bidder: { not: winner } } : {}),
            refundStatus: 'None',
          },
          data: { refundStatus: 'Refundable' },
        });
      }

      auctionFinalizationsTotal.inc();
      invalidateAuction(listingId.toString()).catch(() => {});

      prisma.auction.count({ where: { status: 'Active' } })
        .then((n) => activeAuctionsGauge.set(n)).catch(() => {});
      break;
    }

    case 'AUCTION_CANCELLED':
    case 'AUCTION_EXTENDED': {
      if (eventType === 'AUCTION_CANCELLED') {
        const { count } = await db.auction.updateMany({
          where: { auctionId: listingId },
          data: { status: 'Cancelled' as const, updatedAtLedger: ledgerSequence },
        });
        if (count === 0) logger.warn('AUCTION_CANCELLED: auction not found', { eventType, auctionId: listingId?.toString(), ledger: ledgerSequence });
        prisma.auction.count({ where: { status: 'Active' } })
          .then((n) => activeAuctionsGauge.set(n)).catch(() => {});
      } else if (eventType === 'AUCTION_EXTENDED') {
        // Update auction's endTime and extensionCount from the extension event
        const { count } = await db.auction.updateMany({
          where: { auctionId: listingId },
          data: {
            endTime: BigInt(data.new_end_time),
            extensionCount: Number(data.extension_count),
            updatedAtLedger: ledgerSequence,
          },
        });
        if (count === 0) logger.warn('AUCTION_EXTENDED: auction not found', { eventType, auctionId: listingId?.toString(), ledger: ledgerSequence });
      }
      invalidateAuction(listingId.toString()).catch(() => {});

      break;
    }

    case 'AUCTION_BID_REFUNDED': {
      // Mark the refunded bid as Claimed so the UI shows the correct state (Issue #466).
      // The event carries bidder + auction_id; we update the most-recent Bid row for
      // this (auctionId, bidder) pair (the one with the highest ledgerSequence).
      if (listingId != null) {
        const latestBid = await db.bid.findFirst({
          where: { auctionId: listingId, bidder: data.bidder },
          orderBy: { ledgerSequence: 'desc' },
        });
        if (latestBid) {
          await db.bid.update({
            where: { id: latestBid.id },
            data: { refundStatus: 'Claimed' },
          });
        }
      }
      break;
    }

    case 'AUCTION_RESERVE_UPDATED': {
      // Update the stored reserve price when the creator makes a pre-bid change (Issue #467).
      if (listingId != null) {
        const { count } = await db.auction.updateMany({
          where: { auctionId: listingId },
          data: {
            reservePrice: data.new_reserve_price,
            updatedAtLedger: ledgerSequence,
          },
        });
        if (count === 0) logger.warn('AUCTION_RESERVE_UPDATED: auction not found', { eventType, auctionId: listingId?.toString(), ledger: ledgerSequence });
      }
      invalidateAuction(listingId?.toString() ?? '').catch(() => {});
      break;
    }

    case 'TOKEN_WHITELISTED': {
      // Upsert the whitelist row.  On a re-add (previously removed token), reset
      // active + housekeeping fields but preserve metadataVersion so the version
      // history is not reset if the token simply got re-listed.
      const existing = await db.whitelistedToken.findUnique({
        where: { address: data.token },
        select: { metadataVersion: true },
      }).catch(() => null);

      const decimalsFromEvent: number | undefined =
        typeof data.decimals === 'number' ? data.decimals : undefined;
      const symbolFromEvent: string | undefined =
        typeof data.symbol === 'string' ? data.symbol : undefined;
      const nameFromEvent: string | undefined =
        typeof data.name === 'string' ? data.name : undefined;

      // A metadata change (new decimal value or first-time addition) bumps the version.
      const hasNewMetadata =
        !existing ||
        decimalsFromEvent !== undefined ||
        symbolFromEvent !== undefined;
      const nextVersion = hasNewMetadata
        ? (existing?.metadataVersion ?? 0) + 1
        : (existing?.metadataVersion ?? 1);

      await db.whitelistedToken.upsert({
        where: { address: data.token },
        create: {
          address: data.token,
          active: true,
          addedAtLedger: ledgerSequence,
          addedBy: data.added_by,
          metadataVersion: nextVersion,
          decimals: decimalsFromEvent ?? null,
          symbol: symbolFromEvent ?? null,
          name: nameFromEvent ?? null,
          sourceLedger: ledgerSequence,
        },
        update: {
          active: true,
          addedAtLedger: ledgerSequence,
          addedBy: data.added_by,
          removedAtLedger: null,
          removedBy: null,
          ...(hasNewMetadata ? {
            metadataVersion: nextVersion,
            decimals: decimalsFromEvent ?? undefined,
            symbol: symbolFromEvent ?? undefined,
            name: nameFromEvent ?? undefined,
            sourceLedger: ledgerSequence,
          } : {}),
        },
      });

      // Record a history row for every version bump so the audit trail is complete.
      if (hasNewMetadata) {
        try {
          await (db as any).tokenMetadataHistory.create({
            data: {
              address: data.token,
              version: nextVersion,
              decimals: decimalsFromEvent ?? null,
              symbol: symbolFromEvent ?? null,
              name: nameFromEvent ?? null,
              sourceLedger: ledgerSequence,
              active: true,
            },
          });
        } catch {
          // Duplicate version on replay — safe to ignore (unique constraint).
        }

        // Evict in-process metadata cache and invalidate Redis keys for all rows
        // that referenced the old decimal precision.
        try {
          const { applyTokenMetadataVersionChange } = await import('./token-metadata.js');
          // Fire-and-forget: cache invalidation must not block the event transaction.
          applyTokenMetadataVersionChange(
            data.token,
            decimalsFromEvent ?? null,
            ledgerSequence,
            symbolFromEvent,
            nameFromEvent,
          ).catch((err: unknown) => {
            logger.warn('[processEvent] Token metadata cache invalidation failed', {
              token: data.token, err: err instanceof Error ? err.message : String(err),
            });
          });
        } catch {
          // token-metadata module unavailable in test env — non-fatal.
        }
      }

      invalidateConfig().catch(() => {});
      break;
    }

    case 'TOKEN_REMOVED': {
      const { count } = await db.whitelistedToken.updateMany({
        where: { address: data.token },
        data: {
          active: false,
          removedAtLedger: ledgerSequence,
          removedBy: data.removed_by,
        },
      });
      if (count === 0) logger.warn('TOKEN_REMOVED: token not found in whitelist', { eventType, token: data.token, ledger: ledgerSequence });
      break;
    }

    case 'OFFER_MADE': {
      const offerExpiresAt = data.expires_at != null ? BigInt(data.expires_at) : null;
      // #528: escrowTxHash records the tx that moved funds into escrow, so
      // the offers UI can link directly to it. Falls back to null for events
      // replayed without a tx hash (e.g. some test fixtures).
      const escrowTxHash = txHash || null;
      const offerListingId = BigInt(data.listing_id);

      // Check whether the parent listing exists; if not, stage in PendingOffer
      // so out-of-order ingestion (OFFER_MADE before LISTING_CREATED) is handled
      // without breaking idempotency or blocking reorg rollback.
      const listingExists = await db.listing.findUnique({
        where: { listingId: offerListingId },
        select: { listingId: true },
      }).catch(() => null);

      if (!listingExists) {
        // Stage in PendingOffer — will be promoted by the DB trigger when
        // LISTING_CREATED for this listingId is ingested.
        await (db as any).pendingOffer.upsert({
          where: { offerId: BigInt(data.offer_id) },
          create: {
            offerId: BigInt(data.offer_id),
            listingId: offerListingId,
            offerer: String(data.offerer ?? ''),
            amount: String(data.amount ?? '0'),
            token: String(data.token ?? ''),
            expiresAt: offerExpiresAt,
            escrowTxHash,
            createdAtLedger: ledgerSequence,
            updatedAtLedger: ledgerSequence,
            rawEventData: data,
          },
          update: {},
        });
        logger.info('[processEvent] Staged OFFER_MADE in PendingOffer (listing not yet indexed)', {
          offerId: data.offer_id,
          listingId: data.listing_id,
          ledger: ledgerSequence,
        });
        offersMadeTotal.inc();
        break;
      }

      // Resolve token metadata version for snapshotting
      let offerTokenMetadataVersion: number | null = null;
      if (data.token) {
        try {
          const { resolveTokenMetadata } = await import('./token-metadata.js');
          const meta = await resolveTokenMetadata(String(data.token));
          offerTokenMetadataVersion = meta.metadataVersion;
        } catch {
          // Non-fatal
        }
      }

      await db.offer.upsert({
        where: { offerId: BigInt(data.offer_id) },
        create: {
          offerId: BigInt(data.offer_id),
          listingId: offerListingId,
          offerer: data.offerer,
          amount: data.amount,
          token: data.token,
          status: 'Pending' as const,
          expiresAt: offerExpiresAt,
          escrowTxHash,
          createdAtLedger: ledgerSequence,
          updatedAtLedger: ledgerSequence,
          ...(offerTokenMetadataVersion !== null
            ? { tokenMetadataVersion: offerTokenMetadataVersion }
            : {}),
        },
        update: {},
      });
      // Guarded so a stale OFFER_MADE cannot reset a terminal offer state.
      await db.offer.updateMany({
        where: { offerId: BigInt(data.offer_id), updatedAtLedger: { lte: ledgerSequence } },
        data: {
          listingId: offerListingId,
          offerer: data.offerer,
          amount: data.amount,
          token: data.token,
          status: 'Pending' as const,
          expiresAt: offerExpiresAt,
          escrowTxHash,
          updatedAtLedger: ledgerSequence,
          ...(offerTokenMetadataVersion !== null
            ? { tokenMetadataVersion: offerTokenMetadataVersion }
            : {}),
        },
      });
      offersMadeTotal.inc();
      invalidateOffer(data.listing_id?.toString()).catch(() => {});
      break;
    }

    case 'OFFER_ACCEPTED': {
      await db.offer.update({
        where: { offerId: BigInt(data.offer_id) },
        data: { status: 'Accepted' as const, refundTxHash: txHash || null, updatedAtLedger: ledgerSequence },
      });
      const { count: listingCount } = await db.listing.updateMany({
        where: { listingId: BigInt(data.listing_id) },
        data: { status: 'Sold' as const, owner: data.offerer, updatedAtLedger: ledgerSequence },
      });
      if (listingCount === 0) logger.error('OFFER_ACCEPTED: listing not found', { eventType, listingId: data.listing_id?.toString(), offerId: data.offer_id?.toString(), ledger: ledgerSequence });

      offersAcceptedTotal.inc();
      salesTotalCounter.labels(data.token ?? 'unknown').inc();
      invalidateOffer(data.listing_id?.toString()).catch(() => {});
      invalidateListing(listingId.toString()).catch(() => {});

      prisma.listing.count({ where: { status: 'Active' } })
        .then((n) => activeListingsGauge.set(n)).catch(() => {});
      break;
    }

    case 'OFFER_REJECTED': {
      await db.offer.update({
        where: { offerId: BigInt(data.offer_id) },
        data: { status: 'Rejected' as const, refundTxHash: txHash || null, updatedAtLedger: ledgerSequence },
      });
      invalidateOffer(data.listing_id?.toString()).catch(() => {});
      break;
    }

    case 'OFFER_WITHDRAWN': {
      await db.offer.update({
        where: { offerId: BigInt(data.offer_id) },
        data: { status: 'Withdrawn' as const, refundTxHash: txHash || null, updatedAtLedger: ledgerSequence },
      });
      invalidateOffer(data.listing_id?.toString()).catch(() => {});
      break;
    }

    // An expired offer reclaimed by (or on behalf of) its offerer: the contract
    // refunds the escrow and moves the offer to its Reclaimed terminal state.
    // Uses a ledger-guarded updateMany so stale replays cannot reset a terminal offer.
    case 'OFFER_RECLAIMED': {
      const { count } = await db.offer.updateMany({
        where: { offerId: BigInt(data.offer_id), updatedAtLedger: { lte: ledgerSequence } },
        data: {
          status: 'Reclaimed' as const,
          refundTxHash: txHash || null,
          updatedAtLedger: ledgerSequence,
        },
      });
      if (count === 0) logger.warn('OFFER_RECLAIMED: offer not found or already terminal', {
        eventType, offerId: data.offer_id?.toString(), ledger: ledgerSequence,
      });
      invalidateOffer(data.listing_id?.toString()).catch(() => {});
      break;
    }

    // Auction configuration changes — invalidate config cache
    case 'AUCTION_CONFIG_UPDATED': {
      invalidateConfig().catch(() => {});
      break;
    }

    // ROYALTY_PAID, ADMIN_TRANSFER_PROPOSED, ADMIN_TRANSFERRED,
    // ARTIST_REVOKED, ARTIST_REINSTATED, CONTRACT_PAUSED, CONTRACT_UNPAUSED:
    // persisted to MarketplaceEvent (with actor) above; no state reduction.

    // Issue #456: listing ownership reconciliation — sync the owner field in DB.
    case 'LISTING_OWNERSHIP_RECONCILED': {
      const newOwner: string | null = data.new_owner?.toString() ?? null;
      if (newOwner && listingId) {
        await db.listing.updateMany({
          where: { listingId },
          data: { owner: newOwner, updatedAtLedger: ledgerSequence },
        });
      }
      invalidateListing(listingId?.toString() ?? '').catch(() => {});
      break;
    }
  }

  // ── Update sync lag gauge ─────────────────────────────────────────────────
  // (best-effort, non-blocking)
  syncLagLedgersGauge.set(Math.max(0, (networkLatestLedgerGauge as any)._getValue?.() ?? 0));

  recordEventDuration();

  // Broadcast to any connected SSE clients after the DB write is complete.
  if (!tx) emitSSEEvent(event);
}
