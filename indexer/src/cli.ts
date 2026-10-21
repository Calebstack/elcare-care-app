/**
 * cli.ts
 *
 * Operator CLI for backfill and gap management.
 *
 * Usage:
 *   npm run cli -- backfill list
 *   npm run cli -- backfill resume <jobId>
 *   npm run cli -- backfill cancel <jobId>
 *   npm run cli -- gaps list
 *   npm run cli -- gaps repair [--gap=<id>]
 *   npm run cli -- gaps open          # alias for gaps list --status=Open
 *
 * ── Backfill range semantics (Issue #925) ─────────────────────────────────────
 *
 * FLAGS
 *   --start=<ledger>   First ledger to ingest (INCLUSIVE).
 *   --end=<ledger>     Last  ledger to ingest (INCLUSIVE, defaults to chain tip).
 *
 * CHECKPOINT INTERACTION
 *   The backfill command NEVER overwrites the live sync cursor stored in
 *   SyncState.  The cursor-interaction rule is:
 *
 *     • "below"       — backfill range is entirely below the live cursor.
 *                       SyncState is left untouched.
 *     • "overlapping" — backfill range straddles the live cursor.
 *                       SyncState is left untouched.
 *     • "ahead"       — backfill range is entirely beyond the live cursor.
 *                       SyncState IS advanced to the highest processed ledger.
 *
 *   On crash / resume, the backfill resumes from
 *   `checkpointLedger + 1`, NOT from `--start`.  The `checkpointLedger`
 *   column in the BackfillJob row is updated atomically per batch inside the
 *   same DB transaction that writes the event rows.  A crash between batches
 *   therefore leaves the checkpoint at the last *fully committed* batch, so
 *   replay is safe and idempotent (duplicate event writes are rejected by the
 *   `eventHash` unique constraint).
 *
 * EXAMPLE — ingest ledgers 1 000 000 to 1 500 000
 *   npm run cli -- backfill --start=1000000 --end=1500000
 *
 *   If the process crashes at ledger 1 200 000 the next run with the same
 *   flags will fast-forward to 1 200 001 automatically.  Alternatively,
 *   resume by job ID (avoids re-validating the range against the chain tip):
 *
 *   npm run cli -- backfill resume 42
 *
 * EXAMPLE — ingest all history not yet covered (start from beginning)
 *   npm run cli -- backfill --start=0   # end defaults to current chain tip
 */

import dotenv from 'dotenv';
import prisma from './db.js';
import {
  listBackfillJobs,
  getBackfillJob,
  cancelBackfillJob,
  runBackfill,
} from './backfill.js';
import {
  runRepairCycle,
  repairGap,
} from './gap-repair.js';
import {
  runEventVerifier,
  serializeVerifierResult,
} from './event-verifier.js';

dotenv.config();

// ── Formatting helpers ────────────────────────────────────────────────────────

function fmt(obj: unknown): string {
  return JSON.stringify(obj, (_k, v) => (typeof v === 'bigint' ? v.toString() : v), 2);
}

function pad(s: string | number, n: number): string {
  return String(s).padEnd(n);
}

function printJobTable(jobs: Awaited<ReturnType<typeof listBackfillJobs>>): void {
  if (jobs.length === 0) {
    console.log('No backfill jobs found.');
    return;
  }
  console.log(
    `${pad('ID', 6)}${pad('STATUS', 12)}${pad('START', 12)}${pad('END', 12)}` +
    `${pad('CHECKPOINT', 12)}${pad('INSERTED', 10)}${pad('GAP_ID', 8)}CREATED`,
  );
  console.log('─'.repeat(90));
  for (const j of jobs) {
    console.log(
      `${pad(j.id, 6)}${pad(j.status, 12)}${pad(j.startLedger, 12)}${pad(j.endLedger, 12)}` +
      `${pad(j.checkpointLedger, 12)}${pad(j.totalInserted, 10)}${pad(j.gapId ?? '-', 8)}` +
      `${j.createdAt.toISOString()}`,
    );
    if (j.error) console.log(`  error: ${j.error}`);
  }
}

function printGapTable(gaps: Awaited<ReturnType<typeof listGaps>>): void {
  if (gaps.length === 0) {
    console.log('No gaps found.');
    return;
  }
  console.log(
    `${pad('ID', 6)}${pad('STATUS', 12)}${pad('SOURCE', 20)}` +
    `${pad('FROM', 12)}${pad('TO', 12)}${pad('LEDGERS', 10)}CREATED`,
  );
  console.log('─'.repeat(90));
  for (const g of gaps) {
    const count = g.toLedger - g.fromLedger + 1;
    console.log(
      `${pad(g.id, 6)}${pad(g.status, 12)}${pad(g.source, 20)}` +
      `${pad(g.fromLedger, 12)}${pad(g.toLedger, 12)}${pad(count, 10)}` +
      `${g.createdAt.toISOString()}`,
    );
    if (g.error) console.log(`  error: ${g.error}`);
  }
}

// ── DB helpers ────────────────────────────────────────────────────────────────

async function listGaps(status?: string) {
  const where: any = {};
  if (status) where.status = status;
  return prisma.ledgerGap.findMany({
    where,
    orderBy: { createdAt: 'asc' },
  });
}

// ── Command handlers ──────────────────────────────────────────────────────────

async function handleBackfill(args: string[]): Promise<void> {
  const sub = args[0];

  switch (sub) {
    case 'list': {
      const jobs = await listBackfillJobs();
      printJobTable(jobs);
      break;
    }

    case 'resume': {
      const rawId = args[1];
      if (!rawId || !/^\d+$/.test(rawId)) {
        console.error('Usage: backfill resume <jobId>');
        process.exit(1);
      }
      const jobId = parseInt(rawId, 10);
      console.log(`Resuming BackfillJob #${jobId}...`);
      const result = await runBackfill({ resumeJobId: jobId });
      console.log(`Done. Status=${result.status}, inserted=${result.totalInserted}`);
      break;
    }

    case 'cancel': {
      const rawId = args[1];
      if (!rawId || !/^\d+$/.test(rawId)) {
        console.error('Usage: backfill cancel <jobId>');
        process.exit(1);
      }
      const jobId = parseInt(rawId, 10);
      const job = await getBackfillJob(jobId);
      if (!job) {
        console.error(`BackfillJob #${jobId} not found`);
        process.exit(1);
      }
      if (job.status === 'Completed' || job.status === 'Cancelled') {
        console.error(`BackfillJob #${jobId} is already ${job.status}`);
        process.exit(1);
      }
      await cancelBackfillJob(jobId);
      console.log(`BackfillJob #${jobId} marked Cancelled.`);
      break;
    }

    default:
      console.error(`Unknown backfill subcommand: "${sub}"`);
      console.error('Available: list | resume <id> | cancel <id>');
      process.exit(1);
  }
}

async function handleGaps(args: string[]): Promise<void> {
  const sub = args[0];

  switch (sub) {
    case 'list':
    case 'open': {
      const status = sub === 'open' ? 'Open' : undefined;
      const gaps = await listGaps(status);
      printGapTable(gaps);
      break;
    }

    case 'repair': {
      // Optional --gap=<id> to repair a specific gap; otherwise run a full cycle
      const gapFlag = args.find((a) => a.startsWith('--gap='))?.split('=')[1];
      if (gapFlag) {
        const gapId = parseInt(gapFlag, 10);
        if (isNaN(gapId)) {
          console.error('--gap must be a valid integer gap ID');
          process.exit(1);
        }
        console.log(`Repairing gap #${gapId}...`);
        const result = await repairGap(gapId);
        console.log(fmt(result));
        process.exit(result.status === 'Repaired' ? 0 : 1);
      } else {
        console.log('Running full gap-repair cycle...');
        const results = await runRepairCycle();
        const repaired = results.filter((r) => r.status === 'Repaired').length;
        const failed   = results.filter((r) => r.status === 'Failed').length;
        console.log(`Cycle complete: ${repaired} repaired, ${failed} failed`);
        if (results.length > 0) console.log(fmt(results));
        process.exit(failed > 0 ? 1 : 0);
      }
    }

    default:
      console.error(`Unknown gaps subcommand: "${sub}"`);
      console.error('Available: list | open | repair [--gap=<id>]');
      process.exit(1);
  }
}

// ── Verifier command ──────────────────────────────────────────────────────────

async function handleVerify(args: string[]): Promise<void> {
  // Parse flags: --from=N --to=N [--contract=C,C2] [--cursor=N] [--window=N]
  function getFlag(name: string): string | undefined {
    return args.find((a) => a.startsWith(`--${name}=`))?.split('=').slice(1).join('=');
  }

  const fromStr = getFlag('from');
  const toStr   = getFlag('to');

  if (!fromStr || !toStr) {
    console.error('Usage: verify --from=<ledger> --to=<ledger> [--contract=C1,C2] [--cursor=N] [--window=N]');
    process.exit(1);
  }

  const fromLedger = parseInt(fromStr, 10);
  const toLedger   = parseInt(toStr, 10);

  if (isNaN(fromLedger) || isNaN(toLedger) || fromLedger > toLedger) {
    console.error('--from and --to must be valid integers with from <= to');
    process.exit(1);
  }

  const contractFlag = getFlag('contract');
  const contractIds  = contractFlag ? contractFlag.split(',').filter(Boolean) : undefined;

  const windowStr  = getFlag('window');
  const cursorStr  = getFlag('cursor');
  const windowSize = windowStr ? parseInt(windowStr, 10) : undefined;
  const cursorLedger = cursorStr ? parseInt(cursorStr, 10) : undefined;

  console.log(`Running event verifier: ledgers ${fromLedger}–${toLedger}...`);

  const result = await runEventVerifier({
    fromLedger,
    toLedger,
    contractIds,
    windowSize,
    cursorLedger,
  });

  const json = serializeVerifierResult(result);
  console.log(JSON.stringify(json, null, 2));

  const hasIssues =
    result.duplicates.length > 0 ||
    result.omissions.length > 0 ||
    result.orphans.length > 0 ||
    result.discontinuities.length > 0;

  console.log('');
  console.log(`Verification ${result.complete ? 'complete' : 'partial (use --cursor=' + result.cursor + ' to resume)'}`);
  console.log(`  Scanned ledgers : ${result.scannedLedgers}`);
  console.log(`  RPC events      : ${result.rpcEventCount}`);
  console.log(`  DB events       : ${result.dbEventCount}`);
  console.log(`  Duplicates      : ${result.duplicates.length}`);
  console.log(`  Omissions       : ${result.omissions.length}`);
  console.log(`  Orphans         : ${result.orphans.length}`);
  console.log(`  Discontinuities : ${result.discontinuities.length}`);

  process.exit(hasIssues ? 1 : 0);
}

// ── Entrypoint ────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const [, , domain, ...rest] = process.argv;

  switch (domain) {
    case 'backfill':
      await handleBackfill(rest);
      break;
    case 'gaps':
      await handleGaps(rest);
      break;
    case 'verify':
      await handleVerify(rest);
      break;
    default:
      console.error(`Unknown command domain: "${domain}"`);
      console.error('Usage: cli <backfill|gaps|verify> <subcommand> [args]');
      process.exit(1);
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('CLI fatal error:', err instanceof Error ? err.message : String(err));
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
