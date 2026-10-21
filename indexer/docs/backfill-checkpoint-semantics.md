# Backfill Checkpoint Semantics

> Issue #925 — Clarify Which Checkpoint Backfill Resumes From

---

## Overview

The backfill command ingests historical Stellar contract events into the
database.  This document explains the `--start` / `--end` boundary semantics,
how the per-batch checkpoint interacts with the live sync cursor, and what
happens on crash / resume.

---

## CLI Flags

| Flag | Type | Default | Description |
|---|---|---|---|
| `--start=<n>` | integer | **required** | First ledger to ingest, **inclusive**. |
| `--end=<n>` | integer | current chain tip | Last ledger to ingest, **inclusive**. |
| `--resume=<jobId>` | integer | — | Resume an existing job; ignores `--start` / `--end`. |
| `--rpc=<url>` | string | `ARCHIVAL_STELLAR_RPC_URL` | Archival RPC endpoint. |
| `--concurrency=<n>` | integer | 5 (max 20) | Parallel batch workers. |

---

## Boundary semantics

Both `--start` and `--end` are **inclusive**: the ledger at `--start` is
fetched and the ledger at `--end` is the last one fetched.

```
--start=1000  --end=1005   →  ledgers 1000, 1001, 1002, 1003, 1004, 1005
```

`--end` must not exceed the current chain tip reported by the RPC node.
`--start` must not be greater than `--end`; both violations are rejected
before any RPC or database call is made, with an error message naming the
offending argument.

---

## How the checkpoint works

The backfill engine divides the range into fixed-size batches
(`BACKFILL_BATCH_SIZE`, default 5 000 ledgers).  Each batch is processed
inside a **single Prisma transaction** that atomically:

1. Writes the decoded event rows.
2. Updates `BackfillJob.checkpointLedger` to the highest ledger in the batch.

If the process crashes between two batches, the `checkpointLedger` column
records the last *fully committed* batch.  The next run — whether via
`--resume` or by passing the same `--start` / `--end` again — resumes from:

```
resumeFrom = checkpointLedger + 1
```

It does **not** restart from `--start`.  Replayed batches are safe because
duplicate event inserts are rejected by the `eventHash` unique constraint.

---

## Live sync cursor interaction

The backfill command respects the live sync cursor stored in `SyncState`
(the `lastLedger` column).  Three outcomes are possible:

| Condition | Label | SyncState updated? |
|---|---|---|
| `endLedger ≤ liveCursor` | **below** | No |
| `startLedger ≤ liveCursor < endLedger` | **overlapping** | No |
| `startLedger > liveCursor` | **ahead** | Yes — advances to processed ledger |

The live cursor is only advanced when the backfill range is entirely _ahead_
of the current sync position.  Historical backfills (below or overlapping)
never touch `SyncState` so the live poller is unaffected.

---

## Examples

### 1. Ingest a specific historical window

```bash
npm run cli -- backfill --start=1000000 --end=1500000
```

- Fetches ledgers 1 000 000 – 1 500 000 in batches of 5 000.
- Does not touch the live cursor (range is historical).

### 2. Resume after a crash

```bash
# List jobs to find the crashed job ID
npm run cli -- backfill list

# Resume from the last checkpoint automatically
npm run cli -- backfill resume 42
```

The job resumes from `checkpointLedger + 1`, not from `startLedger`.

### 3. Ingest all un-indexed history

```bash
# --end defaults to the current chain tip
npm run cli -- backfill --start=0
```

### 4. Ingest the next N ledgers ahead of the live cursor

```bash
# After the live poller has reached ledger 2 000 000:
npm run cli -- backfill --start=2000001 --end=2050000
```

This is an **"ahead"** interaction — `SyncState` is advanced as each batch
completes so the live poller can continue from `2 050 001` without a gap.

---

## Confirmation that SyncState is NOT overwritten for historical ranges

`determineCursorInteraction()` in `backfill.ts` is the single function that
decides whether `SyncState` is written.  Its contract is:

```typescript
// endLedger <= liveCursor → 'below'  → no SyncState write
// startLedger <= liveCursor → 'overlapping' → no SyncState write
// startLedger > liveCursor  → 'ahead'       → SyncState write
export function determineCursorInteraction(
  startLedger: number,
  endLedger:   number,
  liveCursor:  number,
): 'below' | 'overlapping' | 'ahead'
```

Unit tests for this function live in
`src/__tests__/backfill-unit.test.ts` (section 1) and
`src/__tests__/backfill-range-validation.test.ts`.
