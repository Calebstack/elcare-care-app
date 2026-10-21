/**
 * rpc-timeout-classification.test.ts
 *
 * Issue #927 — Test RPC Timeout Error Classification
 *
 * Protects the boundary between retryable timeout errors and non-retryable
 * permanent (invalid-request) errors in retry.ts without requiring a live
 * Stellar RPC node.
 *
 * Acceptance criteria:
 *  • Timeouts follow the configured retry policy (retried up to maxAttempts).
 *  • Permanent invalid requests are NOT treated as transient.
 *  • Tests complete without real delays or external network access.
 *    (baseDelayMs: 0, jitterFactor: 0 throughout)
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

// ── Suppress prom-client double-registration ──────────────────────────────────
vi.mock('prom-client', () => {
  const makeMetric = () => ({
    labels:     vi.fn().mockReturnThis(),
    set:        vi.fn(),
    inc:        vi.fn(),
    observe:    vi.fn(),
    startTimer: vi.fn(() => vi.fn()),
  });
  return {
    default: {
      Gauge:                  vi.fn(() => makeMetric()),
      Counter:                vi.fn(() => makeMetric()),
      Histogram:              vi.fn(() => makeMetric()),
      collectDefaultMetrics:  vi.fn(),
      register: {
        contentType:     'text/plain',
        metrics:         vi.fn().mockResolvedValue(''),
        getSingleMetric: vi.fn().mockReturnValue(null),
      },
    },
  };
});

vi.mock('../metrics.js', () => ({
  rpcRetryExhaustedCounter: { inc: vi.fn() },
}));

vi.mock('../logger.js', () => ({
  logger: { warn: vi.fn(), error: vi.fn(), info: vi.fn(), debug: vi.fn() },
}));

// ── Also mock timeout.ts so we don't pull in real timers ─────────────────────
vi.mock('../timeout.js', () => ({
  withTimeout:            vi.fn((fn: any) => fn()),
  enforceDeadline:        vi.fn().mockReturnValue(300_000),
  calculateRetryAttempts: vi.fn().mockReturnValue(5),
  TIMEOUT_BUDGETS: {
    rpc:   { operationTimeoutMs: 30_000, totalBudgetMs: 300_000, useAbortSignal: false },
    db:    { operationTimeoutMs: 5_000,  totalBudgetMs: 60_000,  useAbortSignal: false },
    ipfs:  { operationTimeoutMs: 10_000, totalBudgetMs: 60_000,  useAbortSignal: false },
    redis: { operationTimeoutMs: 2_000,  totalBudgetMs: 30_000,  useAbortSignal: false },
  },
  TimeoutError:      class TimeoutError extends Error { constructor(msg: string) { super(msg); this.name = 'TimeoutError'; } },
  CancellationError: class CancellationError extends Error { constructor(msg: string) { super(msg); this.name = 'CancellationError'; } },
}));

import {
  isRpcRetryable,
  withRpcRetry,
  withExponentialBackoff,
  circuitBreakers,
  STELLAR_RPC_RETRY_CONFIG,
} from '../retry.js';

// ── Reset circuit-breaker singletons before each test ────────────────────────
beforeEach(() => {
  vi.clearAllMocks();
  Object.values(circuitBreakers).forEach((cb) => cb.recordSuccess());
});

// ─────────────────────────────────────────────────────────────────────────────
// Error fixture builders
// ─────────────────────────────────────────────────────────────────────────────

/** Simulates a network-level timeout (ETIMEDOUT / ECONNRESET code). */
function makeTimeoutError(variant: 'ETIMEDOUT' | 'ECONNRESET' | 'message' = 'ETIMEDOUT'): Error {
  if (variant === 'message') {
    return new Error('Request timed out after 30000ms');
  }
  const err = new Error(`connect ${variant} 1.2.3.4:443`) as any;
  err.code = variant;
  return err as Error;
}

/** Simulates an HTTP timeout from a Stellar RPC gateway (408 / 504). */
function makeHttpTimeoutError(status: 408 | 504): Error {
  const err = new Error(`HTTP ${status}`) as any;
  err.response = { status };
  return err as Error;
}

/** Simulates a permanent invalid-request error (400 / 404). */
function makePermanentError(status: 400 | 404 | 422): Error {
  const err = new Error(`HTTP ${status} Invalid request`) as any;
  err.response = { status };
  return err as Error;
}

/** Simulates a Stellar RPC "invalid param" JSON-RPC error (no HTTP status). */
function makeStellarInvalidParamError(): Error {
  return new Error('getEvents: start ledger must be within the ledger retention window');
}

/** Simulates a 429 rate-limit error (transient, should retry). */
function makeRateLimitError(): Error {
  const err = new Error('HTTP 429 Too Many Requests') as any;
  err.response = { status: 429 };
  return err as Error;
}

/** Simulates a 500 internal server error (transient, should retry). */
function makeServerError(status = 500): Error {
  const err = new Error(`HTTP ${status}`) as any;
  err.response = { status };
  return err as Error;
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. isRpcRetryable predicate — boundary tests
// ─────────────────────────────────────────────────────────────────────────────

describe('isRpcRetryable — timeout errors are retryable', () => {
  it('ETIMEDOUT code → retryable', () => {
    expect(isRpcRetryable(makeTimeoutError('ETIMEDOUT'))).toBe(true);
  });

  it('ECONNRESET code → retryable', () => {
    expect(isRpcRetryable(makeTimeoutError('ECONNRESET'))).toBe(true);
  });

  it('"timed out" in message → retryable', () => {
    expect(isRpcRetryable(makeTimeoutError('message'))).toBe(true);
  });

  it('HTTP 504 gateway timeout → retryable', () => {
    // 504 is >= 500 so the >=500 branch catches it
    expect(isRpcRetryable(makeHttpTimeoutError(504))).toBe(true);
  });

  it('HTTP 500 internal server error → retryable', () => {
    expect(isRpcRetryable(makeServerError(500))).toBe(true);
  });

  it('HTTP 429 rate-limited → retryable', () => {
    expect(isRpcRetryable(makeRateLimitError())).toBe(true);
  });
});

describe('isRpcRetryable — permanent errors are NOT retryable', () => {
  it('HTTP 400 bad request → NOT retryable', () => {
    expect(isRpcRetryable(makePermanentError(400))).toBe(false);
  });

  it('HTTP 404 not found → NOT retryable', () => {
    expect(isRpcRetryable(makePermanentError(404))).toBe(false);
  });

  it('HTTP 422 unprocessable entity → NOT retryable', () => {
    expect(isRpcRetryable(makePermanentError(422))).toBe(false);
  });

  it('Stellar invalid-param error (no HTTP status) → NOT retryable', () => {
    // "start ledger must be within retention window" is a permanent logic error
    expect(isRpcRetryable(makeStellarInvalidParamError())).toBe(false);
  });

  it('plain string error → NOT retryable', () => {
    expect(isRpcRetryable('some unexpected string')).toBe(false);
  });

  it('null → NOT retryable', () => {
    expect(isRpcRetryable(null)).toBe(false);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. withRpcRetry — timeout errors exhaust retry budget (no real delays)
// ─────────────────────────────────────────────────────────────────────────────

describe('withRpcRetry — timeout errors follow configured retry policy', () => {
  it('retries up to maxAttempts on ETIMEDOUT then rethrows', async () => {
    const fn = vi.fn().mockRejectedValue(makeTimeoutError('ETIMEDOUT'));

    await expect(
      withRpcRetry(fn, { maxAttempts: 3, baseDelayMs: 0, jitterFactor: 0 }),
    ).rejects.toThrow(/ETIMEDOUT/i);

    // 3 total attempts
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('retries on network timeout message variant', async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(makeTimeoutError('message'))
      .mockResolvedValue('recovered');

    const result = await withRpcRetry(fn, { maxAttempts: 3, baseDelayMs: 0, jitterFactor: 0 });
    expect(result).toBe('recovered');
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it('retries on ECONNRESET', async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(makeTimeoutError('ECONNRESET'))
      .mockRejectedValueOnce(makeTimeoutError('ECONNRESET'))
      .mockResolvedValue('ok');

    await expect(
      withRpcRetry(fn, { maxAttempts: 5, baseDelayMs: 0, jitterFactor: 0 }),
    ).resolves.toBe('ok');

    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('retries on HTTP 429 rate-limit', async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(makeRateLimitError())
      .mockResolvedValue('ok');

    await expect(
      withRpcRetry(fn, { maxAttempts: 3, baseDelayMs: 0, jitterFactor: 0 }),
    ).resolves.toBe('ok');

    expect(fn).toHaveBeenCalledTimes(2);
  });

  it('retries on HTTP 500 server error', async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(makeServerError(500))
      .mockResolvedValue('ok');

    await expect(
      withRpcRetry(fn, { maxAttempts: 3, baseDelayMs: 0, jitterFactor: 0 }),
    ).resolves.toBe('ok');

    expect(fn).toHaveBeenCalledTimes(2);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 3. withRpcRetry — permanent errors are NOT retried
// ─────────────────────────────────────────────────────────────────────────────

describe('withRpcRetry — permanent errors are NOT retried', () => {
  it('does NOT retry HTTP 400 — throws immediately on first attempt', async () => {
    const fn = vi.fn().mockRejectedValue(makePermanentError(400));

    await expect(
      withRpcRetry(fn, { maxAttempts: 5, baseDelayMs: 0, jitterFactor: 0 }),
    ).rejects.toThrow('HTTP 400');

    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('does NOT retry HTTP 404', async () => {
    const fn = vi.fn().mockRejectedValue(makePermanentError(404));

    await expect(
      withRpcRetry(fn, { maxAttempts: 5, baseDelayMs: 0 }),
    ).rejects.toThrow('HTTP 404');

    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('does NOT retry Stellar invalid-param error', async () => {
    const fn = vi.fn().mockRejectedValue(makeStellarInvalidParamError());

    await expect(
      withRpcRetry(fn, { maxAttempts: 5, baseDelayMs: 0 }),
    ).rejects.toThrow(/start ledger|retention window/i);

    expect(fn).toHaveBeenCalledTimes(1);
  });

  it('does NOT retry HTTP 422', async () => {
    const fn = vi.fn().mockRejectedValue(makePermanentError(422));

    await expect(
      withRpcRetry(fn, { maxAttempts: 5, baseDelayMs: 0 }),
    ).rejects.toThrow('HTTP 422');

    expect(fn).toHaveBeenCalledTimes(1);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 4. Bounded retry count — fake timers to keep tests fast
// ─────────────────────────────────────────────────────────────────────────────

describe('withExponentialBackoff — bounded retry count with fake timers', () => {
  beforeEach(() => {
    // Intercept setTimeout so no real time passes
    vi.spyOn(global, 'setTimeout').mockImplementation((fn: any) => {
      fn();
      return 0 as any;
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('retries exactly maxAttempts times then exhausts', async () => {
    const fn = vi.fn().mockRejectedValue(makeTimeoutError('ETIMEDOUT'));

    await expect(
      withExponentialBackoff(fn, {
        maxAttempts:  4,
        baseDelayMs:  100,
        maxDelayMs:   1_000,
        jitterFactor: 0,
        retryable:    isRpcRetryable,
        operation:    'rpc',
      }),
    ).rejects.toThrow(/ETIMEDOUT/i);

    expect(fn).toHaveBeenCalledTimes(4);
  });

  it('succeeds on the last allowed attempt', async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(makeTimeoutError('ETIMEDOUT'))
      .mockRejectedValueOnce(makeTimeoutError('ECONNRESET'))
      .mockResolvedValue('final-ok');

    const result = await withExponentialBackoff(fn, {
      maxAttempts:  3,
      baseDelayMs:  100,
      jitterFactor: 0,
      retryable:    isRpcRetryable,
    });

    expect(result).toBe('final-ok');
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it('permanent error short-circuits — does not consume all attempts', async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(makeTimeoutError('ETIMEDOUT')) // retryable
      .mockRejectedValue(makePermanentError(400));          // permanent → stop

    await expect(
      withExponentialBackoff(fn, {
        maxAttempts:  10,
        baseDelayMs:  100,
        jitterFactor: 0,
        retryable:    isRpcRetryable,
      }),
    ).rejects.toThrow('HTTP 400');

    // 1 timeout retry + 1 permanent → 2 total (not 10)
    expect(fn).toHaveBeenCalledTimes(2);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 5. STELLAR_RPC_RETRY_CONFIG sanity — matches the policy under test
// ─────────────────────────────────────────────────────────────────────────────

describe('STELLAR_RPC_RETRY_CONFIG correctness', () => {
  it('uses isRpcRetryable as its retryable predicate', () => {
    // The predicate stored on the config must agree with the exported function
    // for all known-retryable and known-permanent error types.
    const predicate = STELLAR_RPC_RETRY_CONFIG.retryable!;

    expect(predicate(makeTimeoutError('ETIMEDOUT'))).toBe(true);
    expect(predicate(makeRateLimitError())).toBe(true);
    expect(predicate(makeServerError(500))).toBe(true);
    expect(predicate(makePermanentError(400))).toBe(false);
    expect(predicate(makePermanentError(404))).toBe(false);
    expect(predicate(makeStellarInvalidParamError())).toBe(false);
  });

  it('maxAttempts is at least 3 (not 1 or 2 — that would be too aggressive)', () => {
    expect(STELLAR_RPC_RETRY_CONFIG.maxAttempts).toBeGreaterThanOrEqual(3);
  });

  it('baseDelayMs is greater than 0 (exponential backoff has a non-zero base)', () => {
    expect(STELLAR_RPC_RETRY_CONFIG.baseDelayMs).toBeGreaterThan(0);
  });

  it('operation label identifies this config as RPC', () => {
    expect(STELLAR_RPC_RETRY_CONFIG.operation).toBe('rpc');
  });
});
