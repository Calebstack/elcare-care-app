/**
 * listing-detail-conditional-get.test.ts
 *
 * Conditional GET coverage for a single endpoint, GET /listings/:id.
 * Exercises the existing etagMiddleware wired into the real router; no extra
 * caching layer is added. Redis and Stellar RPC are not used: Redis is mocked
 * as unavailable and the database is an in-memory stub.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import express from 'express';
import request from 'supertest';

const mockPrisma = vi.hoisted(() => ({
  listing: { findUnique: vi.fn(), findMany: vi.fn(), count: vi.fn(), aggregate: vi.fn() },
  ipfsMetadata: { findUnique: vi.fn().mockResolvedValue(null) },
}));

vi.mock('../db', () => ({ default: mockPrisma }));
vi.mock('../prisma-write', () => ({ default: mockPrisma }));
vi.mock('../redis.js', () => ({
  default: { isOpen: false, isReady: false, get: vi.fn().mockRejectedValue(new Error('No Redis')) },
}));

import router from '../api/routes.js';
import { _resetConfirmedVersion } from '../api/etag-middleware.js';

const listing = (price: string) => ({ listingId: 7n, artist: 'GABC', price, token: null });

describe('GET /listings/:id — conditional GET', () => {
  let app: express.Application;

  beforeEach(() => {
    _resetConfirmedVersion();
    mockPrisma.listing.findUnique.mockReset();
    app = express();
    app.use(express.json());
    app.use(router);
  });

  it('returns 304 with no body when If-None-Match matches the ETag', async () => {
    mockPrisma.listing.findUnique.mockResolvedValue(listing('1000'));

    const first = await request(app).get('/listings/7');
    expect(first.status).toBe(200);
    const etag = first.get('ETag');
    expect(etag).toMatch(/^"[a-f0-9]+"$/);

    const second = await request(app).get('/listings/7').set('If-None-Match', etag as string);
    expect(second.status).toBe(304);
    expect(second.text).toBeFalsy();
    expect(second.body).toEqual({});
    expect(second.get('Content-Length') ?? '0').toBe('0');
  });

  it('does not reuse a stale ETag after the listing changes', async () => {
    mockPrisma.listing.findUnique.mockResolvedValue(listing('1000'));
    const first = await request(app).get('/listings/7');
    const staleEtag = first.get('ETag') as string;

    mockPrisma.listing.findUnique.mockResolvedValue(listing('2500'));
    const second = await request(app).get('/listings/7').set('If-None-Match', staleEtag);

    expect(second.status).toBe(200);
    expect(second.body.price).toBe('2500');
    expect(second.get('ETag')).not.toBe(staleEtag);
  });
});
