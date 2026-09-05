// Unit tests for the undersea-cables seeder. The @worldwideview/seeder-sdk is
// fully mocked so no network or runtime deps ever load in the test
// environment (same pattern as packages/earthquakes/src/__tests__/index.test.ts):
// withRetry runs the wrapped function directly, fetchWithTimeout returns a
// fake Response built from plain objects.
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('@worldwideview/seeder-sdk', () => ({
  fetchWithTimeout: vi.fn(),
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
}));

import seeder, { fetchUnderseaCables, SOURCE_URL } from './index';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

const featureCollection = {
  type: 'FeatureCollection',
  name: 'submarine_cables',
  features: [
    {
      type: 'Feature',
      properties: { cable_id: 1, name: 'Example Cable' },
      geometry: { type: 'MultiLineString', coordinates: [[[0, 0], [1, 1]]] },
    },
  ],
};

const okResponse = (body: unknown) => ({
  ok: true,
  status: 200,
  json: async () => body,
});

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(fetchWithTimeout).mockResolvedValue(okResponse(featureCollection) as never);
});

describe('fetchUnderseaCables', () => {
  it('returns the parsed FeatureCollection on the happy path', async () => {
    const result = await fetchUnderseaCables();

    expect(result).toEqual(featureCollection);
    expect(fetchWithTimeout).toHaveBeenCalledTimes(1);
    expect(fetchWithTimeout).toHaveBeenCalledWith(
      SOURCE_URL,
      {
        headers: {
          'User-Agent': 'WorldWideView/1.0',
          Accept: 'application/json',
        },
      },
      30_000
    );
    expect(withRetry).toHaveBeenCalledWith(expect.any(Function), 2);
  });

  it('throws a descriptive error on a non-2xx response', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      ok: false,
      status: 500,
      json: async () => ({}),
    } as never);

    await expect(fetchUnderseaCables()).rejects.toThrow(/HTTP 500/);
  });

  it('fails cleanly on malformed JSON', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => {
        throw new SyntaxError('Unexpected end of JSON input');
      },
    } as never);

    await expect(fetchUnderseaCables()).rejects.toThrow(/malformed JSON/);
  });

  it('rejects a payload that is not a FeatureCollection', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue(okResponse({ type: 'Feature' }) as never);

    await expect(fetchUnderseaCables()).rejects.toThrow(/FeatureCollection/);
  });

  it('rejects a FeatureCollection without a features array', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue(
      okResponse({ type: 'FeatureCollection', name: 'submarine_cables' }) as never
    );

    await expect(fetchUnderseaCables()).rejects.toThrow(/features array/);
  });

  it('returns null on an empty FeatureCollection so the scheduler skips publishing', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue(
      okResponse({ type: 'FeatureCollection', name: 'submarine_cables', features: [] }) as never
    );

    const result = await fetchUnderseaCables();
    expect(result).toBeNull();
  });
});

describe('default export contract', () => {
  it('registers as "undersea-cables" on a 6-hour interval with a fetch handler', () => {
    expect(seeder.name).toBe('undersea-cables');
    expect(seeder.interval).toBe(21600000);
    expect(seeder.interval).toBe(6 * 60 * 60 * 1000);
    expect(typeof seeder.fetch).toBe('function');
  });
});