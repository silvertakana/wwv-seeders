// Unit tests for the global-news-gdelt seeder (GDELT v1 GKG GEOJSON mode).
//
// SOURCE SUBSTITUTION (2026-08-24): the seeder now uses the GDELT v1
// gkg_geojson endpoint (point GeoJSON with coordinates) instead of the v2 DOC
// artlist API (which 429s / "fetch failed" from the engine container while the
// v1 endpoint returns HTTP 200 — verified live). The tests parse a REAL slice
// of a live gkg_geojson response (captured 2026-08-24; ./fixtures/gkg-geojson.json).
//
// The @worldwideview/seeder-sdk is fully mocked so better-sqlite3 native
// bindings never load in the test environment (same pattern as
// packages/live-disasters/src/__tests__/index.test.ts).
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { readFileSync } from 'node:fs';

vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn((fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(async () => ({
    text: async () => '',
    json: async () => ({}),
  })),
  db: {
    prepare: vi.fn(() => ({
      run: vi.fn(() => ({ changes: 0 })),
    })),
  },
}));

import {
  buildQueryUrl,
  extractFeatures,
  mapFeatureToItem,
  parseGkgResponse,
  seedGlobalNews,
  type NewsArticleItem,
} from '../index';
import { setLiveSnapshot, fetchWithTimeout, db } from '@worldwideview/seeder-sdk';

const FIXTURE = JSON.parse(
  readFileSync(new URL('./fixtures/gkg-geojson.json', import.meta.url), 'utf8')
) as unknown;

// db.prepare is invoked once per top-level statement at module load
// (index 0 = CREATE TABLE IF NOT EXISTS guard, index 1 = INSERT OR IGNORE).
const prepareMock = vi.mocked(db.prepare);
const createTableSql = prepareMock.mock.calls[0][0] as string;
const insertSql = prepareMock.mock.calls[1][0] as string;
const insertRunMock = prepareMock.mock.results[1].value.run as ReturnType<typeof vi.fn>;

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.unstubAllEnvs();
});

describe('extractFeatures', () => {
  it('extracts the features array from a FeatureCollection', () => {
    const feats = extractFeatures({ type: 'FeatureCollection', features: [1, 2] });
    expect(feats.length).toBe(2);
  });

  it('accepts {results: [...]} and bare arrays', () => {
    expect(extractFeatures({ results: [1] }).length).toBe(1);
    expect(extractFeatures([1, 2, 3]).length).toBe(3);
  });

  it('returns [] for non-object payloads', () => {
    expect(extractFeatures(null).length).toBe(0);
    expect(extractFeatures('x').length).toBe(0);
    expect(extractFeatures({ total: 3 }).length).toBe(0);
  });
});

describe('mapFeatureToItem', () => {
  it('maps a real GKG feature to an item with coordinates', () => {
    const feat = {
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [106.829, -6.1744] },
      properties: {
        url: 'https://example.com/a',
        name: 'Jakarta, Indonesia',
        urltone: -4.4,
        nummentions: 12,
        urlpubtimedate: '2026-08-24T14:00:00Z',
        language: 'id',
        sourcecountry: 'ID',
      },
    };
    const item = mapFeatureToItem(feat);
    expect(item).not.toBeNull();
    expect(item!.id).toBe('https://example.com/a');
    expect(item!.lat).toBeCloseTo(-6.1744, 3);
    expect(item!.lon).toBeCloseTo(106.829, 3);
    expect(item!.tone).toBeCloseTo(-4.4, 1);
    expect(item!.language).toBe('id');
  });

  it('drops features without a URL', () => {
    expect(mapFeatureToItem({ geometry: { coordinates: [0, 0] }, properties: {} })).toBeNull();
  });

  it('drops features without coordinates', () => {
    expect(
      mapFeatureToItem({ geometry: null, properties: { url: 'https://x.com/1' } })
    ).toBeNull();
  });

  it('drops features with non-numeric coordinates', () => {
    expect(
      mapFeatureToItem({
        geometry: { coordinates: ['a', 'b'] },
        properties: { url: 'https://x.com/2' },
      })
    ).toBeNull();
  });
});

describe('parseGkgResponse against the REAL fixture', () => {
  const items = parseGkgResponse(FIXTURE);

  it('returns only features that have URL + coordinates', () => {
    expect(items.length).toBeGreaterThan(0);
    for (const item of items as NewsArticleItem[]) {
      expect(item.id).toMatch(/^https?:\/\//);
      expect(Number.isFinite(item.lat)).toBe(true);
      expect(Number.isFinite(item.lon)).toBe(true);
    }
  });

  it('real fixture yields a meaningful article count', () => {
    // The captured fixture is a live response slice; assert a sane floor.
    expect(items.length).toBeGreaterThanOrEqual(1);
    expect(items.length).toBeLessThanOrEqual(500);
  });
});

describe('buildQueryUrl', () => {
  it('encodes the query and caps records', () => {
    const url = buildQueryUrl('conflict OR protest');
    expect(url).toContain('query=conflict%20OR%20protest');
    expect(url).toContain('gkg_geojson');
    expect(url).toContain('maxrows=500');
  });
});

describe('seedGlobalNews', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    insertRunMock.mockReturnValue({ changes: 1 });
  });

  it('writes a snapshot when queries return articles', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => FIXTURE,
    } as never);

    await seedGlobalNews();

    expect(fetchWithTimeout).toHaveBeenCalledTimes(3);
    expect(setLiveSnapshot).toHaveBeenCalledTimes(1);
    const [pluginId, snapshot, ttl] = vi.mocked(setLiveSnapshot).mock.calls[0];
    expect(pluginId).toBe('global-news-gdelt');
    expect(snapshot.source).toBe('global-news-gdelt');
    expect(snapshot.totalCount).toBeGreaterThan(0);
    expect(ttl).toBeGreaterThan(0);
  });

  it('skips the snapshot when every query fails', async () => {
    const errorSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('fetch failed'));

    await expect(seedGlobalNews()).resolves.toBeUndefined();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});

describe('module-level SQLite wiring', () => {
  it('ensures the global_news_gdelt table with a CREATE TABLE IF NOT EXISTS guard', () => {
    expect(createTableSql).toContain('CREATE TABLE IF NOT EXISTS global_news_gdelt');
    expect(createTableSql).toContain('id TEXT PRIMARY KEY');
  });

  it('prepares an INSERT OR IGNORE against the global_news_gdelt table', () => {
    expect(insertSql).toContain('INSERT OR IGNORE INTO global_news_gdelt');
  });
});

describe('default export contract', () => {
  it('registers as "global-news-gdelt" on a 15-minute cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('global-news-gdelt');
    expect(seeder.cron).toBe('*/15 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});