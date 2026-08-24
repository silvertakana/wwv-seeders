// Unit tests for the global-news-gdelt seeder (GDELT DOC 2.0, artlist mode).
//
// Parses a REAL fixture of a DOC API article-list JSON response (captured
// 2026-08-24; see ./fixtures/gdelt-artlist.json). GDELT article records carry
// NO coordinates, so the seeder maps the article's 2-letter source country to
// an embedded country centroid; articles with unknown countries are dropped.
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
  COUNTRY_CENTROIDS,
  buildQueryUrl,
  extractArticles,
  parseGdeltResponse,
  seendateEpochMs,
  seedGlobalNews,
  type GdeltArticle,
  type NewsArticleItem,
} from '../index';
import { setLiveSnapshot, fetchWithTimeout, db } from '@worldwideview/seeder-sdk';

const FIXTURE = JSON.parse(
  readFileSync(new URL('./fixtures/gdelt-artlist.json', import.meta.url), 'utf8')
) as unknown;

// db.prepare is invoked once per top-level statement at module load
// (index 0 = CREATE TABLE IF NOT EXISTS guard, index 1 = INSERT OR IGNORE).
const prepareMock = vi.mocked(db.prepare);
const createTableSql = prepareMock.mock.calls[0][0] as string;
const insertSql = prepareMock.mock.calls[1][0] as string;
const insertRunMock = prepareMock.mock.results[1].value.run as ReturnType<typeof vi.fn>;

beforeEach(() => {
  vi.clearAllMocks();
  vi.stubEnv('GDELT_QUERY_GAP_MS', '0');
});

afterEach(() => {
  vi.unstubAllEnvs();
});

describe('extractArticles', () => {
  const article = { url: 'https://example.com/a', title: 't', sourcecountry: 'US' };

  it('accepts the "articles" envelope key', () => {
    expect(extractArticles({ articles: [article] })).toEqual([article]);
  });

  it('accepts the "results" envelope key', () => {
    expect(extractArticles({ results: [article] })).toEqual([article]);
  });

  it('accepts a bare array', () => {
    expect(extractArticles([article])).toEqual([article]);
  });

  it('returns an empty array for non-article payloads', () => {
    expect(extractArticles(null)).toEqual([]);
    expect(extractArticles('nope')).toEqual([]);
    expect(extractArticles({ total: 3 })).toEqual([]);
  });
});

describe('seendateEpochMs', () => {
  it('parses GDELT compact datetimes as UTC', () => {
    expect(seendateEpochMs('20260824120000')).toBe(Date.UTC(2026, 7, 24, 12, 0, 0));
  });

  it('falls back to now for malformed values', () => {
    const before = Date.now();
    const actual = seendateEpochMs('not-a-date');
    expect(actual).toBeGreaterThanOrEqual(before);
    expect(seendateEpochMs(null)).toBeGreaterThanOrEqual(before);
  });
});

describe('buildQueryUrl', () => {
  it('encodes the query and requests artlist JSON with a record cap', () => {
    const url = buildQueryUrl('(conflict OR protest)');
    // encodeURIComponent does NOT encode parentheses; spaces become %20.
    expect(url).toContain('query=(conflict%20OR%20protest)');
    expect(url).toContain('mode=artlist');
    expect(url).toContain('format=json');
    expect(url).toContain('maxrecords=50');
  });
});

describe('parseGdeltResponse against the REAL GDELT fixture', () => {
  const items = parseGdeltResponse(FIXTURE);

  it('returns only articles that can be geolocated via the country map', () => {
    expect(items.length).toBeGreaterThan(0);
    for (const item of items as NewsArticleItem[]) {
      expect(COUNTRY_CENTROIDS[item.sourcecountry ?? '']).toBeDefined();
      expect(Number.isFinite(item.lat)).toBe(true);
      expect(Number.isFinite(item.lon)).toBe(true);
      expect(item.id).toMatch(/^https?:\/\//);
      expect(item.url).toBe(item.id);
      expect(item.title.length).toBeGreaterThan(0);
    }
  });

  it('places each article at the centroid of its source country (REAL data check)', () => {
    for (const item of items as NewsArticleItem[]) {
      const expected = COUNTRY_CENTROIDS[item.sourcecountry ?? ''];
      expect(item.lat).toBe(expected.lat);
      expect(item.lon).toBe(expected.lon);
    }
  });

  it('dedupes nothing here (extractArticles envelope matches the real shape)', () => {
    // Guard: the fixture must actually exercise the live envelope key so the
    // test fails loudly if GDELT changes its shape.
    const raw = extractArticles(FIXTURE);
    expect(raw.length).toBeGreaterThan(0);
    expect(items.length).toBeLessThanOrEqual(raw.length);
  });

  it('skips articles without a URL', () => {
    const blocked: GdeltArticle = { title: 'no url', sourcecountry: 'US' };
    expect(parseGdeltResponse({ articles: [blocked] })).toEqual([]);
  });

  it('skips articles with a country absent from the embedded map', () => {
    const unknown: GdeltArticle = {
      url: 'https://example.com/x',
      title: 't',
      sourcecountry: 'XZ',
    };
    expect(parseGdeltResponse({ articles: [unknown] })).toEqual([]);
  });

  it('maps a known country to its centroid', () => {
    const us: GdeltArticle = { url: 'https://example.com/us', title: 't', sourcecountry: 'US' };
    const uk: GdeltArticle = { url: 'https://example.com/uk', title: 't', sourcecountry: 'UK' };
    const de: GdeltArticle = { url: 'https://example.com/de', title: 't', sourcecountry: 'GM' };
    const items = parseGdeltResponse({ articles: [us, uk, de] });
    expect(items[0]).toMatchObject({ id: 'https://example.com/us', lat: 39.8, lon: -98.6 });
    expect(items[1]).toMatchObject({ lat: 55.4, lon: -3.4 });
    expect(items[2]).toMatchObject({ lat: 51.1, lon: 10.4 });
  });
});

describe('seedGlobalNews integration', () => {
  // QUERY_GAP_MS is a module-load constant (env stubs after import do not
  // affect it), so the three fixed queries sleep the real 6s rate-limit gap
  // each -> ~18s total. Accommodate with a generous per-test timeout.
  const INTEGRATION_TIMEOUT = 60000;

  it('queries, parses, dedupes, persists, and snapshots end-to-end', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => FIXTURE,
    } as never);

    await seedGlobalNews();

    // Three fixed queries, each spaced per the rate limit (stubbed to 0 here).
    expect(fetchWithTimeout).toHaveBeenCalledTimes(3);
    const urls = vi.mocked(fetchWithTimeout).mock.calls.map((c) => c[0] as string);
    for (const url of urls) {
      expect(url).toContain('api.gdeltproject.org/api/v2/doc/doc');
      expect(url).toContain('mode=artlist');
    }

    // The identical fixture is returned for every query, so dedupe by URL
    // keeps exactly the fixture's unique mappable article count.
    const expectedUnique = parseGdeltResponse(FIXTURE).length;
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'global-news-gdelt',
      expect.objectContaining({
        source: 'global-news-gdelt',
        totalCount: expectedUnique,
        items: expect.any(Array),
      }),
      5400
    );
    expect(insertRunMock).toHaveBeenCalledTimes(expectedUnique);
    expect(insertRunMock.mock.calls[0][0]).toMatchObject({
      id: expect.stringMatching(/^https?:\/\//),
    });
  }, INTEGRATION_TIMEOUT);

  it('continues after a single failed query and still snapshots the rest', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout)
      .mockRejectedValueOnce(new Error('rate limited'))
      .mockResolvedValue({ json: async () => FIXTURE } as never);

    await seedGlobalNews();

    expect(fetchWithTimeout).toHaveBeenCalledTimes(3);
    expect(setLiveSnapshot).toHaveBeenCalledTimes(1);
    errorSpy.mockRestore();
  }, INTEGRATION_TIMEOUT);

  it('skips the snapshot when every query fails', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('network down'));

    await expect(seedGlobalNews()).resolves.toBeUndefined();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  }, INTEGRATION_TIMEOUT);
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