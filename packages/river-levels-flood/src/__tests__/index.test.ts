// Unit tests for the river-levels-flood seeder (USGS NWIS instantaneous values).
//
// Parses a REAL fixture captured from the iv service on 2026-08-24: a 50-site
// batch request (parameterCd=00065) that returned 43 current series, exactly
// matching the package's runtime batch shape.
//
// The @worldwideview/seeder-sdk is fully mocked so better-sqlite3 native
// bindings never load in the test environment (same pattern as
// packages/live-disasters/src/__tests__/index.test.ts).
import { describe, it, expect, vi, beforeEach } from 'vitest';
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

import { chunkArray, parseIvResponse, seedRiverLevels, type RiverLevelItem } from '../index';
import { GAUGES } from '../gauges';
import { setLiveSnapshot, fetchWithTimeout, db } from '@worldwideview/seeder-sdk';

// Minimal structural mirror of the ns1:timeSeriesResponseType envelope, used to
// mutate copies of the fixture in the robustness tests.
interface SeriesLike {
  sourceInfo?: {
    siteName?: string;
    siteCode?: Array<{ value?: string }>;
    geoLocation?: { geogLocation?: { latitude?: number; longitude?: number } };
  };
  values?: Array<{ value?: Array<{ value?: string; dateTime?: string }> }>;
}
interface FixtureLike {
  value?: { timeSeries?: SeriesLike[] };
}

const FIXTURE = JSON.parse(
  readFileSync(new URL('./fixtures/usgs-iv-batch.json', import.meta.url), 'utf8')
) as FixtureLike;

// db.prepare is invoked once per top-level statement at module load
// (index 0 = CREATE TABLE IF NOT EXISTS guard, index 1 = INSERT OR IGNORE).
const prepareMock = vi.mocked(db.prepare);
const createTableSql = prepareMock.mock.calls[0][0] as string;
const insertSql = prepareMock.mock.calls[1][0] as string;
const insertRunMock = prepareMock.mock.results[1].value.run as ReturnType<typeof vi.fn>;

beforeEach(() => {
  vi.clearAllMocks();
});

describe('chunkArray', () => {
  it('splits the 103 embedded gauges into batches of 50', () => {
    const chunks = chunkArray(GAUGES, 50);
    expect(chunks.map((c) => c.length)).toEqual([50, 50, 3]);
  });

  it('returns a single chunk when the array fits', () => {
    expect(chunkArray([1, 2, 3], 50)).toEqual([[1, 2, 3]]);
  });

  it('handles empty input and odd sizes', () => {
    expect(chunkArray([], 50)).toEqual([]);
    expect(chunkArray([1, 2, 3, 4, 5, 6, 7], 3)).toEqual([[1, 2, 3], [4, 5, 6], [7]]);
  });
});

describe('embedded gauge list', () => {
  it('contains 100+ verified USGS stage gauges with code and name', () => {
    expect(GAUGES.length).toBeGreaterThanOrEqual(100);
    for (const gauge of GAUGES) {
      expect(typeof gauge.code).toBe('string');
      expect(gauge.code).toMatch(/^\d{8,}$/);
      expect(typeof gauge.name).toBe('string');
      expect(gauge.name.length).toBeGreaterThan(0);
    }
  });
});

describe('parseIvResponse against the REAL USGS fixture', () => {
  it('parses all 43 real series into river-level items', () => {
    const items = parseIvResponse(FIXTURE);
    expect(items).toHaveLength(43);
  });

  it('maps the first real series (MERRIMACK RIVER AT LAWRENCE, MA)', () => {
    const items = parseIvResponse(FIXTURE);
    expect(items[0]).toMatchObject({
      id: '01100500',
      name: 'MERRIMACK RIVER AT LAWRENCE, MA',
      stage_ft: 10.01,
      dateTime: '2026-08-24T09:30:00.000-04:00',
    });
    expect(items[0].lat).toBeCloseTo(42.7048333, 5);
    expect(items[0].lon).toBeCloseTo(-71.15313889, 5);
  });

  it('maps subsequent real series with their latest reading', () => {
    const items = parseIvResponse(FIXTURE);
    expect(items[1].id).toBe('01184000');
    expect(items[1].stage_ft).toBeCloseTo(8.25, 5);
    expect(items[2].id).toBe('01358000');
    expect(items[2].stage_ft).toBeCloseTo(16.31, 5);
  });

  it('emits only finite lat/lon and numeric stages', () => {
    for (const item of parseIvResponse(FIXTURE) as RiverLevelItem[]) {
      expect(Number.isFinite(item.lat)).toBe(true);
      expect(Number.isFinite(item.lon)).toBe(true);
      expect(Number.isFinite(item.stage_ft)).toBe(true);
    }
  });

  it('skips a series that lacks geolocation', () => {
    const clone = structuredClone(FIXTURE) as FixtureLike;
    const series = clone.value?.timeSeries ?? [];
    delete series[0].sourceInfo?.geoLocation;
    const items = parseIvResponse(clone);
    expect(items).toHaveLength(42);
    expect(items.some((i) => i.id === '01100500')).toBe(false);
  });

  it('skips a series with no readings', () => {
    const clone = structuredClone(FIXTURE) as FixtureLike;
    const series = clone.value?.timeSeries ?? [];
    series[1].values = [];
    const items = parseIvResponse(clone);
    expect(items).toHaveLength(42);
    expect(items.some((i) => i.id === '01184000')).toBe(false);
  });

  it('skips a series whose latest stage is not numeric', () => {
    const clone = structuredClone(FIXTURE) as FixtureLike;
    const series = clone.value?.timeSeries ?? [];
    const readings = series[2].values?.[0]?.value ?? [];
    readings[readings.length - 1].value = 'ice';
    const items = parseIvResponse(clone);
    expect(items).toHaveLength(42);
    expect(items.some((i) => i.id === '01358000')).toBe(false);
  });

  it('returns an empty array for a non-envelope payload', () => {
    expect(parseIvResponse({ nope: true })).toEqual([]);
    expect(parseIvResponse(null)).toEqual([]);
  });
});

describe('seedRiverLevels integration', () => {
  it('batches the gauge list, parses, dedupes, persists, and snapshots end-to-end', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => FIXTURE,
    } as never);

    await seedRiverLevels();

    // 103 gauges / 50 per batch = 3 requests, each with the contact User-Agent.
    expect(fetchWithTimeout).toHaveBeenCalledTimes(3);
    for (const [, options] of vi.mocked(fetchWithTimeout).mock.calls) {
      expect((options as { headers: Record<string, string> }).headers['User-Agent']).toMatch(
        /^wwv-plugin-river-levels /
      );
    }

    // The same 43-series fixture is returned for every batch, so dedupe by
    // site code keeps exactly 43 unique items.
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'river-levels-flood',
      expect.objectContaining({
        source: 'river-levels-flood',
        totalCount: 43,
        items: expect.any(Array),
      }),
      5400
    );
    expect(insertRunMock).toHaveBeenCalledTimes(43);

    const args = insertRunMock.mock.calls[0][0] as {
      id: string;
      payload: string;
      source_ts: number;
      fetched_at: number;
    };
    expect(args.id).toBe('01100500');
    expect(typeof args.source_ts).toBe('number');
    expect(typeof args.fetched_at).toBe('number');
    const payload = JSON.parse(args.payload) as RiverLevelItem;
    expect(payload).toMatchObject({ id: '01100500', stage_ft: 10.01 });
  });

  it('logs and skips the snapshot when a batch fetch fails', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('network down'));

    await expect(seedRiverLevels()).resolves.toBeUndefined();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});

describe('module-level SQLite wiring', () => {
  it('ensures the river_levels_flood table with a CREATE TABLE IF NOT EXISTS guard', () => {
    expect(createTableSql).toContain('CREATE TABLE IF NOT EXISTS river_levels_flood');
    expect(createTableSql).toContain('id TEXT PRIMARY KEY');
  });

  it('prepares an INSERT OR IGNORE against the river_levels_flood table', () => {
    expect(insertSql).toContain('INSERT OR IGNORE INTO river_levels_flood');
  });
});

describe('default export contract', () => {
  it('registers as "river-levels-flood" on a 15-minute cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('river-levels-flood');
    expect(seeder.cron).toBe('*/15 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});