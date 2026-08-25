// Unit tests for the gdacs-disasters seeder (GDACS JSON event list).
//
// Parses a REAL fixture captured from
// https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH on 2026-08-25
// (6 features: 2 with iscurrent=true, 4 with iscurrent=false, spanning TC/FL/WF
// event types and Orange/Red alert levels, so the current-only filter is
// exercised against live data).
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

import {
  parseGdacsDate,
  parseGdacsPayload,
  seedGdacsDisasters,
  type GdacsDisasterItem,
} from '../index';
import { setLiveSnapshot, fetchWithTimeout, db } from '@worldwideview/seeder-sdk';

const FIXTURE = JSON.parse(
  readFileSync(new URL('./fixtures/gdacs-sample.json', import.meta.url), 'utf8')
) as { type: string; features: unknown[] };

// db.prepare is invoked once per top-level statement at module load
// (index 0 = CREATE TABLE IF NOT EXISTS guard, index 1 = INSERT OR IGNORE).
const prepareMock = vi.mocked(db.prepare);
const createTableSql = prepareMock.mock.calls[0][0] as string;
const insertSql = prepareMock.mock.calls[1][0] as string;
const insertRunMock = prepareMock.mock.results[1].value.run as ReturnType<typeof vi.fn>;

beforeEach(() => {
  vi.clearAllMocks();
});

describe('parseGdacsDate', () => {
  it('treats a zone-less GDACS timestamp as UTC', () => {
    expect(parseGdacsDate('2026-08-18T12:00:00')).toBe('2026-08-18T12:00:00.000Z');
  });

  it('passes an already-zoned value through unchanged', () => {
    expect(parseGdacsDate('2026-08-18T12:00:00Z')).toBe('2026-08-18T12:00:00.000Z');
    expect(parseGdacsDate('2026-08-18T14:00:00+02:00')).toBe('2026-08-18T12:00:00.000Z');
  });

  it('returns null for empty or unparseable input', () => {
    expect(parseGdacsDate(null)).toBeNull();
    expect(parseGdacsDate('')).toBeNull();
    expect(parseGdacsDate('not-a-date')).toBeNull();
  });
});

describe('parseGdacsPayload against the REAL GDACS fixture', () => {
  it('keeps only current, non-temporary events', () => {
    expect(FIXTURE.features).toHaveLength(6);
    const items = parseGdacsPayload(FIXTURE);
    expect(items).toHaveLength(2);
  });

  it('maps the first current event (TC SAUDEL-26) with expected fields and coordinates', () => {
    const items = parseGdacsPayload(FIXTURE);
    const first = items[0];
    expect(first).toMatchObject({
      id: 'TC-1001305',
      latitude: 27.2,
      longitude: 129,
      timestamp: '2026-08-18T12:00:00.000Z',
      eventtype: 'TC',
      eventname: 'SAUDEL-26',
      name: 'Tropical Cyclone SAUDEL-26',
      alertlevel: 'Orange',
      alertscore: 2,
      episodealertlevel: 'Orange',
      country: 'Japan, Northern Mariana Islands, China',
      iso3: 'CHN',
      glide: 'TC-2026-000161-CHN',
      source: 'JTWC',
      fromdate: '2026-08-18T12:00:00.000Z',
      todate: '2026-08-25T12:00:00.000Z',
      severity: 212.9616,
      severitytext: 'Hurricane/Typhoon > 74 mph (maximum wind speed of 213 km/h)',
      severityunit: 'km/h',
    });
    expect(first.reportUrl).toMatch(/^https:\/\/www\.gdacs\.org\/report\.aspx/);
    expect(first.detailsUrl).toMatch(/^https:\/\/www\.gdacs\.org\/gdacsapi\/api\/events\/geteventdata/);
  });

  it('maps the second current event (Flood in China) as a flat GeoEntity item', () => {
    const items = parseGdacsPayload(FIXTURE);
    const second = items[1];
    expect(second).toMatchObject({
      id: 'FL-1104081',
      latitude: 23.9954,
      longitude: 121.6029,
      eventtype: 'FL',
      alertlevel: 'Orange',
    });
    for (const item of items as GdacsDisasterItem[]) {
      expect(typeof item.id).toBe('string');
      expect(Number.isFinite(item.latitude)).toBe(true);
      expect(Number.isFinite(item.longitude)).toBe(true);
      expect(Number.isNaN(Date.parse(item.timestamp))).toBe(false);
    }
  });

  it('emits only finite, in-range coordinates', () => {
    for (const item of parseGdacsPayload(FIXTURE) as GdacsDisasterItem[]) {
      expect(item.latitude).toBeGreaterThanOrEqual(-90);
      expect(item.latitude).toBeLessThanOrEqual(90);
      expect(item.longitude).toBeGreaterThanOrEqual(-180);
      expect(item.longitude).toBeLessThanOrEqual(180);
    }
  });

  it('returns an empty array for a non-FeatureCollection payload', () => {
    expect(parseGdacsPayload({ nope: true })).toEqual([]);
    expect(parseGdacsPayload(null)).toEqual([]);
    expect(parseGdacsPayload({ features: 'nope' })).toEqual([]);
  });

  it('drops features without usable Point geometry', () => {
    const items = parseGdacsPayload({
      type: 'FeatureCollection',
      features: [
        { properties: { eventtype: 'EQ', eventid: 1, iscurrent: 'true' } },
        {
          geometry: { type: 'Point', coordinates: ['x', 5] },
          properties: { eventtype: 'EQ', eventid: 2, iscurrent: 'true' },
        },
        {
          geometry: { type: 'Point', coordinates: [10, 200] },
          properties: { eventtype: 'EQ', eventid: 3, iscurrent: 'true' },
        },
        {
          geometry: { type: 'Point', coordinates: [10, 20] },
          properties: { eventtype: 'EQ', eventid: 4, iscurrent: 'true' },
        },
      ],
    });
    expect(items).toHaveLength(1);
    expect(items[0].id).toBe('EQ-4');
  });
});

describe('seedGdacsDisasters integration', () => {
  it('fetches, parses, persists, and snapshots end-to-end', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => FIXTURE,
    } as never);

    await seedGdacsDisasters();

    expect(vi.mocked(fetchWithTimeout).mock.calls[0][0]).toBe(
      'https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH'
    );

    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'gdacs-disasters',
      expect.objectContaining({
        source: 'gdacs-disasters',
        totalCount: 2,
        items: expect.any(Array),
      }),
      1800
    );
    expect(insertRunMock).toHaveBeenCalledTimes(2);

    const args = insertRunMock.mock.calls[0][0] as {
      id: string;
      payload: string;
      source_ts: number;
      fetched_at: number;
    };
    expect(args.id).toBe('TC-1001305');
    expect(typeof args.source_ts).toBe('number');
    expect(typeof args.fetched_at).toBe('number');
    const payload = JSON.parse(args.payload) as GdacsDisasterItem;
    expect(payload).toMatchObject({ eventtype: 'TC', alertlevel: 'Orange' });
  });

  it('logs and skips the snapshot when the fetch fails', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('network down'));

    await expect(seedGdacsDisasters()).resolves.toBeUndefined();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});

describe('module-level SQLite wiring', () => {
  it('ensures the gdacs_disasters table with a CREATE TABLE IF NOT EXISTS guard', () => {
    expect(createTableSql).toContain('CREATE TABLE IF NOT EXISTS gdacs_disasters');
    expect(createTableSql).toContain('id TEXT PRIMARY KEY');
  });

  it('prepares an INSERT OR IGNORE against the gdacs_disasters table', () => {
    expect(insertSql).toContain('INSERT OR IGNORE INTO gdacs_disasters');
  });
});

describe('default export contract', () => {
  it('registers as "gdacs-disasters" on a 5-minute cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('gdacs-disasters');
    expect(seeder.cron).toBe('*/5 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});