// Unit tests for the severe-weather-alerts seeder (NOAA/NWS active alerts).
//
// Parses a REAL fixture captured from https://api.weather.gov/alerts/active on
// 2026-08-24 (10 features: 6 with Polygon geometry, 4 with null geometry, so
// the polygon->representative-point logic is exercised against live data).
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
  polygonRepresentativePoint,
  parseActiveAlerts,
  seedSevereWeatherAlerts,
  type NwsAlertFeature,
  type SevereWeatherAlertItem,
} from '../index';
import { setLiveSnapshot, fetchWithTimeout, db } from '@worldwideview/seeder-sdk';

const FIXTURE = JSON.parse(
  readFileSync(new URL('./fixtures/nws-active-alerts.json', import.meta.url), 'utf8')
) as { type: string; features: NwsAlertFeature[] };

// db.prepare is invoked once per top-level statement at module load
// (index 0 = CREATE TABLE IF NOT EXISTS guard, index 1 = INSERT OR IGNORE).
const prepareMock = vi.mocked(db.prepare);
const createTableSql = prepareMock.mock.calls[0][0] as string;
const insertSql = prepareMock.mock.calls[1][0] as string;
const insertRunMock = prepareMock.mock.results[1].value.run as ReturnType<typeof vi.fn>;

beforeEach(() => {
  vi.clearAllMocks();
});

describe('polygonRepresentativePoint', () => {
  it('computes the outer-ring average centroid for a Polygon', () => {
    const point = polygonRepresentativePoint({
      type: 'Polygon',
      coordinates: [
        [
          [0, 0],
          [0, 10],
          [10, 10],
          [10, 0],
          [0, 0],
        ],
      ],
    });
    // The ring closes on the first vertex, so the mean of the 5 vertices is 4.
    expect(point).toEqual({ lat: 4, lon: 4 });
  });

  it('returns null for null geometry (never emitted)', () => {
    expect(polygonRepresentativePoint(null)).toBeNull();
  });

  it('returns null for a degenerate ring with fewer than 3 real vertices', () => {
    expect(
      polygonRepresentativePoint({
        type: 'Polygon',
        coordinates: [
          [
            [0, 0],
            [1, 1],
          ],
        ],
      })
    ).toBeNull();
  });

  it('prefers the largest polygon of a MultiPolygon', () => {
    const point = polygonRepresentativePoint({
      type: 'MultiPolygon',
      coordinates: [
        [
          [
            [0, 0],
            [0, 2],
            [2, 0],
            [0, 0],
          ],
        ],
        [
          [
            [10, 10],
            [10, 40],
            [40, 40],
            [40, 10],
            [10, 10],
          ],
        ],
      ],
    });
    // Closed 5-vertex ring repeats the first corner, so the mean is 22.
    expect(point).toEqual({ lat: 22, lon: 22 });
  });
});

describe('parseActiveAlerts against the REAL NWS fixture', () => {
  it('maps every Polygon feature and drops every null-geometry feature', () => {
    // The fixture has 10 features: 6 Polygon + 4 null geometry.
    expect(FIXTURE.features).toHaveLength(10);
    const items = parseActiveAlerts(FIXTURE);
    expect(items).toHaveLength(6);
  });

  it('maps the first real alert with expected field values and centroid', () => {
    const items = parseActiveAlerts(FIXTURE);
    const first = items[0];
    expect(first).toMatchObject({
      id: 'urn:oid:2.49.0.1.840.0.f2c652cbb28730b59fc273589968f3773218bc09.001.1',
      event: 'Special Weather Statement',
      severity: 'Moderate',
      urgency: 'Expected',
      areaDesc: 'Madison; Warren; Hinds',
    });
    expect(first.headline).toMatch(/^Special Weather Statement issued August /);
    expect(first.lat).toBeCloseTo(32.38, 4);
    expect(first.lon).toBeCloseTo(-90.833636, 4);
    expect(first.description?.length).toBeGreaterThan(100);
    expect(first.instruction?.length).toBeGreaterThan(10);
    expect(first.sent).toMatch(/^2026-08-25T01:49:00/);
    expect(first.effective).toMatch(/^2026-08-25T01:49:00/);
    expect(first.expires).toMatch(/^2026-08-25T02:45:00/);
  });

  it('maps the second real alert (Flash Flood Warning, Severe/Immediate)', () => {
    const items = parseActiveAlerts(FIXTURE);
    const second = items[1];
    expect(second).toMatchObject({
      id: 'urn:oid:2.49.0.1.840.0.bf0808644cf12d457c7616fe4b65f312b6dd15ac.001.1',
      event: 'Flash Flood Warning',
      severity: 'Severe',
      urgency: 'Immediate',
    });
    expect(second.lat).toBeCloseTo(33.859167, 4);
    expect(second.lon).toBeCloseTo(-91.866667, 4);
  });

  it('emits only finite, in-range coordinates', () => {
    for (const item of parseActiveAlerts(FIXTURE) as SevereWeatherAlertItem[]) {
      expect(Number.isFinite(item.lat)).toBe(true);
      expect(Number.isFinite(item.lon)).toBe(true);
      expect(item.lat).toBeGreaterThanOrEqual(-90);
      expect(item.lat).toBeLessThanOrEqual(90);
      expect(item.lon).toBeGreaterThanOrEqual(-180);
      expect(item.lon).toBeLessThanOrEqual(180);
    }
  });

  it('returns an empty array for a non-FeatureCollection payload', () => {
    expect(parseActiveAlerts({ nope: true })).toEqual([]);
    expect(parseActiveAlerts(null)).toEqual([]);
  });
});

describe('seedSevereWeatherAlerts integration', () => {
  it('fetches with a User-Agent, parses, persists, and snapshots end-to-end', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => FIXTURE,
    } as never);

    await seedSevereWeatherAlerts();

    // The NWS request must carry the descriptive User-Agent.
    const callArgs = vi.mocked(fetchWithTimeout).mock.calls[0];
    expect(callArgs[0]).toBe('https://api.weather.gov/alerts/active');
    expect((callArgs[1] as { headers: Record<string, string> }).headers['User-Agent']).toMatch(
      /^wwv-plugin\/1\.0 /
    );

    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'severe-weather-alerts',
      expect.objectContaining({
        source: 'severe-weather-alerts',
        totalCount: 6,
        items: expect.any(Array),
      }),
      1800
    );
    expect(insertRunMock).toHaveBeenCalledTimes(6);

    const args = insertRunMock.mock.calls[0][0] as {
      id: string;
      payload: string;
      source_ts: number;
      fetched_at: number;
    };
    expect(args.id).toBe('urn:oid:2.49.0.1.840.0.f2c652cbb28730b59fc273589968f3773218bc09.001.1');
    expect(typeof args.source_ts).toBe('number');
    expect(typeof args.fetched_at).toBe('number');
    const payload = JSON.parse(args.payload) as SevereWeatherAlertItem;
    expect(payload).toMatchObject({ event: 'Special Weather Statement', severity: 'Moderate' });
  });

  it('logs and skips the snapshot when the fetch fails', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('network down'));

    await expect(seedSevereWeatherAlerts()).resolves.toBeUndefined();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});

describe('module-level SQLite wiring', () => {
  it('ensures the severe_weather_alerts table with a CREATE TABLE IF NOT EXISTS guard', () => {
    expect(createTableSql).toContain('CREATE TABLE IF NOT EXISTS severe_weather_alerts');
    expect(createTableSql).toContain('id TEXT PRIMARY KEY');
  });

  it('prepares an INSERT OR IGNORE against the severe_weather_alerts table', () => {
    expect(insertSql).toContain('INSERT OR IGNORE INTO severe_weather_alerts');
  });
});

describe('default export contract', () => {
  it('registers as "severe-weather-alerts" on a 5-minute cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('severe-weather-alerts');
    expect(seeder.cron).toBe('*/5 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});