// Unit tests for the air-quality-openaq seeder (OpenAQ API v3).
//
// The fixtures are REAL slices of the live OpenAQ "latest" endpoints captured
// 2026-08-25 (one per parameter id: 2=pm25, 1=pm10, 3=o3, 5=no2), sanitized to
// only the fields the parser consumes (no API key, no SDK fields, no extra
// envelope metadata beyond meta.found).
//
// @worldwideview/seeder-sdk is fully mocked so better-sqlite3 native bindings
// never load in the test environment (same pattern as live-disasters and
// global-news-gdelt).
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { readFileSync } from 'node:fs';

vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn((fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(async () => ({
    ok: true,
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
  OPENAQ_SOURCE_URL,
  PARAMETER_IDS,
  aqiCategory,
  epaAqiFromPm25,
  fetchParameterPayload,
  mergeReadings,
  parseAllParameters,
  parseOpenAqResult,
  parseParameterPayload,
  seedAirQuality,
  type AirQualityItem,
  type ParsedReading,
} from '../index';
import { setLiveSnapshot, fetchWithTimeout, db } from '@worldwideview/seeder-sdk';

const readFixture = (name: string): unknown =>
  JSON.parse(readFileSync(new URL(`./fixtures/${name}`, import.meta.url), 'utf8')) as unknown;

const FIXTURES: Record<number, unknown> = {
  2: readFixture('param-2.json'),
  1: readFixture('param-1.json'),
  3: readFixture('param-3.json'),
  5: readFixture('param-5.json'),
};

// The sanitized fixture row shape (only the fields the parser consumes).
interface FixtureRow {
  datetime: { utc: string } | null;
  value: number;
  coordinates: { latitude: number; longitude: number } | null;
  sensorsId: number;
  locationsId: number;
}

const rows = (payload: unknown): FixtureRow[] => (payload as { results: FixtureRow[] }).results;

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

describe('table guard', () => {
  it('self-guards the air_quality table before any insert', () => {
    expect(createTableSql).toMatch(/CREATE TABLE IF NOT EXISTS air_quality/);
    expect(createTableSql).toContain('id TEXT PRIMARY KEY');
    expect(createTableSql).toContain('payload TEXT NOT NULL');
    expect(insertSql).toMatch(/INSERT OR IGNORE INTO air_quality/);
  });
});

describe('epaAqiFromPm25', () => {
  it('returns null for invalid inputs', () => {
    expect(epaAqiFromPm25(-1)).toBeNull();
    expect(epaAqiFromPm25(NaN)).toBeNull();
    expect(epaAqiFromPm25(Infinity)).toBeNull();
  });

  it('hits every EPA breakpoint boundary exactly', () => {
    const cases: Array<[number, number]> = [
      [0, 0],
      [12.0, 50],
      [12.1, 51],
      [35.4, 100],
      [35.5, 101],
      [55.4, 150],
      [55.5, 151],
      [150.4, 200],
      [150.5, 201],
      [250.4, 300],
      [250.5, 301],
      [500.4, 500],
      [600, 500], // above the index ceiling: clamped
    ];
    for (const [pm25, expected] of cases) {
      expect(epaAqiFromPm25(pm25)).toBe(expected);
    }
  });

  it('computes interior values piecewise-linearly', () => {
    expect(epaAqiFromPm25(22)).toBe(72); // 22 ug/m3 -> Moderate
    expect(epaAqiFromPm25(154.81)).toBe(205); // heavy spike -> 201-300 band
  });
});

describe('aqiCategory', () => {
  it('labels every EPA band', () => {
    expect(aqiCategory(0)).toBe('Good');
    expect(aqiCategory(50)).toBe('Good');
    expect(aqiCategory(51)).toBe('Moderate');
    expect(aqiCategory(100)).toBe('Moderate');
    expect(aqiCategory(101)).toBe('Unhealthy for Sensitive Groups');
    expect(aqiCategory(150)).toBe('Unhealthy for Sensitive Groups');
    expect(aqiCategory(151)).toBe('Unhealthy');
    expect(aqiCategory(200)).toBe('Unhealthy');
    expect(aqiCategory(201)).toBe('Very Unhealthy');
    expect(aqiCategory(300)).toBe('Very Unhealthy');
    expect(aqiCategory(301)).toBe('Hazardous');
    expect(aqiCategory(999)).toBe('Hazardous');
  });

  it('returns Unknown for missing or invalid aqi', () => {
    expect(aqiCategory(NaN)).toBe('Unknown');
    expect(aqiCategory(-5)).toBe('Unknown');
  });
});

describe('parseOpenAqResult', () => {
  it('maps a real live pm25 result to a reading', () => {
    const reading = parseOpenAqResult(rows(FIXTURES[2])[0], 'pm25');
    expect(reading).not.toBeNull();
    expect(reading!.field).toBe('pm25');
    expect(Number.isFinite(reading!.lat)).toBe(true);
    expect(Number.isFinite(reading!.lon)).toBe(true);
    expect(reading!.value).toBeGreaterThanOrEqual(0);
    expect(reading!.key).toBe(`${reading!.sensorsId}|${reading!.locationsId}|${reading!.datetimeUtc}`);
  });

  it('drops rows without usable coordinates', () => {
    const base = { datetime: { utc: '2026-08-25T11:00:00Z' }, value: 12, sensorsId: 1, locationsId: 2 };
    expect(parseOpenAqResult({ ...base }, 'pm25')).toBeNull();
    expect(parseOpenAqResult({ ...base, coordinates: null }, 'pm25')).toBeNull();
    expect(parseOpenAqResult({ ...base, coordinates: { latitude: 'x', longitude: 45 } }, 'pm25')).toBeNull();
    expect(parseOpenAqResult({ ...base, coordinates: { latitude: NaN, longitude: 45 } }, 'pm25')).toBeNull();
  });

  it('drops rows with missing ids, datetime, or non-positive values', () => {
    const good = {
      datetime: { utc: '2026-08-25T11:00:00Z' },
      value: 12,
      coordinates: { latitude: 10, longitude: 20 },
      sensorsId: 1,
      locationsId: 2,
    };
    expect(parseOpenAqResult({ ...good, sensorsId: undefined }, 'pm25')).toBeNull();
    expect(parseOpenAqResult({ ...good, locationsId: undefined }, 'pm25')).toBeNull();
    expect(parseOpenAqResult({ ...good, datetime: {} }, 'pm25')).toBeNull();
    expect(parseOpenAqResult({ ...good, value: -1 }, 'pm25')).toBeNull(); // real fixture has a -1 row
    expect(parseOpenAqResult({ ...good, value: 'n/a' }, 'pm25')).toBeNull();
  });

  it('rejects non-object results', () => {
    expect(parseOpenAqResult(null, 'pm25')).toBeNull();
    expect(parseOpenAqResult('x', 'pm25')).toBeNull();
  });
});

describe('parseParameterPayload', () => {
  it('returns [] for unknown parameter ids and non-array payloads', () => {
    expect(parseParameterPayload(FIXTURES[2], 999)).toEqual([]);
    expect(parseParameterPayload({ results: 'nope' }, 2)).toEqual([]);
    expect(parseParameterPayload(null, 2)).toEqual([]);
  });

  it('drops malformed rows from a real fixture (e.g. negative value)', () => {
    const readings = parseParameterPayload(FIXTURES[2], 2);
    expect(readings.length).toBeGreaterThan(0);
    for (const reading of readings) {
      expect(reading.value).toBeGreaterThanOrEqual(0);
      expect(Number.isFinite(reading.lat)).toBe(true);
      expect(Number.isFinite(reading.lon)).toBe(true);
    }
    const rawCount = ((FIXTURES[2] as { results: unknown[] }).results).length;
    expect(readings.length).toBeLessThanOrEqual(rawCount);
  });
});

describe('parseAllParameters (real fixtures, one pull per parameter)', () => {
  it('produces placed, well-shaped items with unique sensor keys', () => {
    const items = parseAllParameters(FIXTURES);

    expect(items.length).toBeGreaterThan(0);
    const ids = new Set(items.map((i) => i.id));
    expect(ids.size).toBe(items.length); // dedupe: no repeated sensor keys

    for (const item of items) {
      expect(Number.isFinite(item.lat)).toBe(true);
      expect(Number.isFinite(item.lon)).toBe(true);
      expect(item.url).toBe(OPENAQ_SOURCE_URL);
      expect(typeof item.date).toBe('string');
      const paramCount = [item.pm25, item.pm10, item.o3, item.no2].filter((v) => typeof v === 'number').length;
      expect(paramCount).toBeGreaterThanOrEqual(1);
      if (item.pm25 !== undefined) {
        expect(item.aqi).toBeGreaterThanOrEqual(0);
        expect(item.aqi).toBeLessThanOrEqual(500);
        expect(['Good', 'Moderate', 'Unhealthy for Sensitive Groups', 'Unhealthy', 'Very Unhealthy', 'Hazardous']).toContain(
          aqiCategory(item.aqi!)
        );
      }
    }
  });

  it('merges same-sensor readings across parameter pulls into one item', () => {
    const pm25Row = rows(FIXTURES[2])[0];
    // A pm10 pull reporting the SAME sensor/location/datetime: values merge,
    // one item carries both pm25 and pm10 plus aqi from pm25.
    const syntheticP1 = {
      results: [
        {
          datetime: pm25Row.datetime,
          value: 45,
          coordinates: pm25Row.coordinates,
          sensorsId: pm25Row.sensorsId,
          locationsId: pm25Row.locationsId,
        },
      ],
    };
    const items = parseAllParameters({ 2: FIXTURES[2], 1: syntheticP1 });
    expect(pm25Row.datetime).not.toBeNull();
    const merged = items.find((i) => i.id === `${pm25Row.sensorsId}|${pm25Row.locationsId}|${pm25Row.datetime!.utc}`);
    expect(merged).toBeDefined();
    expect(merged!.pm25).toBe(pm25Row.value);
    expect(merged!.pm10).toBe(45);
    expect(merged!.aqi).toBe(epaAqiFromPm25(pm25Row.value));
    expect(pm25Row.value).toBeGreaterThanOrEqual(0);
  });

  it('mergeReadings keeps all parameter values on one key', () => {
    const row = rows(FIXTURES[2])[0];
    const readings: ParsedReading[] = [
      {
        key: '1|1|2026-08-25T00:00:00Z',
        sensorsId: 1,
        locationsId: 1,
        datetimeUtc: '2026-08-25T00:00:00Z',
        field: 'pm25',
        value: 10,
        lat: row.coordinates!.latitude,
        lon: row.coordinates!.longitude,
      },
      {
        key: '1|1|2026-08-25T00:00:00Z',
        sensorsId: 1,
        locationsId: 1,
        datetimeUtc: '2026-08-25T00:00:00Z',
        field: 'no2',
        value: 25,
        lat: row.coordinates!.latitude,
        lon: row.coordinates!.longitude,
      },
    ];
    const items = mergeReadings(readings);
    expect(items).toHaveLength(1);
    expect(items[0].pm25).toBe(10);
    expect(items[0].no2).toBe(25);
    expect(items[0].aqi).toBe(42); // 10 ug/m3 -> band 0-50: (50/12)*10
  });
});

describe('fetchParameterPayload', () => {
  it('uses the parameter endpoint with limit 1000 and offset pages', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      ok: true,
      json: async () => ({ results: Array.from({ length: 1000 }, () => ({})), meta: { found: 1000 } }),
    } as unknown as Response);
    await fetchParameterPayload(2, 'test-key');
    expect(fetchWithTimeout).toHaveBeenCalled();
    const url = vi.mocked(fetchWithTimeout).mock.calls[0][0] as string;
    expect(url).toBe('https://api.openaq.org/v3/parameters/2/latest?limit=1000&offset=0');
    const opts = vi.mocked(fetchWithTimeout).mock.calls[0][1] as { headers: Record<string, string> };
    expect(opts.headers['X-API-Key']).toBe('test-key');
  });

  it('skips (returns empty) when the API responds non-OK', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({ ok: false, status: 429 } as unknown as Response);
    const payload = await fetchParameterPayload(2, 'test-key');
    expect((payload as { results: unknown[] }).results).toEqual([]);
  });
});

describe('seedAirQuality', () => {
  it('skips when OPENAQ_API_KEY is not set', async () => {
    vi.stubEnv('OPENAQ_API_KEY', '');
    await seedAirQuality();
    expect(fetchWithTimeout).not.toHaveBeenCalled();
    expect(setLiveSnapshot).not.toHaveBeenCalled();
  });

  it('pulls all 4 parameters and saves a snapshot', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      ok: true,
      json: async () => FIXTURES[2],
    } as unknown as Response);
    vi.stubEnv('OPENAQ_API_KEY', 'test-key');

    await seedAirQuality();

    expect(fetchWithTimeout).toHaveBeenCalledTimes(PARAMETER_IDS.length); // one page per parameter
    expect(insertRunMock).toHaveBeenCalled();
    expect(setLiveSnapshot).toHaveBeenCalledTimes(1);
    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as {
      source: string;
      totalCount: number;
      items: AirQualityItem[];
    };
    expect(snapshot.source).toBe('air-quality-openaq');
    expect(snapshot.totalCount).toBe(snapshot.items.length);
    expect(snapshot.totalCount).toBeGreaterThan(0);
  });
});