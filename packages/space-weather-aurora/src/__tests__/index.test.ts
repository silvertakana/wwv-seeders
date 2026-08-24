// Unit tests for the space-weather-aurora seeder's pure logic (NOAA SWPC
// ovation/Kp parsing, item mapping, and end-to-end snapshot wiring).
//
// The @worldwideview/seeder-sdk is fully mocked so better-sqlite3 native
// bindings never load in the test environment (same pattern as
// packages/live-disasters/src/__tests__/index.test.ts).
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn((fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(async () => ({ json: async () => ({}), text: async () => '' })),
  db: {
    prepare: vi.fn(() => ({
      run: vi.fn(() => ({ changes: 0 })),
    })),
  },
}));

import {
  toUtcIso,
  mapOvationToItems,
  mapLatestKpToItem,
  seedSpaceWeatherAurora,
  type AuroraPointItem,
  type KpRecord,
  type OvationPayload,
} from '../index';
import { setLiveSnapshot, fetchWithTimeout, withRetry, db } from '@worldwideview/seeder-sdk';

// db.prepare is invoked once per top-level statement at module load
// (index 0 = CREATE TABLE IF NOT EXISTS guard, index 1 = INSERT OR IGNORE).
const prepareMock = vi.mocked(db.prepare);
const createTableSql = prepareMock.mock.calls[0][0] as string;
const insertSql = prepareMock.mock.calls[1][0] as string;
const insertRunMock = prepareMock.mock.results[1].value.run as ReturnType<typeof vi.fn>;

// Real slice of https://services.swpc.noaa.gov/json/ovation_aurora_latest.json
// captured 2026-08-24T13:36:00Z (2 zero-intensity + 14 aurora oval points).
// Triples are [Longitude, Latitude, Aurora] per the file's "Data Format".
const OVATION_FIXTURE: OvationPayload = {
  'Observation Time': '2026-08-24T13:36:00Z',
  'Forecast Time': '2026-08-24T14:50:00Z',
  'Data Format': '[Longitude, Latitude, Aurora]',
  type: 'MultiPoint',
  coordinates: [
    [0, -89, 0],
    [0, -80, 0],
    [0, -90, 3],
    [0, -88, 3],
    [0, -87, 4],
    [0, -86, 3],
    [0, -85, 2],
    [0, -84, 2],
    [0, -83, 1],
    [0, -82, 1],
    [0, -81, 1],
    [0, -1, 1],
    [0, 0, 2],
    [0, 45, 1],
    [0, 46, 1],
    [0, 47, 1],
  ],
};

// Real slice of https://services.swpc.noaa.gov/json/planetary_k_index_1m.json
// captured 2026-08-24T13:37..13:39Z (last 3 of 358 records).
const KP_FIXTURE: KpRecord[] = [
  { time_tag: '2026-08-24T13:37:00', kp_index: 0, estimated_kp: 0, kp: '0Z' },
  { time_tag: '2026-08-24T13:38:00', kp_index: 0, estimated_kp: 0, kp: '0Z' },
  { time_tag: '2026-08-24T13:39:00', kp_index: 0, estimated_kp: 0, kp: '0Z' },
];

beforeEach(() => {
  vi.clearAllMocks();
});

describe('toUtcIso', () => {
  it('keeps a Z-suffixed UTC timestamp', () => {
    expect(toUtcIso('2026-08-24T13:36:00Z')).toBe('2026-08-24T13:36:00.000Z');
  });

  it('appends Z to a trailing-Z-less NOAA timestamp', () => {
    expect(toUtcIso('2026-08-24T13:39:00')).toBe('2026-08-24T13:39:00.000Z');
  });

  it('falls back to now for an unparseable timestamp', () => {
    const before = Date.now();
    const iso = toUtcIso('not-a-date');
    const after = Date.now();
    const ts = new Date(iso).getTime();
    expect(ts).toBeGreaterThanOrEqual(before);
    expect(ts).toBeLessThanOrEqual(after);
  });
});

describe('mapOvationToItems', () => {
  it('maps [lon, lat, intensity] triples to aurora-oval points', () => {
    const items = mapOvationToItems(OVATION_FIXTURE);
    expect(items).toHaveLength(14);
    expect(items[0]).toMatchObject({ id: 'aurora-0--90', lon: 0, lat: -90, intensity: 3 });
  });

  it('drops zero-intensity points that sit outside the oval', () => {
    const items = mapOvationToItems(OVATION_FIXTURE);
    expect(items.every((p) => (p.intensity ?? 0) > 0)).toBe(true);
    expect(items.some((p) => p.lat === -89)).toBe(false); // [0,-89,0] filtered
  });

  it('carries the ovation observation and forecast times', () => {
    const items = mapOvationToItems(OVATION_FIXTURE);
    expect(items[0]?.observedAt).toBe('2026-08-24T13:36:00Z');
    expect(items[0]?.forecastTime).toBe('2026-08-24T14:50:00Z');
    expect(items[0]?.kind).toBe('aurora-oval');
  });

  it('rejects non-finite coordinates and out-of-range latitudes', () => {
    const payload: OvationPayload = {
      'Observation Time': '2026-08-24T13:36:00Z',
      coordinates: [
        [0, 45, 1],
        [0, Number.NaN, 1],
        [0, 95, 1],
        [0, -70, 3],
      ],
    };
    const items = mapOvationToItems(payload);
    expect(items).toHaveLength(2);
    expect(items.map((p) => p.lat).sort()).toEqual([-70, 45]);
  });

  it('thins the grid down when it exceeds the snapshot cap (20k)', () => {
    const coords: number[][] = [];
    for (let lon = 0; lon < 360; lon++) {
      for (let lat = -60; lat <= 60; lat += 2) {
        coords.push([lon, lat, 1]); // 360 * 61 = 21,960 points
      }
    }
    const items = mapOvationToItems({ coordinates: coords });
    expect(items.length).toBeGreaterThan(0);
    expect(items.length).toBeLessThan(coords.length);
  });
});

describe('mapLatestKpToItem', () => {
  it('emits the most recent Kp reading as a synthetic global point', () => {
    const item = mapLatestKpToItem(KP_FIXTURE);
    expect(item).not.toBeNull();
    expect(item).toMatchObject({
      id: 'kp-2026-08-24T13:39:00.000Z',
      kind: 'kp-index',
      lat: 0,
      lon: 0,
      kpIndex: 0,
      intensity: null,
      forecastTime: null,
    });
    expect(item?.observedAt).toBe('2026-08-24T13:39:00.000Z');
  });

  it('returns null for an empty Kp list', () => {
    expect(mapLatestKpToItem([])).toBeNull();
  });
});

describe('seedSpaceWeatherAurora integration', () => {
  it('fetches both feeds, persists points, and snapshots end-to-end', async () => {
    vi.mocked(fetchWithTimeout).mockImplementation(async (url: string) => {
      const body = url.includes('ovation') ? OVATION_FIXTURE : KP_FIXTURE;
      return { json: async () => body } as never;
    });

    await seedSpaceWeatherAurora();

    expect(withRetry).toHaveBeenCalledTimes(2);
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'space-weather-aurora',
      expect.objectContaining({
        source: 'space-weather-aurora',
        totalCount: 15, // 14 aurora points + 1 Kp point
        items: expect.any(Array),
      }),
      900
    );
    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as {
      items: AuroraPointItem[];
      totalCount: number;
    };
    expect(snapshot.items).toHaveLength(snapshot.totalCount);
    expect(snapshot.items.filter((p) => p.kind === 'aurora-oval')).toHaveLength(14);
    expect(snapshot.items.filter((p) => p.kind === 'kp-index')).toHaveLength(1);
  });

  it('persists each item with id, JSON payload, source_ts, and fetched_at', async () => {
    vi.mocked(fetchWithTimeout).mockImplementation(async (url: string) => {
      const body = url.includes('ovation') ? OVATION_FIXTURE : KP_FIXTURE;
      return { json: async () => body } as never;
    });

    await seedSpaceWeatherAurora();

    expect(insertRunMock).toHaveBeenCalledTimes(15);
    const args = insertRunMock.mock.calls[0][0] as {
      id: string;
      payload: string;
      source_ts: number;
      fetched_at: number;
    };
    expect(args.id).toBe('aurora-0--90');
    expect(args.source_ts).toBe(new Date('2026-08-24T13:36:00Z').getTime());
    expect(typeof args.fetched_at).toBe('number');
    const payload = JSON.parse(args.payload) as AuroraPointItem;
    expect(payload).toMatchObject({ id: 'aurora-0--90', kind: 'aurora-oval', intensity: 3 });

    const lastArgs = insertRunMock.mock.calls[14][0] as { id: string; payload: string };
    expect(lastArgs.id).toBe('kp-2026-08-24T13:39:00.000Z');
  });

  it('logs and skips the snapshot when the fetch fails', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('network down'));

    await expect(seedSpaceWeatherAurora()).resolves.toBeUndefined();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});

describe('module-level SQLite wiring', () => {
  it('ensures the space_weather_aurora table with a CREATE TABLE IF NOT EXISTS guard', () => {
    expect(createTableSql).toContain('CREATE TABLE IF NOT EXISTS space_weather_aurora');
    expect(createTableSql).toContain('id TEXT PRIMARY KEY');
  });

  it('prepares an INSERT OR IGNORE against the space_weather_aurora table', () => {
    expect(insertSql).toContain('INSERT OR IGNORE INTO space_weather_aurora');
  });
});

describe('default export contract', () => {
  it('registers as "space-weather-aurora" on a 5-minute cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('space-weather-aurora');
    expect(seeder.cron).toBe('*/5 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});