// Unit tests for the near-earth-objects seeder's pure logic (NASA NeoWs
// parsing, positional-placeholder mapping, and end-to-end snapshot wiring).
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
  placeholderPoint,
  mapNeoFeedToItems,
  seedNearEarthObjects,
  type NeoWsFeed,
  type NearEarthObjectItem,
} from '../index';
import { setLiveSnapshot, fetchWithTimeout, withRetry, db } from '@worldwideview/seeder-sdk';

// db.prepare is invoked once per top-level statement at module load
// (index 0 = CREATE TABLE IF NOT EXISTS guard, index 1 = INSERT OR IGNORE).
// Capture the run mock for the INSERT so tests can assert per-item inserts.
const prepareMock = vi.mocked(db.prepare);
const createTableSql = prepareMock.mock.calls[0][0] as string;
const insertSql = prepareMock.mock.calls[1][0] as string;
const insertRunMock = prepareMock.mock.results[1].value.run as ReturnType<typeof vi.fn>;

// Real slice of the NASA NeoWs single-day feed for 2026-08-24 (fetched live
// with the DEMO_KEY token; values copied verbatim, trimmed to the fields the
// parser reads). Asteroid 3374389 is augmented with a second close-approach
// entry (2026-08-25, same entry format) to exercise the one-item-per-approach
// expansion; every other value is exactly what the API returned.
const FEED_FIXTURE: NeoWsFeed = {
  element_count: 4,
  near_earth_objects: {
    '2026-08-24': [
      {
        id: '3374389',
        name: '(2007 HL4)',
        nasa_jpl_url: 'https://ssd.jpl.nasa.gov/tools/sbdb_lookup.html#/?sstr=3374389',
        absolute_magnitude_h: 24.1,
        estimated_diameter: {
          kilometers: {
            estimated_diameter_min: 0.040230458,
            estimated_diameter_max: 0.0899580388,
          },
        },
        is_potentially_hazardous_asteroid: false,
        close_approach_data: [
          {
            close_approach_date: '2026-08-24',
            relative_velocity: { kilometers_per_second: '4.0516048025' },
            miss_distance: { kilometers: '34623113.407563317' },
            orbiting_body: 'Earth',
          },
          {
            close_approach_date: '2026-08-25',
            relative_velocity: { kilometers_per_second: '4.0291704871' },
            miss_distance: { kilometers: '34277055.4647415' },
            orbiting_body: 'Earth',
          },
        ],
      },
      {
        id: '3774093',
        name: '(2017 HV3)',
        nasa_jpl_url: 'https://ssd.jpl.nasa.gov/tools/sbdb_lookup.html#/?sstr=3774093',
        absolute_magnitude_h: 23.75,
        estimated_diameter: {
          kilometers: {
            estimated_diameter_min: 0.0472666667,
            estimated_diameter_max: 0.1056914799,
          },
        },
        is_potentially_hazardous_asteroid: false,
        close_approach_data: [
          {
            close_approach_date: '2026-08-24',
            relative_velocity: { kilometers_per_second: '9.082799047' },
            miss_distance: { kilometers: '27073027.420095931' },
            orbiting_body: 'Earth',
          },
        ],
      },
      {
        id: '3840689',
        name: '(2019 FO)',
        nasa_jpl_url: 'https://ssd.jpl.nasa.gov/tools/sbdb_lookup.html#/?sstr=3840689',
        absolute_magnitude_h: 24.7,
        estimated_diameter: {
          kilometers: {
            estimated_diameter_min: 0.0305179233,
            estimated_diameter_max: 0.0682401509,
          },
        },
        is_potentially_hazardous_asteroid: false,
        close_approach_data: [
          {
            close_approach_date: '2026-08-24',
            relative_velocity: { kilometers_per_second: '11.8795885427' },
            miss_distance: { kilometers: '47124398.899167305' },
            orbiting_body: 'Earth',
          },
        ],
      },
      {
        id: '3893945',
        name: '(2019 XV)',
        nasa_jpl_url: 'https://ssd.jpl.nasa.gov/tools/sbdb_lookup.html#/?sstr=3893945',
        absolute_magnitude_h: 29.2,
        estimated_diameter: {
          kilometers: {
            estimated_diameter_min: 0.0038419789,
            estimated_diameter_max: 0.008590926,
          },
        },
        is_potentially_hazardous_asteroid: false,
        close_approach_data: [
          {
            close_approach_date: '2026-08-24',
            relative_velocity: { kilometers_per_second: '13.9197653106' },
            miss_distance: { kilometers: '62333549.332048197' },
            orbiting_body: 'Earth',
          },
        ],
      },
    ],
  },
};

beforeEach(() => {
  vi.clearAllMocks();
});

describe('placeholderPoint', () => {
  it('is deterministic for the same seed', () => {
    expect(placeholderPoint('3374389')).toEqual(placeholderPoint('3374389'));
  });

  it('stays inside the globe-friendly bounds for real asteroid ids', () => {
    for (const id of ['3374389', '3774093', '3840689', '3893945']) {
      const { lat, lon } = placeholderPoint(id);
      expect(lat).toBeGreaterThanOrEqual(-85);
      expect(lat).toBeLessThan(85);
      expect(lon).toBeGreaterThanOrEqual(-180);
      expect(lon).toBeLessThan(180);
    }
  });

  it('spreads different asteroid ids to different points', () => {
    expect(placeholderPoint('3374389')).not.toEqual(placeholderPoint('3893945'));
  });
});

describe('mapNeoFeedToItems', () => {
  it('maps a full close approach with every real property', () => {
    const items = mapNeoFeedToItems(FEED_FIXTURE);

    expect(items[0]).toMatchObject({
      id: 'neo-3374389-2026-08-24',
      name: '(2007 HL4)',
      lat: 70.1961,
      lon: -178.7245,
      closeApproachDate: '2026-08-24',
      orbitingBody: 'Earth',
      missDistanceKm: 34623113.407563317,
      relativeVelocityKms: 4.0516048025,
      diameterKmMin: 0.040230458,
      diameterKmMax: 0.0899580388,
      absoluteMagnitudeH: 24.1,
      potentiallyHazardous: false,
      nasaJplUrl: 'https://ssd.jpl.nasa.gov/tools/sbdb_lookup.html#/?sstr=3374389',
    });
  });

  it('emits one item per close approach (4 objects, 5 approaches)', () => {
    const items = mapNeoFeedToItems(FEED_FIXTURE);
    expect(items).toHaveLength(5);
    expect(items[0].id).toBe('neo-3374389-2026-08-24');
    expect(items[1].id).toBe('neo-3374389-2026-08-25');
  });

  it('keeps the positional placeholder stable across an asteroid\'s approaches', () => {
    const items = mapNeoFeedToItems(FEED_FIXTURE);
    expect(items[0].lat).toBe(items[1].lat);
    expect(items[0].lon).toBe(items[1].lon);
  });

  it('coerces NeoWs string numerics and keeps real NEO fields', () => {
    const items = mapNeoFeedToItems(FEED_FIXTURE);
    expect(items[4]).toMatchObject({
      id: 'neo-3893945-2026-08-24',
      name: '(2019 XV)',
      missDistanceKm: 62333549.332048197,
      relativeVelocityKms: 13.9197653106,
      diameterKmMax: 0.008590926,
    });
  });

  it('skips an object with neither id nor name', () => {
    const feed: NeoWsFeed = {
      near_earth_objects: {
        '2026-08-24': [{ close_approach_data: [{ close_approach_date: '2026-08-24' }] }],
      },
    };
    expect(mapNeoFeedToItems(feed)).toHaveLength(0);
  });

  it('skips an approach entry without a close_approach_date', () => {
    const feed: NeoWsFeed = {
      near_earth_objects: {
        '2026-08-24': [
          { id: '1', name: 'X', close_approach_data: [{ orbiting_body: 'Earth' }] },
        ],
      },
    };
    expect(mapNeoFeedToItems(feed)).toHaveLength(0);
  });

  it('returns an empty list for missing or malformed near_earth_objects', () => {
    expect(mapNeoFeedToItems({})).toHaveLength(0);
    expect(mapNeoFeedToItems({ near_earth_objects: {} })).toHaveLength(0);
  });
});

describe('seedNearEarthObjects integration', () => {
  it('fetches, parses, persists, and snapshots end-to-end with mocked IO', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => FEED_FIXTURE,
    } as never);

    await seedNearEarthObjects();

    expect(withRetry).toHaveBeenCalledTimes(1);
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'near-earth-objects',
      expect.objectContaining({
        source: 'near-earth-objects',
        totalCount: 5,
        items: expect.any(Array),
      }),
      3600
    );
    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as {
      items: NearEarthObjectItem[];
      totalCount: number;
    };
    expect(snapshot.items).toHaveLength(snapshot.totalCount);
    expect(snapshot.items[0]).toMatchObject({
      id: 'neo-3374389-2026-08-24',
      name: '(2007 HL4)',
      closeApproachDate: '2026-08-24',
      potentiallyHazardous: false,
    });
    expect(snapshot.items[1]).toMatchObject({ id: 'neo-3374389-2026-08-25' });
  });

  it('persists each item with id, JSON payload, source_ts, and fetched_at', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => FEED_FIXTURE,
    } as never);

    await seedNearEarthObjects();

    expect(insertRunMock).toHaveBeenCalledTimes(5);
    const args = insertRunMock.mock.calls[0][0] as {
      id: string;
      payload: string;
      source_ts: number;
      fetched_at: number;
    };
    expect(args.id).toBe('neo-3374389-2026-08-24');
    expect(args.source_ts).toBe(new Date('2026-08-24T00:00:00.000Z').getTime());
    expect(typeof args.fetched_at).toBe('number');
    const payload = JSON.parse(args.payload) as NearEarthObjectItem;
    expect(payload).toMatchObject({
      id: 'neo-3374389-2026-08-24',
      name: '(2007 HL4)',
      closeApproachDate: '2026-08-24',
    });
  });

  it('warns and skips the snapshot on an invalid response shape', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => ({ error: 'oops' }),
    } as never);

    await seedNearEarthObjects();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    warnSpy.mockRestore();
  });

  it('logs and skips the snapshot when the fetch fails', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('network down'));

    await expect(seedNearEarthObjects()).resolves.toBeUndefined();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});

describe('module-level SQLite wiring', () => {
  it('ensures the near_earth_objects table with a CREATE TABLE IF NOT EXISTS guard', () => {
    expect(createTableSql).toContain('CREATE TABLE IF NOT EXISTS near_earth_objects');
    expect(createTableSql).toContain('id TEXT PRIMARY KEY');
  });

  it('prepares an INSERT OR IGNORE against the near_earth_objects table', () => {
    expect(insertSql).toContain('INSERT OR IGNORE INTO near_earth_objects');
  });
});

describe('default export contract', () => {
  it('registers as "near-earth-objects" on an hourly cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('near-earth-objects');
    expect(seeder.cron).toBe('0 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});