// Unit tests for the hurricane-storms seeder's pure logic (NHC storm mapping,
// active-storm snapshot wiring, and default export contract).
//
// The @worldwideview/seeder-sdk is fully mocked so better-sqlite3 native
// bindings never load in the test environment (same pattern as the earthquakes
// seeder's __tests__/index.test.ts).
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn((fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(async () => ({ json: async () => ({}) })),
  db: {
    prepare: vi.fn(() => ({
      run: vi.fn(() => ({ changes: 0 })),
    })),
  },
}));

import {
  mapActiveStormToItem,
  seedHurricaneStorms,
  type HurricaneStormItem,
  type NhcActiveStorm,
} from '../index';
import {
  db,
  fetchWithTimeout,
  setLiveSnapshot,
  withRetry,
} from '@worldwideview/seeder-sdk';

// db.prepare is invoked once at module load (top-level insertStorm statement).
const prepareMock = vi.mocked(db.prepare);
const insertRunMock = prepareMock.mock.results[0].value.run as ReturnType<typeof vi.fn>;

const STORM: NhcActiveStorm = {
  id: 1,
  name: 'AL052026',
  classification: 'HU',
  intensity: 120,
  pressure: 955,
  latitudeNumeric: 25.4,
  longitudeNumeric: -78.2,
  movementDir: 'NW',
  movementSpeed: 12,
  lastUpdate: '2026-08-24T12:00:00Z',
  publicAdvisory: { url: 'https://www.nhc.noaa.gov/archive/2026/al05/public/' },
  forecastDiscussion: { url: 'https://www.nhc.noaa.gov/archive/2026/al05/dis/' },
  forecastTrack: {
    kmzFile: 'https://www.nhc.noaa.gov/storm_graphics/2026/AL052026/AL052026.kmz',
    zipFile: 'https://www.nhc.noaa.gov/storm_graphics/2026/AL052026/AL052026.zip',
  },
};

beforeEach(() => {
  vi.clearAllMocks();
});

describe('mapActiveStormToItem', () => {
  it('maps a valid storm to the persisted item shape', () => {
    const item = mapActiveStormToItem(STORM)!;
    expect(item).not.toBeNull();
    expect(item.id).toBe('1');
    expect(item.name).toBe('AL052026');
    expect(item.classification).toBe('HU');
    expect(item.intensity).toBe(120);
    expect(item.pressure).toBe(955);
    expect(item.lat).toBe(25.4);
    expect(item.lon).toBe(-78.2);
    expect(item.lastUpdate).toBe('2026-08-24T12:00:00Z');
    expect(item.advisoryUrl).toBe('https://www.nhc.noaa.gov/archive/2026/al05/public/');
    expect(item.forecastUrl).toBe('https://www.nhc.noaa.gov/storm_graphics/2026/AL052026/AL052026.kmz');
    expect(item.discussionUrl).toBe('https://www.nhc.noaa.gov/archive/2026/al05/dis/');
  });

  it('returns null when the storm has no finite coordinates', () => {
    expect(mapActiveStormToItem({ ...STORM, latitudeNumeric: null })).toBeNull();
    expect(mapActiveStormToItem({ ...STORM, longitudeNumeric: null })).toBeNull();
    expect(mapActiveStormToItem({ ...STORM, latitudeNumeric: Number.NaN })).toBeNull();
    expect(mapActiveStormToItem({ ...STORM, longitudeNumeric: 'x' as unknown as number })).toBeNull();
  });

  it('falls back to null for missing optional values', () => {
    const item = mapActiveStormToItem({
      ...STORM,
      name: null,
      intensity: null,
      pressure: null,
      publicAdvisory: undefined,
      forecastDiscussion: null,
      forecastTrack: undefined,
    })!;
    expect(item.name).toBe('Unnamed Storm');
    expect(item.intensity).toBeNull();
    expect(item.pressure).toBeNull();
    expect(item.advisoryUrl).toBeNull();
    expect(item.forecastUrl).toBeNull();
    expect(item.discussionUrl).toBeNull();
  });
});

describe('seedHurricaneStorms integration', () => {
  it('fetches, maps, persists, and snapshots active storms end-to-end', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => ({
        activeStorms: [
          STORM,
          { ...STORM, id: 2, latitudeNumeric: null }, // invalid coords -> skipped
        ],
      }),
    } as never);

    await seedHurricaneStorms();

    expect(withRetry).toHaveBeenCalledTimes(1);
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'hurricane-storms',
      expect.objectContaining({
        source: 'hurricane-storms',
        totalCount: 1,
        items: expect.any(Array),
      }),
      3600
    );
    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as {
      items: HurricaneStormItem[];
      totalCount: number;
    };
    expect(snapshot.items).toHaveLength(1);
    expect(snapshot.items[0]).toMatchObject({
      id: '1',
      name: 'AL052026',
      classification: 'HU',
      intensity: 120,
      lat: 25.4,
      lon: -78.2,
    });
    expect(insertRunMock).toHaveBeenCalledTimes(1);
  });

  it('warns and skips the snapshot on an invalid response', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => ({}),
    } as never);

    await seedHurricaneStorms();

    expect(warnSpy).toHaveBeenCalledWith('[HurricaneStorms] Invalid response from NOAA NHC');
    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    warnSpy.mockRestore();
  });
});

describe('default export contract', () => {
  it('registers as "hurricane-storms" on an hourly cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('hurricane-storms');
    expect(seeder.cron).toBe('0 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});