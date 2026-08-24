// Unit tests for the aviation-weather-hazards seeder's pure logic (NOAA AWC
// METAR/SIGMET parsing, polygon centroids, and end-to-end snapshot wiring).
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
  mapMetarToItem,
  computePolygonCentroid,
  mapSigmetToItem,
  seedAviationWeatherHazards,
  type AviationHazardItem,
  type MetarStation,
  type SigmetPolygon,
} from '../index';
import { setLiveSnapshot, fetchWithTimeout, withRetry, db } from '@worldwideview/seeder-sdk';

// db.prepare is invoked once per top-level statement at module load
// (index 0 = CREATE TABLE IF NOT EXISTS guard, index 1 = INSERT OR IGNORE).
const prepareMock = vi.mocked(db.prepare);
const createTableSql = prepareMock.mock.calls[0][0] as string;
const insertSql = prepareMock.mock.calls[1][0] as string;
const insertRunMock = prepareMock.mock.results[1].value.run as ReturnType<typeof vi.fn>;

// Real slice of https://aviationweather.gov/api/data/metar?ids=KMCI&format=json
// captured 2026-08-24T12:56Z.
const METAR_FIXTURE: MetarStation[] = [
  {
    icaoId: 'KMCI',
    receiptTime: '2026-08-24T12:56:36.259Z',
    obsTime: 1787575980,
    reportTime: '2026-08-24T13:00:00.000Z',
    temp: 20,
    dewp: 13.3,
    wdir: 120,
    wspd: 9,
    visib: '10+',
    altim: 1016.7,
    slp: 1015.8,
    qcField: 4,
    metarType: 'METAR',
    rawOb: 'METAR KMCI 241253Z 12009KT 10SM OVC110 20/13 A3002 RMK AO2 SLP158 T02000133',
    lat: 39.2975,
    lon: -94.7309,
    elev: 308,
    name: 'Kansas City Intl, MO, US',
    cover: 'OVC',
    clouds: [{ cover: 'OVC', base: 11000 }],
    fltCat: 'VFR',
  },
];

// Real slice of https://aviationweather.gov/api/data/sigmet?format=json
// captured 2026-08-24T12:49Z (first 2 of 11 convective SIGMETs).
const SIGMET_FIXTURE: SigmetPolygon[] = [
  {
    icaoId: 'KKCI',
    alphaChar: 'W',
    seriesId: '56W',
    validTimeFrom: 1787576100,
    validTimeTo: 1787583300,
    airSigmetType: 'SIGMET',
    hazard: 'CONVECTIVE',
    altitudeHi1: 35000,
    severity: 5,
    movementDir: 230,
    movementSpd: 20,
    rawAirSigmet: 'WSUS33 KKCI 241255\nSIGW \nCONVECTIVE SIGMET 56W\nVALID UNTIL 1455Z\nUT',
    coords: [
      { lat: 40.145, lon: -111.223 },
      { lat: 39.055, lon: -109.866 },
      { lat: 38.185, lon: -110.999 },
      { lat: 39.142, lon: -112.594 },
      { lat: 40.145, lon: -111.223 },
    ],
  },
  {
    icaoId: 'KKCI',
    alphaChar: 'W',
    seriesId: '57W',
    validTimeFrom: 1787576100,
    validTimeTo: 1787583300,
    airSigmetType: 'SIGMET',
    hazard: 'CONVECTIVE',
    altitudeHi1: 32000,
    severity: 5,
    movementDir: 180,
    movementSpd: 15,
    rawAirSigmet: 'WSUS33 KKCI 241255\nSIGW \nCONVECTIVE SIGMET 57W\nVALID UNTIL 1455Z\nAZ',
    coords: [
      { lat: 35.187, lon: -110.424 },
      { lat: 31.485, lon: -111.218 },
      { lat: 31.714, lon: -112.003 },
      { lat: 35.055, lon: -112.045 },
      { lat: 35.187, lon: -110.424 },
    ],
  },
];

beforeEach(() => {
  vi.clearAllMocks();
});

describe('mapMetarToItem', () => {
  it('maps a station record with lat/lon, wind, temperature, and visibility', () => {
    const item = mapMetarToItem(METAR_FIXTURE[0]);
    expect(item).not.toBeNull();
    expect(item).toMatchObject({
      id: 'KMCI',
      kind: 'metar',
      lat: 39.2975,
      lon: -94.7309,
      temp: 20,
      windDir: 120,
      windSpeed: 9,
      visibility: '10+',
      flightCategory: 'VFR',
      name: 'Kansas City Intl, MO, US',
    });
    expect(item?.rawReport).toContain('METAR KMCI');
  });

  it('rejects a station without finite coordinates', () => {
    const item = mapMetarToItem({ icaoId: 'KXXX', lat: null, lon: null });
    expect(item).toBeNull();
  });
});

describe('computePolygonCentroid', () => {
  it('averages polygon vertices, skipping the closing ring vertex', () => {
    const centroid = computePolygonCentroid(SIGMET_FIXTURE[0].coords);
    expect(centroid).not.toBeNull();
    expect(centroid?.lat).toBeCloseTo((40.145 + 39.055 + 38.185 + 39.142) / 4, 4);
    expect(centroid?.lon).toBeCloseTo((-111.223 - 109.866 - 110.999 - 112.594) / 4, 4);
  });

  it('returns null for an empty or invalid coordinate list', () => {
    expect(computePolygonCentroid([])).toBeNull();
    expect(computePolygonCentroid(null)).toBeNull();
    expect(computePolygonCentroid([{ lat: null, lon: null }])).toBeNull();
  });
});

describe('mapSigmetToItem', () => {
  it('maps a SIGMET polygon to a representative point', () => {
    const item = mapSigmetToItem(SIGMET_FIXTURE[0]);
    expect(item).not.toBeNull();
    expect(item).toMatchObject({
      id: 'KKCI-56W',
      kind: 'sigmet',
      hazard: 'CONVECTIVE',
      severity: 5,
      validTimeTo: 1787583300,
      windDir: 230,
      windSpeed: 20,
    });
    expect(item?.lat).toBeCloseTo((40.145 + 39.055 + 38.185 + 39.142) / 4, 4);
    expect(item?.lon).toBeCloseTo((-111.223 - 109.866 - 110.999 - 112.594) / 4, 4);
    expect(item?.rawReport).toContain('CONVECTIVE SIGMET 56W');
  });

  it('skips a SIGMET without usable polygon coordinates', () => {
    expect(mapSigmetToItem({ seriesId: '59W', coords: [] })).toBeNull();
  });
});

describe('seedAviationWeatherHazards integration', () => {
  it('fetches METAR + SIGMET, persists, and snapshots end-to-end', async () => {
    vi.mocked(fetchWithTimeout).mockImplementation(async (url: string) => {
      const body = url.includes('/metar?') ? METAR_FIXTURE : SIGMET_FIXTURE;
      return { json: async () => body } as never;
    });

    await seedAviationWeatherHazards();

    expect(withRetry).toHaveBeenCalledTimes(2);
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'aviation-weather-hazards',
      expect.objectContaining({
        source: 'aviation-weather-hazards',
        totalCount: 3, // 1 METAR station + 2 SIGMETs
        items: expect.any(Array),
      }),
      2700
    );
    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as {
      items: AviationHazardItem[];
      totalCount: number;
    };
    expect(snapshot.items).toHaveLength(snapshot.totalCount);
    expect(snapshot.items.filter((i) => i.kind === 'metar')).toHaveLength(1);
    expect(snapshot.items.filter((i) => i.kind === 'sigmet')).toHaveLength(2);
  });

  it('persists METAR and SIGMET items with their own source timestamps', async () => {
    vi.mocked(fetchWithTimeout).mockImplementation(async (url: string) => {
      const body = url.includes('/metar?') ? METAR_FIXTURE : SIGMET_FIXTURE;
      return { json: async () => body } as never;
    });

    await seedAviationWeatherHazards();

    expect(insertRunMock).toHaveBeenCalledTimes(3);
    const metarArgs = insertRunMock.mock.calls[0][0] as {
      id: string;
      payload: string;
      source_ts: number;
      fetched_at: number;
    };
    expect(metarArgs.id).toBe('KMCI');
    expect(metarArgs.source_ts).toBe(new Date('2026-08-24T13:00:00.000Z').getTime());
    expect(typeof metarArgs.fetched_at).toBe('number');

    const sigmetArgs = insertRunMock.mock.calls[1][0] as {
      id: string;
      payload: string;
      source_ts: number;
    };
    expect(sigmetArgs.id).toBe('KKCI-56W');
    expect(sigmetArgs.source_ts).toBe(1787576100 * 1000); // validTimeFrom epoch seconds -> ms
  });

  it('logs and skips the snapshot when the fetch fails', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('network down'));

    await expect(seedAviationWeatherHazards()).resolves.toBeUndefined();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});

describe('module-level SQLite wiring', () => {
  it('ensures the aviation_weather_hazards table with a CREATE TABLE IF NOT EXISTS guard', () => {
    expect(createTableSql).toContain('CREATE TABLE IF NOT EXISTS aviation_weather_hazards');
    expect(createTableSql).toContain('id TEXT PRIMARY KEY');
  });

  it('prepares an INSERT OR IGNORE against the aviation_weather_hazards table', () => {
    expect(insertSql).toContain('INSERT OR IGNORE INTO aviation_weather_hazards');
  });
});

describe('default export contract', () => {
  it('registers as "aviation-weather-hazards" on a 15-minute cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('aviation-weather-hazards');
    expect(seeder.cron).toBe('*/15 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});