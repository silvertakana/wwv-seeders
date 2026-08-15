// Unit tests for the earthquakes seeder's pure logic (test-site proximity
// detection, GeoJSON feature mapping, and end-to-end snapshot wiring).
//
// The @worldwideview/seeder-sdk is fully mocked so better-sqlite3 native
// bindings never load in the test environment (same pattern as
// packages/market-tracker/src/__tests__/seederContract.test.ts and
// packages/wildfires/src/__tests__/index.test.ts). haversineKm is mocked so
// threshold boundaries (9.9 vs 10.1 km) can be asserted deterministically.
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn((fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(async () => ({ json: async () => ({}) })),
  haversineKm: vi.fn(),
  db: {
    prepare: vi.fn(() => ({
      run: vi.fn(() => ({ changes: 0 })),
    })),
  },
}));

import {
  detectTestSite,
  mapFeatureToItem,
  KNOWN_TEST_SITES,
  type EarthquakeItem,
  type USGSFeature,
} from '../index';
import { seedEarthquakes } from '../index';
import {
  setLiveSnapshot,
  fetchWithTimeout,
  withRetry,
  haversineKm,
  db,
} from '@worldwideview/seeder-sdk';

// db.prepare is invoked once at module load (top-level insertEarthquake
// statement). Capture the returned run mock so tests can assert per-feature
// insert calls after seedEarthquakes().
const prepareMock = vi.mocked(db.prepare);
const insertRunMock = prepareMock.mock.results[0].value.run as ReturnType<typeof vi.fn>;

function makeFeature(
  overrides: {
    id?: string;
    properties?: Partial<USGSFeature['properties']>;
    coordinates?: number[];
  } = {}
): USGSFeature {
  return {
    id: overrides.id ?? 'us7000abc',
    properties: {
      place: '12 km NE of Ridgecrest, CA',
      mag: 4.6,
      time: 1712000000000,
      url: 'https://earthquake.usgs.gov/earthquakes/eventpage/us7000abc',
      ...overrides.properties,
    },
    geometry: { coordinates: overrides.coordinates ?? [-117.5, 35.7, 8.0] },
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  // Default: every test site is far away (100 km), so tests that do not care
  // about proximity get non-near behavior. Tests override per case.
  vi.mocked(haversineKm).mockImplementation(() => 100);
});

describe('detectTestSite', () => {
  it('flags nearTestSite within 10km (9.9 km)', () => {
    vi.mocked(haversineKm).mockReturnValue(9.9);

    const result = detectTestSite(35.7, -117.5);

    expect(result).toEqual({
      nearTestSite: true,
      nearestSiteName: KNOWN_TEST_SITES[0].name,
      distanceToTestSiteKm: 9.9,
    });
  });

  it('does not flag at 10.1 km', () => {
    vi.mocked(haversineKm).mockReturnValue(10.1);

    const result = detectTestSite(35.7, -117.5);

    expect(result.nearTestSite).toBe(false);
    expect(result.nearestSiteName).toBeNull();
    expect(result.distanceToTestSiteKm).toBe(10.1);
  });

  it('treats exactly 10 km as NOT near (strict < 10 boundary)', () => {
    vi.mocked(haversineKm).mockReturnValue(10);

    const result = detectTestSite(35.7, -117.5);

    expect(result.nearTestSite).toBe(false);
    expect(result.nearestSiteName).toBeNull();
    expect(result.distanceToTestSiteKm).toBe(10);
  });

  it('reports the distance below 50 km', () => {
    vi.mocked(haversineKm).mockReturnValue(49.9);

    const result = detectTestSite(35.7, -117.5);

    expect(result.nearTestSite).toBe(false);
    expect(result.distanceToTestSiteKm).toBe(49.9);
  });

  it('omits the distance at exactly 50 km (strict < 50 boundary)', () => {
    vi.mocked(haversineKm).mockReturnValue(50);

    const result = detectTestSite(35.7, -117.5);

    expect(result.distanceToTestSiteKm).toBeUndefined();
  });

  it('omits the distance above 50 km', () => {
    vi.mocked(haversineKm).mockReturnValue(50.1);

    const result = detectTestSite(35.7, -117.5);

    expect(result.nearTestSite).toBe(false);
    expect(result.distanceToTestSiteKm).toBeUndefined();
  });

  it('handles a 0 km distance (exact test-site coordinates)', () => {
    vi.mocked(haversineKm).mockReturnValue(0);

    const result = detectTestSite(35.7, -117.5);

    expect(result.nearTestSite).toBe(true);
    expect(result.nearestSiteName).toBe(KNOWN_TEST_SITES[0].name);
    expect(result.distanceToTestSiteKm).toBe(0);
  });

  it('names the nearest site when it is within 10km', () => {
    // Distances in KNOWN_TEST_SITES order: nearest is index 2 (Nevada).
    const distances = [50, 40, 5, 60, 30, 20, 45, 55];
    let call = 0;
    vi.mocked(haversineKm).mockImplementation(() => distances[call++]);

    const result = detectTestSite(35.7, -117.5);

    expect(result.nearTestSite).toBe(true);
    expect(result.nearestSiteName).toBe(KNOWN_TEST_SITES[2].name);
    expect(result.distanceToTestSiteKm).toBe(5);
  });

  it('leaves nearestSiteName null when the nearest site is outside 10km', () => {
    // Nearest is index 2 (12 km) -> too far to name, but distance is reported.
    const distances = [100, 80, 12, 200];
    let call = 0;
    vi.mocked(haversineKm).mockImplementation(() => distances[call++]);

    const result = detectTestSite(35.7, -117.5);

    expect(result.nearTestSite).toBe(false);
    expect(result.nearestSiteName).toBeNull();
    expect(result.distanceToTestSiteKm).toBe(12);
  });

  it('checks every known test site exactly once with (lat, lon, siteLat, siteLon)', () => {
    detectTestSite(35.7, -117.5);

    expect(haversineKm).toHaveBeenCalledTimes(KNOWN_TEST_SITES.length);
    KNOWN_TEST_SITES.forEach((site, i) => {
      expect(haversineKm).toHaveBeenNthCalledWith(i + 1, 35.7, -117.5, site.lat, site.lon);
    });
  });

  it('exposes 8 known test sites with name and coordinates', () => {
    expect(KNOWN_TEST_SITES).toHaveLength(8);
    for (const site of KNOWN_TEST_SITES) {
      expect(typeof site.name).toBe('string');
      expect(typeof site.lat).toBe('number');
      expect(typeof site.lon).toBe('number');
    }
    expect(KNOWN_TEST_SITES.map((s) => s.name)).toEqual([
      'Punggye-ri (North Korea)',
      'Lop Nur (China)',
      'Nevada Test Site (USA)',
      'Semipalatinsk (Kazakhstan)',
      'Novaya Zemlya (Russia)',
      'Pokhran (India)',
      'Chagai (Pakistan)',
      'Moruroa (France)',
    ]);
  });
});

describe('mapFeatureToItem', () => {
  it('destructures geometry.coordinates into lon, lat, and depth', () => {
    const item = mapFeatureToItem(makeFeature({ coordinates: [-117.5, 35.7, 8.0] }));

    expect(item.lon).toBe(-117.5);
    expect(item.lat).toBe(35.7);
    expect(item.depth_km).toBe(8.0);
  });

  it('maps id, place, magnitude, occurredAt, and url', () => {
    const item = mapFeatureToItem(
      makeFeature({
        id: 'us7000xyz',
        properties: {
          place: '4 km SSW of Volcano, HI',
          mag: 5.1,
          time: 1712000111222,
          url: 'https://earthquake.usgs.gov/earthquakes/eventpage/us7000xyz',
        },
      })
    );

    expect(item.id).toBe('us7000xyz');
    expect(item.place).toBe('4 km SSW of Volcano, HI');
    expect(item.magnitude).toBe(5.1);
    expect(item.occurredAt).toBe(1712000111222);
    expect(item.url).toBe('https://earthquake.usgs.gov/earthquakes/eventpage/us7000xyz');
  });

  it('ignores extra coordinate elements beyond lon/lat/depth', () => {
    const item = mapFeatureToItem(makeFeature({ coordinates: [10.0, 20.0, 30.0, 99.0] }));

    expect(item.lon).toBe(10.0);
    expect(item.lat).toBe(20.0);
    expect(item.depth_km).toBe(30.0);
  });

  it('allows 2-element coordinates (depth becomes undefined)', () => {
    const item = mapFeatureToItem(makeFeature({ coordinates: [10.0, 20.0] }));

    expect(item.lon).toBe(10.0);
    expect(item.lat).toBe(20.0);
    expect(item.depth_km).toBeUndefined();
  });

  it('passes through a null magnitude', () => {
    const item = mapFeatureToItem(
      makeFeature({ properties: { mag: null as unknown as number } })
    );

    expect(item.magnitude).toBeNull();
  });

  it('propagates test-site proximity into the item', () => {
    vi.mocked(haversineKm).mockReturnValue(9.9);

    const item = mapFeatureToItem(makeFeature());

    expect(item.nearTestSite).toBe(true);
    expect(item.nearestSiteName).toBe(KNOWN_TEST_SITES[0].name);
    expect(item.distanceToTestSiteKm).toBe(9.9);
  });

  it('keeps proximity flags false and distance undefined for a distant quake', () => {
    const item = mapFeatureToItem(makeFeature());

    expect(item.nearTestSite).toBe(false);
    expect(item.nearestSiteName).toBeNull();
    expect(item.distanceToTestSiteKm).toBeUndefined();
  });
});

describe('seedEarthquakes integration', () => {
  function makeGeoJson(features: USGSFeature[]) {
    return { type: 'FeatureCollection', features };
  }

  it('fetches, maps, persists, and snapshots end-to-end with mocked IO', async () => {
    // First feature: every site 9.9 km away (near). Second feature: every
    // site 100 km away (far). Each feature triggers 8 haversine calls.
    let call = 0;
    vi.mocked(haversineKm).mockImplementation(() => (call++ < 8 ? 9.9 : 100));
    const near = makeFeature({ id: 'us7000abc', properties: { place: 'Near Punggye-ri' } });
    const far = makeFeature({
      id: 'us7000def',
      coordinates: [10.0, 20.0, 5.0],
      properties: { place: 'Somewhere far' },
    });
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => makeGeoJson([near, far]),
    } as never);

    await seedEarthquakes();

    expect(withRetry).toHaveBeenCalledTimes(1);
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'earthquakes',
      expect.objectContaining({
        source: 'earthquakes',
        totalCount: 2,
        items: expect.any(Array),
      }),
      3600
    );
    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as {
      items: EarthquakeItem[];
      totalCount: number;
    };
    expect(snapshot.items).toHaveLength(snapshot.totalCount);
    expect(snapshot.items[0]).toMatchObject({
      id: 'us7000abc',
      place: 'Near Punggye-ri',
      lat: 35.7,
      lon: -117.5,
      depth_km: 8.0,
      nearTestSite: true,
      nearestSiteName: KNOWN_TEST_SITES[0].name,
      distanceToTestSiteKm: 9.9,
    });
    expect(snapshot.items[1]).toMatchObject({
      id: 'us7000def',
      lat: 20.0,
      lon: 10.0,
      depth_km: 5.0,
      nearTestSite: false,
      distanceToTestSiteKm: undefined,
    });
  });

  it('persists each feature with id, JSON payload, source_ts, and fetched_at', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => makeGeoJson([makeFeature({ id: 'us7000abc' })]),
    } as never);

    await seedEarthquakes();

    expect(insertRunMock).toHaveBeenCalledTimes(1);
    const args = insertRunMock.mock.calls[0][0] as {
      id: string;
      payload: string;
      source_ts: number;
      fetched_at: number;
    };
    expect(args.id).toBe('us7000abc');
    expect(args.source_ts).toBe(1712000000000);
    expect(typeof args.fetched_at).toBe('number');
    const payload = JSON.parse(args.payload) as EarthquakeItem;
    expect(payload).toMatchObject({
      id: 'us7000abc',
      place: '12 km NE of Ridgecrest, CA',
      magnitude: 4.6,
      lat: 35.7,
      lon: -117.5,
      depth_km: 8.0,
      occurredAt: 1712000000000,
      url: 'https://earthquake.usgs.gov/earthquakes/eventpage/us7000abc',
    });
  });

  it('warns and skips the snapshot on an invalid response', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => ({}),
    } as never);

    await seedEarthquakes();

    expect(warnSpy).toHaveBeenCalledWith('[Earthquakes] Invalid response from USGS');
    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    warnSpy.mockRestore();
  });
});

describe('default export contract', () => {
  it('registers as "earthquakes" on an hourly cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('earthquakes');
    expect(seeder.cron).toBe('0 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});
