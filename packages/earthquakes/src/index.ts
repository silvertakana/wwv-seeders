import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry, haversineKm } from '@worldwideview/seeder-sdk';

export const KNOWN_TEST_SITES = [
  { name: 'Punggye-ri (North Korea)', lat: 41.278, lon: 129.088 },
  { name: 'Lop Nur (China)', lat: 40.75, lon: 89.6 },
  { name: 'Nevada Test Site (USA)', lat: 37.13, lon: -116.04 },
  { name: 'Semipalatinsk (Kazakhstan)', lat: 50.4, lon: 77.8 },
  { name: 'Novaya Zemlya (Russia)', lat: 73.3, lon: 54.9 },
  { name: 'Pokhran (India)', lat: 27.09, lon: 71.75 },
  { name: 'Chagai (Pakistan)', lat: 28.79, lon: 64.91 },
  { name: 'Moruroa (France)', lat: -21.83, lon: -138.88 },
];

export interface TestSiteProximity {
  nearTestSite: boolean;
  nearestSiteName: string | null;
  distanceToTestSiteKm: number | undefined;
}

export interface EarthquakeItem {
  id: string;
  place: string;
  magnitude: number | null;
  depth_km: number;
  lat: number;
  lon: number;
  occurredAt: number;
  url: string;
  nearTestSite: boolean;
  nearestSiteName: string | null;
  distanceToTestSiteKm: number | undefined;
}

export interface USGSFeature {
  id: string;
  properties: {
    place: string;
    mag: number | null;
    time: number;
    url: string;
  };
  geometry: {
    coordinates: number[];
  };
}

// Detect proximity to known nuclear test sites. Within 10km is highly
// suspicious (flags nearTestSite and names the site); the distance itself is
// only reported when the nearest site is within 50km.
export function detectTestSite(lat: number, lon: number): TestSiteProximity {
  let nearTestSite = false;
  let nearestSiteName: string | null = null;
  let minDistance = Infinity;

  for (const site of KNOWN_TEST_SITES) {
    const dist = haversineKm(lat, lon, site.lat, site.lon);
    if (dist < minDistance) {
      minDistance = dist;
      if (dist < 10) { // Within 10km is highly suspicious
        nearTestSite = true;
        nearestSiteName = site.name;
      }
    }
  }

  return {
    nearTestSite,
    nearestSiteName,
    distanceToTestSiteKm: minDistance < 50 ? minDistance : undefined
  };
}

// Map a USGS GeoJSON feature to the earthquake item shape persisted to SQLite
// and pushed to the live cache. geometry.coordinates is [lon, lat, depth].
export function mapFeatureToItem(feature: USGSFeature): EarthquakeItem {
  const { id, properties, geometry } = feature;
  const [lon, lat, depth] = geometry.coordinates;
  const sourceTs = properties.time;

  const proximity = detectTestSite(lat, lon);

  return {
    id,
    place: properties.place,
    magnitude: properties.mag,
    depth_km: depth,
    lat,
    lon,
    occurredAt: sourceTs,
    url: properties.url,
    nearTestSite: proximity.nearTestSite,
    nearestSiteName: proximity.nearestSiteName,
    distanceToTestSiteKm: proximity.distanceToTestSiteKm
  };
}

const insertEarthquake = db.prepare('INSERT OR IGNORE INTO earthquakes (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)');

export async function seedEarthquakes() {
  console.log('[Earthquakes] Polling USGS...');
  
  // USGS GeoJSON Feed: 4.5+ magnitude, past 7 days
  const url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_week.geojson';

  const res = await withRetry(() => fetchWithTimeout(url));
  const data = await res.json();
  const fetchedAt = Date.now();

  if (!data?.features || !Array.isArray(data.features)) {
    console.warn('[Earthquakes] Invalid response from USGS');
    return;
  }

  const items: EarthquakeItem[] = [];
  let insertedCount = 0;

  for (const feature of data.features) {
    const item = mapFeatureToItem(feature);

    items.push(item);

    // Save to SQLite
    const result = insertEarthquake.run({
      id: item.id,
      payload: JSON.stringify(item),
      source_ts: item.occurredAt,
      fetched_at: fetchedAt
    });
    if (result.changes > 0) insertedCount++;
  }

  console.log(`[Earthquakes] Parsed ${items.length} earthquakes. Saved ${insertedCount} new to SQLite.`);

  // Save to Redis Live Cache
  await setLiveSnapshot('earthquakes', {
    source: "earthquakes",
    fetchedAt: new Date().toISOString(),
    items: items,
    totalCount: items.length
  }, 3600); // 1 hour TTL
}

export default {
  name: "earthquakes",
  cron: "0 * * * *", // Every hour
  fn: seedEarthquakes
};
