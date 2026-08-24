import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot, fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

const NEO_FEED_URL = 'https://api.nasa.gov/neo/rest/v1/feed';
// DEMO_KEY is NASA's public demo token (30 req/hour/IP). Set NASA_API_KEY
// for a production key; the seeder degrades gracefully with the demo token.
const NASA_API_KEY = process.env.NASA_API_KEY ?? 'DEMO_KEY';
const PLUGIN_ID = 'near-earth-objects';
const SNAPSHOT_TTL_SECONDS = 3600;

/**
 * Raw shape of one date bucket inside NeoWs `near_earth_objects`. Every
 * field the parser reads is optional so a malformed upstream entry can never
 * crash the seeder.
 */
export interface NeoWsFeed {
  element_count?: number;
  near_earth_objects?: Record<string, NeoWsNeo[]>;
}

export interface NeoWsNeo {
  id?: string;
  name?: string;
  nasa_jpl_url?: string;
  absolute_magnitude_h?: number | null;
  estimated_diameter?: {
    kilometers?: {
      estimated_diameter_min?: number | null;
      estimated_diameter_max?: number | null;
    };
  };
  is_potentially_hazardous_asteroid?: boolean;
  close_approach_data?: NeoWsCloseApproach[];
}

export interface NeoWsCloseApproach {
  close_approach_date?: string;
  relative_velocity?: {
    kilometers_per_second?: string | number;
  };
  miss_distance?: {
    kilometers?: string | number;
  };
  orbiting_body?: string;
}

/**
 * Item shape persisted to SQLite and pushed to the live cache. Each close
 * approach becomes one item (a single asteroid can have several), so item
 * count >= NeoWs `element_count`.
 *
 * lat/lon are a POSITIONAL PLACEHOLDER: NEOs are space objects with no
 * surface coordinates, so the point cannot be real. It is a deterministic
 * per-asteroid anchor (derived from the asteroid id hash) mapped onto
 * [-85, 85] lat / [-180, 180] lon purely so the globe has somewhere stable
 * to render the entity. All real data lives in the properties below; the
 * frontend should render `nasaJplUrl` via urlProp and treat the point as
 * visualization-only.
 */
export interface NearEarthObjectItem {
  id: string;
  name: string;
  lat: number; // positional placeholder, stable per asteroid id
  lon: number; // positional placeholder, stable per asteroid id
  closeApproachDate: string; // YYYY-MM-DD
  orbitingBody: string | null;
  missDistanceKm: number | null;
  relativeVelocityKms: number | null;
  diameterKmMin: number | null;
  diameterKmMax: number | null;
  absoluteMagnitudeH: number | null;
  potentiallyHazardous: boolean;
  nasaJplUrl: string | null;
}

// FNV-1a 32-bit hash: stable across runs and platforms, no Math.random, so
// the placeholder point for a given asteroid never moves between polls.
function hashString(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/** Deterministic positional placeholder for a space object (see item docs). */
export function placeholderPoint(seed: string): { lat: number; lon: number } {
  const hash = hashString(seed);
  const lat = -85 + ((hash % 1_000_000) / 1_000_000) * 170;
  const lon = -180 + ((Math.floor(hash / 1_000_000) % 1_000_000) / 1_000_000) * 360;
  return {
    lat: Math.round(lat * 10_000) / 10_000,
    lon: Math.round(lon * 10_000) / 10_000,
  };
}

function finiteNumberOrNull(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

// NeoWs ships velocity and miss distance as strings; coerce defensively.
function toFiniteNumber(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined || value === '') return null;
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

/** Flatten a NeoWs feed into one item per close-approach entry. */
export function mapNeoFeedToItems(feed: NeoWsFeed): NearEarthObjectItem[] {
  const items: NearEarthObjectItem[] = [];
  const byDate = feed.near_earth_objects;
  if (!byDate || typeof byDate !== 'object') return items;

  for (const dateKey of Object.keys(byDate)) {
    for (const neo of byDate[dateKey] ?? []) {
      const objectId = neo.id;
      const name = neo.name;
      if (!objectId && !name) continue;

      // Stable per asteroid (seeded by the object id, not the approach date),
      // so an asteroid with several approaches keeps one visualization point.
      const point = placeholderPoint(objectId ?? name ?? 'neo');
      const displayName = name ?? objectId ?? 'Unnamed NEO';

      for (const approach of neo.close_approach_data ?? []) {
        const closeApproachDate = approach.close_approach_date;
        if (!closeApproachDate) continue;

        items.push({
          id: `neo-${objectId ?? name}-${closeApproachDate}`,
          name: displayName,
          lat: point.lat,
          lon: point.lon,
          closeApproachDate,
          orbitingBody: approach.orbiting_body ?? null,
          missDistanceKm: toFiniteNumber(approach.miss_distance?.kilometers),
          relativeVelocityKms: toFiniteNumber(approach.relative_velocity?.kilometers_per_second),
          diameterKmMin: finiteNumberOrNull(neo.estimated_diameter?.kilometers?.estimated_diameter_min),
          diameterKmMax: finiteNumberOrNull(neo.estimated_diameter?.kilometers?.estimated_diameter_max),
          absoluteMagnitudeH: finiteNumberOrNull(neo.absolute_magnitude_h),
          potentiallyHazardous: neo.is_potentially_hazardous_asteroid === true,
          nasaJplUrl: neo.nasa_jpl_url ?? null,
        });
      }
    }
  }
  return items;
}

// Ensure the per-plugin SQLite table exists (INSERT OR IGNORE below requires
// it). The engine may pre-create tables, so IF NOT EXISTS keeps this safe.
try {
  db.prepare(
    'CREATE TABLE IF NOT EXISTS near_earth_objects (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)'
  ).run();
} catch (err) {
  console.error('[NearEarthObjects] could not ensure SQLite table:', err instanceof Error ? err.message : err);
}

const insertNeo = db.prepare(
  'INSERT OR IGNORE INTO near_earth_objects (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)'
);

export async function seedNearEarthObjects() {
  console.log('[NearEarthObjects] Polling NASA NeoWs...');
  try {
    const today = new Date().toISOString().slice(0, 10);
    const res = await withRetry(
      () => fetchWithTimeout(`${NEO_FEED_URL}?start_date=${today}&end_date=${today}&api_key=${NASA_API_KEY}`)
    );
    const data = (await res.json()) as NeoWsFeed;
    const fetchedAt = Date.now();

    if (!data || !data.near_earth_objects || typeof data.near_earth_objects !== 'object') {
      console.warn('[NearEarthObjects] Invalid response from NASA NeoWs');
      return;
    }

    const items = mapNeoFeedToItems(data);
    let insertedCount = 0;
    for (const item of items) {
      const sourceTs = new Date(`${item.closeApproachDate}T00:00:00.000Z`).getTime();
      const result = insertNeo.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: Number.isFinite(sourceTs) ? sourceTs : fetchedAt,
        fetched_at: fetchedAt,
      });
      if (result.changes > 0) insertedCount++;
    }

    console.log(`[NearEarthObjects] Parsed ${items.length} close approaches. Saved ${insertedCount} new to SQLite.`);

    // Save to Redis Live Cache
    await setLiveSnapshot(
      PLUGIN_ID,
      {
        source: PLUGIN_ID,
        fetchedAt: new Date().toISOString(),
        items,
        totalCount: items.length,
      },
      SNAPSHOT_TTL_SECONDS // 1 hour TTL
    );
  } catch (err) {
    console.error('[NearEarthObjects] seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: PLUGIN_ID,
  cron: '0 * * * *', // Every hour
  fn: seedNearEarthObjects,
};