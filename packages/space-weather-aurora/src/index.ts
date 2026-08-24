import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot, fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

const OVATION_URL = 'https://services.swpc.noaa.gov/json/ovation_aurora_latest.json';
const KP_URL = 'https://services.swpc.noaa.gov/json/planetary_k_index_1m.json';
const PLUGIN_ID = 'space-weather-aurora';
const SNAPSHOT_TTL_SECONDS = 900; // 3x the 5-minute cadence
const MAX_AURORA_POINTS = 20000; // keep live-cache snapshots bounded

/** Raw shape of NOAA SWPC ovation_aurora_latest.json. */
export interface OvationPayload {
  'Observation Time'?: string;
  'Forecast Time'?: string;
  'Data Format'?: string;
  type?: string;
  // [Longitude, Latitude, Aurora-probability] triples (see "Data Format").
  coordinates: number[][];
}

export interface KpRecord {
  time_tag: string;
  kp_index: number | null;
  estimated_kp: number | null;
  kp: string | null;
}

export interface AuroraPointItem {
  id: string;
  kind: 'aurora-oval' | 'kp-index';
  lat: number;
  lon: number;
  intensity: number | null;
  kpIndex: number | null;
  observedAt: string; // ISO 8601 UTC
  forecastTime: string | null;
}

/** Normalize a NOAA timestamp (UTC, sometimes without a trailing Z) to ISO 8601. */
export function toUtcIso(value: string): string {
  const date = value.endsWith('Z') ? new Date(value) : new Date(`${value}Z`);
  return Number.isNaN(date.getTime()) ? new Date().toISOString() : date.toISOString();
}

function thinIfNeeded(items: AuroraPointItem[]): AuroraPointItem[] {
  if (items.length <= MAX_AURORA_POINTS) return items;
  const stride = Math.ceil(items.length / MAX_AURORA_POINTS);
  return items.filter((_, i) => i % stride === 0);
}

/**
 * Map the ovation grid to aurora-oval points. The file's "Data Format"
 * documents each triple as [Longitude, Latitude, Aurora], so element 0 is
 * longitude and element 1 is latitude. Zero-intensity points sit outside the
 * oval and are dropped.
 */
export function mapOvationToItems(payload: OvationPayload): AuroraPointItem[] {
  const observedAt = payload['Observation Time'] ?? new Date().toISOString();
  const forecastTime = payload['Forecast Time'] ?? null;
  const items: AuroraPointItem[] = [];

  for (const coord of payload.coordinates ?? []) {
    const lon = Number(coord[0]);
    const lat = Number(coord[1]);
    const intensity = Number(coord[2]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat) || !Number.isFinite(intensity)) continue;
    if (lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;
    if (intensity <= 0) continue;

    items.push({
      id: `aurora-${lon}-${lat}`,
      kind: 'aurora-oval',
      lat,
      lon,
      intensity,
      kpIndex: null,
      observedAt,
      forecastTime,
    });
  }

  return thinIfNeeded(items);
}

/**
 * The Kp index is a global geomagnetic index with no station location; emit
 * the most recent reading as a single synthetic point at the prime meridian so
 * the globe can display current geomagnetic activity.
 */
export function mapLatestKpToItem(entries: KpRecord[]): AuroraPointItem | null {
  if (!Array.isArray(entries) || entries.length === 0) return null;
  const latest = entries[entries.length - 1];
  const kpIndex = typeof latest.kp_index === 'number' && Number.isFinite(latest.kp_index) ? latest.kp_index : null;
  const observedAt = latest.time_tag ? toUtcIso(latest.time_tag) : new Date().toISOString();
  return {
    id: `kp-${observedAt}`,
    kind: 'kp-index',
    lat: 0,
    lon: 0,
    intensity: null,
    kpIndex,
    observedAt,
    forecastTime: null,
  };
}

// Ensure the per-plugin SQLite table exists (INSERT OR IGNORE below requires
// it). The engine may pre-create tables, so IF NOT EXISTS keeps this safe.
try {
  db.prepare(
    'CREATE TABLE IF NOT EXISTS space_weather_aurora (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)'
  ).run();
} catch (err) {
  console.error('[SpaceWeatherAurora] could not ensure SQLite table:', err instanceof Error ? err.message : err);
}

const insertAuroraPoint = db.prepare(
  'INSERT OR IGNORE INTO space_weather_aurora (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)'
);

export async function seedSpaceWeatherAurora() {
  console.log('[SpaceWeatherAurora] Polling NOAA SWPC...');
  try {
    const [ovationRes, kpRes] = await Promise.all([
      withRetry(() => fetchWithTimeout(OVATION_URL)),
      withRetry(() => fetchWithTimeout(KP_URL)),
    ]);
    const ovation = (await ovationRes.json()) as OvationPayload;
    const kpEntries = (await kpRes.json()) as KpRecord[];
    const fetchedAt = Date.now();

    const items: AuroraPointItem[] = mapOvationToItems(ovation);
    const kpItem = mapLatestKpToItem(kpEntries);
    if (kpItem) items.push(kpItem);

    let insertedCount = 0;
    for (const item of items) {
      const result = insertAuroraPoint.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: new Date(item.observedAt).getTime(),
        fetched_at: fetchedAt,
      });
      if (result.changes > 0) insertedCount++;
    }

    console.log(`[SpaceWeatherAurora] Parsed ${items.length} points. Saved ${insertedCount} new to SQLite.`);

    // Save to Redis Live Cache
    await setLiveSnapshot(
      PLUGIN_ID,
      {
        source: PLUGIN_ID,
        fetchedAt: new Date().toISOString(),
        items,
        totalCount: items.length,
      },
      SNAPSHOT_TTL_SECONDS
    );
  } catch (err) {
    console.error('[SpaceWeatherAurora] seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: PLUGIN_ID,
  cron: '*/5 * * * *', // Every 5 minutes
  fn: seedSpaceWeatherAurora,
};