import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

// NOAA/NWS API: currently active alerts. A descriptive User-Agent is REQUIRED
// (the API rejects requests without one, often with HTTP 400).
const NWS_ALERTS_URL = 'https://api.weather.gov/alerts/active';
const NWS_USER_AGENT = 'wwv-plugin/1.0 (contact: batch@worldwideview.dev)';
const PLUGIN_ID = 'severe-weather-alerts';
const SNAPSHOT_TTL_SECONDS = 1800; // 30 min for a 5-minute cadence

export interface SevereWeatherAlertItem {
  id: string;
  lat: number;
  lon: number;
  event: string | null;
  severity: string | null;
  urgency: string | null;
  headline: string | null;
  description: string | null;
  instruction: string | null;
  sent: string | null;
  effective: string | null;
  expires: string | null;
  areaDesc: string | null;
}

type GeoPoint = [number, number];

interface PolygonGeometry {
  type: 'Polygon';
  coordinates: GeoPoint[][];
}

interface MultiPolygonGeometry {
  type: 'MultiPolygon';
  coordinates: GeoPoint[][][];
}

export type NwsGeometry = PolygonGeometry | MultiPolygonGeometry | null;

export interface NwsAlertFeature {
  id?: string;
  geometry: NwsGeometry;
  properties: {
    id?: string;
    areaDesc?: string | null;
    event?: string | null;
    severity?: string | null;
    urgency?: string | null;
    sent?: string | null;
    effective?: string | null;
    expires?: string | null;
    headline?: string | null;
    description?: string | null;
    instruction?: string | null;
  } | null;
}

/**
 * Compute a representative point for an alert geometry. api.weather.gov gives
 * alerts a POLYGON (or MultiPolygon) footprint; we reduce it to a single point
 * by averaging the vertices of the outer ring of the largest polygon (a simple
 * centroid, good enough to anchor an alert marker on the globe). Alerts without
 * geometry (null) yield null and are never emitted.
 */
export function polygonRepresentativePoint(geometry: NwsGeometry): { lat: number; lon: number } | null {
  if (!geometry) return null;
  const polygons = geometry.type === 'Polygon' ? [geometry.coordinates] : geometry.coordinates;

  let outer: GeoPoint[] | null = null;
  for (const polygon of polygons) {
    const ring = polygon[0];
    if (!Array.isArray(ring) || ring.length < 3) continue;
    if (outer === null || ring.length > outer.length) outer = ring;
  }
  if (outer === null) return null;

  let sumLat = 0;
  let sumLon = 0;
  let n = 0;
  for (const point of outer) {
    const lon = Number(point[0]);
    const lat = Number(point[1]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
    sumLon += lon;
    sumLat += lat;
    n += 1;
  }
  if (n < 3) return null;

  const lat = Math.max(-90, Math.min(90, sumLat / n));
  const lon = (((sumLon / n) + 540) % 360) - 180; // normalize to [-180, 180]
  return { lat, lon };
}

export function parseActiveAlerts(payload: unknown): SevereWeatherAlertItem[] {
  if (typeof payload !== 'object' || payload === null) return [];
  const features = (payload as { features?: unknown }).features;
  if (!Array.isArray(features)) return [];

  const items: SevereWeatherAlertItem[] = [];
  for (const raw of features) {
    if (typeof raw !== 'object' || raw === null) continue;
    const feature = raw as NwsAlertFeature;

    const point = polygonRepresentativePoint(feature.geometry);
    if (!point) continue; // never emit null-geometry alerts

    const p = feature.properties ?? {};
    items.push({
      id: p.id ?? feature.id ?? `nws-${items.length}`,
      lat: point.lat,
      lon: point.lon,
      event: p.event ?? null,
      severity: p.severity ?? null,
      urgency: p.urgency ?? null,
      headline: p.headline ?? null,
      description: p.description ?? null,
      instruction: p.instruction ?? null,
      sent: p.sent ?? null,
      effective: p.effective ?? null,
      expires: p.expires ?? null,
      areaDesc: p.areaDesc ?? null,
    });
  }
  return items;
}

function alertEpochMs(item: SevereWeatherAlertItem): number {
  const value = item.sent ?? item.effective;
  if (value) {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) return parsed.getTime();
  }
  return Date.now();
}

// Ensure the per-plugin SQLite table exists (INSERT OR IGNORE below requires
// it). The engine may pre-create tables, so IF NOT EXISTS keeps this safe.
try {
  db.prepare(
    'CREATE TABLE IF NOT EXISTS severe_weather_alerts (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)'
  ).run();
} catch (err) {
  console.error(
    '[SevereWeatherAlerts] could not ensure SQLite table:',
    err instanceof Error ? err.message : err
  );
}

const insertAlert = db.prepare(
  'INSERT OR IGNORE INTO severe_weather_alerts (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)'
);

export async function seedSevereWeatherAlerts() {
  try {
    console.log('[SevereWeatherAlerts] Polling NWS active alerts...');

    const res = await withRetry(() =>
      fetchWithTimeout(NWS_ALERTS_URL, { headers: { 'User-Agent': NWS_USER_AGENT } })
    );
    const data = (await res.json()) as unknown;
    const fetchedAt = Date.now();

    const items = parseActiveAlerts(data);

    let insertedCount = 0;
    for (const item of items) {
      const result = insertAlert.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: alertEpochMs(item),
        fetched_at: fetchedAt,
      });
      if (result.changes > 0) insertedCount++;
    }

    console.log(`[SevereWeatherAlerts] Parsed ${items.length} alerts. Saved ${insertedCount} new to SQLite.`);

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
    console.error('[SevereWeatherAlerts] seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: PLUGIN_ID,
  cron: '*/5 * * * *', // Every 5 minutes
  fn: seedSevereWeatherAlerts,
};