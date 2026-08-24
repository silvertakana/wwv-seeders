import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot, fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

const AWC_BASE = 'https://aviationweather.gov/api/data';
// A representative set of major hubs; the AWC METAR endpoint accepts a
// comma-separated icaoId list.
const METAR_STATIONS = ['KMCI', 'KJFK', 'KLAX', 'KORD', 'KATL', 'KDFW', 'EGLL', 'LFPG', 'EDDF', 'RJTT', 'YSSY', 'VIDP'];
const PLUGIN_ID = 'aviation-weather-hazards';
const SNAPSHOT_TTL_SECONDS = 2700; // 3x the 15-minute cadence

/** One record in the AWC METAR JSON response. */
export interface MetarStation {
  icaoId?: string | null;
  receiptTime?: string | null;
  obsTime?: number | null;
  reportTime?: string | null;
  temp?: number | null;
  dewp?: number | null;
  wdir?: number | null;
  wspd?: number | null;
  visib?: string | number | null;
  altim?: number | null;
  slp?: number | null;
  qcField?: number | null;
  metarType?: string | null;
  cover?: string | null;
  clouds?: Array<{ cover?: string | null; base?: number | null }> | null;
  lat?: number | null;
  lon?: number | null;
  elev?: number | null;
  name?: string | null;
  fltCat?: string | null;
  rawOb?: string | null;
}

export interface SigmetCoordinate {
  lat?: number | null;
  lon?: number | null;
}

/** One record in the AWC SIGMET JSON response (polygon in `coords`). */
export interface SigmetPolygon {
  icaoId?: string | null;
  alphaChar?: string | null;
  seriesId?: string | null;
  receiptTime?: string | null;
  creationTime?: string | null;
  validTimeFrom?: number | null; // epoch seconds
  validTimeTo?: number | null; // epoch seconds
  airSigmetType?: string | null;
  hazard?: string | null;
  altitudeHi1?: number | null;
  altitudeHi2?: number | null;
  altitudeLow1?: number | null;
  altitudeLow2?: number | null;
  postProcessFlag?: number | null;
  severity?: number | null;
  movementDir?: number | null;
  movementSpd?: number | null;
  rawAirSigmet?: string | null;
  coords?: SigmetCoordinate[] | null;
}

/** Unified item persisted to SQLite and pushed to the live cache. */
export interface AviationHazardItem {
  id: string;
  kind: 'metar' | 'sigmet';
  lat: number;
  lon: number;
  name: string | null;
  temp: number | null;
  windDir: number | null;
  windSpeed: number | null;
  visibility: string | number | null;
  flightCategory: string | null;
  hazard: string | null;
  severity: number | null;
  validTimeTo: number | null;
  rawReport: string | null;
}

function finiteOrNull(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

export function mapMetarToItem(metar: MetarStation): AviationHazardItem | null {
  const lat = metar.lat;
  const lon = metar.lon;
  if (typeof lat !== 'number' || !Number.isFinite(lat)) return null;
  if (typeof lon !== 'number' || !Number.isFinite(lon)) return null;

  return {
    id: metar.icaoId ?? `metar-${lat}-${lon}`,
    kind: 'metar',
    lat,
    lon,
    name: metar.name ?? metar.icaoId ?? null,
    temp: finiteOrNull(metar.temp),
    windDir: finiteOrNull(metar.wdir),
    windSpeed: finiteOrNull(metar.wspd),
    visibility: metar.visib ?? null,
    flightCategory: metar.fltCat ?? null,
    hazard: null,
    severity: null,
    validTimeTo: null,
    rawReport: metar.rawOb ?? null,
  };
}

/**
 * Representative (centroid) point of a SIGMET polygon. The ring's closing
 * vertex is skipped so it is not double-weighted.
 */
export function computePolygonCentroid(
  coords: SigmetCoordinate[] | null | undefined
): { lat: number; lon: number } | null {
  if (!Array.isArray(coords) || coords.length === 0) return null;

  const first = coords[0];
  const last = coords[coords.length - 1];
  const vertices =
    first &&
    last &&
    first.lat === last.lat &&
    first.lon === last.lon &&
    coords.length > 1
      ? coords.slice(0, -1)
      : coords;

  let latSum = 0;
  let lonSum = 0;
  let count = 0;
  for (const c of vertices) {
    if (typeof c.lat !== 'number' || !Number.isFinite(c.lat)) continue;
    if (typeof c.lon !== 'number' || !Number.isFinite(c.lon)) continue;
    latSum += c.lat;
    lonSum += c.lon;
    count++;
  }
  if (count === 0) return null;
  return { lat: latSum / count, lon: lonSum / count };
}

export function mapSigmetToItem(sigmet: SigmetPolygon): AviationHazardItem | null {
  const centroid = computePolygonCentroid(sigmet.coords);
  if (!centroid) return null;

  return {
    id: `${sigmet.icaoId ?? 'awc'}-${sigmet.seriesId ?? `${centroid.lat}-${centroid.lon}`}`,
    kind: 'sigmet',
    lat: centroid.lat,
    lon: centroid.lon,
    name: `${sigmet.airSigmetType ?? 'SIGMET'} ${sigmet.seriesId ?? ''}`.trim(),
    temp: null,
    windDir: finiteOrNull(sigmet.movementDir),
    windSpeed: finiteOrNull(sigmet.movementSpd),
    visibility: null,
    flightCategory: null,
    hazard: sigmet.hazard ?? null,
    severity: finiteOrNull(sigmet.severity),
    validTimeTo: typeof sigmet.validTimeTo === 'number' ? sigmet.validTimeTo : null,
    rawReport: sigmet.rawAirSigmet ?? null,
  };
}

// Ensure the per-plugin SQLite table exists (INSERT OR IGNORE below requires
// it). The engine may pre-create tables, so IF NOT EXISTS keeps this safe.
try {
  db.prepare(
    'CREATE TABLE IF NOT EXISTS aviation_weather_hazards (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)'
  ).run();
} catch (err) {
  console.error('[AviationWeatherHazards] could not ensure SQLite table:', err instanceof Error ? err.message : err);
}

const insertHazard = db.prepare(
  'INSERT OR IGNORE INTO aviation_weather_hazards (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)'
);

export async function seedAviationWeatherHazards() {
  console.log('[AviationWeatherHazards] Polling NOAA AWC...');
  try {
    const metarUrl = `${AWC_BASE}/metar?ids=${METAR_STATIONS.join(',')}&format=json`;
    const [metarRes, sigmetRes] = await Promise.all([
      withRetry(() => fetchWithTimeout(metarUrl)),
      withRetry(() => fetchWithTimeout(`${AWC_BASE}/sigmet?format=json`)),
    ]);
    const metarPayload = (await metarRes.json()) as MetarStation[];
    const sigmetPayload = (await sigmetRes.json()) as SigmetPolygon[];
    const fetchedAt = Date.now();

    const items: AviationHazardItem[] = [];
    let insertedCount = 0;

    if (Array.isArray(metarPayload)) {
      for (const metar of metarPayload) {
        const item = mapMetarToItem(metar);
        if (!item) continue;
        items.push(item);
        const sourceTs = metar.reportTime ? new Date(metar.reportTime).getTime() : fetchedAt;
        const result = insertHazard.run({
          id: item.id,
          payload: JSON.stringify(item),
          source_ts: sourceTs,
          fetched_at: fetchedAt,
        });
        if (result.changes > 0) insertedCount++;
      }
    }

    if (Array.isArray(sigmetPayload)) {
      for (const sigmet of sigmetPayload) {
        const item = mapSigmetToItem(sigmet);
        if (!item) continue;
        items.push(item);
        const sourceTs = typeof sigmet.validTimeFrom === 'number' ? sigmet.validTimeFrom * 1000 : fetchedAt;
        const result = insertHazard.run({
          id: item.id,
          payload: JSON.stringify(item),
          source_ts: sourceTs,
          fetched_at: fetchedAt,
        });
        if (result.changes > 0) insertedCount++;
      }
    }

    console.log(
      `[AviationWeatherHazards] Parsed ${items.length} hazard points. Saved ${insertedCount} new to SQLite.`
    );

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
    console.error('[AviationWeatherHazards] seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: PLUGIN_ID,
  cron: '*/15 * * * *', // Every 15 minutes
  fn: seedAviationWeatherHazards,
};