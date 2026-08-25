// Air quality seeder backed by the OpenAQ API v3 "latest" endpoints.
//
// The plugin frontend fetches ${engineBase}/api/air-quality-openaq and consumes
// items shaped { lat, lon, pm25?, pm10?, o3?, no2?, aqi?, date?, url? }. The
// engine serves that endpoint from this seeder's SQLite table, so the seeder
// self-guards the table (CREATE TABLE IF NOT EXISTS) before inserting,
// mirroring live-disasters and global-news-gdelt.
//
// OpenAQ v3: GET /parameters/{id}/latest?limit=1000&offset=N with the
// X-API-Key header. Parameter ids: 2=pm25, 1=pm10, 3=o3 (mass), 5=no2 (mass);
// the endpoint caps `limit` at 1000 (422 above). Documented rate limit is
// 60 req/min and 2000 req/hour per key, so requests are paced and the global
// pull is paginated within modest page bounds.
import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

const OPENAQ_BASE = 'https://api.openaq.org/v3/parameters';
export const OPENAQ_SOURCE_URL = 'https://openaq.org';
const PLUGIN_ID = 'air-quality-openaq';
const SNAPSHOT_TTL_SECONDS = 1800; // 30 min
const PAGE_SIZE = 1000; // OpenAQ caps limit at 1000 (422 above)
const MAX_PAGES_PER_PARAMETER = 3;
const REQUEST_DELAY_MS = 800; // keep well under the 60 req/min limit

// OpenAQ v3 parameter ids used by this seeder.
export const PARAMETER_IDS = [2, 1, 3, 5] as const;

export type AqField = 'pm25' | 'pm10' | 'o3' | 'no2';

export const PARAMETER_FIELD: Record<number, AqField> = {
  1: 'pm10',
  2: 'pm25',
  3: 'o3',
  5: 'no2',
};

export interface AirQualityItem {
  id: string; // sensorsId|locationsId|datetime - dedupe key, DB primary key
  lat: number;
  lon: number;
  pm25?: number;
  pm10?: number;
  o3?: number;
  no2?: number;
  aqi?: number;
  date?: string;
  url?: string;
}

// EPA PM2.5 (24-hour) concentration breakpoints with the matching AQI window.
// Bands are contiguous ascending; the first band whose cHigh covers the
// concentration wins. Above the index ceiling the AQI is clamped to 500.
export const EPA_PM25_BANDS = [
  { cLow: 0, cHigh: 12.0, iLow: 0, iHigh: 50 },
  { cLow: 12.1, cHigh: 35.4, iLow: 51, iHigh: 100 },
  { cLow: 35.5, cHigh: 55.4, iLow: 101, iHigh: 150 },
  { cLow: 55.5, cHigh: 150.4, iLow: 151, iHigh: 200 },
  { cLow: 150.5, cHigh: 250.4, iLow: 201, iHigh: 300 },
  { cLow: 250.5, cHigh: 500.4, iLow: 301, iHigh: 500 },
] as const;

export function epaAqiFromPm25(pm25: number): number | null {
  if (!Number.isFinite(pm25) || pm25 < 0) return null;
  const band = EPA_PM25_BANDS.find((b) => pm25 <= b.cHigh);
  if (!band) return 500; // above the index ceiling: clamp
  const { cLow, cHigh, iLow, iHigh } = band;
  return Math.round(((iHigh - iLow) / (cHigh - cLow)) * (pm25 - cLow) + iLow);
}

// One AQI category label per EPA band; 'Unknown' for missing/invalid values
// (the frontend shows an "unknown" band when aqi is absent).
export function aqiCategory(aqi: number): string {
  if (!Number.isFinite(aqi) || aqi < 0) return 'Unknown';
  if (aqi <= 50) return 'Good';
  if (aqi <= 100) return 'Moderate';
  if (aqi <= 150) return 'Unhealthy for Sensitive Groups';
  if (aqi <= 200) return 'Unhealthy';
  if (aqi <= 300) return 'Very Unhealthy';
  return 'Hazardous';
}

export interface ParsedReading {
  key: string; // sensorsId|locationsId|datetime.utc - dedupe key
  sensorsId: number;
  locationsId: number;
  datetimeUtc: string;
  field: AqField;
  value: number;
  lat: number;
  lon: number;
}

// Map one OpenAQ v3 "latest" result to a ParsedReading. Rows without finite
// coordinates, without sensorsId/locationsId, without a UTC datetime, or with
// a non-positive value are dropped: they could never be placed on the globe
// and/or carry no usable measurement.
export function parseOpenAqResult(result: unknown, field: AqField): ParsedReading | null {
  if (result === null || typeof result !== 'object') return null;
  const r = result as Record<string, unknown>;

  const coords = r.coordinates as Record<string, unknown> | null;
  const lat = typeof coords?.latitude === 'number' ? coords.latitude : NaN;
  const lon = typeof coords?.longitude === 'number' ? coords.longitude : NaN;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

  const { sensorsId, locationsId } = r;
  if (typeof sensorsId !== 'number' || typeof locationsId !== 'number') return null;

  const datetime = r.datetime as Record<string, unknown> | null;
  const utc = typeof datetime?.utc === 'string' ? datetime.utc : null;
  if (!utc) return null;

  const value = typeof r.value === 'number' ? r.value : Number(r.value);
  if (!Number.isFinite(value) || value < 0) return null;

  return {
    key: `${sensorsId}|${locationsId}|${utc}`,
    sensorsId,
    locationsId,
    datetimeUtc: utc,
    field,
    value,
    lat,
    lon,
  };
}

export function parseParameterPayload(payload: unknown, parameterId: number): ParsedReading[] {
  const field = PARAMETER_FIELD[parameterId];
  if (!field || payload === null || typeof payload !== 'object') return [];
  const results = (payload as Record<string, unknown>).results;
  if (!Array.isArray(results)) return [];
  const readings: ParsedReading[] = [];
  for (const result of results) {
    const reading = parseOpenAqResult(result, field);
    if (reading) readings.push(reading);
  }
  return readings;
}

// Merge readings across the parameter pulls into one item per sensor (dedupe
// key: sensorsId + locationsId + datetime). A sensor measures one parameter,
// so the merged item carries that parameter's value; aqi derives from pm25
// wherever a pm25 reading exists.
export function mergeReadings(readings: ParsedReading[]): AirQualityItem[] {
  const byKey = new Map<string, AirQualityItem>();
  for (const reading of readings) {
    let item = byKey.get(reading.key);
    if (!item) {
      item = {
        id: reading.key,
        lat: reading.lat,
        lon: reading.lon,
        date: reading.datetimeUtc,
        url: OPENAQ_SOURCE_URL,
      };
      byKey.set(reading.key, item);
    }
    item[reading.field] = reading.value;
    if (reading.field === 'pm25') {
      const aqi = epaAqiFromPm25(reading.value);
      if (aqi !== null) item.aqi = aqi;
    }
  }
  return Array.from(byKey.values());
}

// Pure entry point: {parameterId -> raw payload}. Used by the seeder and by
// the unit tests (which feed real sanitized API fixtures).
export function parseAllParameters(payloads: Record<number, unknown>): AirQualityItem[] {
  const readings: ParsedReading[] = [];
  for (const parameterId of PARAMETER_IDS) {
    const payload = payloads[parameterId];
    if (payload === undefined) continue;
    readings.push(...parseParameterPayload(payload, parameterId));
  }
  return mergeReadings(readings);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Fetch one parameter's latest readings, paginating within bounds. Returns a
// payload-shaped { results } object ready for parseParameterPayload. On a
// non-OK page (e.g. 429 throttle) or a thrown fetch error the parameter is
// skipped with a warning; the pm25 pull may still land, which is the
// documented fallback when throttled.
export async function fetchParameterPayload(parameterId: number, apiKey: string): Promise<unknown> {
  const results: unknown[] = [];
  for (let page = 0; page < MAX_PAGES_PER_PARAMETER; page++) {
    const url = `${OPENAQ_BASE}/${parameterId}/latest?limit=${PAGE_SIZE}&offset=${page * PAGE_SIZE}`;
    let res: Response;
    try {
      res = await withRetry(() =>
        fetchWithTimeout(url, {
          headers: { 'X-API-Key': apiKey, 'User-Agent': 'WWV-Data-Engine' },
        })
      );
    } catch (err) {
      console.warn(
        `[AirQualityOpenAQ] param ${parameterId} page ${page} failed: ${err instanceof Error ? err.message : String(err)}`
      );
      break;
    }
    if (!res.ok) {
      console.warn(`[AirQualityOpenAQ] param ${parameterId} page ${page}: HTTP ${res.status}`);
      break;
    }
    const data = (await res.json()) as Record<string, unknown>;
    const pageResults = Array.isArray(data.results) ? (data.results as unknown[]) : [];
    results.push(...pageResults);
    if (pageResults.length < PAGE_SIZE) break;
    await sleep(REQUEST_DELAY_MS);
  }
  return { results };
}

// Ensure the per-plugin SQLite table exists (INSERT OR IGNORE below requires
// it). The engine may pre-create tables, so IF NOT EXISTS keeps this safe.
try {
  db.prepare(
    'CREATE TABLE IF NOT EXISTS air_quality (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)'
  ).run();
} catch (err) {
  console.error('[AirQualityOpenAQ] could not ensure SQLite table:', err instanceof Error ? err.message : err);
}

const insertReading = db.prepare(
  'INSERT OR IGNORE INTO air_quality (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)'
);

export async function seedAirQuality(): Promise<void> {
  const apiKey = process.env.OPENAQ_API_KEY;
  if (!apiKey) {
    console.warn('[AirQualityOpenAQ] OPENAQ_API_KEY not set - skipping.');
    return;
  }

  try {
    console.log('[AirQualityOpenAQ] Polling OpenAQ v3 latest readings...');
    const payloads: Record<number, unknown> = {};
    for (const parameterId of PARAMETER_IDS) {
      try {
        payloads[parameterId] = await fetchParameterPayload(parameterId, apiKey);
      } catch (err) {
        console.warn(
          `[AirQualityOpenAQ] param ${parameterId} skipped: ${err instanceof Error ? err.message : String(err)}`
        );
      }
    }

    const items = parseAllParameters(payloads);
    if (items.length === 0) {
      console.warn('[AirQualityOpenAQ] No readings after all pulls; skipping snapshot.');
      return;
    }

    const fetchedAt = Date.now();
    let insertedCount = 0;
    for (const item of items) {
      const result = insertReading.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: item.date ? Date.parse(item.date) || fetchedAt : fetchedAt,
        fetched_at: fetchedAt,
      });
      if (result.changes > 0) insertedCount++;
    }

    console.log(`[AirQualityOpenAQ] Parsed ${items.length} sensors. Saved ${insertedCount} new to SQLite.`);

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
    console.error('[AirQualityOpenAQ] seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: PLUGIN_ID,
  cron: '*/30 * * * *', // Every 30 minutes
  fn: seedAirQuality,
};