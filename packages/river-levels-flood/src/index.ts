import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';
import { GAUGES } from './gauges';

// USGS NWIS instantaneous values (stage height, parameterCd=00065). An explicit
// comma-separated sites= list is REQUIRED: stateCd/period/siteStatus combos
// return 400/404. USGS asks for a contact User-Agent, which we set below.
const USGS_IV_URL = 'https://waterservices.usgs.gov/nwis/iv/';
const USGS_USER_AGENT = 'wwv-plugin-river-levels (contact: batch@worldwideview.dev)';
const PLUGIN_ID = 'river-levels-flood';
const SNAPSHOT_TTL_SECONDS = 5400; // 90 min for a 15-minute cadence
const BATCH_SIZE = 50; // sites per NWIS request

export interface RiverLevelItem {
  id: string; // USGS site code
  lat: number;
  lon: number;
  name: string;
  stage_ft: number;
  dateTime: string; // ISO 8601 with offset, e.g. 2026-08-24T09:30:00.000-04:00
}

// Minimal structural types for the ns1:timeSeriesResponseType envelope.
interface UsgsGeogLocation {
  latitude?: number;
  longitude?: number;
}

interface UsgsSourceInfo {
  siteName?: string;
  siteCode?: Array<{ value?: string }>;
  geoLocation?: { geogLocation?: UsgsGeogLocation };
}

interface UsgsSeriesValue {
  value?: string;
  dateTime?: string;
}

interface UsgsTimeSeries {
  sourceInfo?: UsgsSourceInfo;
  values?: Array<{ value?: UsgsSeriesValue[] }>;
}

interface UsgsIvEnvelope {
  value?: { timeSeries?: UsgsTimeSeries[] };
}

export function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

/**
 * Parse one NWIS iv JSON response into river-level items. A series is skipped
 * when it lacks usable coordinates (cannot be placed on the globe) or when its
 * latest stage value is not a finite number.
 */
export function parseIvResponse(payload: unknown): RiverLevelItem[] {
  if (typeof payload !== 'object' || payload === null) return [];
  const envelope = payload as UsgsIvEnvelope;
  const series = envelope.value?.timeSeries;
  if (!Array.isArray(series)) return [];

  const items: RiverLevelItem[] = [];
  for (const entry of series) {
    const info = entry.sourceInfo ?? {};
    const siteCode = info.siteCode?.[0]?.value;
    const latitude = Number(info.geoLocation?.geogLocation?.latitude);
    const longitude = Number(info.geoLocation?.geogLocation?.longitude);
    if (!siteCode || !Number.isFinite(latitude) || !Number.isFinite(longitude)) continue;

    const readings = entry.values?.[0]?.value ?? [];
    if (readings.length === 0) continue;
    const latest = readings[readings.length - 1]; // values are ordered oldest -> newest
    const stageFt = Number(latest.value);
    if (!Number.isFinite(stageFt)) continue;

    items.push({
      id: siteCode,
      lat: latitude,
      lon: longitude,
      name: info.siteName ?? siteCode,
      stage_ft: stageFt,
      dateTime: latest.dateTime ?? new Date().toISOString(),
    });
  }
  return items;
}

function itemEpochMs(item: RiverLevelItem): number {
  const parsed = new Date(item.dateTime);
  return Number.isNaN(parsed.getTime()) ? Date.now() : parsed.getTime();
}

// Ensure the per-plugin SQLite table exists (INSERT OR IGNORE below requires
// it). The engine may pre-create tables, so IF NOT EXISTS keeps this safe.
try {
  db.prepare(
    'CREATE TABLE IF NOT EXISTS river_levels_flood (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)'
  ).run();
} catch (err) {
  console.error('[RiverLevelsFlood] could not ensure SQLite table:', err instanceof Error ? err.message : err);
}

const insertGauge = db.prepare(
  'INSERT OR IGNORE INTO river_levels_flood (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)'
);

export async function seedRiverLevels() {
  try {
    console.log(`[RiverLevelsFlood] Polling USGS NWIS for ${GAUGES.length} gauges in batches of ${BATCH_SIZE}...`);

    // Dedupe across batches by site code (a site is never duplicated across
    // chunks, but this also guards against upstream repeating a series).
    const byId = new Map<string, RiverLevelItem>();
    const fetchedAt = Date.now();

    for (const chunk of chunkArray(GAUGES, BATCH_SIZE)) {
      const url = `${USGS_IV_URL}?format=json&sites=${chunk.map((g) => g.code).join(',')}&parameterCd=00065`;
      const res = await withRetry(() =>
        fetchWithTimeout(url, { headers: { 'User-Agent': USGS_USER_AGENT } })
      );
      const data = (await res.json()) as unknown;
      const items = parseIvResponse(data);
      for (const item of items) byId.set(item.id, item);
      console.log(`[RiverLevelsFlood] Batch of ${chunk.length} requested -> ${items.length} with data`);
    }

    const items = [...byId.values()];
    let insertedCount = 0;
    for (const item of items) {
      const result = insertGauge.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: itemEpochMs(item),
        fetched_at: fetchedAt,
      });
      if (result.changes > 0) insertedCount++;
    }

    console.log(`[RiverLevelsFlood] Parsed ${items.length} river gauges. Saved ${insertedCount} new to SQLite.`);

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
    console.error('[RiverLevelsFlood] seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: PLUGIN_ID,
  cron: '*/15 * * * *', // Every 15 minutes
  fn: seedRiverLevels,
};