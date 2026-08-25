import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

// GDACS (Global Disaster Alert and Coordination System) — official Joint
// Research Centre feed. JSON API: geteventlist/SEARCH returns a GeoJSON
// FeatureCollection (verified live 2026-08-25). The endpoint ignores all
// pagination parameters and always returns the most recent 100 events ordered
// by todate; a single request is the complete snapshot. Feeds refresh ~every
// 6 minutes. Data is CC BY 4.0 (credit GDACS).
const GDACS_API_URL = 'https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH';
const PLUGIN_ID = 'gdacs-disasters';
const SNAPSHOT_TTL_SECONDS = 1800; // 30 min for a 5-minute cadence

export interface GdacsDisasterItem {
  id: string; // `${eventtype}-${eventid}`
  latitude: number;
  longitude: number;
  timestamp: string; // ISO 8601 (fromdate, interpreted as UTC)
  eventtype: string | null; // EQ | TC | FL | VO | WF | DR
  eventname: string | null;
  name: string | null;
  description: string | null;
  alertlevel: string | null; // Green | Orange | Red
  alertscore: number | null;
  episodealertlevel: string | null;
  country: string | null;
  iso3: string | null;
  glide: string | null;
  source: string | null; // NEIC | JTWC | GLOFAS | GWIS | GDO | ...
  fromdate: string | null;
  todate: string | null;
  datemodified: string | null;
  severity: number | null;
  severitytext: string | null;
  severityunit: string | null;
  reportUrl: string | null;
  detailsUrl: string | null;
}

interface GdacsFeature {
  type?: string;
  geometry?: { type?: string; coordinates?: unknown } | null;
  properties?: Record<string, unknown> | null;
}

/**
 * GDACS timestamps arrive as "YYYY-MM-DDTHH:mm:ss" with NO timezone suffix
 * (GDACS times are UTC). Interpret a zone-less value as UTC (deterministic,
 * machine independent) and normalize to ISO; already-zoned values pass
 * through. Returns null for unparseable input.
 */
export function parseGdacsDate(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (trimmed.length === 0) return null;
  // Only append Z when the value carries no offset marker ('Z', '+hh:mm', '-hh:mm').
  const normalized = /(?:Z|[+-]\d{2}:?\d{2})$/.test(trimmed) ? trimmed : `${trimmed}Z`;
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

function num(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function str(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

export function parseGdacsPayload(payload: unknown): GdacsDisasterItem[] {
  if (typeof payload !== 'object' || payload === null) return [];
  const features = (payload as { features?: unknown }).features;
  if (!Array.isArray(features)) return [];

  const items: GdacsDisasterItem[] = [];

  for (const raw of features) {
    if (typeof raw !== 'object' || raw === null) continue;
    const feature = raw as GdacsFeature;
    const p = feature.properties ?? {};

    // Only surface current (latest-episode), non-temporary alerts.
    if (String(p.iscurrent) !== 'true') continue;
    if (String(p.istemporary) === 'true') continue;

    const coords = feature.geometry?.coordinates;
    if (!Array.isArray(coords) || coords.length < 2) continue;
    const lon = Number(coords[0]);
    const lat = Number(coords[1]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
    if (lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;

    const eventtype = str(p.eventtype);
    const eventid = num(p.eventid);
    const id = eventtype && eventid !== null ? `${eventtype}-${eventid}` : null;
    if (!id) continue;

    const url = (typeof p.url === 'object' && p.url !== null ? p.url : {}) as Record<
      string,
      unknown
    >;
    const severityData =
      typeof p.severitydata === 'object' && p.severitydata !== null
        ? (p.severitydata as Record<string, unknown>)
        : {};

    items.push({
      id,
      latitude: lat,
      longitude: lon,
      timestamp: parseGdacsDate(str(p.fromdate)) ?? new Date().toISOString(),
      eventtype: eventtype ?? null,
      eventname: str(p.eventname),
      name: str(p.name),
      description: str(p.description),
      alertlevel: str(p.alertlevel),
      alertscore: num(p.alertscore),
      episodealertlevel: str(p.episodealertlevel),
      country: str(p.country),
      iso3: str(p.iso3),
      glide: str(p.glide),
      source: str(p.source),
      fromdate: parseGdacsDate(str(p.fromdate)),
      todate: parseGdacsDate(str(p.todate)),
      datemodified: parseGdacsDate(str(p.datemodified)),
      severity: num(severityData.severity),
      severitytext: str(severityData.severitytext),
      severityunit: str(severityData.severityunit),
      reportUrl: str(url.report),
      detailsUrl: str(url.details),
    });
  }

  return items;
}

// Ensure the per-plugin SQLite table exists (INSERT OR IGNORE below requires
// it). The engine may pre-create tables, so IF NOT EXISTS keeps this safe.
try {
  db.prepare(
    'CREATE TABLE IF NOT EXISTS gdacs_disasters (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)'
  ).run();
} catch (err) {
  console.error('[GdacsDisasters] could not ensure SQLite table:', err instanceof Error ? err.message : err);
}

const insertGdacsDisaster = db.prepare(
  'INSERT OR IGNORE INTO gdacs_disasters (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)'
);

export async function seedGdacsDisasters() {
  try {
    console.log('[GdacsDisasters] Polling GDACS event list...');

    const res = await withRetry(() => fetchWithTimeout(GDACS_API_URL));
    const data = (await res.json()) as unknown;
    const fetchedAt = Date.now();

    const items = parseGdacsPayload(data);

    let insertedCount = 0;
    for (const item of items) {
      const sourceTs = Date.parse(item.timestamp);
      const result = insertGdacsDisaster.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: Number.isNaN(sourceTs) ? fetchedAt : sourceTs,
        fetched_at: fetchedAt,
      });
      if (result.changes > 0) insertedCount++;
    }

    console.log(`[GdacsDisasters] Parsed ${items.length} current events. Saved ${insertedCount} new to SQLite.`);

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
    console.error('[GdacsDisasters] seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: PLUGIN_ID,
  cron: '*/5 * * * *', // Every 5 minutes (GDACS refreshes ~every 6 min)
  fn: seedGdacsDisasters,
};