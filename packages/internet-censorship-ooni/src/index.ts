import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot, fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

const OONI_API_BASE = 'https://api.ooni.io/api/v1/measurements';
const OONI_LIMIT = 10; // small per-country fetch; OONI is rate-limited and payloads are large
const PLUGIN_ID = 'internet-censorship-ooni';
const SNAPSHOT_TTL_SECONDS = 3600;

// A small, probe-dense set of jurisdictions where blocking is regularly seen.
const PROBE_COUNTRIES = ['US', 'CN', 'IR', 'RU', 'IN', 'BR', 'EG', 'TR', 'GB', 'DE', 'UA', 'KZ'];

// OONI measurements do not expose probe coordinates, so each country is
// represented by its approximate national centroid.
const COUNTRY_CENTROIDS: Record<string, { lat: number; lon: number }> = {
  US: { lat: 39.83, lon: -98.58 },
  CN: { lat: 35.86, lon: 104.19 },
  IR: { lat: 32.43, lon: 53.69 },
  RU: { lat: 61.52, lon: 105.32 },
  IN: { lat: 20.59, lon: 78.96 },
  BR: { lat: -14.24, lon: -51.93 },
  EG: { lat: 26.82, lon: 30.8 },
  TR: { lat: 38.96, lon: 35.24 },
  GB: { lat: 55.38, lon: -3.44 },
  DE: { lat: 51.16, lon: 10.45 },
  UA: { lat: 48.38, lon: 31.17 },
  KZ: { lat: 48.02, lon: 66.92 },
};

export interface OoniScores {
  blocking_general?: number;
  blocking_global?: number;
  blocking_country?: number;
  blocking_isp?: number;
  blocking_local?: number;
  confirmed?: boolean;
  fingerprints?: Array<Record<string, unknown>> | null;
  analysis?: { blocking_type?: string | null } | null;
}

/** One result in the OONI /measurements list response. */
export interface OoniMeasurement {
  anomaly: boolean;
  confirmed: boolean;
  failure: boolean;
  input: string | null;
  probe_asn: string | null;
  probe_cc: string;
  report_id?: string | null;
  scores?: OoniScores | null;
  test_name?: string | null;
  measurement_url?: string | null;
  measurement_start_time?: string | null;
  measurement_uid?: string | null;
  verification_status?: string | null;
}

/** Top-level shape of the OONI measurements API: { metadata, results }. */
export interface OoniResponse {
  metadata?: Record<string, unknown> | null;
  results?: OoniMeasurement[] | null;
}

export interface CensorshipEventItem {
  id: string;
  lat: number;
  lon: number;
  probeCc: string;
  probeAsn: string | null;
  testName: string | null;
  input: string | null;
  anomaly: boolean;
  confirmed: boolean;
  blockingGeneral: number | null;
  measuredAt: string; // ISO 8601 UTC
}

/** A measurement counts as a censorship event when an anomaly/confirmation or
 * any country/ISP-level blocking score is present. */
export function isCensorshipEvent(m: OoniMeasurement): boolean {
  const blocking = m.scores
    ? Math.max(
        m.scores.blocking_general ?? 0,
        m.scores.blocking_country ?? 0,
        m.scores.blocking_isp ?? 0,
        m.scores.blocking_local ?? 0
      )
    : 0;
  return m.anomaly === true || m.confirmed === true || blocking > 0;
}

export function mapMeasurementToItem(m: OoniMeasurement): CensorshipEventItem | null {
  const centroid = COUNTRY_CENTROIDS[m.probe_cc];
  if (!centroid) return null; // no coordinates available -> skip
  if (!isCensorshipEvent(m)) return null;

  const measuredAt = m.measurement_start_time ?? new Date().toISOString();
  return {
    id: m.measurement_uid ?? `${m.probe_cc}-${measuredAt}-${m.probe_asn ?? 'unknown'}`,
    lat: centroid.lat,
    lon: centroid.lon,
    probeCc: m.probe_cc,
    probeAsn: m.probe_asn ?? null,
    testName: m.test_name ?? null,
    input: m.input ?? null,
    anomaly: m.anomaly === true,
    confirmed: m.confirmed === true,
    blockingGeneral:
      typeof m.scores?.blocking_general === 'number' && Number.isFinite(m.scores.blocking_general)
        ? m.scores.blocking_general
        : null,
    measuredAt,
  };
}

export function parseOoniResponse(payload: OoniResponse): CensorshipEventItem[] {
  if (!Array.isArray(payload.results)) return [];
  const items: CensorshipEventItem[] = [];
  for (const m of payload.results) {
    const item = mapMeasurementToItem(m);
    if (item) items.push(item);
  }
  return items;
}

// Ensure the per-plugin SQLite table exists (INSERT OR IGNORE below requires
// it). The engine may pre-create tables, so IF NOT EXISTS keeps this safe.
try {
  db.prepare(
    'CREATE TABLE IF NOT EXISTS internet_censorship_ooni (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)'
  ).run();
} catch (err) {
  console.error('[InternetCensorshipOoni] could not ensure SQLite table:', err instanceof Error ? err.message : err);
}

const insertCensorship = db.prepare(
  'INSERT OR IGNORE INTO internet_censorship_ooni (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)'
);

export async function seedInternetCensorshipOoni() {
  console.log('[InternetCensorshipOoni] Polling OONI API...');
  try {
    const fetchedAt = Date.now();
    const items: CensorshipEventItem[] = [];
    let insertedCount = 0;
    let completedCountries = 0;

    for (const cc of PROBE_COUNTRIES) {
      const url = `${OONI_API_BASE}?probe_cc=${encodeURIComponent(cc)}&limit=${OONI_LIMIT}`;
      try {
        const res = await withRetry(() => fetchWithTimeout(url));
        const payload = (await res.json()) as OoniResponse;
        const parsed = parseOoniResponse(payload);
        completedCountries++;

        for (const item of parsed) {
          items.push(item);
          const result = insertCensorship.run({
            id: item.id,
            payload: JSON.stringify(item),
            source_ts: new Date(item.measuredAt).getTime(),
            fetched_at: fetchedAt,
          });
          if (result.changes > 0) insertedCount++;
        }
      } catch (countryErr) {
        // One rate-limited or failing country must not sink the whole sweep.
        console.error(
          `[InternetCensorshipOoni] country ${cc} failed:`,
          countryErr instanceof Error ? countryErr.message : countryErr
        );
      }
    }

    // A fully failed sweep means the upstream is unreachable; publishing an
    // empty snapshot would wrongly wipe the live cache to "no censorship".
    if (completedCountries === 0) {
      console.warn('[InternetCensorshipOoni] all country fetches failed; skipping snapshot');
      return;
    }

    console.log(
      `[InternetCensorshipOoni] Parsed ${items.length} censorship events. Saved ${insertedCount} new to SQLite.`
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
    console.error('[InternetCensorshipOoni] seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: PLUGIN_ID,
  cron: '0 * * * *', // Every hour
  fn: seedInternetCensorshipOoni,
};