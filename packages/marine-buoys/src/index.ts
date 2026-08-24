import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

/**
 * One parsed row of NOAA NDBC `latest_obs.txt`. Numeric columns are null when
 * the source reports them missing (NDBC uses "MM"). Column order follows the
 * `#STN` header: STN LAT LON YYYY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD
 * PRES PTDY ATMP WTMP DEWP VIS TIDE.
 */
export interface BuoyObservation {
  stn: string;
  lat: number | null;
  lon: number | null;
  year: number | null;
  month: number | null;
  day: number | null;
  hour: number | null;
  minute: number | null;
  wdir: number | null;
  wspd: number | null;
  gst: number | null;
  wvht: number | null;
  dpd: number | null;
  apd: number | null;
  mwd: number | null;
  pres: number | null;
  ptdy: number | null;
  atmp: number | null;
  wtmp: number | null;
  dewp: number | null;
  vis: number | null;
  tide: number | null;
}

/** Parse a raw token; "MM", empty, and unparseable values become null. */
export function parseObsValue(raw: string | undefined): number | null {
  if (raw === undefined || raw === '' || raw === 'MM') return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

/**
 * Parse the NDBC latest_obs.txt text feed into BuoyObservation rows.
 *
 * The file is a whitespace-delimited fixed-width table. The first line starts
 * with `#STN` and holds the column names; a `#text` units line and any other
 * `#` comment lines are skipped. Rows whose LAT or LON are not finite numbers
 * are dropped (a buoy without coordinates cannot be placed on the globe).
 */
export function parseLatestObs(text: string): BuoyObservation[] {
  const lines = text.split(/\r?\n/);
  let header: string[] | null = null;
  const rows: BuoyObservation[] = [];

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;

    if (line.startsWith('#')) {
      if (line.startsWith('#STN')) {
        header = line.slice(1).split(/\s+/);
      }
      continue;
    }
    if (!header) continue;

    const cols = line.split(/\s+/);
    if (cols.length < 10) continue; // Too short to be a data row

    const at = (name: string): string | undefined => {
      // Prefer exact match (the header spells MM/month lowercase as "mm",
      // hour as "hh"); fall back to case-insensitive for the rest.
      let idx = header!.indexOf(name);
      if (idx < 0) idx = header!.findIndex((col) => col.toUpperCase() === name.toUpperCase());
      return idx >= 0 ? cols[idx] : undefined;
    };

    const stn = at('STN') ?? '';
    if (!stn) continue;

    const lat = parseObsValue(at('LAT'));
    const lon = parseObsValue(at('LON'));
    if (lat === null || lon === null) continue; // Skip non-finite lat/lon

    rows.push({
      stn,
      lat,
      lon,
      year: parseObsValue(at('YYYY')),
      month: parseObsValue(at('MM')),
      day: parseObsValue(at('DD')),
      hour: parseObsValue(at('hh')),
      minute: parseObsValue(at('mm')),
      wdir: parseObsValue(at('WDIR')),
      wspd: parseObsValue(at('WSPD')),
      gst: parseObsValue(at('GST')),
      wvht: parseObsValue(at('WVHT')),
      dpd: parseObsValue(at('DPD')),
      apd: parseObsValue(at('APD')),
      mwd: parseObsValue(at('MWD')),
      pres: parseObsValue(at('PRES')),
      ptdy: parseObsValue(at('PTDY')),
      atmp: parseObsValue(at('ATMP')),
      wtmp: parseObsValue(at('WTMP')),
      dewp: parseObsValue(at('DEWP')),
      vis: parseObsValue(at('VIS')),
      tide: parseObsValue(at('TIDE')),
    });
  }

  return rows;
}

/** Observation instant as epoch ms (UTC); falls back to now when time parts are missing. */
function observationEpochMs(obs: BuoyObservation): number {
  const { year, month, day, hour, minute } = obs;
  if (year === null || month === null || day === null || hour === null || minute === null) {
    return Date.now();
  }
  return Date.UTC(year, month - 1, day, hour, minute);
}

const insertBuoy = db.prepare('INSERT OR IGNORE INTO marine_buoys (stn, payload, source_ts, fetched_at) VALUES (@stn, @payload, @source_ts, @fetched_at)');

export async function seedMarineBuoys() {
  console.log('[MarineBuoys] Polling NOAA NDBC latest observations...');

  try {
    const url = 'https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt';

    const res = await withRetry(() => fetchWithTimeout(url));
    const text = await res.text();
    const items = parseLatestObs(text);
    const fetchedAt = Date.now();

    let insertedCount = 0;
    for (const item of items) {
      const result = insertBuoy.run({
        stn: item.stn,
        payload: JSON.stringify(item),
        source_ts: observationEpochMs(item),
        fetched_at: fetchedAt,
      });
      if (result.changes > 0) insertedCount++;
    }

    console.log(`[MarineBuoys] Parsed ${items.length} buoys. Saved ${insertedCount} new to SQLite.`);

    // Save to Redis Live Cache
    await setLiveSnapshot('marine-buoys', {
      source: 'marine-buoys',
      fetchedAt: new Date().toISOString(),
      items: items,
      totalCount: items.length,
    }, 3600); // 1 hour TTL
  } catch (err) {
    console.error('[MarineBuoys] Seeder failed:', err instanceof Error ? err.message : String(err));
  }
}

export default {
  name: 'marine-buoys',
  cron: '0 * * * *', // Every hour
  fn: seedMarineBuoys,
};