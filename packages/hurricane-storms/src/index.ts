import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot, fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

const NHC_STORMS_URL = 'https://www.nhc.noaa.gov/CurrentStorms.json';

/**
 * Raw shape of one entry in NOAA NHC's CurrentStorms.json `activeStorms` array.
 * Most fields may be null; only storms with finite numeric coordinates are kept.
 */
export interface NhcActiveStorm {
  id: string | number;
  name: string | null;
  classification: string | null;
  intensity: number | null;
  pressure: number | null;
  latitudeNumeric: number | null;
  longitudeNumeric: number | null;
  movementDir: string | null;
  movementSpeed: number | null;
  lastUpdate: string | null;
  publicAdvisory?: { url?: string | null } | null;
  forecastAdvisory?: { url?: string | null } | null;
  forecastDiscussion?: { url?: string | null } | null;
  forecastTrack?: { kmzFile?: string | null; zipFile?: string | null } | null;
}

/** Item shape persisted to SQLite and pushed to the live cache. */
export interface HurricaneStormItem {
  id: string;
  name: string;
  classification: string | null;
  intensity: number | null;
  pressure: number | null;
  lat: number;
  lon: number;
  lastUpdate: string | null;
  advisoryUrl: string | null;
  forecastUrl: string | null;
  discussionUrl: string | null;
}

/** Map a raw NHC storm to the item shape; returns null when coordinates are invalid. */
export function mapActiveStormToItem(storm: NhcActiveStorm): HurricaneStormItem | null {
  const lat = storm.latitudeNumeric;
  const lon = storm.longitudeNumeric;
  if (typeof lat !== 'number' || !Number.isFinite(lat)) return null;
  if (typeof lon !== 'number' || !Number.isFinite(lon)) return null;

  return {
    id: String(storm.id),
    name: storm.name ?? 'Unnamed Storm',
    classification: storm.classification,
    intensity: finiteOrNull(storm.intensity),
    pressure: finiteOrNull(storm.pressure),
    lat,
    lon,
    lastUpdate: storm.lastUpdate,
    advisoryUrl: storm.publicAdvisory?.url ?? null,
    forecastUrl: storm.forecastTrack?.kmzFile ?? null,
    discussionUrl: storm.forecastDiscussion?.url ?? null,
  };
}

/** Keep only finite numbers; everything else maps to null. */
function finiteOrNull(value: number | null): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

const insertStorm = db.prepare(
  'INSERT OR IGNORE INTO hurricane_storms (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)'
);

export async function seedHurricaneStorms() {
  console.log('[HurricaneStorms] Polling NOAA NHC...');
  try {
    const res = await withRetry(() => fetchWithTimeout(NHC_STORMS_URL));
    const data = await res.json();
    const fetchedAt = Date.now();

    if (!data || !Array.isArray(data.activeStorms)) {
      console.warn('[HurricaneStorms] Invalid response from NOAA NHC');
      return;
    }

    const items: HurricaneStormItem[] = [];
    let insertedCount = 0;

    for (const storm of data.activeStorms as NhcActiveStorm[]) {
      const item = mapActiveStormToItem(storm);
      if (!item) continue;

      items.push(item);

      // Save to SQLite
      const sourceTs = item.lastUpdate ? new Date(item.lastUpdate).getTime() : fetchedAt;
      const result = insertStorm.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: sourceTs,
        fetched_at: fetchedAt
      });
      if (result.changes > 0) insertedCount++;
    }

    console.log(`[HurricaneStorms] Parsed ${items.length} active storms. Saved ${insertedCount} new to SQLite.`);

    // Save to Redis Live Cache
    await setLiveSnapshot('hurricane-storms', {
      source: 'hurricane-storms',
      fetchedAt: new Date().toISOString(),
      items: items,
      totalCount: items.length
    }, 3600); // 1 hour TTL
  } catch (err) {
    console.error('[HurricaneStorms] Seeder failed:', err instanceof Error ? err.message : String(err));
  }
}

export default {
  name: 'hurricane-storms',
  cron: '0 * * * *', // Every hour
  fn: seedHurricaneStorms
};