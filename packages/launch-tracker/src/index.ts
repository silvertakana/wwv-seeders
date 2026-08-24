import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

export interface LaunchTrackerItem {
  id: string;
  name: string;
  net: string | null;
  status: string | null;
  padName: string | null;
  latitude: number;
  longitude: number;
  location: string | null;
  mission: string | null;
  rocket: string | null;
  provider: string | null;
  url: string | null;
  webcast_live: boolean;
}

export interface LL2Launch {
  id: string;
  name: string;
  net?: string | null;
  url?: string | null;
  status?: { id?: number | null; name?: string | null } | null;
  pad?: {
    name?: string | null;
    latitude?: number | string | null;
    longitude?: number | string | null;
    location?: { name?: string | null } | null;
  } | null;
  mission?: { name?: string | null } | null;
  rocket?: { configuration?: { name?: string | null; family?: string | null } } | null;
  launch_service_provider?: { name?: string | null } | null;
  provider?: { name?: string | null } | null;
  webcast_live?: boolean;
}

function str(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

/** Coerce a coordinate to a finite number; null/undefined/empty never become 0. */
function coord(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

/**
 * Map a raw LL2 launch to the item shape persisted to SQLite and pushed to the
 * live cache. Returns null when the launch is not upcoming (NET in the future)
 * or the pad latitude/longitude are missing or non-numeric — those can't be
 * pinned on the globe.
 */
export function mapLaunchToItem(launch: LL2Launch, now: number = Date.now()): LaunchTrackerItem | null {
  const lat = coord(launch.pad?.latitude);
  const lon = coord(launch.pad?.longitude);
  if (lat === null || lon === null) return null;

  const netIso = str(launch.net);
  const netMs = netIso ? Date.parse(netIso) : NaN;
  if (!Number.isFinite(netMs) || netMs <= now) return null; // upcoming only

  return {
    id: launch.id,
    name: launch.name,
    net: netIso,
    status: str(launch.status?.name),
    padName: str(launch.pad?.name),
    latitude: lat,
    longitude: lon,
    location: str(launch.pad?.location?.name),
    mission: str(launch.mission?.name),
    rocket: str(launch.rocket?.configuration?.name),
    provider: str(launch.provider?.name) ?? str(launch.launch_service_provider?.name) ?? str(launch.rocket?.configuration?.family),
    url: str(launch.url),
    webcast_live: launch.webcast_live ?? false,
  };
}

// The seeder-sdk's initDB() does not know the launch_tracker table; create it
// idempotently (same schema as the earthquakes table) before preparing inserts.
db.exec(`
  CREATE TABLE IF NOT EXISTS launch_tracker (
    id TEXT PRIMARY KEY,
    payload JSON NOT NULL,
    source_ts INTEGER NOT NULL,
    fetched_at INTEGER NOT NULL
  )
`);
const insertLaunch = db.prepare('INSERT OR IGNORE INTO launch_tracker (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)');

export async function seedLaunchTracker() {
  try {
    console.log('[Launch Tracker] Polling Launch Library 2 (2.3.0)...');

    // LL2 2.3.0 upcoming launches — verified live: { results: [...] }.
    const url = 'https://ll.thespacedevs.com/2.3.0/launches/upcoming/?limit=100';

    const res = await withRetry(() => fetchWithTimeout(url));
    const data = await res.json();
    const fetchedAt = Date.now();

    if (!data?.results || !Array.isArray(data.results)) {
      console.warn('[Launch Tracker] Invalid response from Launch Library 2');
      return;
    }

    const items: LaunchTrackerItem[] = [];
    let insertedCount = 0;

    for (const launch of data.results) {
      const item = mapLaunchToItem(launch);

      if (item === null) continue;

      items.push(item);

      // Save to SQLite
      const sourceTs = item.net ? Date.parse(item.net) : fetchedAt;
      const result = insertLaunch.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: Number.isFinite(sourceTs) ? sourceTs : fetchedAt,
        fetched_at: fetchedAt
      });
      if (result.changes > 0) insertedCount++;
    }

    console.log(`[Launch Tracker] Parsed ${items.length} upcoming launches. Saved ${insertedCount} new to SQLite.`);

    // Save to Redis Live Cache
    await setLiveSnapshot('launch-tracker', {
      source: "launch-tracker",
      fetchedAt: new Date().toISOString(),
      items: items,
      totalCount: items.length
    }, 3600); // 1 hour TTL
  } catch (err) {
    console.error('[Launch Tracker] Seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: "launch-tracker",
  cron: "0 * * * *", // Every hour
  fn: seedLaunchTracker
};