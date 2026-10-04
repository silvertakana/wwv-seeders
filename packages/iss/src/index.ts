import { setLiveSnapshot } from '@worldwideview/seeder-sdk';

// WhereTheISS.at position endpoint for the ISS (NORAD 25544).
export const SOURCE_URL = 'https://api.wheretheiss.at/v1/satellites/25544';

// The station moves fast enough that a 5 second cadence keeps the globe marker
// close to its real position; the snapshot TTL is 60s so /api/iss never serves
// a stale entry between polls.
export const POLL_INTERVAL_MS = 5_000;
export const SNAPSHOT_TTL_SECONDS = 60;

// A 5 second cadence cannot be expressed as a cron expression, so this seeder
// uses the init form and owns its timers (same shape as the satellite seeder).
const FETCH_TIMEOUT_MS = 4_000;

// Track ring: 10 fixes at 3 minute spacing reproduces the existing 27 minute
// trail the globe plugin draws behind the station.
export const TRACK_SAMPLE_INTERVAL_MS = 180_000;
export const TRACK_MAX_POINTS = 10;

// Upstream payload, published verbatim as items[0] (the globe plugin does its
// own unit conversion, so nothing here is renamed or converted).
export interface IssPosition {
  id: number;
  name: string;
  latitude: number;
  longitude: number;
  altitude: number;
  velocity: number;
  visibility: string;
  footprint: number;
  timestamp: number;
  units: string;
}

export interface IssTrackPoint {
  latitude: number;
  longitude: number;
  timestamp: number;
}

export interface IssSnapshot {
  source: string;
  fetchedAt: string;
  items: IssPosition[];
  track: IssTrackPoint[];
  totalCount: number;
}

// Last good fix, kept across failures so a blip upstream never clears the
// snapshot or empties the trail.
let latestPosition: IssPosition | null = null;

// Oldest-first ring of track samples, capped at TRACK_MAX_POINTS.
const track: IssTrackPoint[] = [];

// A malformed or non-2xx response must not blank the published snapshot, so
// this throws instead and pollIss returns without publishing.
async function fetchPosition(): Promise<IssPosition> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const response = await fetch(SOURCE_URL, { signal: controller.signal });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status} from ${SOURCE_URL}`);
    }

    const parsed = (await response.json()) as Partial<IssPosition> | null;
    if (!parsed || typeof parsed.latitude !== 'number' || typeof parsed.longitude !== 'number') {
      throw new Error('response is missing latitude/longitude');
    }

    return parsed as IssPosition;
  } finally {
    clearTimeout(timeout);
  }
}

async function pollIss(): Promise<void> {
  try {
    const position = await fetchPosition();
    latestPosition = position;

    await setLiveSnapshot(
      'iss',
      {
        source: 'iss',
        fetchedAt: new Date().toISOString(),
        items: [position],
        track: [...track],
        totalCount: 1,
      },
      SNAPSHOT_TTL_SECONDS
    );
  } catch (err) {
    // Keep the last published snapshot: a failed poll must never publish an
    // empty one. Swallow the error so the interval loop survives it.
    console.error(`[IssSeeder] Poll failed: ${err instanceof Error ? err.message : String(err)}`);
  }
}

// Sample the latest fix onto the ring, dropping the oldest point once full.
function sampleTrack(): void {
  if (!latestPosition) return;

  track.push({
    latitude: latestPosition.latitude,
    longitude: latestPosition.longitude,
    timestamp: latestPosition.timestamp,
  });

  while (track.length > TRACK_MAX_POINTS) track.shift();
}

// Test-only: clear the retained fix and the track ring. Module-level state
// outlives a single test case, so tests must reset it between runs.
export function resetIssState(): void {
  latestPosition = null;
  track.length = 0;
}

export function startIssSeeder(): void {
  console.log('[IssSeeder] Starting ISS position seeder.');

  void pollIss();
  setInterval(() => void pollIss(), POLL_INTERVAL_MS);
  setInterval(sampleTrack, TRACK_SAMPLE_INTERVAL_MS);
}

export default {
  name: 'iss',
  init: startIssSeeder,
};
