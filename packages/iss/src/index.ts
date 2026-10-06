import { setLiveSnapshot } from '@worldwideview/seeder-sdk';

// WhereTheISS.at position endpoint for the ISS (NORAD 25544).
export const SOURCE_URL = 'https://api.wheretheiss.at/v1/satellites/25544';

// The station moves fast enough that a 5 second cadence keeps the globe marker
// close to its real position.
export const POLL_INTERVAL_MS = 5_000;

// The TTL has to exceed the SDK's write cadence, not the poll cadence:
// setLiveSnapshot broadcasts to WebSocket consumers on every call but throttles
// the Redis write to one per 5 minutes. A TTL shorter than that throttle lets
// the key expire between writes, so /api/iss 404s until the next one (60s of
// life per 300s cycle). 600s is 2x the throttle: the key never lapses between
// writes, and a dead seeder still clears the snapshot within 10 minutes, which
// keeps a 404 an honest liveness signal.
export const SNAPSHOT_TTL_SECONDS = 600;

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

// Upstream contract. This seeder is pinned to one satellite and one unit, so a
// response carrying anything else means the source changed underneath us.
const ISS_NORAD_ID = 25544;
const EXPECTED_UNITS = 'kilometers';

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

// A malformed or non-2xx response must not blank the published snapshot, so
// this throws instead and pollIss returns without publishing.
//
// The whole contract is checked, not just latitude/longitude: the globe plugin
// renders `new Date(timestamp * 1000)` and converts altitude/velocity using the
// declared units, so a partial payload draws a broken frame and replaces a good
// snapshot with it. Throwing keeps the last good fix instead.
function parsePosition(raw: unknown): IssPosition {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new Error('response is not an object');
  }

  const value = raw as Partial<IssPosition>;

  if (value.id !== ISS_NORAD_ID) {
    throw new Error(`unexpected satellite id ${String(value.id)} (expected ${ISS_NORAD_ID})`);
  }
  if (typeof value.name !== 'string' || !value.name.trim()) {
    throw new Error('response is missing name');
  }
  if (!isFiniteNumber(value.latitude) || value.latitude < -90 || value.latitude > 90) {
    throw new Error(`latitude is not a number in [-90, 90]: ${String(value.latitude)}`);
  }
  if (!isFiniteNumber(value.longitude) || value.longitude < -180 || value.longitude > 180) {
    throw new Error(`longitude is not a number in [-180, 180]: ${String(value.longitude)}`);
  }
  if (!isFiniteNumber(value.altitude) || value.altitude < 0) {
    throw new Error(`altitude is not a non-negative number: ${String(value.altitude)}`);
  }
  if (!isFiniteNumber(value.velocity) || value.velocity < 0) {
    throw new Error(`velocity is not a non-negative number: ${String(value.velocity)}`);
  }
  if (typeof value.visibility !== 'string' || !value.visibility.trim()) {
    throw new Error('response is missing visibility');
  }
  if (!isFiniteNumber(value.footprint) || value.footprint < 0) {
    throw new Error(`footprint is not a non-negative number: ${String(value.footprint)}`);
  }
  if (!isFiniteNumber(value.timestamp) || value.timestamp <= 0) {
    throw new Error(`timestamp is not a positive number: ${String(value.timestamp)}`);
  }
  // The plugin builds a Date from this value; an unrepresentable one would
  // surface there as an Invalid Date rather than as bad data here.
  if (Number.isNaN(new Date(value.timestamp * 1000).getTime())) {
    throw new Error(`timestamp is not a representable date: ${String(value.timestamp)}`);
  }
  if (value.units !== EXPECTED_UNITS) {
    throw new Error(`unexpected units "${String(value.units)}" (expected ${EXPECTED_UNITS})`);
  }

  return value as IssPosition;
}

async function fetchPosition(): Promise<IssPosition> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const response = await fetch(SOURCE_URL, { signal: controller.signal });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status} from ${SOURCE_URL}`);
    }

    return parsePosition(await response.json());
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