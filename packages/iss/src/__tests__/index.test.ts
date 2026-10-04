// Unit tests for the ISS position seeder. The @worldwideview/seeder-sdk is
// fully mocked (same pattern as packages/earthquakes/src/__tests__/index.test.ts)
// and global fetch is stubbed, so no network or native dependency ever loads.
//
// Fake timers drive the two real timers the seeder owns: the 5s poll interval
// and the 180s track-sampling interval. Time is advanced in explicit 5s steps so
// the poll count at every ring boundary is exact rather than approximate.
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

vi.mock('@worldwideview/seeder-sdk', () => ({
  setLiveSnapshot: vi.fn(async () => undefined),
}));

import seeder, {
  resetIssState,
  startIssSeeder,
  SOURCE_URL,
  POLL_INTERVAL_MS,
  SNAPSHOT_TTL_SECONDS,
  TRACK_SAMPLE_INTERVAL_MS,
  TRACK_MAX_POINTS,
  type IssSnapshot,
} from '../index';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';

const fetchMock = vi.fn();

// Frozen clock so fetchedAt is deterministic.
const T0 = new Date('2026-05-01T00:00:00.000Z');
const BASE_TIMESTAMP = 1777593600;

// Upstream position, published verbatim (altitude in km, timestamp in seconds).
function makePosition(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: 25544,
    name: 'iss',
    latitude: 51.6416,
    longitude: -2.9302,
    altitude: 419.241,
    velocity: 27600.582,
    visibility: 'daylight',
    footprint: 4523.4521,
    timestamp: BASE_TIMESTAMP,
    units: 'kilometers',
    ...overrides,
  };
}

function okResponse(body: unknown) {
  return { ok: true, status: 200, json: async () => body };
}

// Collect rejections that escape a timer callback. A poll error must never
// escape, so any entry here is a seeder bug (vitest only warns about these).
function captureUnhandledRejections() {
  const seen: unknown[] = [];
  const listener = (reason: unknown) => {
    seen.push(reason);
  };
  process.on('unhandledRejection', listener);
  return {
    seen,
    stop: () => process.off('unhandledRejection', listener),
  };
}

function snapshotAt(call: number): IssSnapshot {
  return vi.mocked(setLiveSnapshot).mock.calls[call][1] as IssSnapshot;
}

function lastSnapshot(): IssSnapshot {
  return snapshotAt(vi.mocked(setLiveSnapshot).mock.calls.length - 1);
}

// Every fetch returns a position derived from the frozen clock and a per-request
// counter, so the ring point taken at a boundary is identifiable from its values.
// timestamp mimics the upstream seconds field: BASE_TIMESTAMP plus the seconds
// elapsed since the frozen clock started.
function positionFor(nowMs: number, callNumber: number) {
  return makePosition({
    latitude: 51.6416 + callNumber / 100,
    longitude: -2.9302 + callNumber / 100,
    timestamp: BASE_TIMESTAMP + Math.floor((nowMs - T0.getTime()) / 1000),
  });
}

// Advance time one 5s tick at a time, letting each poll settle.
async function tick(count: number) {
  for (let i = 0; i < count; i++) {
    await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS);
  }
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(T0);
  vi.clearAllMocks();
  resetIssState();
  fetchMock.mockReset();
  vi.stubGlobal('fetch', fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe('published snapshot shape', () => {
  it('publishes source, fetchedAt, items, track, and totalCount on the first poll', async () => {
    const position = makePosition();
    fetchMock.mockResolvedValue(okResponse(position));

    startIssSeeder();
    await vi.advanceTimersByTimeAsync(0);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][0]).toBe(SOURCE_URL);
    expect(fetchMock.mock.calls[0][1].signal).toBeInstanceOf(AbortSignal);

    expect(setLiveSnapshot).toHaveBeenCalledTimes(1);
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'iss',
      {
        source: 'iss',
        fetchedAt: '2026-05-01T00:00:00.000Z',
        items: [position],
        track: [],
        totalCount: 1,
      },
      SNAPSHOT_TTL_SECONDS
    );

    // The position object is the upstream payload verbatim: no field renamed,
    // no unit converted, timestamp still in SECONDS.
    expect(lastSnapshot().items[0]).toEqual(position);
    expect(lastSnapshot().items[0].timestamp).toBe(BASE_TIMESTAMP);
    expect(lastSnapshot().items[0].altitude).toBe(419.241);
    expect(lastSnapshot().items[0].units).toBe('kilometers');
    expect(setLiveSnapshot).toHaveBeenCalledWith('iss', expect.anything(), 60);
  });

  it('polls immediately then every 5 seconds', async () => {
    fetchMock.mockImplementation(async () =>
      okResponse(positionFor(Date.now(), fetchMock.mock.calls.length))
    );

    startIssSeeder();
    await vi.advanceTimersByTimeAsync(0);
    expect(setLiveSnapshot).toHaveBeenCalledTimes(1);

    await tick(2);

    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(setLiveSnapshot).toHaveBeenCalledTimes(3);
    expect(lastSnapshot().fetchedAt).toBe('2026-05-01T00:00:10.000Z');
  });
});

describe('track ring', () => {
  it('takes one sample per 180s boundary and caps the ring at 10, dropping the oldest', async () => {
    fetchMock.mockImplementation(async (url: string, init: { signal: AbortSignal }) => {
      expect(url).toBe(SOURCE_URL);
      expect(init.signal).toBeInstanceOf(AbortSignal);
      return okResponse(positionFor(Date.now(), fetchMock.mock.calls.length));
    });

    startIssSeeder();
    await vi.advanceTimersByTimeAsync(0);

    // 36 ticks = 180s: exactly one boundary, so the ring holds one sample.
    await tick(35);
    expect(lastSnapshot().track).toHaveLength(0);
    await tick(1);
    expect(lastSnapshot().track).toHaveLength(1);

    // 72 ticks = 360s: a second boundary. Ring samples stay 180s apart.
    await tick(36);
    expect(lastSnapshot().track).toHaveLength(2);
    expect(lastSnapshot().track[1].timestamp - lastSnapshot().track[0].timestamp).toBe(180);

    // Out to 36 minutes (432 ticks = 12 boundaries): capped at 10, oldest dropped.
    await tick(360);

    const track = lastSnapshot().track;
    expect(TRACK_MAX_POINTS).toBe(10);
    expect(track).toHaveLength(10);

    // 12 boundaries passed (t=180s .. t=2160s) and only the newest 10 survive:
    // the two earliest are gone.
    const timestamps = track.map((point) => point.timestamp);
    expect(timestamps[0]).toBe(BASE_TIMESTAMP + 180 * 3 - 5);
    expect(timestamps[0]).not.toBe(BASE_TIMESTAMP + 180);
    expect(timestamps[1] - timestamps[0]).toBe(180);
    expect(timestamps[9]).toBe(BASE_TIMESTAMP + 180 * 12 - 5);
    expect(timestamps[9] - timestamps[0]).toBe(180 * 9);

    // Each ring point carries the fix that was latest when the boundary fired.
    // Polls land at t=0, 5, 10, ...; the boundary at t=180k fires before the
    // poll at t=180k, so the newest fix there is the one 5s earlier.
    const firstPollIndex = (timestamps[0] - BASE_TIMESTAMP + 5) / 5;
    const lastPollIndex = (timestamps[9] - BASE_TIMESTAMP + 5) / 5;
    expect(firstPollIndex).toBe(108);
    expect(lastPollIndex).toBe(432);
    expect(track[0].latitude).toBeCloseTo(51.6416 + firstPollIndex / 100, 10);
    expect(track[0].longitude).toBeCloseTo(-2.9302 + firstPollIndex / 100, 10);
    expect(track[9].latitude).toBeCloseTo(51.6416 + lastPollIndex / 100, 10);
    expect(track[9].longitude).toBeCloseTo(-2.9302 + lastPollIndex / 100, 10);
  });

  it('does not sample the ring between 180s boundaries', async () => {
    fetchMock.mockImplementation(async () =>
      okResponse(positionFor(Date.now(), fetchMock.mock.calls.length))
    );

    startIssSeeder();
    await vi.advanceTimersByTimeAsync(0);

    // 35 ticks = 175s: just short of the first boundary.
    await tick(35);
    expect(lastSnapshot().track).toHaveLength(0);

    await tick(1);
    expect(lastSnapshot().track).toHaveLength(1);

    // Another 35 ticks = 355s: still short of the second boundary.
    await tick(35);
    expect(lastSnapshot().track).toHaveLength(1);

    await tick(1);
    expect(lastSnapshot().track).toHaveLength(2);
    expect(lastSnapshot().track[1].timestamp - lastSnapshot().track[0].timestamp).toBe(
      TRACK_SAMPLE_INTERVAL_MS / 1000
    );
  });
});

describe('failure handling', () => {
  it('keeps the last snapshot and does not throw when a fetch fails', async () => {
    const position = makePosition();
    fetchMock.mockResolvedValueOnce(okResponse(position));
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    startIssSeeder();
    await vi.advanceTimersByTimeAsync(0);
    expect(setLiveSnapshot).toHaveBeenCalledTimes(1);

    const escapes = captureUnhandledRejections();
    fetchMock.mockRejectedValue(new Error('upstream exploded'));
    await tick(2);

    // Three polls happened in total; only the first published.
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(setLiveSnapshot).toHaveBeenCalledTimes(1);
    expect(lastSnapshot().items[0]).toEqual(position);
    expect(lastSnapshot().fetchedAt).toBe('2026-05-01T00:00:00.000Z');
    expect(errorSpy).toHaveBeenCalledTimes(2);
    expect(String(errorSpy.mock.calls[0][0])).toContain('upstream exploded');
    // The failure is handled inside the timer: nothing escapes as a rejection.
    expect(escapes.seen).toEqual([]);
    escapes.stop();

    // The seeder is still alive: a later success publishes again.
    const recovered = makePosition({ latitude: -12.5, longitude: 130.5 });
    fetchMock.mockResolvedValue(okResponse(recovered));
    await tick(1);

    expect(setLiveSnapshot).toHaveBeenCalledTimes(2);
    expect(lastSnapshot().items[0]).toEqual(recovered);

    errorSpy.mockRestore();
  });

  it('keeps the previous snapshot on a non-2xx response', async () => {
    const position = makePosition();
    fetchMock.mockResolvedValueOnce(okResponse(position));
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    startIssSeeder();
    await vi.advanceTimersByTimeAsync(0);

    const escapes = captureUnhandledRejections();
    fetchMock.mockResolvedValue({ ok: false, status: 503, json: async () => ({}) });
    await tick(1);

    expect(setLiveSnapshot).toHaveBeenCalledTimes(1);
    expect(lastSnapshot().items[0]).toEqual(position);
    expect(String(errorSpy.mock.calls[0][0])).toContain('HTTP 503');
    expect(escapes.seen).toEqual([]);
    escapes.stop();

    errorSpy.mockRestore();
  });

  it('keeps the previous snapshot when the body is not a position', async () => {
    const escapes = captureUnhandledRejections();
    fetchMock.mockResolvedValueOnce(okResponse(makePosition()));
    fetchMock.mockResolvedValue(okResponse({ id: 25544 }));
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    startIssSeeder();
    await vi.advanceTimersByTimeAsync(0);
    await tick(1);

    expect(setLiveSnapshot).toHaveBeenCalledTimes(1);
    expect(String(errorSpy.mock.calls[0][0])).toContain('missing latitude/longitude');
    expect(escapes.seen).toEqual([]);
    escapes.stop();

    errorSpy.mockRestore();
  });
});

describe('default export contract', () => {
  it('registers as "iss" with an init function', () => {
    expect(seeder.name).toBe('iss');
    expect(typeof seeder.init).toBe('function');
  });
});
