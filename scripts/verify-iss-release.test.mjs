// Tests for the release gate that decides whether a freshly deployed ISS
// seeder is actually publishing fresh data. The gate replaced an inline shell
// expression that accepted any numeric-looking timestamp, so these cases pin
// the specific inputs that expression let through.
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { validateSnapshot, verifyIssRelease } from './verify-iss-release.mjs';

const EPOCH = Date.parse('2026-10-06T00:00:00Z');
const THROTTLE_MS = 300000;

const POSITION = {
    id: 25544,
    name: 'iss',
    latitude: -43.5321,
    longitude: 172.6362,
    altitude: 418.5,
    velocity: 27580.4,
    visibility: 'daylight',
    footprint: 4523.7,
    timestamp: EPOCH / 1000,
    units: 'kilometers',
};

// The exact object the seeder publishes: the engine's /api/:id route returns it
// verbatim because it already carries items.
function snapshotAt(timeMs) {
    return {
        source: 'iss',
        fetchedAt: new Date(timeMs).toISOString(),
        items: [{ ...POSITION, timestamp: timeMs / 1000 }],
        track: [],
        totalCount: 1,
    };
}

const valid = () => snapshotAt(EPOCH);
const withTrack = (track) => ({ ...valid(), track });

// ---- validateSnapshot: rejection ------------------------------------------

const INVALID = [
    ['a timestamp-only payload', { items: [{ timestamp: 100 }] }],
    ['null', null],
    ['an array', []],
    ['a bare string', 'iss'],
    ['a number', 42],
    ['the wrong source', { ...valid(), source: 'earthquakes' }],
    ['no source', (() => { const p = valid(); delete p.source; return p; })()],
    ['no items', (() => { const p = valid(); delete p.items; return p; })()],
    ['an empty items array', { ...valid(), items: [] }],
    ['two positions', { ...valid(), items: [POSITION, POSITION], totalCount: 2 }],
    ['a totalCount that disagrees', { ...valid(), totalCount: 2 }],
    ['a different satellite', { ...valid(), items: [{ ...POSITION, id: 25545 }] }],
    ['missing units', (() => { const p = valid(); delete p.items[0].units; return p; })()],
    ['imperial units', { ...valid(), items: [{ ...POSITION, units: 'miles' }] }],
    ['a blank name', { ...valid(), items: [{ ...POSITION, name: '   ' }] }],
    ['a missing name', (() => { const p = valid(); delete p.items[0].name; return p; })()],
    ['a blank visibility', { ...valid(), items: [{ ...POSITION, visibility: '' }] }],
    ['a missing visibility', (() => { const p = valid(); delete p.items[0].visibility; return p; })()],
    ['a negative altitude', { ...valid(), items: [{ ...POSITION, altitude: -1 }] }],
    ['a negative velocity', { ...valid(), items: [{ ...POSITION, velocity: -1 }] }],
    ['a negative footprint', { ...valid(), items: [{ ...POSITION, footprint: -1 }] }],
    ['a NaN altitude', { ...valid(), items: [{ ...POSITION, altitude: NaN }] }],
    ['an Infinity velocity', { ...valid(), items: [{ ...POSITION, velocity: Infinity }] }],
    ['a string footprint', { ...valid(), items: [{ ...POSITION, footprint: '4500' }] }],
    ['a numeric-string timestamp', { ...valid(), items: [{ ...POSITION, timestamp: '1791093306' }] }],
    ['a zero timestamp', { ...valid(), items: [{ ...POSITION, timestamp: 0 }] }],
    ['a negative timestamp', { ...valid(), items: [{ ...POSITION, timestamp: -1 }] }],
    ['a timestamp past the Date range', { ...valid(), items: [{ ...POSITION, timestamp: 1e20 }] }],
    ['a timestamp whose ms conversion overflows', { ...valid(), items: [{ ...POSITION, timestamp: 1e308 }] }],
    ['a NaN timestamp', { ...valid(), items: [{ ...POSITION, timestamp: NaN }] }],
    ['a missing fetchedAt', (() => { const p = valid(); delete p.fetchedAt; return p; })()],
    ['a numeric fetchedAt', { ...valid(), fetchedAt: 1791093306000 }],
    ['an unparseable fetchedAt', { ...valid(), fetchedAt: 'yesterday' }],
    ['a missing track', (() => { const p = valid(); delete p.track; return p; })()],
    ['a string track', withTrack('nope')],
    ['a null track', withTrack(null)],
    ['a null track point', withTrack([null])],
    ['a numeric track point', withTrack([1])],
    ['an array track point', withTrack([[1, 2]])],
    ['a track point without a timestamp', withTrack([{ latitude: 0, longitude: 0 }])],
    ['a track point past the north pole', withTrack([{ latitude: 91, longitude: 0, timestamp: EPOCH / 1000 }])],
    ['a track point past the south pole', withTrack([{ latitude: -91, longitude: 0, timestamp: EPOCH / 1000 }])],
    ['a track point past the antimeridian', withTrack([{ latitude: 0, longitude: 181, timestamp: EPOCH / 1000 }])],
    ['a track point with an overflowing timestamp', withTrack([{ latitude: 0, longitude: 0, timestamp: 1e308 }])],
    ['a track point with a zero timestamp', withTrack([{ latitude: 0, longitude: 0, timestamp: 0 }])],
    ['eleven track points', withTrack(Array.from({ length: 11 }, (_, i) => ({ latitude: 0, longitude: i, timestamp: EPOCH / 1000 })))],
];

for (const [label, payload] of INVALID) {
    test('rejects ' + label, () => {
        assert.throws(() => validateSnapshot(payload, EPOCH));
    });
}

// Stale and future-dated are the two directions the freshness window closes.
test('rejects a snapshot older than the freshness window', () => {
    assert.throws(() => validateSnapshot(snapshotAt(EPOCH - 361000), EPOCH), /stale or future-dated/);
});

test('rejects a snapshot dated further ahead than the clock-skew allowance', () => {
    assert.throws(() => validateSnapshot(snapshotAt(EPOCH + 31000), EPOCH), /stale or future-dated/);
});

// ---- validateSnapshot: acceptance -----------------------------------------

test('accepts the seeder payload and reports the observed epoch', () => {
    assert.deepEqual(validateSnapshot(valid(), EPOCH), {
        timestamp: EPOCH / 1000,
        fetchedAtMs: EPOCH,
    });
});

test('accepts a fix on the equator at the prime meridian', () => {
    const payload = { ...valid(), items: [{ ...POSITION, latitude: 0, longitude: 0, timestamp: EPOCH / 1000 }] };
    assert.equal(validateSnapshot(payload, EPOCH).timestamp, EPOCH / 1000);
});

test('accepts an empty track', () => {
    assert.equal(validateSnapshot(withTrack([]), EPOCH).timestamp, EPOCH / 1000);
});

test('accepts a full ten-point track on the coordinate boundaries', () => {
    const track = Array.from({ length: 10 }, (_, i) => ({
        latitude: i % 2 === 0 ? -90 : 90,
        longitude: i % 2 === 0 ? -180 : 180,
        timestamp: EPOCH / 1000,
    }));
    assert.equal(validateSnapshot(withTrack(track), EPOCH).timestamp, EPOCH / 1000);
});

test('accepts a snapshot exactly on the freshness boundary', () => {
    assert.equal(validateSnapshot(snapshotAt(EPOCH - 360000), EPOCH).timestamp, (EPOCH - 360000) / 1000);
});

// ---- verifyIssRelease: a deterministic engine ------------------------------

// The fake engine models the one behaviour that matters: the seeder writes the
// snapshot to Redis at most once per 300s, so the persisted value plateaus for
// a whole throttle window and then steps forward. phaseMs shifts where in that
// cycle the observation starts.
function engine(options = {}) {
    const {
        healthStatus = 200,
        issStatus = 200,
        frozen = false,
        phaseMs = 0,
        advanceMs = 0,
        badJson = false,
        backwards = false,
        failIssAfterMs = null,
    } = options;

    let elapsed = 0;

    const fetchImpl = async (url) => {
        const isHealth = url.pathname === '/health';
        if (advanceMs) elapsed += advanceMs;
        return {
            status: isHealth ? healthStatus : issStatus,
            json: async () => {
                if (isHealth) return { status: 'ok' };
                if (badJson) throw new SyntaxError('Unexpected token < in JSON at position 0');
                if (failIssAfterMs !== null && elapsed >= failIssAfterMs) {
                    throw new Error('upstream gone');
                }
                if (backwards) return snapshotAt(EPOCH - elapsed);
                if (frozen) return snapshotAt(EPOCH);
                return snapshotAt(EPOCH + Math.floor((elapsed + phaseMs) / THROTTLE_MS) * THROTTLE_MS);
            },
        };
    };

    // Timing options pass straight through: the gate reads observeMs and
    // throttleMs from the same object, so swallowing them here would silently
    // run every case on the defaults.
    return {
        ...options,
        clock: () => elapsed,
        wallNow: () => EPOCH + elapsed,
        sleep: async (ms) => { elapsed += ms; },
        fetchImpl,
    };
}

const ORIGIN = 'http://localhost:5000';

for (const phaseMs of [0, 20000]) {
    test('waits the entire observation window and accepts throttle phase ' + phaseMs, async () => {
        const report = await verifyIssRelease(ORIGIN, engine({ phaseMs }));

        assert.equal(report.ok, true);
        assert.equal(report.observedMs, 400000);
        assert.ok(report.latest.timestamp > report.baseline.timestamp);
        assert.ok(report.latest.fetchedAtMs > report.baseline.fetchedAtMs);
    });
}

test('an advance first seen before the throttle window is still evidence later', async () => {
    // With phase 20000 the step forward lands at 280s, before the 300s mark. A
    // gate that only sampled the step itself would miss it and fail here.
    const report = await verifyIssRelease(ORIGIN, engine({ phaseMs: 20000 }));
    assert.ok(report.latest.timestamp > report.baseline.timestamp);
});

test('rejects a permanently frozen snapshot', async () => {
    // A 400s window is longer than the 360s freshness bound, so a snapshot that
    // never changes is rejected for going stale before the non-advance check is
    // ever reached. Shrinking the window below the freshness bound isolates the
    // non-advance branch: the payload stays fresh the whole time and the only
    // reason left to reject it is that it never moved.
    await assert.rejects(
        verifyIssRelease(ORIGIN, engine({ frozen: true, observeMs: 320000, throttleMs: 300000 })),
        /did not advance/
    );
});

test('rejects a frozen snapshot over the default window too, by going stale', async () => {
    await assert.rejects(verifyIssRelease(ORIGIN, engine({ frozen: true })), /stale or future-dated/);
});

test('rejects when the health endpoint is never ready', async () => {
    await assert.rejects(verifyIssRelease(ORIGIN, engine({ healthStatus: 503 })), /startup deadline exceeded/);
});

test('rejects when health is ready but the ISS snapshot endpoint is not', async () => {
    await assert.rejects(verifyIssRelease(ORIGIN, engine({ issStatus: 503 })), /startup deadline exceeded/);
});

test('rejects a 404 from a seeder that is not running', async () => {
    await assert.rejects(verifyIssRelease(ORIGIN, engine({ issStatus: 404 })), /startup deadline exceeded/);
});

test('rejects an unreadable body', async () => {
    await assert.rejects(verifyIssRelease(ORIGIN, engine({ badJson: true })), /startup deadline exceeded/);
});

test('rejects a snapshot that moves backwards', async () => {
    await assert.rejects(verifyIssRelease(ORIGIN, engine({ backwards: true })), /moved backwards/);
});

test('rejects a failure that starts during monitoring, after good readiness', async () => {
    await assert.rejects(
        verifyIssRelease(ORIGIN, engine({ failIssAfterMs: 50000 })),
        /upstream gone/
    );
});

test('a request that overruns its budget fails the phase', async () => {
    await assert.rejects(
        verifyIssRelease(ORIGIN, engine({ advanceMs: 60000 })),
        /startup deadline exceeded/
    );
});

test('rejects a stalled body instead of waiting on it', async () => {
    let aborted = false;

    await assert.rejects(
        verifyIssRelease(ORIGIN, {
            startupMs: 20, requestMs: 5, observeMs: 40, throttleMs: 30, pollMs: 5,
            fetchImpl: async (_url, init) => {
                init.signal.addEventListener('abort', () => { aborted = true; });
                return { status: 200, json: () => new Promise(() => {}) };
            },
        }),
        /startup deadline exceeded/
    );

    assert.equal(aborted, true, 'the request was never aborted, so a stall could hang the release');
});

test('rejects a connection that never resolves', async () => {
    await assert.rejects(
        verifyIssRelease(ORIGIN, {
            startupMs: 40, requestMs: 10, observeMs: 60, throttleMs: 30, pollMs: 10,
            fetchImpl: () => new Promise(() => {}),
        }),
        /startup deadline exceeded/
    );
});

// ---- verifyIssRelease: configuration guards --------------------------------

test('refuses an origin carrying credentials', async () => {
    await assert.rejects(verifyIssRelease('http://user:pw@localhost:5000', engine()), /credentials/);
});

test('refuses a URL that is not a bare origin', async () => {
    await assert.rejects(verifyIssRelease('http://localhost:5000/api/iss', engine()), /engine origin/);
});

test('refuses plain HTTP to a host that is not local', async () => {
    await assert.rejects(verifyIssRelease('http://dataenginev2.worldwideview.dev', engine()), /HTTPS/);
});

test('refuses an observation window shorter than the throttle it must cross', async () => {
    await assert.rejects(verifyIssRelease(ORIGIN, { observeMs: 1000, throttleMs: 300000 }), /invalid timings/);
});

// ---- the CLI ---------------------------------------------------------------

const SCRIPT = fileURLToPath(new URL('./verify-iss-release.mjs', import.meta.url));

test('the CLI exits non-zero when no engine origin is given', () => {
    const result = spawnSync(process.execPath, [SCRIPT], { encoding: 'utf8' });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /ISS verification failed/);
});

test('the CLI exits non-zero for an origin it must refuse', () => {
    const result = spawnSync(process.execPath, [SCRIPT, 'http://example.com'], { encoding: 'utf8' });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /ISS verification failed/);
});
