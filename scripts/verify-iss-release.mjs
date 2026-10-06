// Release gate for the ISS seeder.
//
// This replaced an inline shell expression that pulled a number out of the
// engine payload with a regex and compared it to the previous run. That check
// accepted a payload carrying nothing but a timestamp, never looked at the HTTP
// status, and bounded its own runtime by counting sleeps rather than by
// measuring time, so a stalled connection could hold the release open and a
// stale snapshot could pass as fresh.
//
// The contract here: readiness gets a bounded startup grace, then a full
// observation window that must span at least one Redis write throttle. Every
// request is bounded by the time actually left in its phase, and the snapshot
// must be a complete, in-range, fresh ISS fix whose persisted epoch steps
// forward. Anything else fails closed.
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';

// One satellite and one unit: a payload carrying anything else means the source
// changed underneath us.
const ISS_NORAD_ID = 25544;
const EXPECTED_UNITS = 'kilometers';

// The seeder publishes at most once per Redis write throttle (300s), so the
// persisted snapshot ages up to that long between writes. 360s accepts a full
// cycle with a minute of margin; 30s tolerates a little clock skew.
const MAX_SNAPSHOT_AGE_MS = 360000;
const MAX_CLOCK_SKEW_MS = 30000;

const MAX_TRACK_POINTS = 10;

const finite = (value) => typeof value === 'number' && Number.isFinite(value);
const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const nonblank = (value) => typeof value === 'string' && value.trim().length > 0;
const check = (condition, message) => {
    if (!condition) throw new Error(message);
};

// A track point the trail renderer can compare and draw: a real place at a real
// time. Bounds matter as much as type does -- 91 degrees and 1e308 seconds are
// both finite numbers, and 1e308 * 1000 overflows to Infinity.
function validatePoint(point) {
    check(object(point), 'invalid point');
    check(finite(point.latitude) && Math.abs(point.latitude) <= 90, 'invalid latitude');
    check(finite(point.longitude) && Math.abs(point.longitude) <= 180, 'invalid longitude');
    const milliseconds = point.timestamp * 1000;
    check(
        finite(point.timestamp) && point.timestamp > 0 && finite(milliseconds)
            && !Number.isNaN(new Date(milliseconds).getTime()),
        'invalid timestamp'
    );
}

/**
 * Validates one engine snapshot and returns the epochs it reported.
 *
 * The engine's /api/:id route returns the seeder's snapshot verbatim when it
 * already carries items, so this validates the seeder's own shape:
 * { source, fetchedAt, items: [position], track, totalCount }.
 *
 * Historical track points are checked for a usable place and time but not for
 * freshness: they are a trail by definition, so they are meant to be old.
 */
export function validateSnapshot(payload, nowMs) {
    check(object(payload) && payload.source === 'iss', 'invalid source');
    check(
        payload.totalCount === 1 && Array.isArray(payload.items) && payload.items.length === 1,
        'expected one ISS position'
    );

    const position = payload.items[0];
    validatePoint(position);
    check(
        position.id === ISS_NORAD_ID && nonblank(position.name) && nonblank(position.visibility)
            && position.units === EXPECTED_UNITS,
        'invalid identity or units'
    );

    for (const field of ['altitude', 'velocity', 'footprint']) {
        check(finite(position[field]) && position[field] >= 0, 'invalid ' + field);
    }

    const fetchedAtMs = typeof payload.fetchedAt === 'string' ? Date.parse(payload.fetchedAt) : NaN;
    for (const time of [position.timestamp * 1000, fetchedAtMs]) {
        check(
            finite(time) && nowMs - time <= MAX_SNAPSHOT_AGE_MS && time - nowMs <= MAX_CLOCK_SKEW_MS,
            'stale or future-dated snapshot'
        );
    }

    check(Array.isArray(payload.track) && payload.track.length <= MAX_TRACK_POINTS, 'invalid track');
    payload.track.forEach(validatePoint);

    return { timestamp: position.timestamp, fetchedAtMs };
}

/**
 * Waits for a freshly deployed engine to serve a fresh, advancing ISS snapshot.
 *
 * Phase one retries until the engine is ready or the startup grace expires.
 * Phase two then observes for a full window regardless of how quickly the
 * snapshot advances, because the write throttle means a healthy snapshot can
 * legitimately sit still for 300s. An advance counts as evidence once it has
 * been observed anywhere at or after the throttle mark, so a step that lands
 * early in the window is not lost.
 */
export async function verifyIssRelease(baseUrl, options = {}) {
    const origin = new URL(baseUrl);
    check(!origin.username && !origin.password, 'URL must not contain credentials');
    check(
        origin.protocol === 'https:'
            || (origin.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(origin.hostname)),
        'use HTTPS or local HTTP'
    );
    check(origin.pathname === '/' && !origin.search && !origin.hash, 'provide an engine origin');

    const {
        fetchImpl = globalThis.fetch,
        clock = () => performance.now(),
        wallNow = () => Date.now(),
        sleep = (ms) => delay(ms),
        startupMs = 180000,
        observeMs = 400000,
        pollMs = 10000,
        requestMs = 10000,
        throttleMs = 300000,
    } = options;

    check(
        [startupMs, observeMs, pollMs, requestMs, throttleMs].every((value) => finite(value) && value > 0)
            && observeMs > throttleMs,
        'invalid timings'
    );

    // One GET, bounded by the earlier of this request's budget and the phase's
    // own deadline. The budget covers reading the body, not just the response
    // headers, so a stalled body cannot outlive the phase.
    async function read(path, phaseDeadline) {
        const end = Math.min(phaseDeadline, clock() + requestMs);
        const budget = end - clock();
        check(budget > 0, 'phase deadline exceeded');

        const controller = new AbortController();
        let timer;
        const timeout = new Promise((_, reject) => {
            timer = setTimeout(() => {
                controller.abort();
                reject(new Error('request deadline exceeded'));
            }, budget);
        });

        try {
            const operation = (async () => {
                const response = await fetchImpl(new URL(path, origin), {
                    signal: controller.signal,
                    cache: 'no-store',
                });
                check(response.status === 200, path + ' returned HTTP ' + response.status);
                return await response.json();
            })();
            // The race surfaces whichever side settles first. This handler only
            // keeps the loser from becoming an unhandled rejection once the
            // timeout wins and the abort makes the fetch reject with AbortError.
            operation.catch(() => {});

            const payload = await Promise.race([operation, timeout]);
            check(clock() <= end, 'request deadline exceeded');
            return payload;
        } finally {
            clearTimeout(timer);
        }
    }

    async function sample(deadline) {
        await read('/health', deadline);
        return validateSnapshot(await read('/api/iss', deadline), wallNow());
    }

    const startupDeadline = clock() + startupMs;
    let baseline;
    let lastError = 'no sample';
    while (clock() < startupDeadline) {
        try {
            baseline = await sample(startupDeadline);
            break;
        } catch (error) {
            lastError = error.message;
        }
        await sleep(Math.max(0, Math.min(5000, startupDeadline - clock())));
    }
    check(Boolean(baseline), 'startup deadline exceeded: ' + lastError);

    const started = clock();
    const deadline = started + observeMs;
    let latest = baseline;
    let advancedAcrossWindow = false;

    while (clock() < deadline) {
        await sleep(Math.min(pollMs, deadline - clock()));
        if (clock() >= deadline) break;

        const next = await sample(deadline);
        check(
            next.timestamp >= latest.timestamp && next.fetchedAtMs >= latest.fetchedAtMs,
            'snapshot moved backwards'
        );
        latest = next;

        if (clock() - started >= throttleMs && next.timestamp > baseline.timestamp
            && next.fetchedAtMs > baseline.fetchedAtMs) {
            advancedAcrossWindow = true;
        }
    }

    check(
        advancedAcrossWindow && latest.timestamp > baseline.timestamp
            && latest.fetchedAtMs > baseline.fetchedAtMs,
        'snapshot did not advance across the observation window'
    );

    return { ok: true, observedMs: clock() - started, baseline, latest };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    verifyIssRelease(process.argv[2])
        .then((report) => {
            console.log(JSON.stringify(report));
        })
        .catch((error) => {
            console.error('ISS verification failed: ' + error.message);
            process.exitCode = 1;
        });
}
