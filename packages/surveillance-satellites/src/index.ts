import { setLiveSnapshot, withRetry, fetchWithTimeout, CHROME_UA } from '@worldwideview/seeder-sdk';
import * as Sentry from '@sentry/node';
import * as satellite from 'satellite.js';

const BASE_URL = 'https://celestrak.org/NORAD/elements/gp.php';
const PROXY_WORKER_URL = 'https://wwv-proxy.titmitna.workers.dev/?url=';

/** CelesTrak groups that represent military / reconnaissance satellites. */
const MILITARY_GROUPS = ['military', 'resource'] as const;

/** TLE_LINE1 + TLE_LINE2 + parsed satrec. */
interface TLERecord {
    OBJECT_NAME: string;
    TLE_LINE1: string;
    TLE_LINE2: string;
    NORAD_CAT_ID: number;
    COUNTRY_CODE?: string;
    OBJECT_TYPE?: string;
    PERIOD?: number;
    group: string;
    satrec: any;
}

/** Shape returned to the frontend plugin. */
interface SatellitePosition {
    noradId: number;
    name: string;
    latitude: number;
    longitude: number;
    altitude: number;
    heading: number;
    speed: number;
    group: string;
    country?: string;
    objectType?: string;
    period?: number;
}

const tleCache = new Map<string, TLERecord[]>();

async function fetchTLEGroup(group: string): Promise<TLERecord[]> {
    const targetUrl = `${BASE_URL}?GROUP=${encodeURIComponent(group)}&FORMAT=tle`;
    const url = `${PROXY_WORKER_URL}${encodeURIComponent(targetUrl)}`;

    let res;
    try {
        res = await withRetry(() => fetchWithTimeout(url, { headers: { 'User-Agent': CHROME_UA } }, 15000));
    } catch (err: any) {
        console.error(`[SurveillanceSatellites] Network error fetching group=${group}: ${err.message}`);
        return [];
    }

    const text = await res.text();
    const lines = text.split('\n').map((l: string) => l.trim()).filter((l: string) => l.length > 0);
    const records: TLERecord[] = [];

    for (let i = 0; i < lines.length - 2; i += 3) {
        try {
            const satrec = satellite.twoline2satrec(lines[i + 1], lines[i + 2]);
            records.push({
                OBJECT_NAME: lines[i],
                TLE_LINE1: lines[i + 1],
                TLE_LINE2: lines[i + 2],
                NORAD_CAT_ID: parseInt(lines[i + 1].substring(2, 7).trim(), 10),
                group,
                satrec,
            });
        } catch {
            // Malformed TLE line — skip
        }
    }
    return records;
}

async function refreshTLEs(): Promise<void> {
    console.log('[SurveillanceSatellites] Refreshing TLEs from Celestrak...');
    for (const group of MILITARY_GROUPS) {
        try {
            const records = await fetchTLEGroup(group);
            if (records.length > 0) {
                tleCache.set(group, records);
                console.log(`[SurveillanceSatellites] Loaded ${records.length} TLEs for group=${group}`);
            }
        } catch (err: any) {
            const isTimeout = err.code === 'UND_ERR_CONNECT_TIMEOUT' || err.name === 'AbortError' || err.message?.includes('timeout');
            if (!isTimeout) {
                Sentry.captureException(err, { extra: { context: 'fetchTLEGroup', group } });
            }
            console.error(`[SurveillanceSatellites] Error fetching ${group}:`, err.message);
        }
    }
}

function propagatePositions(): SatellitePosition[] {
    const now = new Date();
    const gmst = satellite.gstime(now);
    const positions: SatellitePosition[] = [];

    for (const [group, records] of tleCache.entries()) {
        for (const rec of records) {
            try {
                const pv = satellite.propagate(rec.satrec, now);
                if (!pv.position || typeof pv.position === 'boolean' || !pv.velocity || typeof pv.velocity === 'boolean') continue;

                const geo = satellite.eciToGeodetic(pv.position as any, gmst);
                const lat = satellite.degreesLat(geo.latitude);
                const lon = satellite.degreesLong(geo.longitude);
                const alt = geo.height; // km

                if (!isFinite(lat) || !isFinite(lon) || !isFinite(alt)) continue;
                if (alt < 0 || alt > 100000) continue;

                const vel = pv.velocity as any;
                const speed = Math.sqrt(vel.x ** 2 + vel.y ** 2 + vel.z ** 2) * 1000; // m/s
                const heading = (Math.atan2(vel.x, vel.z) * (180 / Math.PI) + 360) % 360;

                positions.push({
                    noradId: rec.NORAD_CAT_ID,
                    name: rec.OBJECT_NAME,
                    latitude: lat,
                    longitude: lon,
                    altitude: alt,
                    heading,
                    speed,
                    group: group === 'military' ? 'military' : 'recon',
                    country: rec.COUNTRY_CODE,
                    objectType: rec.OBJECT_TYPE,
                    period: rec.PERIOD,
                });
            } catch {
                // Propagation error — skip satellite
            }
        }
    }
    return positions;
}

async function publishPositions(): Promise<void> {
    const positions = propagatePositions();
    if (positions.length === 0) return;

    try {
        // Store as { satellites: [...] } — matches what the frontend plugin reads via data.satellites
        await setLiveSnapshot('surveillance-satellites', { satellites: positions }, 60 * 60);
    } catch (err) {
        console.error('[SurveillanceSatellites] Error publishing snapshot:', err);
        Sentry.captureException(err, { extra: { context: 'publishPositions' } });
    }
}

function startSurveillanceSatelliteSeeder(): void {
    console.log('[SurveillanceSatellites] Starting seeder.');

    refreshTLEs().then(() => {
        publishPositions();
        setInterval(publishPositions, 15_000);          // propagate every 15 s
    });

    setInterval(refreshTLEs, 60 * 60 * 1000);           // refresh TLEs every 1 h
}

export default {
    name: 'surveillance-satellites',
    init: startSurveillanceSatelliteSeeder,
};
