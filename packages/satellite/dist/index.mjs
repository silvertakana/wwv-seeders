// src/index.ts
import { setLiveSnapshot } from "@worldwideview/seeder-sdk";
import { withRetry, fetchWithTimeout, CHROME_UA } from "@worldwideview/seeder-sdk";
import * as Sentry from "@sentry/node";
import * as satellite from "satellite.js";
var BASE_URL = "https://celestrak.org/NORAD/elements/gp.php";
var PROXY_WORKER_URL = "https://wwv-proxy.titmitna.workers.dev/?url=";
var DEFAULT_GROUPS = [
  "stations",
  // ISS, Tiangong, etc.
  "visual",
  // Brightest 100 satellites
  "weather",
  // Weather satellites
  "gps-ops",
  // GPS constellation
  "resource",
  // Earth observation / reconnaissance
  "military"
  // Military reconnaissance
];
var globalsTLECache = /* @__PURE__ */ new Map();
async function fetchTLEGroup(group) {
  const targetUrl = `${BASE_URL}?GROUP=${encodeURIComponent(group)}&FORMAT=tle`;
  const url = `${PROXY_WORKER_URL}${encodeURIComponent(targetUrl)}`;
  let res;
  try {
    res = await withRetry(() => fetchWithTimeout(url, { headers: { "User-Agent": CHROME_UA } }, 15e3));
  } catch (err) {
    console.error(`[SatelliteSeeder] Network error fetching group=${group}: ${err.message}`);
    return [];
  }
  const text = await res.text();
  const lines = text.split("\n").map((l) => l.trim()).filter((l) => l.length > 0);
  const data = [];
  for (let i = 0; i < lines.length - 2; i += 3) {
    data.push({
      OBJECT_NAME: lines[i],
      TLE_LINE1: lines[i + 1],
      TLE_LINE2: lines[i + 2],
      NORAD_CAT_ID: parseInt(lines[i + 1].substring(2, 7).trim(), 10)
    });
  }
  return data;
}
async function refreshAllTLEs() {
  console.log("[SatelliteSeeder] Refreshing TLEs from Celestrak...");
  for (const group of DEFAULT_GROUPS) {
    try {
      const records = await fetchTLEGroup(group);
      if (records.length > 0) {
        globalsTLECache.set(group, records);
      }
    } catch (err) {
      console.error(`[SatelliteSeeder] Error fetching ${group}:`, err.message);
      const isTimeout = err.code === "UND_ERR_CONNECT_TIMEOUT" || err.name === "AbortError" || err.message.includes("fetch failed") || err.message.includes("timeout");
      if (!isTimeout) {
        Sentry.captureException(err, { extra: { context: "fetchTLEGroup", group } });
      }
    }
  }
}
function propagateAll(records, time, group) {
  const gmst = satellite.gstime(time);
  const results = [];
  for (const rec of records) {
    try {
      if (!rec.satrec) {
        rec.satrec = satellite.twoline2satrec(rec.TLE_LINE1, rec.TLE_LINE2);
      }
      const satrec = rec.satrec;
      const pv = satellite.propagate(satrec, time);
      if (!pv.position || typeof pv.position === "boolean" || !pv.velocity || typeof pv.velocity === "boolean") {
        continue;
      }
      const geo = satellite.eciToGeodetic(pv.position, gmst);
      const lat = satellite.degreesLat(geo.latitude);
      const lon = satellite.degreesLong(geo.longitude);
      const alt = geo.height;
      if (!isFinite(lat) || !isFinite(lon) || !isFinite(alt)) continue;
      if (alt < 0 || alt > 1e5) continue;
      const vel = pv.velocity;
      const speed = Math.sqrt(vel.x ** 2 + vel.y ** 2 + vel.z ** 2) * 1e3;
      const heading = (Math.atan2(vel.x, vel.z) * (180 / Math.PI) + 360) % 360;
      results.push({
        noradId: rec.NORAD_CAT_ID,
        name: rec.OBJECT_NAME,
        latitude: lat,
        longitude: lon,
        altitude: alt,
        heading,
        speed,
        group,
        country: rec.COUNTRY_CODE,
        objectType: rec.OBJECT_TYPE,
        period: rec.PERIOD
      });
    } catch {
    }
  }
  return results;
}
async function computeAndPublishPositions() {
  const now = /* @__PURE__ */ new Date();
  const civilianObj = /* @__PURE__ */ Object.create(null);
  let totalCivilian = 0;
  for (const group of DEFAULT_GROUPS) {
    const records = globalsTLECache.get(group);
    if (!records) continue;
    const isMilitary = group === "military" || group === "resource";
    if (isMilitary) continue;
    const positions = propagateAll(records, now, group);
    for (const p of positions) {
      civilianObj[p.noradId] = p;
      totalCivilian++;
    }
  }
  try {
    if (totalCivilian > 0) {
      await setLiveSnapshot("satellite", civilianObj, 60 * 60);
    }
  } catch (err) {
    console.error(`[SatelliteSeeder] Error publishing to Redis:`, err);
    Sentry.captureException(err, { extra: { context: "publishPositions" } });
  }
}
var syncParams = {
  tleFetchInterval: 1e3 * 60 * 60,
  // 1 hour
  publishInterval: 1e3 * 15,
  // 15 seconds
  tleIntervalId: null,
  publishIntervalId: null
};
function startSatelliteSeeder() {
  console.log("[SatelliteSeeder] Starting satellite TLE seeder.");
  refreshAllTLEs().then(() => {
    computeAndPublishPositions();
    syncParams.publishIntervalId = setInterval(computeAndPublishPositions, syncParams.publishInterval);
  });
  syncParams.tleIntervalId = setInterval(refreshAllTLEs, syncParams.tleFetchInterval);
}
var index_default = {
  name: "satellite",
  init: startSatelliteSeeder
};
export {
  DEFAULT_GROUPS,
  index_default as default,
  globalsTLECache,
  propagateAll,
  startSatelliteSeeder
};
