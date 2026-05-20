// src/index.ts
import { setLiveSnapshot, withRetry, fetchWithTimeout, CHROME_UA } from "@worldwideview/seeder-sdk";
import * as Sentry from "@sentry/node";
import * as satellite from "satellite.js";
var BASE_URL = "https://celestrak.org/NORAD/elements/gp.php";
var PROXY_WORKER_URL = "https://wwv-proxy.titmitna.workers.dev/?url=";
var MILITARY_GROUPS = ["military", "resource"];
var tleCache = /* @__PURE__ */ new Map();
async function fetchTLEGroup(group) {
  const targetUrl = `${BASE_URL}?GROUP=${encodeURIComponent(group)}&FORMAT=tle`;
  const url = `${PROXY_WORKER_URL}${encodeURIComponent(targetUrl)}`;
  let res;
  try {
    res = await withRetry(() => fetchWithTimeout(url, { headers: { "User-Agent": CHROME_UA } }, 15e3));
  } catch (err) {
    console.error(`[SurveillanceSatellites] Network error fetching group=${group}: ${err.message}`);
    return [];
  }
  const text = await res.text();
  const lines = text.split("\n").map((l) => l.trim()).filter((l) => l.length > 0);
  const records = [];
  for (let i = 0; i < lines.length - 2; i += 3) {
    try {
      const satrec = satellite.twoline2satrec(lines[i + 1], lines[i + 2]);
      records.push({
        OBJECT_NAME: lines[i],
        TLE_LINE1: lines[i + 1],
        TLE_LINE2: lines[i + 2],
        NORAD_CAT_ID: parseInt(lines[i + 1].substring(2, 7).trim(), 10),
        group,
        satrec
      });
    } catch {
    }
  }
  return records;
}
async function refreshTLEs() {
  var _a;
  console.log("[SurveillanceSatellites] Refreshing TLEs from Celestrak...");
  for (const group of MILITARY_GROUPS) {
    try {
      const records = await fetchTLEGroup(group);
      if (records.length > 0) {
        tleCache.set(group, records);
        console.log(`[SurveillanceSatellites] Loaded ${records.length} TLEs for group=${group}`);
      }
    } catch (err) {
      const isTimeout = err.code === "UND_ERR_CONNECT_TIMEOUT" || err.name === "AbortError" || ((_a = err.message) == null ? void 0 : _a.includes("timeout"));
      if (!isTimeout) {
        Sentry.captureException(err, { extra: { context: "fetchTLEGroup", group } });
      }
      console.error(`[SurveillanceSatellites] Error fetching ${group}:`, err.message);
    }
  }
}
function propagatePositions() {
  const now = /* @__PURE__ */ new Date();
  const gmst = satellite.gstime(now);
  const positions = [];
  for (const [group, records] of tleCache.entries()) {
    for (const rec of records) {
      try {
        const pv = satellite.propagate(rec.satrec, now);
        if (!pv.position || typeof pv.position === "boolean" || !pv.velocity || typeof pv.velocity === "boolean") continue;
        const geo = satellite.eciToGeodetic(pv.position, gmst);
        const lat = satellite.degreesLat(geo.latitude);
        const lon = satellite.degreesLong(geo.longitude);
        const alt = geo.height;
        if (!isFinite(lat) || !isFinite(lon) || !isFinite(alt)) continue;
        if (alt < 0 || alt > 1e5) continue;
        const vel = pv.velocity;
        const speed = Math.sqrt(vel.x ** 2 + vel.y ** 2 + vel.z ** 2) * 1e3;
        const heading = (Math.atan2(vel.x, vel.z) * (180 / Math.PI) + 360) % 360;
        positions.push({
          noradId: rec.NORAD_CAT_ID,
          name: rec.OBJECT_NAME,
          latitude: lat,
          longitude: lon,
          altitude: alt,
          heading,
          speed,
          group: group === "military" ? "military" : "recon",
          country: rec.COUNTRY_CODE,
          objectType: rec.OBJECT_TYPE,
          period: rec.PERIOD
        });
      } catch {
      }
    }
  }
  return positions;
}
async function publishPositions() {
  const positions = propagatePositions();
  if (positions.length === 0) return;
  try {
    await setLiveSnapshot("surveillance-satellites", { satellites: positions }, 60 * 60);
  } catch (err) {
    console.error("[SurveillanceSatellites] Error publishing snapshot:", err);
    Sentry.captureException(err, { extra: { context: "publishPositions" } });
  }
}
function startSurveillanceSatelliteSeeder() {
  console.log("[SurveillanceSatellites] Starting seeder.");
  refreshTLEs().then(() => {
    publishPositions();
    setInterval(publishPositions, 15e3);
  });
  setInterval(refreshTLEs, 60 * 60 * 1e3);
}
var index_default = {
  name: "surveillance-satellites",
  init: startSurveillanceSatelliteSeeder
};
export {
  index_default as default
};
