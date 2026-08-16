// ../../node_modules/.pnpm/@worldwideview+seeder-sdk@1.0.0/node_modules/@worldwideview/seeder-sdk/dist/index.mjs
import Database from "better-sqlite3";
import path from "path";
import { Redis } from "ioredis";
import dotenv from "dotenv";
import path2 from "path";
import zlib from "zlib";
import geoip from "geoip-lite";
var dbPath = process.env.DB_PATH || path.join(process.cwd(), "data", "engine.db");
var db = new Database(dbPath, {
  // Use verbose logging if needed for debugging
  // verbose: console.log
});
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.pragma("busy_timeout = 5000");
function initDB() {
  console.log(`[DB] Initializing SQLite database at ${dbPath}`);
  db.exec(`
    CREATE TABLE IF NOT EXISTS iranwar_events (
      event_id TEXT PRIMARY KEY,
      payload JSON NOT NULL,
      timestamp TEXT NOT NULL,
      fetched_at INTEGER NOT NULL
    )
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS earthquakes (
      id TEXT PRIMARY KEY,
      payload JSON NOT NULL,
      source_ts INTEGER NOT NULL,
      fetched_at INTEGER NOT NULL
    )
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS wildfires (
      id TEXT PRIMARY KEY,
      payload JSON NOT NULL,
      source_ts INTEGER NOT NULL,
      fetched_at INTEGER NOT NULL
    )
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS maritime_history (
      mmsi TEXT NOT NULL,
      ts INTEGER NOT NULL,
      lat REAL NOT NULL,
      lon REAL NOT NULL,
      hdg REAL,
      spd REAL,
      fetched_at INTEGER NOT NULL,
      PRIMARY KEY (mmsi, ts)
    )
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_maritime_history_mmsi_ts ON maritime_history(mmsi, ts);
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_maritime_history_ts ON maritime_history(ts);
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS aviation_history (
      icao24 TEXT NOT NULL,
      ts INTEGER NOT NULL,
      lat REAL,
      lon REAL,
      alt REAL,
      hdg REAL,
      spd REAL,
      fetched_at INTEGER NOT NULL,
      PRIMARY KEY (icao24, ts)
    )
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_aviation_history_icao24_ts ON aviation_history(icao24, ts);
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_aviation_history_ts ON aviation_history(ts);
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS military_aviation_history (
      hex TEXT NOT NULL,
      ts INTEGER NOT NULL,
      lat REAL,
      lon REAL,
      alt REAL,
      hdg REAL,
      spd REAL,
      fetched_at INTEGER NOT NULL,
      PRIMARY KEY (hex, ts)
    )
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_military_aviation_history_hex_ts ON military_aviation_history(hex, ts);
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_military_aviation_history_ts ON military_aviation_history(ts);
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS gps_jamming (
      id TEXT PRIMARY KEY,
      payload JSON NOT NULL,
      source_ts INTEGER NOT NULL,
      fetched_at INTEGER NOT NULL
    )
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS conflict_events (
      id TEXT PRIMARY KEY,
      payload JSON NOT NULL,
      source_ts INTEGER NOT NULL,
      fetched_at INTEGER NOT NULL
    )
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS civil_unrest (
      id TEXT PRIMARY KEY,
      payload JSON NOT NULL,
      source_ts INTEGER NOT NULL,
      fetched_at INTEGER NOT NULL
    )
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS cyber_attacks (
      id TEXT PRIMARY KEY,
      payload JSON NOT NULL,
      source_ts INTEGER NOT NULL,
      fetched_at INTEGER NOT NULL
    )
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_cyber_attacks_source_ts ON cyber_attacks(source_ts);
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS sanctions (
      id TEXT PRIMARY KEY,
      payload JSON NOT NULL,
      source_ts INTEGER NOT NULL,
      fetched_at INTEGER NOT NULL
    )
  `);
  console.log("[DB] All tables initialized successfully.");
}
initDB();
dotenv.config({ path: path2.resolve(process.cwd(), ".env.local") });
var redisUrl = process.env.REDIS_URL || "redis://redis:6379";
if (redisUrl.includes("upstash.io") && redisUrl.startsWith("redis://")) {
  console.warn("\n\x1B[33m[CONFIG WARNING]\x1B[0m \u{1F6A8} Upstash environment detected via redis:// without TLS.");
  console.warn("Automatically upgrading process connection pipeline to rediss:// protocol...\n");
  redisUrl = redisUrl.replace(/^redis:\/\//, "rediss://");
}
console.log(`[Redis] Connecting to ${redisUrl.replace(/:[^:@]+@/, ":***@")} ...`);
var redis = new Redis(redisUrl, {
  // Common reconnect strategy
  retryStrategy(times) {
    const delay = Math.min(times * 50, 2e3);
    return delay;
  },
  maxRetriesPerRequest: 3
});
redis.on("error", (err) => {
  console.error("[Redis] Connection Error against URL:", redisUrl.replace(/:[^:@]+@/, ":***@"));
  console.error("[Redis] Error Object:", err);
});
redis.on("ready", () => {
  console.log("[Redis] Connected and ready.");
});
var lastSnapshotTimes = /* @__PURE__ */ new Map();
var SNAPSHOT_THROTTLE_MS = 5 * 60 * 1e3;
async function setLiveSnapshot(source, payload, ttlSeconds) {
  try {
    if (typeof globalThis.broadcastPluginData === "function") {
      globalThis.broadcastPluginData(source, payload);
    }
    const now = Date.now();
    const lastTime = lastSnapshotTimes.get(source) || 0;
    if (now - lastTime < SNAPSHOT_THROTTLE_MS) {
      return;
    }
    lastSnapshotTimes.set(source, now);
    const key = `data:${source}:live`;
    const jsonStr = JSON.stringify(payload);
    const compressed = zlib.gzipSync(Buffer.from(jsonStr, "utf-8"));
    await redis.set(key, compressed, "EX", ttlSeconds);
    await redis.set(`meta:${source}:last_run`, Date.now().toString(), "EX", ttlSeconds * 2);
    console.log(`[Redis] Snapshot saved to Redis for ${source} (${(compressed.length / 1024).toFixed(2)} KB)`);
  } catch (error) {
    console.error(`[Redis] Failed to snapshot ${source}:`, error);
  }
}
async function withRetry(fn, maxRetries = 3, delayMs = 1e3) {
  let lastErr;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (attempt < maxRetries) {
        const wait = delayMs * Math.pow(2, attempt);
        console.warn(`[Retry] Attempt ${attempt + 1}/${maxRetries} failed: ${err.message}. Waiting ${wait}ms...`);
        await new Promise((r) => setTimeout(r, wait));
      }
    }
  }
  throw lastErr;
}
async function fetchWithTimeout(url, options = {}, timeoutMs = 15e3) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status} - ${response.statusText}`);
    }
    return response;
  } finally {
    clearTimeout(id);
  }
}
var CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36";

// src/index.ts
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
