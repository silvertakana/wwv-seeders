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

// src/index.ts
var OPENAQ_BASE = "https://api.openaq.org/v3/parameters";
var OPENAQ_SOURCE_URL = "https://openaq.org";
var PLUGIN_ID = "air-quality-openaq";
var SNAPSHOT_TTL_SECONDS = 1800;
var PAGE_SIZE = 1e3;
var MAX_PAGES_PER_PARAMETER = 3;
var REQUEST_DELAY_MS = 800;
var PARAMETER_IDS = [2, 1, 3, 5];
var PARAMETER_FIELD = {
  1: "pm10",
  2: "pm25",
  3: "o3",
  5: "no2"
};
var EPA_PM25_BANDS = [
  { cLow: 0, cHigh: 12, iLow: 0, iHigh: 50 },
  { cLow: 12.1, cHigh: 35.4, iLow: 51, iHigh: 100 },
  { cLow: 35.5, cHigh: 55.4, iLow: 101, iHigh: 150 },
  { cLow: 55.5, cHigh: 150.4, iLow: 151, iHigh: 200 },
  { cLow: 150.5, cHigh: 250.4, iLow: 201, iHigh: 300 },
  { cLow: 250.5, cHigh: 500.4, iLow: 301, iHigh: 500 }
];
function epaAqiFromPm25(pm25) {
  if (!Number.isFinite(pm25) || pm25 < 0) return null;
  const band = EPA_PM25_BANDS.find((b) => pm25 <= b.cHigh);
  if (!band) return 500;
  const { cLow, cHigh, iLow, iHigh } = band;
  return Math.round((iHigh - iLow) / (cHigh - cLow) * (pm25 - cLow) + iLow);
}
function aqiCategory(aqi) {
  if (!Number.isFinite(aqi) || aqi < 0) return "Unknown";
  if (aqi <= 50) return "Good";
  if (aqi <= 100) return "Moderate";
  if (aqi <= 150) return "Unhealthy for Sensitive Groups";
  if (aqi <= 200) return "Unhealthy";
  if (aqi <= 300) return "Very Unhealthy";
  return "Hazardous";
}
function parseOpenAqResult(result, field) {
  if (result === null || typeof result !== "object") return null;
  const r = result;
  const coords = r.coordinates;
  const lat = typeof coords?.latitude === "number" ? coords.latitude : NaN;
  const lon = typeof coords?.longitude === "number" ? coords.longitude : NaN;
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  const { sensorsId, locationsId } = r;
  if (typeof sensorsId !== "number" || typeof locationsId !== "number") return null;
  const datetime = r.datetime;
  const utc = typeof datetime?.utc === "string" ? datetime.utc : null;
  if (!utc) return null;
  const value = typeof r.value === "number" ? r.value : Number(r.value);
  if (!Number.isFinite(value) || value < 0) return null;
  return {
    key: `${sensorsId}|${locationsId}|${utc}`,
    sensorsId,
    locationsId,
    datetimeUtc: utc,
    field,
    value,
    lat,
    lon
  };
}
function parseParameterPayload(payload, parameterId) {
  const field = PARAMETER_FIELD[parameterId];
  if (!field || payload === null || typeof payload !== "object") return [];
  const results = payload.results;
  if (!Array.isArray(results)) return [];
  const readings = [];
  for (const result of results) {
    const reading = parseOpenAqResult(result, field);
    if (reading) readings.push(reading);
  }
  return readings;
}
function mergeReadings(readings) {
  const byKey = /* @__PURE__ */ new Map();
  for (const reading of readings) {
    let item = byKey.get(reading.key);
    if (!item) {
      item = {
        id: reading.key,
        lat: reading.lat,
        lon: reading.lon,
        date: reading.datetimeUtc,
        url: OPENAQ_SOURCE_URL
      };
      byKey.set(reading.key, item);
    }
    item[reading.field] = reading.value;
    if (reading.field === "pm25") {
      const aqi = epaAqiFromPm25(reading.value);
      if (aqi !== null) item.aqi = aqi;
    }
  }
  return Array.from(byKey.values());
}
function parseAllParameters(payloads) {
  const readings = [];
  for (const parameterId of PARAMETER_IDS) {
    const payload = payloads[parameterId];
    if (payload === void 0) continue;
    readings.push(...parseParameterPayload(payload, parameterId));
  }
  return mergeReadings(readings);
}
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
async function fetchParameterPayload(parameterId, apiKey) {
  const results = [];
  for (let page = 0; page < MAX_PAGES_PER_PARAMETER; page++) {
    const url = `${OPENAQ_BASE}/${parameterId}/latest?limit=${PAGE_SIZE}&offset=${page * PAGE_SIZE}`;
    let res;
    try {
      res = await withRetry(
        () => fetchWithTimeout(url, {
          headers: { "X-API-Key": apiKey, "User-Agent": "WWV-Data-Engine" }
        })
      );
    } catch (err) {
      console.warn(
        `[AirQualityOpenAQ] param ${parameterId} page ${page} failed: ${err instanceof Error ? err.message : String(err)}`
      );
      break;
    }
    if (!res.ok) {
      console.warn(`[AirQualityOpenAQ] param ${parameterId} page ${page}: HTTP ${res.status}`);
      break;
    }
    const data = await res.json();
    const pageResults = Array.isArray(data.results) ? data.results : [];
    results.push(...pageResults);
    if (pageResults.length < PAGE_SIZE) break;
    await sleep(REQUEST_DELAY_MS);
  }
  return { results };
}
try {
  db.prepare(
    "CREATE TABLE IF NOT EXISTS air_quality (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)"
  ).run();
} catch (err) {
  console.error("[AirQualityOpenAQ] could not ensure SQLite table:", err instanceof Error ? err.message : err);
}
var insertReading = db.prepare(
  "INSERT OR IGNORE INTO air_quality (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)"
);
async function seedAirQuality() {
  const apiKey = process.env.OPENAQ_API_KEY;
  if (!apiKey) {
    console.warn("[AirQualityOpenAQ] OPENAQ_API_KEY not set - skipping.");
    return;
  }
  try {
    console.log("[AirQualityOpenAQ] Polling OpenAQ v3 latest readings...");
    const payloads = {};
    for (const parameterId of PARAMETER_IDS) {
      try {
        payloads[parameterId] = await fetchParameterPayload(parameterId, apiKey);
      } catch (err) {
        console.warn(
          `[AirQualityOpenAQ] param ${parameterId} skipped: ${err instanceof Error ? err.message : String(err)}`
        );
      }
    }
    const items = parseAllParameters(payloads);
    if (items.length === 0) {
      console.warn("[AirQualityOpenAQ] No readings after all pulls; skipping snapshot.");
      return;
    }
    const fetchedAt = Date.now();
    let insertedCount = 0;
    for (const item of items) {
      const result = insertReading.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: item.date ? Date.parse(item.date) || fetchedAt : fetchedAt,
        fetched_at: fetchedAt
      });
      if (result.changes > 0) insertedCount++;
    }
    console.log(`[AirQualityOpenAQ] Parsed ${items.length} sensors. Saved ${insertedCount} new to SQLite.`);
    await setLiveSnapshot(
      PLUGIN_ID,
      {
        source: PLUGIN_ID,
        fetchedAt: (/* @__PURE__ */ new Date()).toISOString(),
        items,
        totalCount: items.length
      },
      SNAPSHOT_TTL_SECONDS
    );
  } catch (err) {
    console.error("[AirQualityOpenAQ] seeder failed:", err instanceof Error ? err.message : err);
  }
}
var index_default = {
  name: PLUGIN_ID,
  cron: "*/30 * * * *",
  // Every 30 minutes
  fn: seedAirQuality
};
export {
  EPA_PM25_BANDS,
  OPENAQ_SOURCE_URL,
  PARAMETER_FIELD,
  PARAMETER_IDS,
  aqiCategory,
  index_default as default,
  epaAqiFromPm25,
  fetchParameterPayload,
  mergeReadings,
  parseAllParameters,
  parseOpenAqResult,
  parseParameterPayload,
  seedAirQuality
};
