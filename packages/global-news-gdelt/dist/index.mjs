var __defProp = Object.defineProperty;
var __defProps = Object.defineProperties;
var __getOwnPropDescs = Object.getOwnPropertyDescriptors;
var __getOwnPropSymbols = Object.getOwnPropertySymbols;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __propIsEnum = Object.prototype.propertyIsEnumerable;
var __defNormalProp = (obj, key, value) => key in obj ? __defProp(obj, key, { enumerable: true, configurable: true, writable: true, value }) : obj[key] = value;
var __spreadValues = (a, b) => {
  for (var prop in b || (b = {}))
    if (__hasOwnProp.call(b, prop))
      __defNormalProp(a, prop, b[prop]);
  if (__getOwnPropSymbols)
    for (var prop of __getOwnPropSymbols(b)) {
      if (__propIsEnum.call(b, prop))
        __defNormalProp(a, prop, b[prop]);
    }
  return a;
};
var __spreadProps = (a, b) => __defProps(a, __getOwnPropDescs(b));

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
    const response = await fetch(url, __spreadProps(__spreadValues({}, options), {
      signal: controller.signal
    }));
    if (!response.ok) {
      throw new Error(`HTTP ${response.status} - ${response.statusText}`);
    }
    return response;
  } finally {
    clearTimeout(id);
  }
}

// src/index.ts
var GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc";
var PLUGIN_ID = "global-news-gdelt";
var SNAPSHOT_TTL_SECONDS = 5400;
var MAX_RECORDS = 50;
var _a;
var QUERY_GAP_MS = Number((_a = process.env.GDELT_QUERY_GAP_MS) != null ? _a : "6000");
var QUERIES = [
  "(conflict OR protest OR election)",
  "(earthquake OR flood OR hurricane)",
  "(strike OR riot OR unrest)"
];
var COUNTRY_CENTROIDS = {
  AF: { lat: 33, lon: 65 },
  AL: { lat: 41.2, lon: 20.1 },
  AR: { lat: -34.6, lon: -64 },
  AU: { lat: -25, lon: 134 },
  AT: { lat: 47.2, lon: 13.2 },
  AZ: { lat: 40.4, lon: 47.6 },
  BD: { lat: 23.7, lon: 90.4 },
  BY: { lat: 53.5, lon: 28 },
  BE: { lat: 50.6, lon: 4.6 },
  BR: { lat: -10, lon: -52 },
  BG: { lat: 42.7, lon: 25.5 },
  CA: { lat: 56.1, lon: -106.3 },
  CL: { lat: -35.7, lon: -71.5 },
  CN: { lat: 35.9, lon: 104.2 },
  CO: { lat: 4.6, lon: -74.3 },
  HR: { lat: 45.1, lon: 15.2 },
  CZ: { lat: 49.8, lon: 15.5 },
  DK: { lat: 56.3, lon: 9.5 },
  EG: { lat: 26.8, lon: 30.8 },
  EE: { lat: 58.6, lon: 25 },
  FI: { lat: 62, lon: 25.7 },
  FR: { lat: 46.2, lon: 2.2 },
  DE: { lat: 51.1, lon: 10.4 },
  GM: { lat: 51.1, lon: 10.4 },
  // FIPS alias for Germany
  GE: { lat: 42.3, lon: 43.4 },
  GR: { lat: 39.1, lon: 21.8 },
  HU: { lat: 47.2, lon: 19.5 },
  IN: { lat: 20.6, lon: 78.9 },
  ID: { lat: -0.8, lon: 113.9 },
  IR: { lat: 32.4, lon: 53.7 },
  IQ: { lat: 33.2, lon: 43.7 },
  IE: { lat: 53.1, lon: -8.2 },
  IL: { lat: 31, lon: 34.9 },
  IT: { lat: 42.8, lon: 12.8 },
  JP: { lat: 36.2, lon: 138.3 },
  JO: { lat: 31.3, lon: 36.3 },
  KZ: { lat: 48, lon: 67 },
  KE: { lat: 0, lon: 37.9 },
  KP: { lat: 40.3, lon: 127.5 },
  KR: { lat: 36.5, lon: 127.8 },
  KW: { lat: 29.3, lon: 47.5 },
  LB: { lat: 33.9, lon: 35.8 },
  LY: { lat: 26.3, lon: 17.2 },
  LT: { lat: 55.2, lon: 23.9 },
  LV: { lat: 56.9, lon: 24.6 },
  MY: { lat: 4.2, lon: 102 },
  MX: { lat: 23.6, lon: -102.5 },
  MD: { lat: 47.4, lon: 28.4 },
  MA: { lat: 31.8, lon: -7.1 },
  MM: { lat: 21.9, lon: 95.9 },
  NP: { lat: 28.4, lon: 84.1 },
  NL: { lat: 52.1, lon: 5.3 },
  NZ: { lat: -41.3, lon: 174.8 },
  NG: { lat: 9.6, lon: 8.1 },
  NO: { lat: 60.5, lon: 8.5 },
  PK: { lat: 30.4, lon: 69.3 },
  PS: { lat: 31.9, lon: 35.2 },
  PE: { lat: -9.2, lon: -75 },
  PH: { lat: 12.9, lon: 121.8 },
  PL: { lat: 52.1, lon: 19.4 },
  PT: { lat: 39.4, lon: -8.2 },
  QA: { lat: 25.3, lon: 51.2 },
  RO: { lat: 45.9, lon: 25 },
  RU: { lat: 61.5, lon: 105.3 },
  SA: { lat: 24, lon: 45 },
  RS: { lat: 44.2, lon: 20.9 },
  SG: { lat: 1.35, lon: 103.8 },
  SK: { lat: 48.7, lon: 19.5 },
  SI: { lat: 46.1, lon: 14.8 },
  SO: { lat: 5.2, lon: 46.2 },
  ZA: { lat: -30.6, lon: 22.9 },
  ES: { lat: 40.2, lon: -3.6 },
  SE: { lat: 60.1, lon: 18.6 },
  CH: { lat: 46.8, lon: 8.2 },
  SY: { lat: 35, lon: 38 },
  TW: { lat: 23.7, lon: 121 },
  TH: { lat: 15.1, lon: 101 },
  TR: { lat: 39, lon: 35 },
  UA: { lat: 49, lon: 31.4 },
  AE: { lat: 23.9, lon: 54.3 },
  GB: { lat: 55.4, lon: -3.4 },
  UK: { lat: 55.4, lon: -3.4 },
  // FIPS alias for the United Kingdom
  US: { lat: 39.8, lon: -98.6 },
  UZ: { lat: 41.4, lon: 64.6 },
  VE: { lat: 6.4, lon: -66.6 },
  VN: { lat: 14.1, lon: 108.3 },
  YE: { lat: 15.6, lon: 48.5 },
  ZM: { lat: -13.1, lon: 27.8 },
  ZW: { lat: -19, lon: 29.9 }
};
function extractArticles(payload) {
  if (Array.isArray(payload)) return payload;
  if (typeof payload !== "object" || payload === null) return [];
  const record = payload;
  for (const key of ["articles", "results"]) {
    const value = record[key];
    if (Array.isArray(value)) return value;
  }
  return [];
}
function parseGdeltResponse(payload) {
  var _a2, _b, _c, _d, _e, _f, _g;
  const items = [];
  for (const article of extractArticles(payload)) {
    if (typeof article !== "object" || article === null) continue;
    const url = (_a2 = article.url) != null ? _a2 : article.url_mobile;
    if (!url) continue;
    const countryCode = ((_c = (_b = article.sourcecountry) != null ? _b : article.country) != null ? _c : "").toUpperCase();
    const centroid = COUNTRY_CENTROIDS[countryCode];
    if (!centroid) continue;
    items.push({
      id: url,
      url,
      title: (_d = article.title) != null ? _d : "(untitled)",
      domain: (_e = article.domain) != null ? _e : null,
      language: (_f = article.language) != null ? _f : null,
      sourcecountry: countryCode,
      lat: centroid.lat,
      lon: centroid.lon,
      seendate: (_g = article.seendate) != null ? _g : null
    });
  }
  return items;
}
function seendateEpochMs(seendate) {
  if (seendate && /^\d{14}$/.test(seendate)) {
    const year = Number(seendate.slice(0, 4));
    const month = Number(seendate.slice(4, 6));
    const day = Number(seendate.slice(6, 8));
    const hour = Number(seendate.slice(8, 10));
    const minute = Number(seendate.slice(10, 12));
    const second = Number(seendate.slice(12, 14));
    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      return Date.UTC(year, month - 1, day, hour, minute, second);
    }
  }
  return Date.now();
}
function buildQueryUrl(query) {
  return `${GDELT_DOC_URL}?query=${encodeURIComponent(query)}&mode=artlist&format=json&maxrecords=${MAX_RECORDS}`;
}
try {
  db.prepare(
    "CREATE TABLE IF NOT EXISTS global_news_gdelt (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)"
  ).run();
} catch (err) {
  console.error("[GlobalNewsGdelt] could not ensure SQLite table:", err instanceof Error ? err.message : err);
}
var insertArticle = db.prepare(
  "INSERT OR IGNORE INTO global_news_gdelt (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)"
);
async function seedGlobalNews() {
  try {
    console.log(`[GlobalNewsGdelt] Polling GDELT DOC API with ${QUERIES.length} queries...`);
    const byUrl = /* @__PURE__ */ new Map();
    const fetchedAt = Date.now();
    let queriesSucceeded = 0;
    for (const query of QUERIES) {
      try {
        const res = await withRetry(() => fetchWithTimeout(buildQueryUrl(query)));
        const data = await res.json();
        const items2 = parseGdeltResponse(data);
        for (const item of items2) byUrl.set(item.id, item);
        queriesSucceeded += 1;
        console.log(`[GlobalNewsGdelt] Query "${query}" -> ${items2.length} mappable articles`);
      } catch (err) {
        console.error(`[GlobalNewsGdelt] query "${query}" failed:`, err instanceof Error ? err.message : err);
      }
      await new Promise((resolve) => setTimeout(resolve, QUERY_GAP_MS));
    }
    if (queriesSucceeded === 0) {
      console.error("[GlobalNewsGdelt] all queries failed; skipping snapshot");
      return;
    }
    const items = [...byUrl.values()];
    let insertedCount = 0;
    for (const item of items) {
      const result = insertArticle.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: seendateEpochMs(item.seendate),
        fetched_at: fetchedAt
      });
      if (result.changes > 0) insertedCount++;
    }
    console.log(`[GlobalNewsGdelt] Parsed ${items.length} unique articles. Saved ${insertedCount} new to SQLite.`);
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
    console.error("[GlobalNewsGdelt] seeder failed:", err instanceof Error ? err.message : err);
  }
}
var index_default = {
  name: PLUGIN_ID,
  cron: "*/15 * * * *",
  // Every 15 minutes (modest polling for a strict rate limit)
  fn: seedGlobalNews
};
export {
  COUNTRY_CENTROIDS,
  buildQueryUrl,
  index_default as default,
  extractArticles,
  parseGdeltResponse,
  seedGlobalNews,
  seendateEpochMs
};
