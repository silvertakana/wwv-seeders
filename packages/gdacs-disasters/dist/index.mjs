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
var GDACS_API_URL = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH";
var PLUGIN_ID = "gdacs-disasters";
var SNAPSHOT_TTL_SECONDS = 1800;
function parseGdacsDate(value) {
  if (!value) return null;
  const trimmed = value.trim();
  if (trimmed.length === 0) return null;
  const normalized = /(?:Z|[+-]\d{2}:?\d{2})$/.test(trimmed) ? trimmed : `${trimmed}Z`;
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}
function num(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}
function str(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}
function parseGdacsPayload(payload) {
  if (typeof payload !== "object" || payload === null) return [];
  const features = payload.features;
  if (!Array.isArray(features)) return [];
  const items = [];
  for (const raw of features) {
    if (typeof raw !== "object" || raw === null) continue;
    const feature = raw;
    const p = feature.properties ?? {};
    if (String(p.iscurrent) !== "true") continue;
    if (String(p.istemporary) === "true") continue;
    const coords = feature.geometry?.coordinates;
    if (!Array.isArray(coords) || coords.length < 2) continue;
    const lon = Number(coords[0]);
    const lat = Number(coords[1]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
    if (lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;
    const eventtype = str(p.eventtype);
    const eventid = num(p.eventid);
    const id = eventtype && eventid !== null ? `${eventtype}-${eventid}` : null;
    if (!id) continue;
    const url = typeof p.url === "object" && p.url !== null ? p.url : {};
    const severityData = typeof p.severitydata === "object" && p.severitydata !== null ? p.severitydata : {};
    items.push({
      id,
      latitude: lat,
      longitude: lon,
      timestamp: parseGdacsDate(str(p.fromdate)) ?? (/* @__PURE__ */ new Date()).toISOString(),
      eventtype: eventtype ?? null,
      eventname: str(p.eventname),
      name: str(p.name),
      description: str(p.description),
      alertlevel: str(p.alertlevel),
      alertscore: num(p.alertscore),
      episodealertlevel: str(p.episodealertlevel),
      country: str(p.country),
      iso3: str(p.iso3),
      glide: str(p.glide),
      source: str(p.source),
      fromdate: parseGdacsDate(str(p.fromdate)),
      todate: parseGdacsDate(str(p.todate)),
      datemodified: parseGdacsDate(str(p.datemodified)),
      severity: num(severityData.severity),
      severitytext: str(severityData.severitytext),
      severityunit: str(severityData.severityunit),
      reportUrl: str(url.report),
      detailsUrl: str(url.details)
    });
  }
  return items;
}
try {
  db.prepare(
    "CREATE TABLE IF NOT EXISTS gdacs_disasters (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)"
  ).run();
} catch (err) {
  console.error("[GdacsDisasters] could not ensure SQLite table:", err instanceof Error ? err.message : err);
}
var insertGdacsDisaster = db.prepare(
  "INSERT OR IGNORE INTO gdacs_disasters (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)"
);
async function seedGdacsDisasters() {
  try {
    console.log("[GdacsDisasters] Polling GDACS event list...");
    const res = await withRetry(() => fetchWithTimeout(GDACS_API_URL));
    const data = await res.json();
    const fetchedAt = Date.now();
    const items = parseGdacsPayload(data);
    let insertedCount = 0;
    for (const item of items) {
      const sourceTs = Date.parse(item.timestamp);
      const result = insertGdacsDisaster.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: Number.isNaN(sourceTs) ? fetchedAt : sourceTs,
        fetched_at: fetchedAt
      });
      if (result.changes > 0) insertedCount++;
    }
    console.log(`[GdacsDisasters] Parsed ${items.length} current events. Saved ${insertedCount} new to SQLite.`);
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
    console.error("[GdacsDisasters] seeder failed:", err instanceof Error ? err.message : err);
  }
}
var index_default = {
  name: PLUGIN_ID,
  cron: "*/5 * * * *",
  // Every 5 minutes (GDACS refreshes ~every 6 min)
  fn: seedGdacsDisasters
};
export {
  index_default as default,
  parseGdacsDate,
  parseGdacsPayload,
  seedGdacsDisasters
};
