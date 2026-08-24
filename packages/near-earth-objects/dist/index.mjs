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
var NEO_FEED_URL = "https://api.nasa.gov/neo/rest/v1/feed";
var NASA_API_KEY = process.env.NASA_API_KEY ?? "DEMO_KEY";
var PLUGIN_ID = "near-earth-objects";
var SNAPSHOT_TTL_SECONDS = 3600;
function hashString(input) {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}
function placeholderPoint(seed) {
  const hash = hashString(seed);
  const lat = -85 + hash % 1e6 / 1e6 * 170;
  const lon = -180 + Math.floor(hash / 1e6) % 1e6 / 1e6 * 360;
  return {
    lat: Math.round(lat * 1e4) / 1e4,
    lon: Math.round(lon * 1e4) / 1e4
  };
}
function finiteNumberOrNull(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
function toFiniteNumber(value) {
  if (value === null || value === void 0 || value === "") return null;
  const n = typeof value === "number" ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}
function mapNeoFeedToItems(feed) {
  const items = [];
  const byDate = feed.near_earth_objects;
  if (!byDate || typeof byDate !== "object") return items;
  for (const dateKey of Object.keys(byDate)) {
    for (const neo of byDate[dateKey] ?? []) {
      const objectId = neo.id;
      const name = neo.name;
      if (!objectId && !name) continue;
      const point = placeholderPoint(objectId ?? name ?? "neo");
      const displayName = name ?? objectId ?? "Unnamed NEO";
      for (const approach of neo.close_approach_data ?? []) {
        const closeApproachDate = approach.close_approach_date;
        if (!closeApproachDate) continue;
        items.push({
          id: `neo-${objectId ?? name}-${closeApproachDate}`,
          name: displayName,
          lat: point.lat,
          lon: point.lon,
          closeApproachDate,
          orbitingBody: approach.orbiting_body ?? null,
          missDistanceKm: toFiniteNumber(approach.miss_distance?.kilometers),
          relativeVelocityKms: toFiniteNumber(approach.relative_velocity?.kilometers_per_second),
          diameterKmMin: finiteNumberOrNull(neo.estimated_diameter?.kilometers?.estimated_diameter_min),
          diameterKmMax: finiteNumberOrNull(neo.estimated_diameter?.kilometers?.estimated_diameter_max),
          absoluteMagnitudeH: finiteNumberOrNull(neo.absolute_magnitude_h),
          potentiallyHazardous: neo.is_potentially_hazardous_asteroid === true,
          nasaJplUrl: neo.nasa_jpl_url ?? null
        });
      }
    }
  }
  return items;
}
try {
  db.prepare(
    "CREATE TABLE IF NOT EXISTS near_earth_objects (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)"
  ).run();
} catch (err) {
  console.error("[NearEarthObjects] could not ensure SQLite table:", err instanceof Error ? err.message : err);
}
var insertNeo = db.prepare(
  "INSERT OR IGNORE INTO near_earth_objects (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)"
);
async function seedNearEarthObjects() {
  console.log("[NearEarthObjects] Polling NASA NeoWs...");
  try {
    const today = (/* @__PURE__ */ new Date()).toISOString().slice(0, 10);
    const res = await withRetry(
      () => fetchWithTimeout(`${NEO_FEED_URL}?start_date=${today}&end_date=${today}&api_key=${NASA_API_KEY}`)
    );
    const data = await res.json();
    const fetchedAt = Date.now();
    if (!data || !data.near_earth_objects || typeof data.near_earth_objects !== "object") {
      console.warn("[NearEarthObjects] Invalid response from NASA NeoWs");
      return;
    }
    const items = mapNeoFeedToItems(data);
    let insertedCount = 0;
    for (const item of items) {
      const sourceTs = (/* @__PURE__ */ new Date(`${item.closeApproachDate}T00:00:00.000Z`)).getTime();
      const result = insertNeo.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: Number.isFinite(sourceTs) ? sourceTs : fetchedAt,
        fetched_at: fetchedAt
      });
      if (result.changes > 0) insertedCount++;
    }
    console.log(`[NearEarthObjects] Parsed ${items.length} close approaches. Saved ${insertedCount} new to SQLite.`);
    await setLiveSnapshot(
      PLUGIN_ID,
      {
        source: PLUGIN_ID,
        fetchedAt: (/* @__PURE__ */ new Date()).toISOString(),
        items,
        totalCount: items.length
      },
      SNAPSHOT_TTL_SECONDS
      // 1 hour TTL
    );
  } catch (err) {
    console.error("[NearEarthObjects] seeder failed:", err instanceof Error ? err.message : err);
  }
}
var index_default = {
  name: PLUGIN_ID,
  cron: "0 * * * *",
  // Every hour
  fn: seedNearEarthObjects
};
export {
  index_default as default,
  mapNeoFeedToItems,
  placeholderPoint,
  seedNearEarthObjects
};
