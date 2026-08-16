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
var GDELT_URL = "http://api.gdeltproject.org/api/v1/gkg_geojson?query=protest OR riot OR demonstration OR strike OR clash&maxrows=2500";
function classifyGdeltEventType(name) {
  const lowerName = name.toLowerCase();
  if (lowerName.includes("riot")) return "Riots";
  if (lowerName.includes("clash")) return "Riots";
  if (lowerName.includes("strike")) return "Strikes";
  if (lowerName.includes("demonstration")) return "Demonstrations";
  return "Protests";
}
function classifyGdeltSubType(name, count) {
  const lowerName = name.toLowerCase();
  if (count > 50 || lowerName.includes("riot") || lowerName.includes("clash")) return "Violent demonstration";
  if (lowerName.includes("strike")) return "Labor strike";
  return "Peaceful protest";
}
var insertUnrest = db.prepare(
  "INSERT OR REPLACE INTO civil_unrest (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)"
);
async function seedCivilUnrest() {
  var _a, _b, _c;
  console.log("[CivilUnrest] Fetching from GDELT API...");
  const res = await withRetry(() => fetchWithTimeout(GDELT_URL, { headers: { "User-Agent": "WWV-Data-Engine" } }, 25e3), 3, 5e3);
  if (!res.ok) {
    console.warn(`[CivilUnrest] Failed to fetch. HTTP ${res.status}`);
    return;
  }
  const json = await res.json();
  const features = json.features;
  if (!features || features.length === 0) {
    console.log("[CivilUnrest] No events returned from GDELT.");
    return;
  }
  const locationMap = /* @__PURE__ */ new Map();
  for (const feature of features) {
    const name = ((_a = feature.properties) == null ? void 0 : _a.name) || "";
    if (!name) continue;
    const coords = (_b = feature.geometry) == null ? void 0 : _b.coordinates;
    if (!Array.isArray(coords) || coords.length < 2) continue;
    const [lon, lat] = coords;
    if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;
    const key = `${lat.toFixed(1)}:${lon.toFixed(1)}`;
    const existing = locationMap.get(key);
    if (existing) {
      existing.count++;
      existing.urls.push(feature.properties.url);
      if (feature.properties.urltone < existing.worstTone) {
        existing.worstTone = feature.properties.urltone;
      }
    } else {
      locationMap.set(key, {
        name,
        lat,
        lon,
        count: 1,
        worstTone: feature.properties.urltone ?? 0,
        date: feature.properties.urlpubtimedate,
        urls: [feature.properties.url]
      });
    }
  }
  const fetchedAt = Date.now();
  const items = [];
  for (const [, loc] of locationMap) {
    if (loc.count < 3) continue;
    const country = ((_c = loc.name.split(",").pop()) == null ? void 0 : _c.trim()) || "Unknown";
    const eventType = classifyGdeltEventType(loc.name);
    const item = {
      id: `gdelt-${loc.lat.toFixed(2)}-${loc.lon.toFixed(2)}`,
      lat: loc.lat,
      lon: loc.lon,
      type: eventType,
      subType: classifyGdeltSubType(loc.name, loc.count),
      actor1: "General Public",
      actor2: "N/A",
      fatalities: 0,
      country,
      location: loc.name,
      date: loc.date,
      source: "GDELT",
      notes: `${loc.count} clustered reports. Worst Tone: ${loc.worstTone.toFixed(1)}`,
      reportCount: loc.count
    };
    items.push(item);
    insertUnrest.run({
      id: item.id,
      payload: JSON.stringify(item),
      source_ts: new Date(item.date).getTime(),
      fetched_at: fetchedAt
    });
  }
  console.log(`[CivilUnrest] Clustered ${features.length} mentions into ${items.length} confirmed unrest events.`);
  await setLiveSnapshot(
    "civil-unrest",
    {
      source: "gdelt",
      fetchedAt: (/* @__PURE__ */ new Date()).toISOString(),
      items,
      totalCount: items.length
    },
    86400
  );
}
var index_default = {
  name: "civil-unrest",
  cron: "*/15 * * * *",
  fn: seedCivilUnrest
};
export {
  index_default as default,
  seedCivilUnrest
};
