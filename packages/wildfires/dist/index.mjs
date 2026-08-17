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
var DEFAULT_TIERS = [
  { level: 1, size: 2 },
  // Macro: ~220km
  { level: 2, size: 0.5 },
  // Meso: ~55km
  { level: 3, size: 0.05 }
  // Micro: ~5.5km
];
function parseCSV(csv) {
  const lines = csv.trim().split("\n");
  if (lines.length < 2) return [];
  const headers = lines[0].split(",").map((h) => h.trim());
  const records = [];
  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split(",");
    if (values.length < headers.length) continue;
    const record = {};
    headers.forEach((header, idx) => {
      var _a;
      record[header] = ((_a = values[idx]) == null ? void 0 : _a.trim()) || "";
    });
    const lat = parseFloat(record["latitude"]);
    const lon = parseFloat(record["longitude"]);
    if (isNaN(lat) || isNaN(lon)) continue;
    records.push({
      latitude: lat,
      longitude: lon,
      bright_ti4: parseFloat(record["bright_ti4"]) || 0,
      scan: parseFloat(record["scan"]) || 0,
      track: parseFloat(record["track"]) || 0,
      acq_date: record["acq_date"] || "",
      acq_time: record["acq_time"] || "",
      satellite: record["satellite"] || "",
      confidence: record["confidence"] || "",
      version: record["version"] || "",
      bright_ti5: parseFloat(record["bright_ti5"]) || 0,
      frp: parseFloat(record["frp"]) || 0,
      daynight: record["daynight"] || ""
    });
  }
  return records;
}
function clusterFires(fires, tiers = DEFAULT_TIERS) {
  const allClusteredFires = [];
  for (const tier of tiers) {
    const clustered = /* @__PURE__ */ new Map();
    for (const fire of fires) {
      const gridId = `${Math.floor(fire.latitude / tier.size)}_${Math.floor(fire.longitude / tier.size)}`;
      const existing = clustered.get(gridId);
      if (existing) {
        existing.frp += fire.frp;
        if (fire.confidence === "high" || fire.confidence === "nominal" && existing.confidence === "low") {
          existing.confidence = fire.confidence;
        }
      } else {
        const id = `firm_${fire.acq_date}_${fire.acq_time}_${Math.round(fire.latitude * 1e3)}_${Math.round(fire.longitude * 1e3)}_t${tier.level}`;
        clustered.set(gridId, { ...fire, tier: tier.level, id });
      }
    }
    allClusteredFires.push(...Array.from(clustered.values()));
  }
  return allClusteredFires;
}
function buildSourceTs(acqDate, acqTime, fetchedAt) {
  const timeStr = acqTime.toString().padStart(4, "0");
  const tsStr = `${acqDate}T${timeStr.substring(0, 2)}:${timeStr.substring(2, 4)}:00Z`;
  let sourceTs = 0;
  try {
    sourceTs = new Date(tsStr).getTime();
    if (isNaN(sourceTs)) sourceTs = fetchedAt;
  } catch (e) {
    sourceTs = fetchedAt;
  }
  return sourceTs;
}
var insertWildfire = db.prepare("INSERT OR IGNORE INTO wildfires (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)");
async function seedWildfires() {
  console.log("[Wildfires] Polling NASA FIRMS...");
  const url = `https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Global_24h.csv`;
  const res = await withRetry(() => fetchWithTimeout(url));
  const csv = await res.text();
  const fires = parseCSV(csv);
  const fetchedAt = Date.now();
  if (fires.length === 0) {
    console.log("[Wildfires] No fires found or parsing failed.");
    return;
  }
  const allClusteredFires = clusterFires(fires);
  let insertedCount = 0;
  const insertMany = db.transaction((firesList) => {
    for (const f of firesList) {
      const sourceTs = buildSourceTs(f.acq_date, f.acq_time, fetchedAt);
      const result = insertWildfire.run({
        id: f.id,
        payload: JSON.stringify(f),
        source_ts: sourceTs,
        fetched_at: fetchedAt
      });
      if (result.changes > 0) insertedCount++;
    }
  });
  insertMany(allClusteredFires);
  console.log(`[Wildfires] Parsed ${fires.length} raw fires -> Clustered ${allClusteredFires.length} points. Saved ${insertedCount} new to SQLite.`);
  await setLiveSnapshot("wildfire", {
    source: "wildfire",
    fetchedAt: (/* @__PURE__ */ new Date()).toISOString(),
    items: allClusteredFires,
    totalCount: allClusteredFires.length
  }, 1800);
}
var index_default = {
  name: "wildfire",
  cron: "*/15 * * * *",
  // Every 15 minutes
  fn: seedWildfires
};
export {
  DEFAULT_TIERS,
  buildSourceTs,
  clusterFires,
  index_default as default,
  parseCSV,
  seedWildfires
};
