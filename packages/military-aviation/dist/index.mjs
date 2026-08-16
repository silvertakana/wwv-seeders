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

// src/index.ts
import * as Sentry from "@sentry/node";
var ADSB_LOL_URL = "https://api.adsb.lol/v2/mil";
var POLLING_INTERVAL_MS = 6e4;
var insertHistory = db.prepare(`
    INSERT OR IGNORE INTO military_aviation_history (hex, ts, lat, lon, alt, hdg, spd, fetched_at)
    VALUES (@hex, @ts, @lat, @lon, @alt, @hdg, @spd, @fetched_at)
`);
async function pollMilitaryAviation() {
  try {
    const response = await fetch(ADSB_LOL_URL, {
      headers: {
        "User-Agent": "WorldWideView-DataEngine"
      }
    });
    if (!response.ok) {
      if (response.status === 429) {
        console.warn("[MilitaryAviation] 429 Rate Limit hit.");
      }
      throw new Error(`Status ${response.status}`);
    }
    const data = await response.json();
    if (!data.ac || !Array.isArray(data.ac)) return;
    const fetchedAt = Math.floor(Date.now() / 1e3);
    const fleetObj = /* @__PURE__ */ Object.create(null);
    let insertedCount = 0;
    const insertMany = db.transaction((aircraft) => {
      for (const ac of aircraft) {
        if (ac.lat == null || ac.lon == null) continue;
        const hex = ac.hex;
        const ts = Math.floor((ac.seen_pos != null ? Date.now() - ac.seen_pos * 1e3 : Date.now()) / 1e3);
        const lon = ac.lon;
        const lat = ac.lat;
        const alt = typeof ac.alt_baro === "number" ? ac.alt_baro : 0;
        const on_ground = ac.alt_baro === "ground";
        const spd = ac.gs || 0;
        const hdg = ac.track || 0;
        if (!on_ground) {
          const result = insertHistory.run({
            hex,
            ts,
            lat,
            lon,
            alt,
            hdg,
            spd,
            fetched_at: fetchedAt
          });
          if (result.changes > 0) insertedCount++;
        }
        fleetObj[hex] = {
          hex,
          flight: ac.flight || null,
          r: ac.r || null,
          t: ac.t || null,
          lat,
          lon,
          alt_baro: ac.alt_baro,
          alt_geom: ac.alt_geom,
          gs: ac.gs,
          track: ac.track,
          squawk: ac.squawk,
          dbFlags: ac.dbFlags,
          category: ac.category,
          emergency: ac.emergency,
          seen: ac.seen,
          seen_pos: ac.seen_pos,
          last_updated: fetchedAt
        };
      }
    });
    insertMany(data.ac);
    await setLiveSnapshot("military-aviation", fleetObj, 60 * 60);
  } catch (err) {
    console.error(`[MilitaryAviation] Polling Error: ${err.message}`);
    if (err.cause) {
      console.error(`[MilitaryAviation] Cause:`, err.cause);
    }
    Sentry.captureException(err, { extra: { cause: err.cause, context: "military-aviation" } });
  }
}
function startMilitaryAviationPoller() {
  console.log("[MilitaryAviation] Starting background polling...");
  setInterval(pollMilitaryAviation, POLLING_INTERVAL_MS);
  pollMilitaryAviation();
}
var index_default = {
  name: "military-aviation",
  init: startMilitaryAviationPoller
};
export {
  index_default as default,
  startMilitaryAviationPoller
};
