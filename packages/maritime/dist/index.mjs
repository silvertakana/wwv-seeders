// src/index.ts
import WebSocket from "ws";

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
var AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream";
var API_KEY = process.env.AISSTREAM_API_KEY;
var messageBuffer = [];
var activeFleetCache = /* @__PURE__ */ new Map();
var FLUSH_INTERVAL_MS = 15e3;
var isFlushIntervalRunning = false;
var RECONNECT_BASE_MS = 5e3;
var RECONNECT_MAX_MS = 5 * 60 * 1e3;
var reconnectAttempts = 0;
var reconnectScheduled = false;
function scheduleReconnect() {
  if (reconnectScheduled) return;
  reconnectScheduled = true;
  const delay = Math.min(
    RECONNECT_BASE_MS * Math.pow(2, reconnectAttempts),
    RECONNECT_MAX_MS
  );
  reconnectAttempts++;
  console.log(`[Maritime] Reconnecting in ${Math.round(delay / 1e3)}s (attempt ${reconnectAttempts})`);
  setTimeout(() => {
    reconnectScheduled = false;
    startMaritimeWebsocket();
  }, delay);
}
var insertHistory = db.prepare(`
  INSERT OR IGNORE INTO maritime_history (mmsi, ts, lat, lon, hdg, spd, fetched_at)
  VALUES (@mmsi, @ts, @lat, @lon, @hdg, @spd, @fetched_at)
`);
async function flushBuffer() {
  var _a, _b;
  if (messageBuffer.length === 0) return;
  const batch = [...messageBuffer];
  messageBuffer = [];
  const fetchedAt = Date.now();
  let insertedCount = 0;
  const insertMany = db.transaction((msgs) => {
    var _a2, _b2;
    for (const msg of msgs) {
      if (!((_a2 = msg.MetaData) == null ? void 0 : _a2.MMSI) || !((_b2 = msg.Message) == null ? void 0 : _b2.PositionReport)) continue;
      const mmsi = msg.MetaData.MMSI.toString();
      const report = msg.Message.PositionReport;
      const ts = Math.floor(new Date(msg.MetaData.time_utc).getTime() / 1e3);
      const result = insertHistory.run({
        mmsi,
        ts,
        lat: report.Latitude,
        lon: report.Longitude,
        hdg: report.TrueHeading,
        spd: report.Sog,
        fetched_at: fetchedAt
      });
      if (result.changes > 0) insertedCount++;
    }
  });
  try {
    insertMany(batch);
    if (insertedCount > 0) {
    }
    const nowSecs = Math.floor(Date.now() / 1e3);
    for (const [mmsi, ship] of activeFleetCache.entries()) {
      if (nowSecs - ship.last_updated > 6 * 3600) {
        activeFleetCache.delete(mmsi);
      }
    }
    for (const msg of batch) {
      if (!((_a = msg.MetaData) == null ? void 0 : _a.MMSI) || !((_b = msg.Message) == null ? void 0 : _b.PositionReport)) continue;
      const mmsi = msg.MetaData.MMSI.toString();
      const report = msg.Message.PositionReport;
      const ts = Math.floor(new Date(msg.MetaData.time_utc).getTime() / 1e3);
      const shipState = {
        id: `mmsi-${mmsi}`,
        mmsi,
        name: msg.MetaData.ShipName ? msg.MetaData.ShipName.trim() : `Unknown (${mmsi})`,
        lat: report.Latitude,
        lon: report.Longitude,
        hdg: report.TrueHeading,
        spd: report.Sog,
        last_updated: ts
      };
      activeFleetCache.set(mmsi, shipState);
    }
    await setLiveSnapshot("maritime", Object.fromEntries(activeFleetCache), 6 * 3600);
  } catch (err) {
    console.error("[Maritime] Buffer flush failed:", err);
    Sentry.captureException(err, { extra: { context: "flushBuffer", type: "maritime" } });
  }
}
function startMaritimeWebsocket() {
  if (!API_KEY) {
    console.warn("[Maritime] Skipping AIS websocket: AISSTREAM_API_KEY not set.");
    return;
  }
  console.log("[Maritime] Connecting to AisStream.io...");
  const ws = new WebSocket(AISSTREAM_URL, { rejectUnauthorized: false });
  let watchdogTimer = null;
  const resetWatchdog = () => {
    if (watchdogTimer) clearTimeout(watchdogTimer);
    watchdogTimer = setTimeout(() => {
      console.warn("[Maritime] Watchdog timeout: No messages received in 30s. Forcing reconnect...");
      ws.terminate();
    }, 3e4);
  };
  ws.on("open", () => {
    console.log("[Maritime] WebSocket connected. Subscribing to global feed...");
    reconnectAttempts = 0;
    const subscriptionMessage = {
      APIKey: API_KEY,
      BoundingBoxes: [[[-90, -180], [90, 180]]],
      FiltersShipMMSI: [],
      FilterMessageTypes: ["PositionReport"]
    };
    ws.send(JSON.stringify(subscriptionMessage));
    resetWatchdog();
  });
  ws.on("message", (data) => {
    var _a;
    resetWatchdog();
    try {
      const msg = JSON.parse(data.toString());
      if (msg.MessageType === "PositionReport" || ((_a = msg.Message) == null ? void 0 : _a.PositionReport)) {
        messageBuffer.push(msg);
      } else if (msg.Error || msg.error) {
        console.error("[Maritime] AISStream Error message received:", msg);
      } else {
        if (msg.MessageType !== "SubscriptionMessage") {
        }
      }
    } catch (e) {
      console.error("[Maritime] AISStream Parse Error. Raw data:", data.toString());
    }
  });
  ws.on("error", (err) => {
    console.error("[Maritime] WebSocket error:", err.message);
    Sentry.captureException(err, { extra: { context: "websocket_error", type: "maritime" } });
  });
  ws.on("close", () => {
    if (watchdogTimer) clearTimeout(watchdogTimer);
    console.log("[Maritime] WebSocket closed.");
    scheduleReconnect();
  });
  if (!isFlushIntervalRunning) {
    setInterval(flushBuffer, FLUSH_INTERVAL_MS);
    isFlushIntervalRunning = true;
  }
}
var index_default = {
  name: "maritime",
  init: startMaritimeWebsocket
};
export {
  index_default as default,
  startMaritimeWebsocket
};
