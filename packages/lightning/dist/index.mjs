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
var HOSTS = [
  "wss://live.lightningmaps.org:443/",
  "wss://live2.lightningmaps.org:443/"
];
var V24_REQUEST = {
  v: 24,
  i: {},
  s: true,
  x: 0,
  w: 0,
  tx: 0,
  tw: 0,
  a: 4,
  z: 5,
  b: true,
  h: "",
  l: 0,
  t: 0,
  from_lightningmaps_org: true,
  p: [90, 180, -90, -180],
  r: "feed"
};
var PLUGIN_ID = "lightning";
var FLUSH_INTERVAL_MS = 1e4;
var SNAPSHOT_TTL_SECONDS = 900;
var STROKE_WINDOW_MS = 12e4;
var MAX_STROKES = 2e4;
var RECONNECT_BASE_MS = 5e3;
var RECONNECT_MAX_MS = 5 * 60 * 1e3;
var HOST_FAILOVER_ATTEMPTS = 3;
var reconnectAttempts = 0;
var reconnectScheduled = false;
var currentHostIndex = 0;
var activeWs = null;
var strokeRing = /* @__PURE__ */ new Map();
function parseStrokesFrame(frame) {
  if (typeof frame !== "object" || frame === null) return [];
  const strokes = frame.strokes;
  if (!Array.isArray(strokes)) return [];
  const out = [];
  for (const raw of strokes) {
    if (typeof raw !== "object" || raw === null) continue;
    const s = raw;
    const src = Number(s.src);
    const id = Number(s.id);
    const time = Number(s.time);
    const lat = Number(s.lat);
    const lon = Number(s.lon);
    if (![src, id, time, lat, lon].every(Number.isFinite)) continue;
    if (lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;
    const stroke = { src, id, time, lat, lon };
    if (Number.isFinite(Number(s.dev))) stroke.dev = Number(s.dev);
    if (Number.isFinite(Number(s.del))) stroke.del = Number(s.del);
    out.push(stroke);
  }
  return out;
}
function strokeToItem(stroke) {
  return {
    id: `${stroke.src}/${stroke.id}`,
    latitude: stroke.lat,
    longitude: stroke.lon,
    timestamp: new Date(stroke.time).toISOString(),
    amplitude: stroke.dev ?? null,
    serverDelayMs: stroke.del ?? null,
    src: stroke.src,
    rawId: stroke.id
  };
}
function ingestStrokes(strokes, nowMs = Date.now()) {
  let added = 0;
  for (const stroke of strokes) {
    if (nowMs - stroke.time > STROKE_WINDOW_MS) continue;
    const item = strokeToItem(stroke);
    if (!strokeRing.has(item.id)) added++;
    strokeRing.set(item.id, item);
  }
  let overflow = 0;
  for (const [key, item] of strokeRing) {
    if (nowMs - Date.parse(item.timestamp) > STROKE_WINDOW_MS) {
      strokeRing.delete(key);
      overflow++;
    }
  }
  while (strokeRing.size > MAX_STROKES) {
    const oldest = strokeRing.keys().next().value;
    if (oldest === void 0) break;
    strokeRing.delete(oldest);
    overflow++;
  }
  return added - overflow;
}
async function flushSnapshot() {
  if (strokeRing.size === 0) return;
  const now = Date.now();
  for (const [key, item] of strokeRing) {
    if (now - Date.parse(item.timestamp) > STROKE_WINDOW_MS) strokeRing.delete(key);
  }
  const items = [...strokeRing.values()];
  try {
    await setLiveSnapshot(
      PLUGIN_ID,
      {
        source: PLUGIN_ID,
        fetchedAt: new Date(now).toISOString(),
        items,
        totalCount: items.length
      },
      SNAPSHOT_TTL_SECONDS
    );
  } catch (err) {
    console.error("[Lightning] snapshot failed:", err instanceof Error ? err.message : err);
  }
}
function scheduleReconnect() {
  if (reconnectScheduled) return;
  reconnectScheduled = true;
  reconnectAttempts++;
  if (reconnectAttempts % HOST_FAILOVER_ATTEMPTS === 0) {
    currentHostIndex = (currentHostIndex + 1) % HOSTS.length;
    console.warn(`[Lightning] Rotating host to ${HOSTS[currentHostIndex]}`);
  }
  const delay = Math.min(
    RECONNECT_BASE_MS * Math.pow(2, reconnectAttempts),
    RECONNECT_MAX_MS
  );
  console.log(`[Lightning] Reconnecting in ${Math.round(delay / 1e3)}s (attempt ${reconnectAttempts})`);
  setTimeout(() => {
    reconnectScheduled = false;
    startLightningWebsocket();
  }, delay);
}
function startLightningWebsocket() {
  if (activeWs && (activeWs.readyState === WebSocket.OPEN || activeWs.readyState === WebSocket.CONNECTING)) {
    return;
  }
  const host = HOSTS[currentHostIndex];
  console.log(`[Lightning] Connecting to ${host}`);
  let ws;
  try {
    ws = new WebSocket(host);
  } catch (err) {
    console.error("[Lightning] socket creation failed:", err instanceof Error ? err.message : err);
    scheduleReconnect();
    return;
  }
  activeWs = ws;
  let watchdogTimer = null;
  const resetWatchdog = () => {
    if (watchdogTimer) clearTimeout(watchdogTimer);
    watchdogTimer = setTimeout(() => {
      console.warn("[Lightning] Watchdog timeout: no frames in 30s. Forcing reconnect...");
      try {
        ws.terminate();
      } catch {
      }
    }, 3e4);
  };
  ws.on("open", () => {
    console.log("[Lightning] WebSocket connected. Sending v24 feed request...");
    reconnectAttempts = 0;
    ws.send(JSON.stringify(V24_REQUEST));
    resetWatchdog();
  });
  ws.on("message", (data) => {
    resetWatchdog();
    try {
      const frame = JSON.parse(data.toString());
      const strokes = parseStrokesFrame(frame);
      if (strokes.length > 0) {
        ingestStrokes(strokes);
      }
    } catch (e) {
      console.error("[Lightning] Parse error on frame:", data.toString().slice(0, 200));
    }
  });
  ws.on("error", (err) => {
    console.error("[Lightning] WebSocket error:", err.message);
  });
  ws.on("close", () => {
    if (watchdogTimer) clearTimeout(watchdogTimer);
    if (activeWs === ws) activeWs = null;
    console.log("[Lightning] WebSocket closed.");
    scheduleReconnect();
  });
}
var flushIntervalStarted = false;
var index_default = {
  name: PLUGIN_ID,
  init: () => {
    try {
      startLightningWebsocket();
      if (!flushIntervalStarted) {
        setInterval(flushSnapshot, FLUSH_INTERVAL_MS);
        flushIntervalStarted = true;
      }
    } catch (err) {
      console.error("[Lightning] init failed:", err instanceof Error ? err.message : err);
    }
  }
};
export {
  index_default as default,
  ingestStrokes,
  parseStrokesFrame,
  startLightningWebsocket,
  strokeToItem
};
