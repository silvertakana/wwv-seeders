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
var OONI_API_BASE = "https://api.ooni.io/api/v1/measurements";
var OONI_LIMIT = 10;
var PLUGIN_ID = "internet-censorship-ooni";
var SNAPSHOT_TTL_SECONDS = 3600;
var PROBE_COUNTRIES = ["US", "CN", "IR", "RU", "IN", "BR", "EG", "TR", "GB", "DE", "UA", "KZ"];
var COUNTRY_CENTROIDS = {
  US: { lat: 39.83, lon: -98.58 },
  CN: { lat: 35.86, lon: 104.19 },
  IR: { lat: 32.43, lon: 53.69 },
  RU: { lat: 61.52, lon: 105.32 },
  IN: { lat: 20.59, lon: 78.96 },
  BR: { lat: -14.24, lon: -51.93 },
  EG: { lat: 26.82, lon: 30.8 },
  TR: { lat: 38.96, lon: 35.24 },
  GB: { lat: 55.38, lon: -3.44 },
  DE: { lat: 51.16, lon: 10.45 },
  UA: { lat: 48.38, lon: 31.17 },
  KZ: { lat: 48.02, lon: 66.92 }
};
function isCensorshipEvent(m) {
  const blocking = m.scores ? Math.max(
    m.scores.blocking_general ?? 0,
    m.scores.blocking_country ?? 0,
    m.scores.blocking_isp ?? 0,
    m.scores.blocking_local ?? 0
  ) : 0;
  return m.anomaly === true || m.confirmed === true || blocking > 0;
}
function mapMeasurementToItem(m) {
  const centroid = COUNTRY_CENTROIDS[m.probe_cc];
  if (!centroid) return null;
  if (!isCensorshipEvent(m)) return null;
  const measuredAt = m.measurement_start_time ?? (/* @__PURE__ */ new Date()).toISOString();
  return {
    id: m.measurement_uid ?? `${m.probe_cc}-${measuredAt}-${m.probe_asn ?? "unknown"}`,
    lat: centroid.lat,
    lon: centroid.lon,
    probeCc: m.probe_cc,
    probeAsn: m.probe_asn ?? null,
    testName: m.test_name ?? null,
    input: m.input ?? null,
    anomaly: m.anomaly === true,
    confirmed: m.confirmed === true,
    blockingGeneral: typeof m.scores?.blocking_general === "number" && Number.isFinite(m.scores.blocking_general) ? m.scores.blocking_general : null,
    measuredAt
  };
}
function parseOoniResponse(payload) {
  if (!Array.isArray(payload.results)) return [];
  const items = [];
  for (const m of payload.results) {
    const item = mapMeasurementToItem(m);
    if (item) items.push(item);
  }
  return items;
}
try {
  db.prepare(
    "CREATE TABLE IF NOT EXISTS internet_censorship_ooni (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)"
  ).run();
} catch (err) {
  console.error("[InternetCensorshipOoni] could not ensure SQLite table:", err instanceof Error ? err.message : err);
}
var insertCensorship = db.prepare(
  "INSERT OR IGNORE INTO internet_censorship_ooni (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)"
);
async function seedInternetCensorshipOoni() {
  console.log("[InternetCensorshipOoni] Polling OONI API...");
  try {
    const fetchedAt = Date.now();
    const items = [];
    let insertedCount = 0;
    let completedCountries = 0;
    for (const cc of PROBE_COUNTRIES) {
      const url = `${OONI_API_BASE}?probe_cc=${encodeURIComponent(cc)}&limit=${OONI_LIMIT}`;
      try {
        const res = await withRetry(() => fetchWithTimeout(url));
        const payload = await res.json();
        const parsed = parseOoniResponse(payload);
        completedCountries++;
        for (const item of parsed) {
          items.push(item);
          const result = insertCensorship.run({
            id: item.id,
            payload: JSON.stringify(item),
            source_ts: new Date(item.measuredAt).getTime(),
            fetched_at: fetchedAt
          });
          if (result.changes > 0) insertedCount++;
        }
      } catch (countryErr) {
        console.error(
          `[InternetCensorshipOoni] country ${cc} failed:`,
          countryErr instanceof Error ? countryErr.message : countryErr
        );
      }
    }
    if (completedCountries === 0) {
      console.warn("[InternetCensorshipOoni] all country fetches failed; skipping snapshot");
      return;
    }
    console.log(
      `[InternetCensorshipOoni] Parsed ${items.length} censorship events. Saved ${insertedCount} new to SQLite.`
    );
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
    console.error("[InternetCensorshipOoni] seeder failed:", err instanceof Error ? err.message : err);
  }
}
var index_default = {
  name: PLUGIN_ID,
  cron: "0 * * * *",
  // Every hour
  fn: seedInternetCensorshipOoni
};
export {
  index_default as default,
  isCensorshipEvent,
  mapMeasurementToItem,
  parseOoniResponse,
  seedInternetCensorshipOoni
};
