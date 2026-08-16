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
function geolocateIp(ip) {
  const lookup = geoip.lookup(ip);
  if (!lookup || !lookup.ll) return null;
  return {
    lat: lookup.ll[0],
    lon: lookup.ll[1],
    country: lookup.country || "Unknown",
    city: lookup.city || "Unknown"
  };
}

// src/index.ts
var OTX_BASE = "https://otx.alienvault.com/api/v1";
var insertCyberAttack = db.prepare(
  "INSERT OR REPLACE INTO cyber_attacks (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)"
);
async function seedCyberAttacks() {
  var _a, _b;
  const apiKey = process.env.OTX_API_KEY;
  if (!apiKey) {
    console.warn("[CyberAttacks] OTX_API_KEY not set \u2014 skipping.");
    return;
  }
  console.log("[CyberAttacks] Polling AlienVault OTX...");
  const since = new Date(Date.now() - 48 * 3600 * 1e3).toISOString();
  const url = `${OTX_BASE}/pulses/subscribed?modified_since=${since}&limit=50`;
  const res = await withRetry(async () => {
    try {
      return await fetchWithTimeout(url, {
        headers: { "X-OTX-API-KEY": apiKey, "User-Agent": "WWV-Data-Engine" }
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error("[CyberAttacks] Fetch error details:", msg);
      throw err;
    }
  });
  const data = await res.json();
  if (!(data == null ? void 0 : data.results) || !Array.isArray(data.results)) {
    console.warn("[CyberAttacks] Invalid OTX response");
    return;
  }
  const pulses = data.results;
  const fetchedAt = Date.now();
  const items = [];
  const seenIps = /* @__PURE__ */ new Set();
  for (const pulse of pulses) {
    const ipIndicators = (pulse.indicators || []).filter(
      (ind) => ind.type === "IPv4"
    );
    for (const ind of ipIndicators) {
      if (seenIps.has(ind.indicator)) continue;
      seenIps.add(ind.indicator);
      const geo = geolocateIp(ind.indicator);
      if (!geo) continue;
      const threatType = classifyThreat(pulse);
      const item = {
        id: `otx-${pulse.id}-${ind.id}`,
        ip: ind.indicator,
        lat: geo.lat,
        lon: geo.lon,
        country: geo.country,
        city: geo.city,
        threatType,
        adversary: pulse.adversary || "Unknown",
        pulseName: pulse.name,
        pulseDescription: ((_a = pulse.description) == null ? void 0 : _a.slice(0, 300)) || "",
        malwareFamilies: pulse.malware_families || [],
        tags: ((_b = pulse.tags) == null ? void 0 : _b.slice(0, 5)) || [],
        targetedCountries: pulse.targeted_countries || [],
        pulseId: pulse.id,
        pulseCreated: pulse.created,
        pulseModified: pulse.modified
      };
      items.push(item);
      insertCyberAttack.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: new Date(pulse.modified).getTime(),
        fetched_at: fetchedAt
      });
    }
  }
  console.log(
    `[CyberAttacks] Processed ${pulses.length} pulses \u2192 ${items.length} geolocated indicators.`
  );
  await setLiveSnapshot(
    "cyber-attacks",
    {
      source: "cyber-attacks",
      fetchedAt: (/* @__PURE__ */ new Date()).toISOString(),
      items,
      totalCount: items.length
    },
    7200
    // 2 hour TTL
  );
}
function classifyThreat(pulse) {
  const tags = (pulse.tags || []).map((t) => t.toLowerCase());
  const name = pulse.name.toLowerCase();
  const desc = (pulse.description || "").toLowerCase();
  const combined = [...tags, name, desc].join(" ");
  if (combined.includes("apt") || combined.includes("advanced persistent"))
    return "APT";
  if (combined.includes("ransomware")) return "Ransomware";
  if (combined.includes("botnet")) return "Botnet";
  if (combined.includes("phishing")) return "Phishing";
  if (combined.includes("ddos")) return "DDoS";
  if (combined.includes("malware") || combined.includes("trojan"))
    return "Malware";
  if (combined.includes("c2") || combined.includes("command and control"))
    return "C2 Server";
  return "Other";
}
var index_default = {
  name: "cyber-attacks",
  cron: "0 */2 * * *",
  // Every 2 hours
  fn: seedCyberAttacks
};
export {
  classifyThreat,
  index_default as default,
  seedCyberAttacks
};
