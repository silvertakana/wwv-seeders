// src/index.ts
import fs from "fs";
import path3 from "path";
import { z } from "zod";

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
import * as Sentry from "@sentry/node";
import { fileURLToPath } from "url";
var __filename2 = fileURLToPath(import.meta.url);
var __dirname2 = path3.dirname(__filename2);
var itemSchema = z.object({
  event_id: z.string().max(255),
  type: z.string().max(255).nullable().optional(),
  location: z.string().max(500).nullable().optional(),
  timestamp: z.string().max(100),
  confidence: z.string().max(100).nullable().optional(),
  event_summary: z.string().max(1e4).nullable().optional(),
  source_url: z.string().max(2e3).nullable().optional(),
  preview_image: z.string().url().max(2e3).nullable().optional(),
  _osint_meta: z.any().optional()
});
var insertEvent = db.prepare(`
  INSERT INTO iranwar_events (event_id, payload, timestamp, fetched_at) 
  VALUES (@event_id, @payload, @timestamp, @fetched_at)
  ON CONFLICT(event_id) DO UPDATE SET 
    payload=excluded.payload, 
    timestamp=excluded.timestamp
`);
var getTopEvents = db.prepare("SELECT payload FROM iranwar_events ORDER BY timestamp DESC LIMIT 500");
var hasHydratedSeed = false;
async function seedIranWarLive() {
  if (!hasHydratedSeed) {
    console.log("[IranWarLive] Initializing: Hydrating/Upserting active fallback seed...");
    const seedPath = path3.join(__dirname2, "..", "..", "seedData", "iranwar_seed.json");
    if (fs.existsSync(seedPath)) {
      const fallbackData = JSON.parse(fs.readFileSync(seedPath, "utf8"));
      const fetchedAt = Date.now();
      let insertedCount = 0;
      let variables = 0;
      const upsertMany = db.transaction((events) => {
        for (const item of events) {
          try {
            const validatedItem = itemSchema.parse(item);
            const result = insertEvent.run({
              event_id: validatedItem.event_id,
              payload: JSON.stringify(validatedItem),
              timestamp: validatedItem.timestamp,
              fetched_at: fetchedAt
            });
            if (result.changes > 0) insertedCount++;
          } catch (err) {
          }
        }
      });
      upsertMany(fallbackData);
      console.log(`[IranWarLive] Boot hydration complete. Merged ${insertedCount} seed events.`);
    }
    hasHydratedSeed = true;
  }
  console.log("[IranWarLive] Polling iranwarlive.com/feed.json...");
  let data = null;
  try {
    const response = await withRetry(() => fetchWithTimeout("https://iranwarlive.com/feed.json", {
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Cache-Control": "no-cache",
        "Accept-Language": "en-US,en;q=0.9"
      }
    }));
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    data = await response.json();
  } catch (err) {
    console.warn(`[IranWarLive] Failed to fetch live feed (anti-bot block?): ${err.message}. Using local database cache.`);
    Sentry.captureException(err, { extra: { context: "iranwarlive_fetch" } });
  }
  if (data && data.items && Array.isArray(data.items) && data.items.length > 0) {
    const fetchedAt = Date.now();
    let insertedCount = 0;
    const placeholders = data.items.map(() => "?").join(",");
    const existingIdsStmt = db.prepare(`SELECT event_id FROM iranwar_events WHERE event_id IN (${placeholders})`);
    const existingIdsRow = existingIdsStmt.all(...data.items.map((i) => i.event_id));
    const existingIds = new Set(existingIdsRow.map((row) => row.event_id));
    const newItems = data.items.filter((item) => !existingIds.has(item.event_id));
    if (newItems.length > 0) {
      console.log(`[IranWarLive] Found ${newItems.length} new events. Hydrating og:images...`);
      for (const item of newItems) {
        if (item.source_url) {
          try {
            const htmlRes = await fetchWithTimeout(item.source_url, { headers: { "User-Agent": "WorldWideView-OSINT/1.0" } }, 5e3);
            const html = await htmlRes.text();
            const ogMatch = html.match(/<meta[^>]*property=["']og:image["'][^>]*content=["']([^"']+)["']/i) || html.match(/<meta[^>]*content=["']([^"']+)["'][^>]*property=["']og:image["']/i);
            if (ogMatch && ogMatch[1]) {
              item.preview_image = ogMatch[1];
            }
          } catch (err) {
            console.warn(`[IranWarLive] Failed to hydrate image for ${item.event_id}: ${err.message}`);
          }
        }
      }
      const insertMany = db.transaction((events) => {
        for (const item of events) {
          try {
            const validatedItem = itemSchema.parse(item);
            const result = insertEvent.run({
              event_id: validatedItem.event_id,
              payload: JSON.stringify(validatedItem),
              timestamp: validatedItem.timestamp,
              fetched_at: fetchedAt
            });
            if (result.changes > 0) insertedCount++;
          } catch (err) {
            console.warn(`[IranWarLive] Skipped item due to validation error: ${err.message}`);
          }
        }
      });
      insertMany(newItems);
      if (insertedCount > 0) {
        console.log(`[IranWarLive] Added ${insertedCount} new hydrated events to history.`);
      }
    } else {
      console.log("[IranWarLive] No new events found.");
    }
  }
  const rows = getTopEvents.all();
  const history = rows.map((row) => JSON.parse(row.payload));
  await setLiveSnapshot("iranwarlive", {
    source: "iranwarlive",
    fetchedAt: (/* @__PURE__ */ new Date()).toISOString(),
    items: history
  }, 3600);
}
var index_default = {
  name: "iranwarlive",
  cron: "*/1 * * * *",
  // Every minute
  fn: seedIranWarLive
};
export {
  index_default as default,
  seedIranWarLive
};
