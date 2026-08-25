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

// src/parse.ts
var GFW_HOME_URL = "https://www.globalforestwatch.org";
function finiteNumber(value) {
  if (value === null || value === void 0) return null;
  if (typeof value === "string" && value.trim() === "") return null;
  const n = typeof value === "number" ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}
function optionalString(value) {
  return typeof value === "string" && value.length > 0 ? value : void 0;
}
function mapFireFeature(feature) {
  const lat = finiteNumber(feature.latitude);
  const lon = finiteNumber(feature.longitude);
  if (lat === null || lon === null) return null;
  const date = optionalString(feature.alert__date);
  const time = optionalString(feature.alert__time_utc);
  const confidence = optionalString(feature.confidence__cat);
  return {
    id: ["fire", lat.toFixed(4), lon.toFixed(4), date ?? "unknown", time ?? ""].filter((s) => s.length > 0).join("-"),
    lat,
    lon,
    alertType: "fire",
    confidence,
    date,
    url: GFW_HOME_URL
  };
}
function extractFireFeatures(payload) {
  if (payload === null || typeof payload !== "object") return [];
  if (Array.isArray(payload)) return payload;
  const obj = payload;
  if (Array.isArray(obj.data)) return obj.data;
  if (Array.isArray(obj.features)) return obj.features;
  return [];
}
function mapGladRow(row, centroid) {
  const iso = optionalString(row.iso);
  const adm1 = finiteNumber(row.adm1);
  const adm2 = finiteNumber(row.adm2);
  if (iso === void 0 || adm1 === null || adm2 === null) return null;
  if (!centroid || !Number.isFinite(centroid.lat) || !Number.isFinite(centroid.lon)) return null;
  const date = optionalString(row.umd_glad_landsat_alerts__date);
  const confidence = optionalString(row.umd_glad_landsat_alerts__confidence);
  return {
    id: `glad-${iso}-${adm1}-${adm2}`,
    lat: centroid.lat,
    lon: centroid.lon,
    alertType: "deforestation",
    confidence: confidence ?? finiteNumber(row.alert__count) ?? void 0,
    date,
    url: GFW_HOME_URL
  };
}
function extractGladRows(payload) {
  if (payload === null || typeof payload !== "object") return [];
  if (Array.isArray(payload)) return payload;
  const obj = payload;
  if (Array.isArray(obj.data)) return obj.data;
  return [];
}
function dedupeGladRows(rows) {
  const seen = /* @__PURE__ */ new Set();
  const out = [];
  for (const row of rows) {
    const iso = typeof row.iso === "string" ? row.iso : "";
    const adm1 = finiteNumber(row.adm1);
    const adm2 = finiteNumber(row.adm2);
    if (adm1 === null || adm2 === null) continue;
    const key = `${iso}.${adm1}.${adm2}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

// src/data/glad-adm2-centroids.json
var glad_adm2_centroids_default = [
  {
    iso: "IDN",
    adm1: 14,
    adm2: 5,
    lat: -1.7471,
    lon: 114.2979,
    name1: "Kalimantan Tengah",
    name2: "Kapuas",
    alerts: 68746
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 4,
    lat: -1.6331,
    lon: 110.6139,
    name1: "Kalimantan Barat",
    name2: "Ketapang",
    alerts: 51731
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 6,
    lat: -2.4431,
    lon: 103.8297,
    name1: "Sumatera Selatan",
    name2: "Musi Banyuasin",
    alerts: 45978
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 9,
    lat: -3.3433,
    lon: 105.3906,
    name1: "Sumatera Selatan",
    name2: "Ogan Komering Ilir",
    alerts: 41268
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 14,
    lat: 0.0933,
    lon: 112.0701,
    name1: "Kalimantan Barat",
    name2: "Sintang",
    alerts: 38395
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 11,
    lat: 0.2674,
    lon: 110.4322,
    name1: "Kalimantan Barat",
    name2: "Sanggau",
    alerts: 37366
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 8,
    lat: -6.4862,
    lon: -53.8867,
    name1: "Par\xE1",
    name2: "Altamira",
    alerts: 34954
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 7,
    lat: -2.9639,
    lon: 102.9487,
    name1: "Sumatera Selatan",
    name2: "Musi Rawas",
    alerts: 34591
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 8,
    lat: -8.9243,
    lon: -70.7445,
    name1: "Acre",
    name2: "Feij\xF3",
    alerts: 33106
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 19,
    lat: 23.5403,
    lon: 112.2021,
    name1: "Guangdong",
    name2: "Zhaoqing",
    alerts: 32274
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 7,
    lat: -0.4795,
    lon: 101.5038,
    name1: "Riau",
    name2: "Kuantan Singingi",
    alerts: 30747
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 14,
    lat: -10.1797,
    lon: -59.8563,
    name1: "Mato Grosso",
    name2: "Aripuan\xE3",
    alerts: 28032
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 12,
    lat: -39e-4,
    lon: 110.9513,
    name1: "Kalimantan Barat",
    name2: "Sekadau",
    alerts: 27893
  },
  {
    iso: "BOL",
    adm1: 8,
    adm2: 7,
    lat: -15.3449,
    lon: -63.2421,
    name1: "Santa Cruz",
    name2: "Guarayos",
    alerts: 27494
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 5,
    lat: 0.3257,
    lon: 101.0692,
    name1: "Riau",
    name2: "Kampar",
    alerts: 26996
  },
  {
    iso: "CHN",
    adm1: 26,
    adm2: 8,
    lat: 29.179,
    lon: 103.5329,
    name1: "Sichuan",
    name2: "Leshan",
    alerts: 26487
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 2,
    lat: 0.8319,
    lon: 112.7999,
    name1: "Kalimantan Barat",
    name2: "Kapuas Hulu",
    alerts: 24813
  },
  {
    iso: "BOL",
    adm1: 8,
    adm2: 9,
    lat: -17.8288,
    lon: -64.4569,
    name1: "Santa Cruz",
    name2: "Manuel Mar\xEDa Caballero",
    alerts: 24217
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 58,
    lat: -5.8711,
    lon: -56.4967,
    name1: "Par\xE1",
    name2: "Itaituba",
    alerts: 24093
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 16,
    lat: -7.8392,
    lon: 139.6974,
    name1: "Papua",
    name2: "Merauke",
    alerts: 23936
  },
  {
    iso: "CHN",
    adm1: 26,
    adm2: 20,
    lat: 28.5717,
    lon: 104.6353,
    name1: "Sichuan",
    name2: "Yibin",
    alerts: 23760
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 25,
    lat: -0.7321,
    lon: 135.5647,
    name1: "Papua",
    name2: "Supiori",
    alerts: 23347
  },
  {
    iso: "IDN",
    adm1: 13,
    adm2: 9,
    lat: -2.9873,
    lon: 116.0658,
    name1: "Kalimantan Selatan",
    name2: "Kota Baru",
    alerts: 23330
  },
  {
    iso: "CHN",
    adm1: 7,
    adm2: 8,
    lat: 24.3591,
    lon: 111.3601,
    name1: "Guangxi",
    name2: "Hezhou",
    alerts: 22381
  },
  {
    iso: "IDN",
    adm1: 3,
    adm2: 1,
    lat: -1.889,
    lon: 105.4843,
    name1: "Bangka Belitung",
    name2: "Bangka Barat",
    alerts: 22319
  },
  {
    iso: "IDN",
    adm1: 3,
    adm2: 2,
    lat: -2.7782,
    lon: 106.3477,
    name1: "Bangka Belitung",
    name2: "Bangka Selatan",
    alerts: 21835
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 60,
    lat: -10.3975,
    lon: -58.6248,
    name1: "Mato Grosso",
    name2: "Juruena",
    alerts: 21741
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 6,
    lat: -1.6985,
    lon: 113.0597,
    name1: "Kalimantan Tengah",
    name2: "Katingan",
    alerts: 21722
  },
  {
    iso: "BOL",
    adm1: 8,
    adm2: 2,
    lat: -17.2547,
    lon: -58.8245,
    name1: "Santa Cruz",
    name2: "Angel Sandoval",
    alerts: 20470
  },
  {
    iso: "CIV",
    adm1: 4,
    adm2: 2,
    lat: 9.4112,
    lon: -7.3881,
    name1: "Dengu\xE9l\xE9",
    name2: "Kabadougou",
    alerts: 19675
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 4,
    lat: -1.0256,
    lon: 113.5331,
    name1: "Kalimantan Tengah",
    name2: "Gunung Mas",
    alerts: 19165
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 12,
    lat: 0.8265,
    lon: 101.8249,
    name1: "Riau",
    name2: "Siak",
    alerts: 18156
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 3,
    lat: -0.2327,
    lon: 103.1579,
    name1: "Riau",
    name2: "Indragiri Hilir",
    alerts: 18097
  },
  {
    iso: "CHN",
    adm1: 3,
    adm2: 1,
    lat: 30.0586,
    lon: 107.8748,
    name1: "Chongqing",
    name2: "Chongqing",
    alerts: 17985
  },
  {
    iso: "PER",
    adm1: 26,
    adm2: 2,
    lat: -8.6937,
    lon: -74.0575,
    name1: "Ucayali",
    name2: "Coronel Portillo",
    alerts: 17069
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 13,
    lat: -2.2807,
    lon: 112.1756,
    name1: "Kalimantan Tengah",
    name2: "Seruyan",
    alerts: 17027
  },
  {
    iso: "CHN",
    adm1: 7,
    adm2: 13,
    lat: 23.4812,
    lon: 110.9918,
    name1: "Guangxi",
    name2: "Wuzhou",
    alerts: 16800
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 8,
    lat: -2.0649,
    lon: 112.713,
    name1: "Kalimantan Tengah",
    name2: "Kotawaringin Timur",
    alerts: 16754
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 11,
    lat: 24.3123,
    lon: 112.8737,
    name1: "Guangdong",
    name2: "Qingyuan",
    alerts: 16724
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 34,
    lat: -4.7153,
    lon: -68.4063,
    name1: "Amazonas",
    name2: "Juta\xED",
    alerts: 16303
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 10,
    lat: 4.63,
    lon: 97.6277,
    name1: "Aceh",
    name2: "Aceh Timur",
    alerts: 15368
  },
  {
    iso: "MYS",
    adm1: 13,
    adm2: 4,
    lat: 5.4076,
    lon: 118.1888,
    name1: "Sabah",
    name2: "Kinabatangan",
    alerts: 15234
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 120,
    lat: -7.2334,
    lon: -52.2527,
    name1: "Par\xE1",
    name2: "S\xE3o F\xE9lix do Xingu",
    alerts: 15181
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 3,
    lat: -0.931,
    lon: 115.1339,
    name1: "Kalimantan Tengah",
    name2: "Barito Utara",
    alerts: 14901
  },
  {
    iso: "CHN",
    adm1: 4,
    adm2: 7,
    lat: 26.2999,
    lon: 117.3952,
    name1: "Fujian",
    name2: "Sanming",
    alerts: 14798
  },
  {
    iso: "IDN",
    adm1: 8,
    adm2: 6,
    lat: -1.6107,
    lon: 103.7563,
    name1: "Jambi",
    name2: "Muaro Jambi",
    alerts: 14692
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 5,
    lat: -3.5494,
    lon: 103.9917,
    name1: "Sumatera Selatan",
    name2: "Muara Enim",
    alerts: 14409
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 7,
    lat: -2.9789,
    lon: 139.9758,
    name1: "Papua",
    name2: "Jayapura",
    alerts: 14314
  },
  {
    iso: "MYS",
    adm1: 8,
    adm2: 10,
    lat: 2.8678,
    lon: 103.113,
    name1: "Pahang",
    name2: "Rompin",
    alerts: 14143
  },
  {
    iso: "CHN",
    adm1: 4,
    adm2: 3,
    lat: 27.3403,
    lon: 118.1426,
    name1: "Fujian",
    name2: "Nanping",
    alerts: 14063
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 60,
    lat: -7.4453,
    lon: -57.3027,
    name1: "Par\xE1",
    name2: "Jacareacanga",
    alerts: 13839
  },
  {
    iso: "IDN",
    adm1: 8,
    adm2: 9,
    lat: -1.0712,
    lon: 103.1119,
    name1: "Jambi",
    name2: "Tanjung Jabung B",
    alerts: 13789
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 9,
    lat: -1.8005,
    lon: 111.4066,
    name1: "Kalimantan Tengah",
    name2: "Lamandau",
    alerts: 13758
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 12,
    lat: -2.7159,
    lon: 113.9546,
    name1: "Kalimantan Tengah",
    name2: "Pulang Pisau",
    alerts: 13720
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 23,
    lat: -3.6195,
    lon: 137.4,
    name1: "Papua",
    name2: "Puncak",
    alerts: 13676
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 10,
    lat: -0.0302,
    lon: 114.2796,
    name1: "Kalimantan Tengah",
    name2: "Murung Raya",
    alerts: 13636
  },
  {
    iso: "IDN",
    adm1: 27,
    adm2: 11,
    lat: 0.926,
    lon: 120.7189,
    name1: "Sulawesi Tengah",
    name2: "Toli-Toli",
    alerts: 13139
  },
  {
    iso: "PNG",
    adm1: 20,
    adm2: 2,
    lat: -5.542,
    lon: 150.4931,
    name1: "West New Britain",
    name2: "Talasea",
    alerts: 12936
  },
  {
    iso: "PER",
    adm1: 18,
    adm2: 3,
    lat: -12.2085,
    lon: -70.0456,
    name1: "Madre de Dios",
    name2: "Tambopata",
    alerts: 12154
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 9,
    lat: 0.2194,
    lon: 102.2676,
    name1: "Riau",
    name2: "Pelalawan",
    alerts: 11965
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 28,
    lat: -4.4892,
    lon: 139.6033,
    name1: "Papua",
    name2: "Yahukimo",
    alerts: 11946
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 26,
    lat: -7.3612,
    lon: -62.4864,
    name1: "Amazonas",
    name2: "Humait\xE1",
    alerts: 11932
  },
  {
    iso: "PER",
    adm1: 26,
    adm2: 1,
    lat: -10.3985,
    lon: -73.2201,
    name1: "Ucayali",
    name2: "Atalaya",
    alerts: 11904
  },
  {
    iso: "PER",
    adm1: 17,
    adm2: 1,
    lat: -4.4679,
    lon: -76.7266,
    name1: "Loreto",
    name2: "Alto Amazonas",
    alerts: 11769
  },
  {
    iso: "IDN",
    adm1: 30,
    adm2: 12,
    lat: 0.207,
    lon: 99.6781,
    name1: "Sumatera Barat",
    name2: "Pasaman Barat",
    alerts: 11660
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 21,
    lat: -8.2395,
    lon: -71.445,
    name1: "Acre",
    name2: "Tarauac\xE1",
    alerts: 11538
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 37,
    lat: -3.7791,
    lon: -60.6973,
    name1: "Amazonas",
    name2: "Manaquiri",
    alerts: 11451
  },
  {
    iso: "PER",
    adm1: 12,
    adm2: 7,
    lat: -11.5732,
    lon: -74.0964,
    name1: "Jun\xEDn",
    name2: "Satipo",
    alerts: 11332
  },
  {
    iso: "IDN",
    adm1: 3,
    adm2: 5,
    lat: -2.907,
    lon: 108.0523,
    name1: "Bangka Belitung",
    name2: "Belitung Timur",
    alerts: 11118
  },
  {
    iso: "IDN",
    adm1: 8,
    adm2: 11,
    lat: -1.3146,
    lon: 102.362,
    name1: "Jambi",
    name2: "Tebo",
    alerts: 11072
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 36,
    lat: -13.1655,
    lon: -61.5452,
    name1: "Rond\xF4nia",
    name2: "Pimenteiras do Oeste",
    alerts: 11043
  },
  {
    iso: "IDN",
    adm1: 8,
    adm2: 1,
    lat: -1.7497,
    lon: 103.0961,
    name1: "Jambi",
    name2: "Batang Hari",
    alerts: 10947
  },
  {
    iso: "IDN",
    adm1: 25,
    adm2: 4,
    lat: -2.3344,
    lon: 119.3977,
    name1: "Sulawesi Barat",
    name2: "Mamuju",
    alerts: 10906
  },
  {
    iso: "CHN",
    adm1: 26,
    adm2: 10,
    lat: 28.4401,
    lon: 105.6674,
    name1: "Sichuan",
    name2: "Luzhou",
    alerts: 10797
  },
  {
    iso: "IDN",
    adm1: 16,
    adm2: 5,
    lat: -0.2246,
    lon: 104.5271,
    name1: "Kepulauan Riau",
    name2: "Lingga",
    alerts: 10771
  },
  {
    iso: "PNG",
    adm1: 22,
    adm2: 1,
    lat: -7.2227,
    lon: 142.2872,
    name1: "Western",
    name2: "Middle Fly",
    alerts: 10769
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 3,
    lat: 25.812,
    lon: 113.1351,
    name1: "Hunan",
    name2: "Chenzhou",
    alerts: 10751
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 5,
    lat: 3.1614,
    lon: 113.4644,
    name1: "Sarawak",
    name2: "Bintulu",
    alerts: 10621
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 5,
    lat: -7.76,
    lon: -59.4321,
    name1: "Amazonas",
    name2: "Apu\xED",
    alerts: 10582
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 19,
    lat: 3.8728,
    lon: 113.8301,
    name1: "Sarawak",
    name2: "Miri",
    alerts: 10423
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 23,
    lat: -7.5733,
    lon: -70.0551,
    name1: "Amazonas",
    name2: "Envira",
    alerts: 10271
  },
  {
    iso: "CIV",
    adm1: 12,
    adm2: 3,
    lat: 8.3722,
    lon: -6.7604,
    name1: "Woroba",
    name2: "Worodougou",
    alerts: 10260
  },
  {
    iso: "IDN",
    adm1: 3,
    adm2: 4,
    lat: -1.9078,
    lon: 105.9188,
    name1: "Bangka Belitung",
    name2: "Bangka",
    alerts: 10209
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 8,
    lat: -0.6313,
    lon: 111.7279,
    name1: "Kalimantan Barat",
    name2: "Melawi",
    alerts: 10175
  },
  {
    iso: "CHN",
    adm1: 7,
    adm2: 1,
    lat: 23.9858,
    lon: 106.2888,
    name1: "Guangxi",
    name2: "Baise",
    alerts: 10126
  },
  {
    iso: "IDN",
    adm1: 22,
    adm2: 8,
    lat: -1.1162,
    lon: 131.5643,
    name1: "Papua Barat",
    name2: "Sorong",
    alerts: 10114
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 1,
    lat: -2.5041,
    lon: 104.6695,
    name1: "Sumatera Selatan",
    name2: "Banyu Asin",
    alerts: 10089
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 10,
    lat: 1.4293,
    lon: 109.3461,
    name1: "Kalimantan Barat",
    name2: "Sambas",
    alerts: 10037
  },
  {
    iso: "BOL",
    adm1: 3,
    adm2: 4,
    lat: -13.1369,
    lon: -64.761,
    name1: "Beni",
    name2: "Mamor\xE9",
    alerts: 9911
  },
  {
    iso: "PER",
    adm1: 18,
    adm2: 1,
    lat: -12.2661,
    lon: -71.2935,
    name1: "Madre de Dios",
    name2: "Manu",
    alerts: 9910
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 80,
    lat: -8.0623,
    lon: -55.612,
    name1: "Par\xE1",
    name2: "Novo Progresso",
    alerts: 9813
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 11,
    lat: -2.0185,
    lon: 113.7691,
    name1: "Kalimantan Tengah",
    name2: "Palangka Raya",
    alerts: 9784
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 17,
    lat: 22.8158,
    lon: 111.796,
    name1: "Guangdong",
    name2: "Yunfu",
    alerts: 9682
  },
  {
    iso: "MEX",
    adm1: 4,
    adm2: 3,
    lat: 19.6169,
    lon: -90.2675,
    name1: "Campeche",
    name2: "Campeche",
    alerts: 9629
  },
  {
    iso: "MYS",
    adm1: 13,
    adm2: 2,
    lat: 6.0582,
    lon: 117.3356,
    name1: "Sabah",
    name2: "Beluran",
    alerts: 9603
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 13,
    lat: -8.7732,
    lon: -68.0476,
    name1: "Amazonas",
    name2: "Boca do Acre",
    alerts: 9500
  },
  {
    iso: "IDN",
    adm1: 30,
    adm2: 4,
    lat: -1.1318,
    lon: 101.554,
    name1: "Sumatera Barat",
    name2: "Dharmasraya",
    alerts: 9439
  },
  {
    iso: "BOL",
    adm1: 3,
    adm2: 6,
    lat: -15.4686,
    lon: -65.6867,
    name1: "Beni",
    name2: "Moxos",
    alerts: 9413
  },
  {
    iso: "GIN",
    adm1: 4,
    adm2: 1,
    lat: 10.0597,
    lon: -9.1063,
    name1: "Kankan",
    name2: "Kankan",
    alerts: 9405
  },
  {
    iso: "PER",
    adm1: 23,
    adm2: 5,
    lat: -7.2785,
    lon: -77.1698,
    name1: "San Mart\xEDn",
    name2: "Mariscal C\xE1ceres",
    alerts: 9296
  },
  {
    iso: "BRA",
    adm1: 23,
    adm2: 12,
    lat: 0.038,
    lon: -61.0112,
    name1: "Roraima",
    name2: "Rorain\xF3polis",
    alerts: 9136
  },
  {
    iso: "CHN",
    adm1: 7,
    adm2: 14,
    lat: 22.4433,
    lon: 110.1832,
    name1: "Guangxi",
    name2: "Yulin",
    alerts: 9050
  },
  {
    iso: "IDN",
    adm1: 8,
    adm2: 10,
    lat: -1.2198,
    lon: 103.9674,
    name1: "Jambi",
    name2: "Tanjung Jabung T",
    alerts: 8811
  },
  {
    iso: "BOL",
    adm1: 6,
    adm2: 1,
    lat: -10.6064,
    lon: -66.9858,
    name1: "Pando",
    name2: "Abun\xE1",
    alerts: 8762
  },
  {
    iso: "CHN",
    adm1: 31,
    adm2: 7,
    lat: 28.9345,
    lon: 118.6768,
    name1: "Zhejiang",
    name2: "Quzhou",
    alerts: 8672
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 18,
    lat: -3.3766,
    lon: 135.5844,
    name1: "Papua",
    name2: "Nabire",
    alerts: 8625
  },
  {
    iso: "BOL",
    adm1: 3,
    adm2: 2,
    lat: -13.9104,
    lon: -66.8949,
    name1: "Beni",
    name2: "General Jos\xE9 Ballivi\xE1n",
    alerts: 8495
  },
  {
    iso: "IDN",
    adm1: 3,
    adm2: 3,
    lat: -2.433,
    lon: 106.2387,
    name1: "Bangka Belitung",
    name2: "Bangka Tengah",
    alerts: 8479
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 7,
    lat: -2.546,
    lon: 111.7468,
    name1: "Kalimantan Tengah",
    name2: "Kotawaringin Barat",
    alerts: 8430
  },
  {
    iso: "CIV",
    adm1: 4,
    adm2: 1,
    lat: 10.1028,
    lon: -7.3996,
    name1: "Dengu\xE9l\xE9",
    name2: "Folon",
    alerts: 8396
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 43,
    lat: -3.9895,
    lon: -58.5998,
    name1: "Amazonas",
    name2: "Nova Olinda do Norte",
    alerts: 8381
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 4,
    lat: -0.5376,
    lon: 102.3274,
    name1: "Riau",
    name2: "Indragiri Hulu",
    alerts: 8358
  },
  {
    iso: "IDN",
    adm1: 8,
    adm2: 5,
    lat: -2.1962,
    lon: 102.0644,
    name1: "Jambi",
    name2: "Merangin",
    alerts: 8351
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 1,
    lat: -1.8116,
    lon: 114.906,
    name1: "Kalimantan Tengah",
    name2: "Barito Selatan",
    alerts: 8275
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 7,
    lat: 2.3824,
    lon: 111.4194,
    name1: "Sarawak",
    name2: "Daro",
    alerts: 8268
  },
  {
    iso: "IDN",
    adm1: 26,
    adm2: 9,
    lat: -2.5507,
    lon: 121.1385,
    name1: "Sulawesi Selatan",
    name2: "Luwu Timur",
    alerts: 8228
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 16,
    lat: 3.547,
    lon: 114.7722,
    name1: "Sarawak",
    name2: "Marudi",
    alerts: 8174
  },
  {
    iso: "BRA",
    adm1: 23,
    adm2: 6,
    lat: 0.9912,
    lon: -61.4704,
    name1: "Roraima",
    name2: "Caracara\xED",
    alerts: 8064
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 45,
    lat: -6.824,
    lon: -60.4772,
    name1: "Amazonas",
    name2: "Novo Aripuan\xE3",
    alerts: 8059
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 7,
    lat: -3.7872,
    lon: -59.5255,
    name1: "Amazonas",
    name2: "Autazes",
    alerts: 8026
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 4,
    lat: 4.8331,
    lon: 95.6785,
    name1: "Aceh",
    name2: "Aceh Jaya",
    alerts: 8007
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 70,
    lat: -3.1602,
    lon: -53.195,
    name1: "Par\xE1",
    name2: "Medicil\xE2ndia",
    alerts: 7992
  },
  {
    iso: "PER",
    adm1: 17,
    adm2: 6,
    lat: -7.2937,
    lon: -75.3822,
    name1: "Loreto",
    name2: "Ucayali",
    alerts: 7988
  },
  {
    iso: "IDN",
    adm1: 5,
    adm2: 8,
    lat: -2.7014,
    lon: 101.4856,
    name1: "Bengkulu",
    name2: "Mukomuko",
    alerts: 7843
  },
  {
    iso: "PNG",
    adm1: 17,
    adm2: 1,
    lat: -9.3132,
    lon: 148.6128,
    name1: "Oro",
    name2: "Ijivitari",
    alerts: 7807
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 20,
    lat: -3.8052,
    lon: 136.4928,
    name1: "Papua",
    name2: "Paniai",
    alerts: 7784
  },
  {
    iso: "SUR",
    adm1: 9,
    adm2: 6,
    lat: 3.4332,
    lon: -54.9507,
    name1: "Sipaliwini",
    name2: "Tapanahony",
    alerts: 7721
  },
  {
    iso: "CHN",
    adm1: 4,
    adm2: 2,
    lat: 25.2923,
    lon: 116.7383,
    name1: "Fujian",
    name2: "Longyan",
    alerts: 7715
  },
  {
    iso: "IDN",
    adm1: 13,
    adm2: 11,
    lat: -3.4424,
    lon: 115.6485,
    name1: "Kalimantan Selatan",
    name2: "Tanah Bumbu",
    alerts: 7691
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 365,
    lat: -25.4795,
    lon: -54.03,
    name1: "Paran\xE1",
    name2: "Serran\xF3polis do Igua\xE7u",
    alerts: 7508
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 39,
    lat: -6.46,
    lon: -61.4468,
    name1: "Amazonas",
    name2: "Manicor\xE9",
    alerts: 7365
  },
  {
    iso: "IDN",
    adm1: 3,
    adm2: 6,
    lat: -2.8701,
    lon: 107.7107,
    name1: "Bangka Belitung",
    name2: "Belitung",
    alerts: 7312
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 7,
    lat: 22.2804,
    lon: 112.6706,
    name1: "Guangdong",
    name2: "Jiangmen",
    alerts: 7303
  },
  {
    iso: "IDN",
    adm1: 30,
    adm2: 15,
    lat: -1.7383,
    lon: 100.8807,
    name1: "Sumatera Barat",
    name2: "Pesisir Selatan",
    alerts: 7037
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 3,
    lat: -1.0921,
    lon: 109.9247,
    name1: "Kalimantan Barat",
    name2: "Kayong Utara",
    alerts: 6966
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 3,
    lat: -3.8306,
    lon: 103.3853,
    name1: "Sumatera Selatan",
    name2: "Lahat",
    alerts: 6964
  },
  {
    iso: "ECU",
    adm1: 22,
    adm2: 4,
    lat: 0.1097,
    lon: -76.7761,
    name1: "Sucumbios",
    name2: "Lago Agrio",
    alerts: 6955
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 3,
    lat: -5.9867,
    lon: 140.366,
    name1: "Papua",
    name2: "Boven Digoel",
    alerts: 6866
  },
  {
    iso: "CIV",
    adm1: 12,
    adm2: 1,
    lat: 8.3822,
    lon: -7.6214,
    name1: "Woroba",
    name2: "Bafing",
    alerts: 6855
  },
  {
    iso: "IDN",
    adm1: 8,
    adm2: 7,
    lat: -2.309,
    lon: 102.6645,
    name1: "Jambi",
    name2: "Sarolangun",
    alerts: 6837
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 173,
    lat: -24.1281,
    lon: -52.8112,
    name1: "Paran\xE1",
    name2: "Jani\xF3polis",
    alerts: 6817
  },
  {
    iso: "CHN",
    adm1: 31,
    adm2: 5,
    lat: 28.1982,
    lon: 119.5102,
    name1: "Zhejiang",
    name2: "Lishui",
    alerts: 6768
  },
  {
    iso: "MEX",
    adm1: 4,
    adm2: 4,
    lat: 18.0992,
    lon: -90.7167,
    name1: "Campeche",
    name2: "Candelaria",
    alerts: 6766
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 2,
    lat: 4.4567,
    lon: 96.1853,
    name1: "Aceh",
    name2: "Aceh Barat",
    alerts: 6754
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 104,
    lat: -4.241,
    lon: -55.2107,
    name1: "Par\xE1",
    name2: "Rur\xF3polis",
    alerts: 6751
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 129,
    lat: -4.1697,
    lon: -51.7738,
    name1: "Par\xE1",
    name2: "Senador Jos\xE9 Porf\xEDrio",
    alerts: 6745
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 19,
    lat: -4.4274,
    lon: 138.2405,
    name1: "Papua",
    name2: "Nduga",
    alerts: 6745
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 11,
    lat: 25.6929,
    lon: 111.7698,
    name1: "Hunan",
    name2: "Yongzhou",
    alerts: 6657
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 136,
    lat: -5.1302,
    lon: -56.0035,
    name1: "Par\xE1",
    name2: "Trair\xE3o",
    alerts: 6638
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 10,
    lat: 1.7704,
    lon: 100.7469,
    name1: "Riau",
    name2: "Rokan Hilir",
    alerts: 6612
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 16,
    lat: -7.3008,
    lon: -64.0619,
    name1: "Amazonas",
    name2: "Canutama",
    alerts: 6609
  },
  {
    iso: "IDN",
    adm1: 27,
    adm2: 5,
    lat: -2.0914,
    lon: 121.5513,
    name1: "Sulawesi Tengah",
    name2: "Morowali",
    alerts: 6597
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 74,
    lat: -10.2939,
    lon: -55.3382,
    name1: "Mato Grosso",
    name2: "Nova Guarita",
    alerts: 6589
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 18,
    lat: -12.2178,
    lon: -51.7395,
    name1: "Mato Grosso",
    name2: "Bom Jesus do Araguaia",
    alerts: 6579
  },
  {
    iso: "IDN",
    adm1: 5,
    adm2: 3,
    lat: -3.332,
    lon: 102.0052,
    name1: "Bengkulu",
    name2: "Bengkulu Utara",
    alerts: 6570
  },
  {
    iso: "CHN",
    adm1: 26,
    adm2: 15,
    lat: 29.6137,
    lon: 104.8855,
    name1: "Sichuan",
    name2: "Neijiang",
    alerts: 6556
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 140,
    lat: -3.5836,
    lon: -53.8073,
    name1: "Par\xE1",
    name2: "Uruar\xE1",
    alerts: 6437
  },
  {
    iso: "PER",
    adm1: 10,
    adm2: 10,
    lat: -9.3783,
    lon: -75.0918,
    name1: "Hu\xE1nuco",
    name2: "Puerto Inca",
    alerts: 6429
  },
  {
    iso: "ECU",
    adm1: 17,
    adm2: 4,
    lat: -0.6551,
    lon: -76.7717,
    name1: "Orellana",
    name2: "Orellana",
    alerts: 6363
  },
  {
    iso: "CHN",
    adm1: 4,
    adm2: 4,
    lat: 26.973,
    lon: 119.4766,
    name1: "Fujian",
    name2: "Ningde",
    alerts: 6333
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 11,
    lat: 0.9394,
    lon: 100.4748,
    name1: "Riau",
    name2: "Rokan Hulu",
    alerts: 6325
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 5,
    lat: 3.1629,
    lon: 97.4354,
    name1: "Aceh",
    name2: "Aceh Selatan",
    alerts: 6274
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 9,
    lat: -8.7586,
    lon: -57.826,
    name1: "Mato Grosso",
    name2: "Apiac\xE1s",
    alerts: 6265
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 11,
    lat: 5.0149,
    lon: 97.1819,
    name1: "Aceh",
    name2: "Aceh Utara",
    alerts: 6153
  },
  {
    iso: "MYS",
    adm1: 1,
    adm2: 7,
    lat: 2.3456,
    lon: 103.7441,
    name1: "Johor",
    name2: "Mersing",
    alerts: 6136
  },
  {
    iso: "CHN",
    adm1: 16,
    adm2: 1,
    lat: 27.5102,
    lon: 116.4333,
    name1: "Jiangxi",
    name2: "Fuzhou",
    alerts: 6104
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 17,
    lat: -4.5005,
    lon: 136.6939,
    name1: "Papua",
    name2: "Mimika",
    alerts: 6046
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 138,
    lat: -25.2341,
    lon: -48.3674,
    name1: "Paran\xE1",
    name2: "Guaraque\xE7aba",
    alerts: 6046
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 25,
    lat: 2.4557,
    lon: 112.4605,
    name1: "Sarawak",
    name2: "Selangau",
    alerts: 6039
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 14,
    lat: 24.8182,
    lon: 113.7718,
    name1: "Guangdong",
    name2: "Shaoguan",
    alerts: 5989
  },
  {
    iso: "IDN",
    adm1: 22,
    adm2: 10,
    lat: -2.0031,
    lon: 133.3085,
    name1: "Papua Barat",
    name2: "Teluk Bintuni",
    alerts: 5989
  },
  {
    iso: "IDN",
    adm1: 22,
    adm2: 4,
    lat: -1.1192,
    lon: 133.5322,
    name1: "Papua Barat",
    name2: "Manokwari",
    alerts: 5989
  },
  {
    iso: "CHN",
    adm1: 26,
    adm2: 11,
    lat: 29.8978,
    lon: 103.78,
    name1: "Sichuan",
    name2: "Meishan",
    alerts: 5969
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 5,
    lat: 27.5468,
    lon: 110.074,
    name1: "Hunan",
    name2: "Huaihua",
    alerts: 5954
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 8,
    lat: 4.5301,
    lon: 96.859,
    name1: "Aceh",
    name2: "Aceh Tengah",
    alerts: 5842
  },
  {
    iso: "GUY",
    adm1: 2,
    adm2: 8,
    lat: 6.2242,
    lon: -59.8397,
    name1: "Cuyuni-Mazaruni",
    name2: "Rest of Region 7",
    alerts: 5831
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 2,
    lat: 1.7977,
    lon: 101.3117,
    name1: "Riau",
    name2: "Dumai",
    alerts: 5816
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 74,
    lat: -3.0704,
    lon: -54.5752,
    name1: "Par\xE1",
    name2: "Moju\xED dos Campos",
    alerts: 5816
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 20,
    lat: 2.8271,
    lon: 112.3685,
    name1: "Sarawak",
    name2: "Mukah",
    alerts: 5793
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 67,
    lat: -15.6542,
    lon: -58.055,
    name1: "Mato Grosso",
    name2: "Mirassol d'Oeste",
    alerts: 5681
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 87,
    lat: -3.6841,
    lon: -50.6317,
    name1: "Par\xE1",
    name2: "Pacaj\xE1",
    alerts: 5632
  },
  {
    iso: "PER",
    adm1: 20,
    adm2: 2,
    lat: -10.2922,
    lon: -74.9799,
    name1: "Pasco",
    name2: "Oxapampa",
    alerts: 5622
  },
  {
    iso: "IDN",
    adm1: 5,
    adm2: 5,
    lat: -4.6075,
    lon: 103.4069,
    name1: "Bengkulu",
    name2: "Kaur",
    alerts: 5591
  },
  {
    iso: "COD",
    adm1: 25,
    adm2: 8,
    lat: -0.7578,
    lon: 24.3679,
    name1: "Tshopo",
    name2: "Opala",
    alerts: 5561
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 2,
    lat: -1.9808,
    lon: 115.1466,
    name1: "Kalimantan Tengah",
    name2: "Barito Timur",
    alerts: 5540
  },
  {
    iso: "PER",
    adm1: 8,
    adm2: 9,
    lat: -12.3496,
    lon: -72.9152,
    name1: "Cusco",
    name2: "La Convenci\xF3n",
    alerts: 5531
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 3,
    lat: 23.013,
    lon: 112.9451,
    name1: "Guangdong",
    name2: "Foshan",
    alerts: 5499
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 55,
    lat: -2.8205,
    lon: -58.5371,
    name1: "Amazonas",
    name2: "Silves",
    alerts: 5489
  },
  {
    iso: "IDN",
    adm1: 28,
    adm2: 7,
    lat: -3.928,
    lon: 121.5766,
    name1: "Sulawesi Tenggara",
    name2: "Kolaka",
    alerts: 5471
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 3,
    lat: 2.4627,
    lon: 114.2958,
    name1: "Sarawak",
    name2: "Belaga",
    alerts: 5426
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 9,
    lat: 0.3583,
    lon: 109.1559,
    name1: "Kalimantan Barat",
    name2: "Pontianak",
    alerts: 5416
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 29,
    lat: -3.1741,
    lon: -58.7753,
    name1: "Amazonas",
    name2: "Itacoatiara",
    alerts: 5353
  },
  {
    iso: "SUR",
    adm1: 1,
    adm2: 6,
    lat: 4.5656,
    lon: -55.0616,
    name1: "Brokopondo",
    name2: "Sarakreek",
    alerts: 5294
  },
  {
    iso: "IDN",
    adm1: 8,
    adm2: 2,
    lat: -1.5989,
    lon: 101.931,
    name1: "Jambi",
    name2: "Bungo",
    alerts: 5292
  },
  {
    iso: "IDN",
    adm1: 28,
    adm2: 10,
    lat: -3.598,
    lon: 122.0352,
    name1: "Sulawesi Tenggara",
    name2: "Konawe",
    alerts: 5285
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 85,
    lat: -11.3836,
    lon: -57.3035,
    name1: "Mato Grosso",
    name2: "Novo Horizonte do Norte",
    alerts: 5259
  },
  {
    iso: "CHN",
    adm1: 30,
    adm2: 16,
    lat: 27.6532,
    lon: 104.001,
    name1: "Yunnan",
    name2: "Zhaotong",
    alerts: 5249
  },
  {
    iso: "MYS",
    adm1: 13,
    adm2: 18,
    lat: 5.766,
    lon: 117.9916,
    name1: "Sabah",
    name2: "Sandakan",
    alerts: 5191
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 132,
    lat: -29.5995,
    lon: -51.0903,
    name1: "Rio Grande do Sul",
    name2: "Dois Irm\xE3os",
    alerts: 5188
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 16,
    lat: -10.0661,
    lon: -68.3711,
    name1: "Acre",
    name2: "Rio Branco",
    alerts: 5167
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 1,
    lat: 2.8024,
    lon: 99.5626,
    name1: "Sumatera Utara",
    name2: "Asahan",
    alerts: 5136
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 16,
    lat: 22.0379,
    lon: 111.7744,
    name1: "Guangdong",
    name2: "Yangjiang",
    alerts: 5126
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 11,
    lat: -3.9972,
    lon: -51.3013,
    name1: "Par\xE1",
    name2: "Anapu",
    alerts: 5123
  },
  {
    iso: "IDN",
    adm1: 27,
    adm2: 4,
    lat: -0.3959,
    lon: 119.8357,
    name1: "Sulawesi Tengah",
    name2: "Donggala",
    alerts: 5044
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 229,
    lat: -28.6513,
    lon: -49.1258,
    name1: "Santa Catarina",
    name2: "Sang\xE3o",
    alerts: 4991
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 14,
    lat: 27.1187,
    lon: 113.518,
    name1: "Hunan",
    name2: "Zhuzhou",
    alerts: 4905
  },
  {
    iso: "SUR",
    adm1: 9,
    adm2: 2,
    lat: 4.3332,
    lon: -55.7829,
    name1: "Sipaliwini",
    name2: "Bven Saramacca",
    alerts: 4890
  },
  {
    iso: "CHN",
    adm1: 4,
    adm2: 9,
    lat: 24.373,
    lon: 117.4414,
    name1: "Fujian",
    name2: "Zhangzhou",
    alerts: 4857
  },
  {
    iso: "IDN",
    adm1: 27,
    adm2: 3,
    lat: 0.978,
    lon: 121.3728,
    name1: "Sulawesi Tengah",
    name2: "Buol",
    alerts: 4797
  },
  {
    iso: "GTM",
    adm1: 12,
    adm2: 3,
    lat: 16.9497,
    lon: -90.6131,
    name1: "Pet\xE9n",
    name2: "La Libertad",
    alerts: 4785
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 19,
    lat: -9.7722,
    lon: -69.383,
    name1: "Acre",
    name2: "Sena Madureira",
    alerts: 4781
  },
  {
    iso: "IDN",
    adm1: 13,
    adm2: 12,
    lat: -3.8201,
    lon: 114.9039,
    name1: "Kalimantan Selatan",
    name2: "Tanah Laut",
    alerts: 4766
  },
  {
    iso: "CHN",
    adm1: 26,
    adm2: 19,
    lat: 29.9162,
    lon: 102.663,
    name1: "Sichuan",
    name2: "Ya'an",
    alerts: 4752
  },
  {
    iso: "BRA",
    adm1: 13,
    adm2: 353,
    lat: -19.4979,
    lon: -44.4091,
    name1: "Minas Gerais",
    name2: "Inha\xFAma",
    alerts: 4733
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 73,
    lat: -10.7003,
    lon: -56.0417,
    name1: "Mato Grosso",
    name2: "Nova Cana\xE3 do Norte",
    alerts: 4713
  },
  {
    iso: "IDN",
    adm1: 5,
    adm2: 10,
    lat: -4.0523,
    lon: 102.6578,
    name1: "Bengkulu",
    name2: "Seluma",
    alerts: 4680
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 52,
    lat: -29.6377,
    lon: -51.9226,
    name1: "Rio Grande do Sul",
    name2: "Bom Retiro do Sul",
    alerts: 4674
  },
  {
    iso: "CHN",
    adm1: 8,
    adm2: 9,
    lat: 28.1683,
    lon: 107.087,
    name1: "Guizhou",
    name2: "Zunyi",
    alerts: 4653
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 26,
    lat: -3.4839,
    lon: 138.4086,
    name1: "Papua",
    name2: "Tolikara",
    alerts: 4601
  },
  {
    iso: "IDN",
    adm1: 14,
    adm2: 14,
    lat: -2.6232,
    lon: 111.1608,
    name1: "Kalimantan Tengah",
    name2: "Sukamara",
    alerts: 4577
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 6,
    lat: -0.3866,
    lon: 109.5376,
    name1: "Kalimantan Barat",
    name2: "Kubu Raya",
    alerts: 4568
  },
  {
    iso: "PER",
    adm1: 23,
    adm2: 10,
    lat: -8.249,
    lon: -76.6366,
    name1: "San Mart\xEDn",
    name2: "Tocache",
    alerts: 4560
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 220,
    lat: -28.0934,
    lon: -49.1906,
    name1: "Santa Catarina",
    name2: "Rio Fortuna",
    alerts: 4560
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 6,
    lat: -3.5364,
    lon: 136.7448,
    name1: "Papua",
    name2: "Intan Jaya",
    alerts: 4559
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 7,
    lat: -26.0137,
    lon: -53.9601,
    name1: "Misiones",
    name2: "General Manuel Belgrano",
    alerts: 4533
  },
  {
    iso: "ECU",
    adm1: 17,
    adm2: 2,
    lat: -0.2801,
    lon: -76.8914,
    name1: "Orellana",
    name2: "La Joya de los Sachas",
    alerts: 4532
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 6,
    lat: -26.3165,
    lon: -54.4226,
    name1: "Misiones",
    name2: "Eldorado",
    alerts: 4510
  },
  {
    iso: "CHN",
    adm1: 31,
    adm2: 10,
    lat: 27.9025,
    lon: 120.4341,
    name1: "Zhejiang",
    name2: "Wenzhou",
    alerts: 4508
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 115,
    lat: -0.9133,
    lon: -47.3329,
    name1: "Par\xE1",
    name2: "Santar\xE9m Novo",
    alerts: 4486
  },
  {
    iso: "BOL",
    adm1: 3,
    adm2: 1,
    lat: -14.4956,
    lon: -64.4667,
    name1: "Beni",
    name2: "Cercado",
    alerts: 4452
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 8,
    lat: -27.0272,
    lon: -54.267,
    name1: "Misiones",
    name2: "Guaran\xED",
    alerts: 4437
  },
  {
    iso: "IDN",
    adm1: 25,
    adm2: 3,
    lat: -1.4857,
    lon: 119.5072,
    name1: "Sulawesi Barat",
    name2: "Mamuju Utara",
    alerts: 4435
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 9,
    lat: -25.8777,
    lon: -54.4053,
    name1: "Misiones",
    name2: "Iguaz\xFA",
    alerts: 4434
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 5,
    lat: 24.0427,
    lon: 114.9573,
    name1: "Guangdong",
    name2: "Heyuan",
    alerts: 4429
  },
  {
    iso: "IDN",
    adm1: 28,
    adm2: 8,
    lat: -4.2627,
    lon: 122.4208,
    name1: "Sulawesi Tenggara",
    name2: "Konawe Selatan",
    alerts: 4423
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 257,
    lat: -23.7372,
    lon: -46.8588,
    name1: "S\xE3o Paulo",
    name2: "Itapecerica da Serra",
    alerts: 4414
  },
  {
    iso: "PNG",
    adm1: 17,
    adm2: 2,
    lat: -8.5405,
    lon: 147.7743,
    name1: "Oro",
    name2: "Sohe",
    alerts: 4393
  },
  {
    iso: "MMR",
    adm1: 14,
    adm2: 3,
    lat: 12.3747,
    lon: 98.9357,
    name1: "Tanintharyi",
    name2: "Mergui",
    alerts: 4383
  },
  {
    iso: "GIN",
    adm1: 4,
    adm2: 3,
    lat: 10.5813,
    lon: -10.0997,
    name1: "Kankan",
    name2: "Kouroussa",
    alerts: 4369
  },
  {
    iso: "IDN",
    adm1: 13,
    adm2: 10,
    lat: -1.8567,
    lon: 115.4982,
    name1: "Kalimantan Selatan",
    name2: "Tabalong",
    alerts: 4367
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 53,
    lat: -11.111,
    lon: -55.5898,
    name1: "Mato Grosso",
    name2: "Ita\xFAba",
    alerts: 4352
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 48,
    lat: -9.7763,
    lon: -54.6211,
    name1: "Mato Grosso",
    name2: "Guarant\xE3 do Norte",
    alerts: 4350
  },
  {
    iso: "IDN",
    adm1: 30,
    adm2: 13,
    lat: 0.3873,
    lon: 100.0826,
    name1: "Sumatera Barat",
    name2: "Pasaman",
    alerts: 4328
  },
  {
    iso: "CHN",
    adm1: 30,
    adm2: 13,
    lat: 23.6909,
    lon: 104.7148,
    name1: "Yunnan",
    name2: "Wenshan Zhuang and Miao",
    alerts: 4325
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 15,
    lat: 3.9819,
    lon: 97.3574,
    name1: "Aceh",
    name2: "Gayo Lues",
    alerts: 4317
  },
  {
    iso: "CHN",
    adm1: 8,
    adm2: 2,
    lat: 27.0682,
    lon: 105.2144,
    name1: "Guizhou",
    name2: "Bijie",
    alerts: 4312
  },
  {
    iso: "MEX",
    adm1: 28,
    adm2: 26,
    lat: 23.6191,
    lon: -99.7739,
    name1: "Tamaulipas",
    name2: "Miquihuana",
    alerts: 4311
  },
  {
    iso: "CHN",
    adm1: 29,
    adm2: 5,
    lat: 29.5006,
    lon: 95.244,
    name1: "Xizang",
    name2: "Nyingtri",
    alerts: 4298
  },
  {
    iso: "MYS",
    adm1: 1,
    adm2: 4,
    lat: 1.7649,
    lon: 103.9481,
    name1: "Johor",
    name2: "Kota Tinggi",
    alerts: 4242
  },
  {
    iso: "MYS",
    adm1: 1,
    adm2: 3,
    lat: 2.0673,
    lon: 103.3909,
    name1: "Johor",
    name2: "Keluang",
    alerts: 4239
  },
  {
    iso: "PER",
    adm1: 23,
    adm2: 1,
    lat: -7.6395,
    lon: -76.3129,
    name1: "San Mart\xEDn",
    name2: "Bellavista",
    alerts: 4233
  },
  {
    iso: "CHN",
    adm1: 26,
    adm2: 21,
    lat: 29.2866,
    lon: 104.6829,
    name1: "Sichuan",
    name2: "Zigong",
    alerts: 4231
  },
  {
    iso: "CHN",
    adm1: 16,
    adm2: 8,
    lat: 28.7746,
    lon: 117.4692,
    name1: "Jiangxi",
    name2: "Shangrao",
    alerts: 4198
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 16,
    lat: 0.7744,
    lon: 99.3694,
    name1: "Sumatera Utara",
    name2: "Mandailing Natal",
    alerts: 4173
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 104,
    lat: -25.9066,
    lon: -53.1538,
    name1: "Paran\xE1",
    name2: "En\xE9as Marques",
    alerts: 4146
  },
  {
    iso: "CHN",
    adm1: 26,
    adm2: 9,
    lat: 27.8871,
    lon: 102.076,
    name1: "Sichuan",
    name2: "Liangshan Yi",
    alerts: 4143
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 21,
    lat: 1.791,
    lon: 111.6602,
    name1: "Sarawak",
    name2: "Pakan",
    alerts: 4141
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 12,
    lat: -4.1006,
    lon: 104.0939,
    name1: "Sumatera Selatan",
    name2: "Ogan Komering Ulu",
    alerts: 4137
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 375,
    lat: -24.25,
    lon: -50.5225,
    name1: "Paran\xE1",
    name2: "Tel\xEAmaco Borba",
    alerts: 4124
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 15,
    lat: 3.7109,
    lon: 98.2214,
    name1: "Sumatera Utara",
    name2: "Langkat",
    alerts: 4118
  },
  {
    iso: "BOL",
    adm1: 8,
    adm2: 5,
    lat: -18.1207,
    lon: -63.9142,
    name1: "Santa Cruz",
    name2: "Florida",
    alerts: 4116
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 43,
    lat: 25.6123,
    lon: -80.5658,
    name1: "Florida",
    name2: "Miami-Dade",
    alerts: 4095
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 1,
    lat: 29.2987,
    lon: 111.5226,
    name1: "Hunan",
    name2: "Changde",
    alerts: 4077
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 75,
    lat: -25.2463,
    lon: -53.7618,
    name1: "Paran\xE1",
    name2: "C\xE9u Azul",
    alerts: 4043
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 154,
    lat: -26.8035,
    lon: -51.3531,
    name1: "Santa Catarina",
    name2: "Macieira",
    alerts: 4029
  },
  {
    iso: "AGO",
    adm1: 12,
    adm2: 6,
    lat: -8.1278,
    lon: 19.6895,
    name1: "Lunda Norte",
    name2: "Cuilo",
    alerts: 3996
  },
  {
    iso: "PNG",
    adm1: 11,
    adm2: 3,
    lat: -4.9207,
    lon: 144.7108,
    name1: "Madang",
    name2: "Middle Ramu",
    alerts: 3992
  },
  {
    iso: "PNG",
    adm1: 20,
    adm2: 1,
    lat: -5.8667,
    lon: 149.4874,
    name1: "West New Britain",
    name2: "Kandrian-Gloucester",
    alerts: 3977
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 14,
    lat: 29.6094,
    lon: -83.1589,
    name1: "Florida",
    name2: "Dixie",
    alerts: 3966
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 8,
    lat: -3.41,
    lon: 104.6009,
    name1: "Sumatera Selatan",
    name2: "Ogan Ilir",
    alerts: 3938
  },
  {
    iso: "PER",
    adm1: 23,
    adm2: 4,
    lat: -6.2675,
    lon: -76.4396,
    name1: "San Mart\xEDn",
    name2: "Lamas",
    alerts: 3926
  },
  {
    iso: "IDN",
    adm1: 30,
    adm2: 7,
    lat: 0.0238,
    lon: 100.5671,
    name1: "Sumatera Barat",
    name2: "Lima Puluh Kota",
    alerts: 3917
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 14,
    lat: -4.9576,
    lon: -59.5347,
    name1: "Amazonas",
    name2: "Borba",
    alerts: 3904
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 30,
    lat: 1.2552,
    lon: 111.5093,
    name1: "Sarawak",
    name2: "Sri Aman",
    alerts: 3899
  },
  {
    iso: "PER",
    adm1: 17,
    adm2: 4,
    lat: -2.85,
    lon: -73.4814,
    name1: "Loreto",
    name2: "Maynas",
    alerts: 3834
  },
  {
    iso: "PNG",
    adm1: 4,
    adm2: 1,
    lat: -4.4772,
    lon: 151.8237,
    name1: "East New Britain",
    name2: "Gazelle",
    alerts: 3822
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 12,
    lat: 2.4063,
    lon: 99.7525,
    name1: "Sumatera Utara",
    name2: "Labuhanbatu Utara",
    alerts: 3789
  },
  {
    iso: "PER",
    adm1: 17,
    adm2: 5,
    lat: -6.0694,
    lon: -74.2548,
    name1: "Loreto",
    name2: "Requena",
    alerts: 3788
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 14,
    lat: -3.6672,
    lon: -56.0163,
    name1: "Par\xE1",
    name2: "Aveiro",
    alerts: 3761
  },
  {
    iso: "IDN",
    adm1: 6,
    adm2: 7,
    lat: 0.688,
    lon: 121.7111,
    name1: "Gorontalo",
    name2: "Pohuwato",
    alerts: 3739
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 12,
    lat: -3.9857,
    lon: 138.271,
    name1: "Papua",
    name2: "Lanny Jaya",
    alerts: 3737
  },
  {
    iso: "MMR",
    adm1: 14,
    adm2: 2,
    lat: 10.991,
    lon: 98.7702,
    name1: "Tanintharyi",
    name2: "Kawthoung",
    alerts: 3717
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 4,
    lat: 26.793,
    lon: 112.573,
    name1: "Hunan",
    name2: "Hengyang",
    alerts: 3713
  },
  {
    iso: "PER",
    adm1: 18,
    adm2: 2,
    lat: -11.1455,
    lon: -70.3372,
    name1: "Madre de Dios",
    name2: "Tahuamanu",
    alerts: 3696
  },
  {
    iso: "CHN",
    adm1: 30,
    adm2: 6,
    lat: 23.4394,
    lon: 103.0367,
    name1: "Yunnan",
    name2: "Honghe Hani and Yi",
    alerts: 3683
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 96,
    lat: -2.5654,
    lon: -50.9556,
    name1: "Par\xE1",
    name2: "Portel",
    alerts: 3682
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 13,
    lat: 2.2787,
    lon: 100.0564,
    name1: "Sumatera Utara",
    name2: "Labuhanbatu",
    alerts: 3668
  },
  {
    iso: "CHN",
    adm1: 30,
    adm2: 12,
    lat: 25.7041,
    lon: 103.9376,
    name1: "Yunnan",
    name2: "Qujing",
    alerts: 3648
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 14,
    lat: -27.1736,
    lon: -55.3378,
    name1: "Misiones",
    name2: "San Ignacio",
    alerts: 3627
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 14,
    lat: 5.0935,
    lon: 96.6092,
    name1: "Aceh",
    name2: "Bireuen",
    alerts: 3608
  },
  {
    iso: "COD",
    adm1: 25,
    adm2: 5,
    lat: 0.5299,
    lon: 24.2031,
    name1: "Tshopo",
    name2: "Isangi",
    alerts: 3605
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 12,
    lat: 29.0652,
    lon: 113.2541,
    name1: "Hunan",
    name2: "Yueyang",
    alerts: 3595
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 51,
    lat: -26.763,
    lon: -51.0847,
    name1: "Santa Catarina",
    name2: "Ca\xE7ador",
    alerts: 3591
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 94,
    lat: -3.8898,
    lon: -54.5007,
    name1: "Par\xE1",
    name2: "Placas",
    alerts: 3578
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 3,
    lat: 5.3805,
    lon: 95.515,
    name1: "Aceh",
    name2: "Aceh Besar",
    alerts: 3575
  },
  {
    iso: "PER",
    adm1: 23,
    adm2: 6,
    lat: -5.8659,
    lon: -77.1507,
    name1: "San Mart\xEDn",
    name2: "Moyobamba",
    alerts: 3564
  },
  {
    iso: "CHN",
    adm1: 16,
    adm2: 2,
    lat: 25.7075,
    lon: 115.2715,
    name1: "Jiangxi",
    name2: "Ganzhou",
    alerts: 3559
  },
  {
    iso: "MYS",
    adm1: 8,
    adm2: 2,
    lat: 3.1671,
    lon: 102.5574,
    name1: "Pahang",
    name2: "Bera",
    alerts: 3556
  },
  {
    iso: "MDG",
    adm1: 5,
    adm2: 1,
    lat: -17.9235,
    lon: 48.3211,
    name1: "Toamasina",
    name2: "Alaotra-Mangoro",
    alerts: 3544
  },
  {
    iso: "ECU",
    adm1: 22,
    adm2: 6,
    lat: -0.3143,
    lon: -76.4183,
    name1: "Sucumbios",
    name2: "Shushufindi",
    alerts: 3526
  },
  {
    iso: "IDN",
    adm1: 30,
    adm2: 18,
    lat: -1.4064,
    lon: 101.2526,
    name1: "Sumatera Barat",
    name2: "Solok Selatan",
    alerts: 3525
  },
  {
    iso: "MYS",
    adm1: 8,
    adm2: 4,
    lat: 4.2573,
    lon: 102.5558,
    name1: "Pahang",
    name2: "Jerantut",
    alerts: 3470
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 4,
    lat: 23.3542,
    lon: 113.536,
    name1: "Guangdong",
    name2: "Guangzhou",
    alerts: 3444
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 10,
    lat: 1.8417,
    lon: 113.5126,
    name1: "Sarawak",
    name2: "Kapit",
    alerts: 3425
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 54,
    lat: -17.3339,
    lon: -54.5973,
    name1: "Mato Grosso",
    name2: "Itiquira",
    alerts: 3423
  },
  {
    iso: "IDN",
    adm1: 30,
    adm2: 17,
    lat: -0.6844,
    lon: 101.0897,
    name1: "Sumatera Barat",
    name2: "Sijunjung",
    alerts: 3417
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 2,
    lat: -24.7835,
    lon: -48.8212,
    name1: "Paran\xE1",
    name2: "Adrian\xF3polis",
    alerts: 3410
  },
  {
    iso: "GIN",
    adm1: 4,
    adm2: 4,
    lat: 10.758,
    lon: -8.6186,
    name1: "Kankan",
    name2: "Mandiana",
    alerts: 3394
  },
  {
    iso: "IDN",
    adm1: 17,
    adm2: 2,
    lat: -5.2169,
    lon: 104.199,
    name1: "Lampung",
    name2: "Lampung Barat",
    alerts: 3384
  },
  {
    iso: "MYS",
    adm1: 13,
    adm2: 11,
    lat: 5.0635,
    lon: 118.3074,
    name1: "Sabah",
    name2: "Lahad Datu",
    alerts: 3377
  },
  {
    iso: "IDN",
    adm1: 18,
    adm2: 6,
    lat: -1.8729,
    lon: 125.1516,
    name1: "Maluku Utara",
    name2: "Kepulauan Sula",
    alerts: 3362
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 18,
    lat: 29.8761,
    lon: -84.8133,
    name1: "Florida",
    name2: "Franklin",
    alerts: 3362
  },
  {
    iso: "MYS",
    adm1: 13,
    adm2: 3,
    lat: 5.2535,
    lon: 116.2352,
    name1: "Sabah",
    name2: "Keningau",
    alerts: 3360
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 11,
    lat: -9.3833,
    lon: -69.8557,
    name1: "Acre",
    name2: "Manoel Urbano",
    alerts: 3329
  },
  {
    iso: "CHN",
    adm1: 8,
    adm2: 5,
    lat: 26.5037,
    lon: 108.5571,
    name1: "Guizhou",
    name2: "Qiandongnan Miao and Dong",
    alerts: 3314
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 89,
    lat: -3.2035,
    lon: -47.6032,
    name1: "Par\xE1",
    name2: "Paragominas",
    alerts: 3280
  },
  {
    iso: "PER",
    adm1: 1,
    adm2: 4,
    lat: -4.1733,
    lon: -78.0339,
    name1: "Amazonas",
    name2: "Condorcanqui",
    alerts: 3277
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 13,
    lat: 4.7672,
    lon: 97.0073,
    name1: "Aceh",
    name2: "Bener Meriah",
    alerts: 3271
  },
  {
    iso: "GIN",
    adm1: 8,
    adm2: 1,
    lat: 8.8925,
    lon: -8.3298,
    name1: "Nz\xE9r\xE9kor\xE9",
    name2: "Beyla",
    alerts: 3238
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 103,
    lat: -4.5169,
    lon: -48.46,
    name1: "Par\xE1",
    name2: "Rondon do Par\xE1",
    alerts: 3228
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 23,
    lat: 1.839,
    lon: 111.3015,
    name1: "Sarawak",
    name2: "Saratok",
    alerts: 3227
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 270,
    lat: -28.8074,
    lon: -49.8685,
    name1: "Santa Catarina",
    name2: "Timb\xE9 do Sul",
    alerts: 3224
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 16,
    lat: -12.9266,
    lon: -61.0916,
    name1: "Rond\xF4nia",
    name2: "Corumbiara",
    alerts: 3219
  },
  {
    iso: "CHN",
    adm1: 31,
    adm2: 4,
    lat: 29.1163,
    lon: 119.9537,
    name1: "Zhejiang",
    name2: "Jinhua",
    alerts: 3211
  },
  {
    iso: "IDN",
    adm1: 13,
    adm2: 13,
    lat: -2.9241,
    lon: 115.0473,
    name1: "Kalimantan Selatan",
    name2: "Tapin",
    alerts: 3184
  },
  {
    iso: "IDN",
    adm1: 13,
    adm2: 3,
    lat: -3.3188,
    lon: 115.0771,
    name1: "Kalimantan Selatan",
    name2: "Banjar",
    alerts: 3183
  },
  {
    iso: "CUB",
    adm1: 13,
    adm2: 8,
    lat: 22.5543,
    lon: -83.9413,
    name1: "Pinar del R\xEDo",
    name2: "Minas de Matahambre",
    alerts: 3181
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 69,
    lat: -14.3477,
    lon: -56.7225,
    name1: "Mato Grosso",
    name2: "Nortel\xE2ndia",
    alerts: 3177
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 27,
    lat: -7.1805,
    lon: -71.4129,
    name1: "Amazonas",
    name2: "Ipixuna",
    alerts: 3174
  },
  {
    iso: "BOL",
    adm1: 3,
    adm2: 5,
    lat: -15.453,
    lon: -64.4588,
    name1: "Beni",
    name2: "Marb\xE1n",
    alerts: 3167
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 117,
    lat: -14.4503,
    lon: -57.3467,
    name1: "Mato Grosso",
    name2: "Santo Afonso",
    alerts: 3148
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 147,
    lat: -28.3846,
    lon: -49.4519,
    name1: "Santa Catarina",
    name2: "Lauro M\xFCller",
    alerts: 3147
  },
  {
    iso: "PNG",
    adm1: 22,
    adm2: 3,
    lat: -8.6021,
    lon: 141.994,
    name1: "Western",
    name2: "South Fly",
    alerts: 3142
  },
  {
    iso: "IDN",
    adm1: 19,
    adm2: 10,
    lat: -3.4219,
    lon: 130.4443,
    name1: "Maluku",
    name2: "Seram Bagian Timur",
    alerts: 3103
  },
  {
    iso: "CMR",
    adm1: 2,
    adm2: 4,
    lat: 5.4276,
    lon: 11.9808,
    name1: "Centre",
    name2: "Mbam et Kim",
    alerts: 3103
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 6,
    lat: 1.0028,
    lon: 102.6503,
    name1: "Riau",
    name2: "Kepulauan Meranti",
    alerts: 3092
  },
  {
    iso: "SUR",
    adm1: 1,
    adm2: 4,
    lat: 5.0611,
    lon: -55.3562,
    name1: "Brokopondo",
    name2: "Kwakoegron",
    alerts: 3061
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 25,
    lat: -3.2921,
    lon: -52.6854,
    name1: "Par\xE1",
    name2: "Brasil Novo",
    alerts: 3035
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 260,
    lat: -23.5509,
    lon: -46.9677,
    name1: "S\xE3o Paulo",
    name2: "Itapevi",
    alerts: 3018
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 23,
    lat: 2.7377,
    lon: 97.9363,
    name1: "Aceh",
    name2: "Subulussalam",
    alerts: 2991
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 1,
    lat: 1.4061,
    lon: 101.6633,
    name1: "Riau",
    name2: "Bengkalis",
    alerts: 2972
  },
  {
    iso: "MYS",
    adm1: 8,
    adm2: 8,
    lat: 3.3334,
    lon: 103.1837,
    name1: "Pahang",
    name2: "Pekan",
    alerts: 2970
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 142,
    lat: -27.2509,
    lon: -51.5903,
    name1: "Santa Catarina",
    name2: "Lacerd\xF3polis",
    alerts: 2967
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 9,
    lat: 22.0154,
    lon: 110.9529,
    name1: "Guangdong",
    name2: "Maoming",
    alerts: 2959
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 13,
    lat: -2.4896,
    lon: 137.9691,
    name1: "Papua",
    name2: "Mamberamo Raya",
    alerts: 2958
  },
  {
    iso: "IDN",
    adm1: 27,
    adm2: 10,
    lat: -1.1116,
    lon: 121.5744,
    name1: "Sulawesi Tengah",
    name2: "Tojo Una-Una",
    alerts: 2945
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 6,
    lat: 23.2451,
    lon: 114.4968,
    name1: "Guangdong",
    name2: "Huizhou",
    alerts: 2942
  },
  {
    iso: "CHN",
    adm1: 8,
    adm2: 6,
    lat: 26.0158,
    lon: 107.2515,
    name1: "Guizhou",
    name2: "Qiannan Buyei and Miao",
    alerts: 2941
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 81,
    lat: -4.5028,
    lon: -50.2716,
    name1: "Par\xE1",
    name2: "Novo Repartimento",
    alerts: 2927
  },
  {
    iso: "CHN",
    adm1: 9,
    adm2: 2,
    lat: 19.198,
    lon: 109.7151,
    name1: "Hainan",
    name2: "Hainan",
    alerts: 2920
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 22,
    lat: -7.022,
    lon: -70.2182,
    name1: "Amazonas",
    name2: "Eirunep\xE9",
    alerts: 2891
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 134,
    lat: -2.6361,
    lon: -48.2676,
    name1: "Par\xE1",
    name2: "Tom\xE9-A\xE7u",
    alerts: 2883
  },
  {
    iso: "IDN",
    adm1: 27,
    adm2: 8,
    lat: -1.6808,
    lon: 120.5211,
    name1: "Sulawesi Tengah",
    name2: "Poso",
    alerts: 2854
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 10,
    lat: -4.601,
    lon: 103.9212,
    name1: "Sumatera Selatan",
    name2: "Ogan Komering Ulu Selatan",
    alerts: 2850
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 98,
    lat: -2.1202,
    lon: -53.6602,
    name1: "Par\xE1",
    name2: "Prainha",
    alerts: 2834
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 30,
    lat: -24.4215,
    lon: -48.8174,
    name1: "S\xE3o Paulo",
    name2: "Apia\xED",
    alerts: 2826
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 21,
    lat: -4.5175,
    lon: 140.5327,
    name1: "Papua",
    name2: "Pegunungan Bintang",
    alerts: 2809
  },
  {
    iso: "IDN",
    adm1: 18,
    adm2: 2,
    lat: -0.8236,
    lon: 127.7051,
    name1: "Maluku Utara",
    name2: "Halmahera Selatan",
    alerts: 2798
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 24,
    lat: 2.0529,
    lon: 111.4572,
    name1: "Sarawak",
    name2: "Sarikei",
    alerts: 2792
  },
  {
    iso: "PNG",
    adm1: 13,
    adm2: 1,
    lat: -10.1302,
    lon: 149.9114,
    name1: "Milne Bay",
    name2: "Alotau",
    alerts: 2781
  },
  {
    iso: "PNG",
    adm1: 5,
    adm2: 2,
    lat: -4.5035,
    lon: 143.764,
    name1: "East Sepik",
    name2: "Angoram",
    alerts: 2777
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 1,
    lat: 1.0105,
    lon: 109.5628,
    name1: "Kalimantan Barat",
    name2: "Bengkayang",
    alerts: 2774
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 239,
    lat: -29.4597,
    lon: -51.2155,
    name1: "Rio Grande do Sul",
    name2: "Linha Nova",
    alerts: 2760
  },
  {
    iso: "IDN",
    adm1: 5,
    adm2: 1,
    lat: -4.3487,
    lon: 103.0237,
    name1: "Bengkulu",
    name2: "Bengkulu Selatan",
    alerts: 2715
  },
  {
    iso: "PER",
    adm1: 26,
    adm2: 3,
    lat: -8.7956,
    lon: -75.4394,
    name1: "Ucayali",
    name2: "Padre Abad",
    alerts: 2709
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 83,
    lat: -12.9964,
    lon: -54.7595,
    name1: "Mato Grosso",
    name2: "Nova Ubirat\xE3",
    alerts: 2705
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 22,
    lat: -3.4602,
    lon: 137.8438,
    name1: "Papua",
    name2: "Puncak Jaya",
    alerts: 2693
  },
  {
    iso: "PNG",
    adm1: 4,
    adm2: 3,
    lat: -5.3037,
    lon: 151.5698,
    name1: "East New Britain",
    name2: "Pomio",
    alerts: 2692
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 8,
    lat: -4.0883,
    lon: 138.8693,
    name1: "Papua",
    name2: "Jayawijaya",
    alerts: 2670
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 10,
    lat: -10.4849,
    lon: -63.7997,
    name1: "Rond\xF4nia",
    name2: "Campo Novo de Rond\xF4nia",
    alerts: 2656
  },
  {
    iso: "MEX",
    adm1: 23,
    adm2: 8,
    lat: 18.5109,
    lon: -88.534,
    name1: "Quintana Roo",
    name2: "Oth\xF3n P. Blanco",
    alerts: 2652
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 49,
    lat: -27.347,
    lon: -50.845,
    name1: "Santa Catarina",
    name2: "Brun\xF3polis",
    alerts: 2643
  },
  {
    iso: "MYS",
    adm1: 9,
    adm2: 5,
    lat: 5.0105,
    lon: 100.5624,
    name1: "Perak",
    name2: "Kerian",
    alerts: 2626
  },
  {
    iso: "BOL",
    adm1: 6,
    adm2: 3,
    lat: -11.7142,
    lon: -67.1078,
    name1: "Pando",
    name2: "Madre de Dios",
    alerts: 2615
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 30,
    lat: -23.1609,
    lon: -50.3464,
    name1: "Paran\xE1",
    name2: "Bandeirantes",
    alerts: 2608
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 41,
    lat: -4.9848,
    lon: -58.0609,
    name1: "Amazonas",
    name2: "Mau\xE9s",
    alerts: 2607
  },
  {
    iso: "SUR",
    adm1: 1,
    adm2: 1,
    lat: 4.9895,
    lon: -55.185,
    name1: "Brokopondo",
    name2: "Brownsweg",
    alerts: 2598
  },
  {
    iso: "COD",
    adm1: 1,
    adm2: 5,
    lat: 4.1943,
    lon: 24.1,
    name1: "Bas-Uele",
    name2: "Bondo",
    alerts: 2597
  },
  {
    iso: "PNG",
    adm1: 11,
    adm2: 6,
    lat: -5.5128,
    lon: 145.2068,
    name1: "Madang",
    name2: "Usino-Bundi",
    alerts: 2593
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 29,
    lat: 2.9768,
    lon: 99.0331,
    name1: "Sumatera Utara",
    name2: "Simalungun",
    alerts: 2580
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 248,
    lat: -29.2056,
    lon: -49.8135,
    name1: "Santa Catarina",
    name2: "S\xE3o Jo\xE3o do Sul",
    alerts: 2575
  },
  {
    iso: "CHN",
    adm1: 4,
    adm2: 1,
    lat: 26.05,
    lon: 119.1762,
    name1: "Fujian",
    name2: "Fuzhou",
    alerts: 2562
  },
  {
    iso: "CHN",
    adm1: 26,
    adm2: 5,
    lat: 30.9124,
    lon: 100.0179,
    name1: "Sichuan",
    name2: "Garz\xEA Tibetan",
    alerts: 2559
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 11,
    lat: -4.6062,
    lon: -61.8044,
    name1: "Amazonas",
    name2: "Beruri",
    alerts: 2539
  },
  {
    iso: "IDN",
    adm1: 27,
    adm2: 2,
    lat: -1.0212,
    lon: 122.5797,
    name1: "Sulawesi Tengah",
    name2: "Banggai",
    alerts: 2528
  },
  {
    iso: "CHN",
    adm1: 16,
    adm2: 3,
    lat: 26.9733,
    lon: 114.8235,
    name1: "Jiangxi",
    name2: "Ji'an",
    alerts: 2523
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 20,
    lat: 5.0681,
    lon: 96.0072,
    name1: "Aceh",
    name2: "Pidie",
    alerts: 2520
  },
  {
    iso: "IDN",
    adm1: 27,
    adm2: 9,
    lat: -1.4085,
    lon: 119.9704,
    name1: "Sulawesi Tengah",
    name2: "Sigi",
    alerts: 2519
  },
  {
    iso: "CHN",
    adm1: 8,
    adm2: 7,
    lat: 25.2662,
    lon: 105.4643,
    name1: "Guizhou",
    name2: "Qianxinan Buyei and Miao",
    alerts: 2517
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 16,
    lat: -26.6741,
    lon: -53.9499,
    name1: "Misiones",
    name2: "San Pedro",
    alerts: 2508
  },
  {
    iso: "BOL",
    adm1: 3,
    adm2: 7,
    lat: -11.4544,
    lon: -65.8786,
    name1: "Beni",
    name2: "Vaca Diez",
    alerts: 2504
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 10,
    lat: 24.202,
    lon: 116.0787,
    name1: "Guangdong",
    name2: "Meizhou",
    alerts: 2502
  },
  {
    iso: "GTM",
    adm1: 12,
    adm2: 6,
    lat: 17.4821,
    lon: -90.3983,
    name1: "Pet\xE9n",
    name2: "San Andr\xE9s",
    alerts: 2498
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 4,
    lat: 29.9503,
    lon: -82.1678,
    name1: "Florida",
    name2: "Bradford",
    alerts: 2497
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 77,
    lat: -12.7839,
    lon: -57.2203,
    name1: "Mato Grosso",
    name2: "Nova Maring\xE1",
    alerts: 2481
  },
  {
    iso: "PER",
    adm1: 17,
    adm2: 3,
    lat: -3.8891,
    lon: -71.7407,
    name1: "Loreto",
    name2: "Mariscal Ram\xF3n Castilla",
    alerts: 2481
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 42,
    lat: -26.1622,
    lon: -53.5518,
    name1: "Paran\xE1",
    name2: "Bom Jesus do Sul",
    alerts: 2470
  },
  {
    iso: "IDN",
    adm1: 18,
    adm2: 4,
    lat: 1.0312,
    lon: 128.3086,
    name1: "Maluku Utara",
    name2: "Halmahera Timur",
    alerts: 2450
  },
  {
    iso: "PNG",
    adm1: 2,
    adm2: 3,
    lat: -8.9681,
    lon: 147.0698,
    name1: "Central",
    name2: "Kairuku-Hiri",
    alerts: 2441
  },
  {
    iso: "COD",
    adm1: 18,
    adm2: 4,
    lat: 2.4347,
    lon: 20.9797,
    name1: "Mongala",
    name2: "Lisala",
    alerts: 2438
  },
  {
    iso: "IDN",
    adm1: 18,
    adm2: 5,
    lat: 1.5423,
    lon: 127.8311,
    name1: "Maluku Utara",
    name2: "Halmahera Utara",
    alerts: 2425
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 3,
    lat: -10.7495,
    lon: -69.2066,
    name1: "Acre",
    name2: "Brasil\xE9ia",
    alerts: 2414
  },
  {
    iso: "SUR",
    adm1: 9,
    adm2: 4,
    lat: 4.4604,
    lon: -56.3273,
    name1: "Sipaliwini",
    name2: "Coppename",
    alerts: 2410
  },
  {
    iso: "BOL",
    adm1: 2,
    adm2: 5,
    lat: -18.3183,
    lon: -64.8708,
    name1: "Cochabamba",
    name2: "Campero",
    alerts: 2409
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 12,
    lat: -26.656,
    lon: -54.5614,
    name1: "Misiones",
    name2: "Montecarlo",
    alerts: 2394
  },
  {
    iso: "PRY",
    adm1: 18,
    adm2: 12,
    lat: -23.5397,
    lon: -56.6159,
    name1: "San Pedro",
    name2: "Tacuat\xED",
    alerts: 2393
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 17,
    lat: -25.96,
    lon: -50.1284,
    name1: "Paran\xE1",
    name2: "Ant\xF4nio Olinto",
    alerts: 2388
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 24,
    lat: -10.4622,
    lon: -61.7573,
    name1: "Rond\xF4nia",
    name2: "Ji-Paran\xE1",
    alerts: 2378
  },
  {
    iso: "PER",
    adm1: 17,
    adm2: 2,
    lat: -3.8591,
    lon: -75.2294,
    name1: "Loreto",
    name2: "Loreto",
    alerts: 2367
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 40,
    lat: -2.4268,
    lon: -65.0396,
    name1: "Amazonas",
    name2: "Mara\xE3",
    alerts: 2364
  },
  {
    iso: "PNG",
    adm1: 16,
    adm2: 2,
    lat: -3.8981,
    lon: 152.5568,
    name1: "New Ireland",
    name2: "Namatanai",
    alerts: 2362
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 5,
    lat: -26.7645,
    lon: -51.6124,
    name1: "Santa Catarina",
    name2: "\xC1gua Doce",
    alerts: 2347
  },
  {
    iso: "PNG",
    adm1: 16,
    adm2: 1,
    lat: -2.5262,
    lon: 150.5348,
    name1: "New Ireland",
    name2: "Kavieng",
    alerts: 2335
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 22,
    lat: 1.141,
    lon: 99.8429,
    name1: "Sumatera Utara",
    name2: "Padang Lawas",
    alerts: 2325
  },
  {
    iso: "IDN",
    adm1: 13,
    adm2: 5,
    lat: -3.0902,
    lon: 114.6256,
    name1: "Kalimantan Selatan",
    name2: "Barito Kuala",
    alerts: 2320
  },
  {
    iso: "CHN",
    adm1: 31,
    adm2: 1,
    lat: 29.9047,
    lon: 119.4876,
    name1: "Zhejiang",
    name2: "Hangzhou",
    alerts: 2317
  },
  {
    iso: "PNG",
    adm1: 18,
    adm2: 4,
    lat: -3.4291,
    lon: 141.3468,
    name1: "Sandaun",
    name2: "Vanimo-Green River",
    alerts: 2310
  },
  {
    iso: "GIN",
    adm1: 3,
    adm2: 3,
    lat: 9.9902,
    lon: -10.7383,
    name1: "Faranah",
    name2: "Faranah",
    alerts: 2296
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 11,
    lat: 1.8384,
    lon: 100.1322,
    name1: "Sumatera Utara",
    name2: "Labuhanbatu Selatan",
    alerts: 2294
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 75,
    lat: -1.0744,
    lon: -54.353,
    name1: "Par\xE1",
    name2: "Monte Alegre",
    alerts: 2288
  },
  {
    iso: "PER",
    adm1: 23,
    adm2: 3,
    lat: -6.7667,
    lon: -76.8943,
    name1: "San Mart\xEDn",
    name2: "Huallaga",
    alerts: 2280
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 380,
    lat: -25.9015,
    lon: -49.1122,
    name1: "Paran\xE1",
    name2: "Tijucas do Sul",
    alerts: 2272
  },
  {
    iso: "IDN",
    adm1: 16,
    adm2: 2,
    lat: 1.0119,
    lon: 104.7033,
    name1: "Kepulauan Riau",
    name2: "Bintan",
    alerts: 2270
  },
  {
    iso: "CHN",
    adm1: 7,
    adm2: 6,
    lat: 25.3534,
    lon: 110.5121,
    name1: "Guangxi",
    name2: "Guilin",
    alerts: 2263
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 123,
    lat: -10.6965,
    lon: -52.6169,
    name1: "Mato Grosso",
    name2: "S\xE3o Jos\xE9 do Xingu",
    alerts: 2261
  },
  {
    iso: "PNG",
    adm1: 10,
    adm2: 2,
    lat: -5.5462,
    lon: 144.6243,
    name1: "Jiwaka",
    name2: "Jimi",
    alerts: 2247
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 20,
    lat: -10.748,
    lon: -63.184,
    name1: "Rond\xF4nia",
    name2: "Governador Jorge Teixeira",
    alerts: 2243
  },
  {
    iso: "MYS",
    adm1: 9,
    adm2: 2,
    lat: 3.9336,
    lon: 101.0252,
    name1: "Perak",
    name2: "Hilir Perak",
    alerts: 2242
  },
  {
    iso: "IDN",
    adm1: 5,
    adm2: 2,
    lat: -3.6875,
    lon: 102.4117,
    name1: "Bengkulu",
    name2: "Bengkulu Tengah",
    alerts: 2224
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 1,
    lat: -5.3691,
    lon: 138.5604,
    name1: "Papua",
    name2: "Asmat",
    alerts: 2218
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 30,
    lat: 1.4912,
    lon: 99.2578,
    name1: "Sumatera Utara",
    name2: "Tapanuli Selatan",
    alerts: 2217
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 18,
    lat: -3.7705,
    lon: -60.1833,
    name1: "Amazonas",
    name2: "Careiro",
    alerts: 2217
  },
  {
    iso: "CHN",
    adm1: 4,
    adm2: 6,
    lat: 25.2115,
    lon: 118.2552,
    name1: "Fujian",
    name2: "Quanzhou",
    alerts: 2215
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 126,
    lat: -24.1603,
    lon: -51.908,
    name1: "Paran\xE1",
    name2: "Godoy Moreira",
    alerts: 2213
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 9,
    lat: 1.9648,
    lon: 112.2295,
    name1: "Sarawak",
    name2: "Kanowit",
    alerts: 2188
  },
  {
    iso: "MEX",
    adm1: 2,
    adm2: 1,
    lat: 25.5325,
    lon: -111.8085,
    name1: "Baja California Sur",
    name2: "Comond\xFA",
    alerts: 2187
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 54,
    lat: 29.6104,
    lon: -81.7439,
    name1: "Florida",
    name2: "Putnam",
    alerts: 2184
  },
  {
    iso: "SUR",
    adm1: 9,
    adm2: 5,
    lat: 4.396,
    lon: -57.2329,
    name1: "Sipaliwini",
    name2: "Kabalebo",
    alerts: 2178
  },
  {
    iso: "CHN",
    adm1: 30,
    adm2: 7,
    lat: 25.3871,
    lon: 102.8751,
    name1: "Yunnan",
    name2: "Kunming",
    alerts: 2161
  },
  {
    iso: "CHN",
    adm1: 31,
    adm2: 9,
    lat: 28.7761,
    lon: 121.0991,
    name1: "Zhejiang",
    name2: "Taizhou",
    alerts: 2160
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 19,
    lat: -3.286,
    lon: -59.5987,
    name1: "Amazonas",
    name2: "Careiro da V\xE1rzea",
    alerts: 2159
  },
  {
    iso: "MYS",
    adm1: 9,
    adm2: 9,
    lat: 4.3271,
    lon: 100.7072,
    name1: "Perak",
    name2: "Manjung",
    alerts: 2152
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 92,
    lat: -23.752,
    lon: -48.5753,
    name1: "S\xE3o Paulo",
    name2: "Buri",
    alerts: 2152
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 312,
    lat: -26.0948,
    lon: -49.6835,
    name1: "Paran\xE1",
    name2: "Rio Negro",
    alerts: 2148
  },
  {
    iso: "PNG",
    adm1: 2,
    adm2: 2,
    lat: -8.3647,
    lon: 147.0051,
    name1: "Central",
    name2: "Goilala",
    alerts: 2141
  },
  {
    iso: "BRA",
    adm1: 23,
    adm2: 7,
    lat: 0.915,
    lon: -59.3384,
    name1: "Roraima",
    name2: "Caroebe",
    alerts: 2125
  },
  {
    iso: "VUT",
    adm1: 5,
    adm2: 1,
    lat: -20.1908,
    lon: 169.8216,
    name1: "Tafea",
    name2: "Aneityum",
    alerts: 2124
  },
  {
    iso: "PER",
    adm1: 23,
    adm2: 9,
    lat: -6.4953,
    lon: -75.9027,
    name1: "San Mart\xEDn",
    name2: "San Mart\xEDn",
    alerts: 2118
  },
  {
    iso: "MYS",
    adm1: 2,
    adm2: 4,
    lat: 5.6952,
    lon: 100.5121,
    name1: "Kedah",
    name2: "Kuala Muda",
    alerts: 2109
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 2,
    lat: -1.004,
    lon: 135.8443,
    name1: "Papua",
    name2: "Biak Numfor",
    alerts: 2103
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 64,
    lat: 29.0569,
    lon: -81.184,
    name1: "Florida",
    name2: "Volusia",
    alerts: 2102
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 3,
    lat: -2.0354,
    lon: -48.4115,
    name1: "Par\xE1",
    name2: "Acar\xE1",
    alerts: 2092
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 7,
    lat: 0.5166,
    lon: 109.7712,
    name1: "Kalimantan Barat",
    name2: "Landak",
    alerts: 2091
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 266,
    lat: -27.0785,
    lon: -50.0924,
    name1: "Santa Catarina",
    name2: "Tai\xF3",
    alerts: 2083
  },
  {
    iso: "MYS",
    adm1: 3,
    adm2: 2,
    lat: 4.9455,
    lon: 101.9618,
    name1: "Kelantan",
    name2: "Gua Musang",
    alerts: 2082
  },
  {
    iso: "IDN",
    adm1: 27,
    adm2: 7,
    lat: 0.0157,
    lon: 120.4511,
    name1: "Sulawesi Tengah",
    name2: "Parigi Moutong",
    alerts: 2062
  },
  {
    iso: "COL",
    adm1: 22,
    adm2: 37,
    lat: 1.9049,
    lon: -78.0472,
    name1: "Nari\xF1o",
    name2: "Mag\xFC\xED",
    alerts: 2054
  },
  {
    iso: "BOL",
    adm1: 2,
    adm2: 7,
    lat: -17.2294,
    lon: -64.9071,
    name1: "Cochabamba",
    name2: "Carrasco",
    alerts: 2045
  },
  {
    iso: "MEX",
    adm1: 16,
    adm2: 110,
    lat: 20.1825,
    lon: -102.0115,
    name1: "Michoac\xE1n",
    name2: "Zin\xE1paro",
    alerts: 2037
  },
  {
    iso: "PRY",
    adm1: 17,
    adm2: 4,
    lat: -24.2044,
    lon: -58.7251,
    name1: "Presidente Hayes",
    name2: "Villa Hayes",
    alerts: 2029
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 63,
    lat: -24.6455,
    lon: -51.2533,
    name1: "Paran\xE1",
    name2: "C\xE2ndido de Abreu",
    alerts: 2026
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 12,
    lat: -9.0969,
    lon: -72.5594,
    name1: "Acre",
    name2: "Marechal Thaumaturgo",
    alerts: 2024
  },
  {
    iso: "AUS",
    adm1: 7,
    adm2: 26,
    lat: -25.528,
    lon: 152.6768,
    name1: "Queensland",
    name2: "Fraser Coast",
    alerts: 2021
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 201,
    lat: -26.7282,
    lon: -49.1733,
    name1: "Santa Catarina",
    name2: "Pomerode",
    alerts: 2014
  },
  {
    iso: "BEN",
    adm1: 6,
    adm2: 1,
    lat: 8.9714,
    lon: 1.8606,
    name1: "Donga",
    name2: "Bassila",
    alerts: 2e3
  },
  {
    iso: "MEX",
    adm1: 5,
    adm2: 62,
    lat: 14.8216,
    lon: -92.2017,
    name1: "Chiapas",
    name2: "Metapa",
    alerts: 1979
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 19,
    lat: 5.1247,
    lon: 96.2166,
    name1: "Aceh",
    name2: "Pidie Jaya",
    alerts: 1978
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 251,
    lat: -24.8757,
    lon: -52.2657,
    name1: "Paran\xE1",
    name2: "Palmital",
    alerts: 1974
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 2,
    lat: -3.7519,
    lon: 102.9536,
    name1: "Sumatera Selatan",
    name2: "Empat Lawang",
    alerts: 1970
  },
  {
    iso: "PER",
    adm1: 10,
    adm2: 8,
    lat: -8.7204,
    lon: -76.6918,
    name1: "Hu\xE1nuco",
    name2: "Mara\xF1\xF3n",
    alerts: 1967
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 7,
    lat: 0.285,
    lon: -53.8939,
    name1: "Par\xE1",
    name2: "Almeirim",
    alerts: 1967
  },
  {
    iso: "MYS",
    adm1: 9,
    adm2: 8,
    lat: 4.8082,
    lon: 100.7299,
    name1: "Perak",
    name2: "Larut and Matang",
    alerts: 1960
  },
  {
    iso: "BRA",
    adm1: 10,
    adm2: 59,
    lat: -4.6073,
    lon: -43.886,
    name1: "Maranh\xE3o",
    name2: "Cod\xF3",
    alerts: 1959
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 51,
    lat: -4.0272,
    lon: -49.0057,
    name1: "Par\xE1",
    name2: "Goian\xE9sia do Par\xE1",
    alerts: 1956
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 125,
    lat: -26.3571,
    lon: -50.7607,
    name1: "Santa Catarina",
    name2: "Irine\xF3polis",
    alerts: 1946
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 22,
    lat: -10.5756,
    lon: -68.5044,
    name1: "Acre",
    name2: "Xapuri",
    alerts: 1940
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 391,
    lat: -23.067,
    lon: -52.1091,
    name1: "Paran\xE1",
    name2: "Uniflor",
    alerts: 1919
  },
  {
    iso: "PRY",
    adm1: 1,
    adm2: 2,
    lat: -21.6064,
    lon: -58.8266,
    name1: "Alto Paraguay",
    name2: "La Victoria",
    alerts: 1914
  },
  {
    iso: "IDN",
    adm1: 22,
    adm2: 1,
    lat: -3.0593,
    lon: 132.9121,
    name1: "Papua Barat",
    name2: "Fakfak",
    alerts: 1906
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 153,
    lat: -24.0451,
    lon: -53.7198,
    name1: "Paran\xE1",
    name2: "Ipor\xE3",
    alerts: 1902
  },
  {
    iso: "AUS",
    adm1: 7,
    adm2: 20,
    lat: -14.2358,
    lon: 143.2591,
    name1: "Queensland",
    name2: "Cook",
    alerts: 1900
  },
  {
    iso: "CHN",
    adm1: 7,
    adm2: 5,
    lat: 23.3101,
    lon: 109.9988,
    name1: "Guangxi",
    name2: "Guigang",
    alerts: 1897
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 117,
    lat: -23.0187,
    lon: -49.7919,
    name1: "S\xE3o Paulo",
    name2: "Canitar",
    alerts: 1894
  },
  {
    iso: "IND",
    adm1: 4,
    adm2: 17,
    lat: 26.0684,
    lon: 93.1476,
    name1: "Assam",
    name2: "Karbi Anglong",
    alerts: 1892
  },
  {
    iso: "MYS",
    adm1: 1,
    adm2: 10,
    lat: 2.4803,
    lon: 102.9643,
    name1: "Johor",
    name2: "Segamat",
    alerts: 1888
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 205,
    lat: -27.1748,
    lon: -48.6164,
    name1: "Santa Catarina",
    name2: "Porto Belo",
    alerts: 1884
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 1,
    lat: -9.9336,
    lon: -66.9502,
    name1: "Acre",
    name2: "Acrel\xE2ndia",
    alerts: 1877
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 360,
    lat: -28.0997,
    lon: -54.8317,
    name1: "Rio Grande do Sul",
    name2: "Salvador das Miss\xF5es",
    alerts: 1874
  },
  {
    iso: "GUF",
    adm1: 2,
    adm2: 1,
    lat: 4.8376,
    lon: -54.2702,
    name1: "Saint-Laurent-du-Maroni",
    name2: "Apatou",
    alerts: 1867
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 55,
    lat: -28.3543,
    lon: -53.7518,
    name1: "Rio Grande do Sul",
    name2: "Bozano",
    alerts: 1866
  },
  {
    iso: "MYS",
    adm1: 9,
    adm2: 7,
    lat: 4.8305,
    lon: 101.0944,
    name1: "Perak",
    name2: "Kuala Kangsar",
    alerts: 1853
  },
  {
    iso: "IDN",
    adm1: 26,
    adm2: 10,
    lat: -2.3953,
    lon: 120.1609,
    name1: "Sulawesi Selatan",
    name2: "Luwu Utara",
    alerts: 1845
  },
  {
    iso: "BOL",
    adm1: 8,
    adm2: 3,
    lat: -17.8135,
    lon: -60.7927,
    name1: "Santa Cruz",
    name2: "Chiquitos",
    alerts: 1833
  },
  {
    iso: "CHN",
    adm1: 31,
    adm2: 8,
    lat: 29.713,
    lon: 120.6281,
    name1: "Zhejiang",
    name2: "Shaoxing",
    alerts: 1828
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 6,
    lat: 2.7024,
    lon: 111.9896,
    name1: "Sarawak",
    name2: "Dalat",
    alerts: 1827
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 91,
    lat: -25.5954,
    lon: -53.1298,
    name1: "Paran\xE1",
    name2: "Cruzeiro do Igua\xE7u",
    alerts: 1825
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 35,
    lat: -11.8945,
    lon: -60.8304,
    name1: "Rond\xF4nia",
    name2: "Pimenta Bueno",
    alerts: 1822
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 71,
    lat: -9.7684,
    lon: -58.0769,
    name1: "Mato Grosso",
    name2: "Nova Bandeirantes",
    alerts: 1821
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 33,
    lat: -9.2924,
    lon: -60.3042,
    name1: "Mato Grosso",
    name2: "Colniza",
    alerts: 1819
  },
  {
    iso: "CIV",
    adm1: 8,
    adm2: 3,
    lat: 7.4649,
    lon: -7.8224,
    name1: "Montagnes",
    name2: "Tonkpi",
    alerts: 1810
  },
  {
    iso: "GAB",
    adm1: 8,
    adm2: 2,
    lat: -1.6489,
    lon: 9.5814,
    name1: "Ogoou\xE9-Maritime",
    name2: "\xC9timbou\xE9",
    alerts: 1802
  },
  {
    iso: "CHN",
    adm1: 8,
    adm2: 3,
    lat: 26.8434,
    lon: 106.7078,
    name1: "Guizhou",
    name2: "Guiyang",
    alerts: 1789
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 21,
    lat: 1.5933,
    lon: 99.7574,
    name1: "Sumatera Utara",
    name2: "Padang Lawas Utara",
    alerts: 1788
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 84,
    lat: 0.2541,
    lon: -57.1497,
    name1: "Par\xE1",
    name2: "Oriximin\xE1",
    alerts: 1784
  },
  {
    iso: "BRA",
    adm1: 10,
    adm2: 165,
    lat: -2.5399,
    lon: -45.7723,
    name1: "Maranh\xE3o",
    name2: "Santa Luzia do Paru\xE1",
    alerts: 1783
  },
  {
    iso: "COL",
    adm1: 32,
    adm2: 4,
    lat: 1.6862,
    lon: -70.7055,
    name1: "Vaup\xE9s",
    name2: "Papunahua",
    alerts: 1778
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 7,
    lat: 4.2314,
    lon: 97.977,
    name1: "Aceh",
    name2: "Aceh Tamiang",
    alerts: 1777
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 14,
    lat: -3.6086,
    lon: 139.0277,
    name1: "Papua",
    name2: "Mamberamo Tengah",
    alerts: 1774
  },
  {
    iso: "THA",
    adm1: 33,
    adm2: 10,
    lat: 6.1048,
    lon: 101.894,
    name1: "Narathiwat",
    name2: "Sungai Padi",
    alerts: 1773
  },
  {
    iso: "PER",
    adm1: 10,
    adm2: 7,
    lat: -9.0583,
    lon: -76.0296,
    name1: "Hu\xE1nuco",
    name2: "Leoncio Prado",
    alerts: 1767
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 11,
    lat: -4.069,
    lon: 104.568,
    name1: "Sumatera Selatan",
    name2: "Ogan Komering Ulu Timur",
    alerts: 1764
  },
  {
    iso: "BRA",
    adm1: 23,
    adm2: 13,
    lat: 0.6358,
    lon: -59.845,
    name1: "Roraima",
    name2: "S\xE3o Jo\xE3o da Baliza",
    alerts: 1760
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 27,
    lat: -3.7335,
    lon: -49.3712,
    name1: "Par\xE1",
    name2: "Breu Branco",
    alerts: 1757
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 115,
    lat: -13.833,
    lon: -55.2941,
    name1: "Mato Grosso",
    name2: "Santa Rita do Trivelato",
    alerts: 1757
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 6,
    lat: 2.3506,
    lon: 97.8458,
    name1: "Aceh",
    name2: "Aceh Singkil",
    alerts: 1756
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 41,
    lat: -8.4582,
    lon: -51.221,
    name1: "Par\xE1",
    name2: "Cumaru do Norte",
    alerts: 1756
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 29,
    lat: -3.8139,
    lon: 139.4586,
    name1: "Papua",
    name2: "Yalimo",
    alerts: 1755
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 41,
    lat: 29.211,
    lon: -82.0549,
    name1: "Florida",
    name2: "Marion",
    alerts: 1751
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 72,
    lat: -25.2414,
    lon: -53.1557,
    name1: "Paran\xE1",
    name2: "Catanduvas",
    alerts: 1744
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 24,
    lat: -2.4719,
    lon: 139.0032,
    name1: "Papua",
    name2: "Sarmi",
    alerts: 1742
  },
  {
    iso: "IDN",
    adm1: 5,
    adm2: 7,
    lat: -3.0904,
    lon: 102.2493,
    name1: "Bengkulu",
    name2: "Lebong",
    alerts: 1740
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 31,
    lat: 1.8351,
    lon: 98.6611,
    name1: "Sumatera Utara",
    name2: "Tapanuli Tengah",
    alerts: 1733
  },
  {
    iso: "NGA",
    adm1: 35,
    adm2: 4,
    lat: 7.5504,
    lon: 11.4418,
    name1: "Taraba",
    name2: "Gashaka",
    alerts: 1733
  },
  {
    iso: "BOL",
    adm1: 3,
    adm2: 3,
    lat: -13.4167,
    lon: -63.3577,
    name1: "Beni",
    name2: "It\xE9nez",
    alerts: 1732
  },
  {
    iso: "IDN",
    adm1: 22,
    adm2: 2,
    lat: -3.613,
    lon: 134.0236,
    name1: "Papua Barat",
    name2: "Kaimana",
    alerts: 1731
  },
  {
    iso: "PER",
    adm1: 1,
    adm2: 6,
    lat: -6.3767,
    lon: -77.4474,
    name1: "Amazonas",
    name2: "Rodr\xEDguez de Mendoza",
    alerts: 1730
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 32,
    lat: 1.9884,
    lon: 99.073,
    name1: "Sumatera Utara",
    name2: "Tapanuli Utara",
    alerts: 1728
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 25,
    lat: -7.2298,
    lon: -72.8446,
    name1: "Amazonas",
    name2: "Guajar\xE1",
    alerts: 1726
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 6,
    lat: -0.6047,
    lon: -55.0392,
    name1: "Par\xE1",
    name2: "Alenquer",
    alerts: 1722
  },
  {
    iso: "PNG",
    adm1: 13,
    adm2: 4,
    lat: -10.4956,
    lon: 152.8184,
    name1: "Milne Bay",
    name2: "Samarai-Murua",
    alerts: 1710
  },
  {
    iso: "PNG",
    adm1: 18,
    adm2: 3,
    lat: -4.6762,
    lon: 141.6972,
    name1: "Sandaun",
    name2: "Telefomin",
    alerts: 1707
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 31,
    lat: 2.6452,
    lon: 113.0173,
    name1: "Sarawak",
    name2: "Tatau",
    alerts: 1706
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 33,
    lat: -3.5245,
    lon: -66.3078,
    name1: "Amazonas",
    name2: "Juru\xE1",
    alerts: 1699
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 27,
    lat: 2.3041,
    lon: 111.9005,
    name1: "Sarawak",
    name2: "Sibu",
    alerts: 1699
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 35,
    lat: -10.4042,
    lon: -51.6986,
    name1: "Mato Grosso",
    name2: "Confresa",
    alerts: 1695
  },
  {
    iso: "PNG",
    adm1: 7,
    adm2: 2,
    lat: -5.2593,
    lon: 143.882,
    name1: "Enga",
    name2: "Kompiam-Ambum",
    alerts: 1694
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 7,
    lat: 26.8728,
    lon: 110.9169,
    name1: "Hunan",
    name2: "Shaoyang",
    alerts: 1691
  },
  {
    iso: "BRA",
    adm1: 13,
    adm2: 362,
    lat: -18.5492,
    lon: -41.2446,
    name1: "Minas Gerais",
    name2: "Itabirinha",
    alerts: 1690
  },
  {
    iso: "COD",
    adm1: 7,
    adm2: 6,
    lat: -7.3384,
    lon: 22.5559,
    name1: "Kasa\xEF-Central",
    name2: "Luiza",
    alerts: 1685
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 483,
    lat: -22.0601,
    lon: -48.1853,
    name1: "S\xE3o Paulo",
    name2: "Ribeir\xE3o Bonito",
    alerts: 1672
  },
  {
    iso: "COD",
    adm1: 15,
    adm2: 2,
    lat: -8.4118,
    lon: 22.7981,
    name1: "Lualaba",
    name2: "Kapanga",
    alerts: 1671
  },
  {
    iso: "IDN",
    adm1: 26,
    adm2: 3,
    lat: -4.6951,
    lon: 120.1298,
    name1: "Sulawesi Selatan",
    name2: "Bone",
    alerts: 1670
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 78,
    lat: -26.8792,
    lon: -53.1907,
    name1: "Santa Catarina",
    name2: "Cunha Por\xE3",
    alerts: 1665
  },
  {
    iso: "COD",
    adm1: 22,
    adm2: 9,
    lat: -2.9103,
    lon: 27.5786,
    name1: "Sud-Kivu",
    name2: "Shabunda",
    alerts: 1661
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 18,
    lat: 4.1554,
    lon: 96.5068,
    name1: "Aceh",
    name2: "Nagan Raya",
    alerts: 1661
  },
  {
    iso: "ECU",
    adm1: 22,
    adm2: 1,
    lat: 0.145,
    lon: -77.2325,
    name1: "Sucumbios",
    name2: "Cascales",
    alerts: 1660
  },
  {
    iso: "PNG",
    adm1: 14,
    adm2: 1,
    lat: -7.4383,
    lon: 146.8168,
    name1: "Morobe",
    name2: "Bulolo",
    alerts: 1656
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 20,
    lat: -4.4364,
    lon: -64.1253,
    name1: "Amazonas",
    name2: "Coari",
    alerts: 1652
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 56,
    lat: -2.8058,
    lon: -47.9215,
    name1: "Par\xE1",
    name2: "Ipixuna do Par\xE1",
    alerts: 1652
  },
  {
    iso: "MYS",
    adm1: 1,
    adm2: 2,
    lat: 1.5184,
    lon: 103.7776,
    name1: "Johor",
    name2: "Johor Baharu",
    alerts: 1647
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 38,
    lat: -27.4335,
    lon: -48.6939,
    name1: "Santa Catarina",
    name2: "Bigua\xE7u",
    alerts: 1646
  },
  {
    iso: "IDN",
    adm1: 30,
    adm2: 1,
    lat: -0.2495,
    lon: 100.1685,
    name1: "Sumatera Barat",
    name2: "Agam",
    alerts: 1643
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 1,
    lat: 29.675,
    lon: -82.3577,
    name1: "Florida",
    name2: "Alachua",
    alerts: 1642
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 370,
    lat: -21.0626,
    lon: -49.516,
    name1: "S\xE3o Paulo",
    name2: "Nova Alian\xE7a",
    alerts: 1638
  },
  {
    iso: "COD",
    adm1: 18,
    adm2: 2,
    lat: 2.551,
    lon: 22.5314,
    name1: "Mongala",
    name2: "Bumba",
    alerts: 1634
  },
  {
    iso: "PNG",
    adm1: 9,
    adm2: 2,
    lat: -5.4327,
    lon: 142.5139,
    name1: "Hela",
    name2: "Koroba-Kopiago",
    alerts: 1632
  },
  {
    iso: "PNG",
    adm1: 2,
    adm2: 4,
    lat: -9.7028,
    lon: 147.8351,
    name1: "Central",
    name2: "Rigo",
    alerts: 1627
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 29,
    lat: -11.6431,
    lon: -62.2802,
    name1: "Rond\xF4nia",
    name2: "Nova Brasil\xE2ndia D'Oeste",
    alerts: 1619
  },
  {
    iso: "PNG",
    adm1: 14,
    adm2: 9,
    lat: -5.9072,
    lon: 147.5432,
    name1: "Morobe",
    name2: "Tewae-Siassi",
    alerts: 1618
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 250,
    lat: -27.5785,
    lon: -48.6563,
    name1: "Santa Catarina",
    name2: "S\xE3o Jos\xE9",
    alerts: 1610
  },
  {
    iso: "HND",
    adm1: 3,
    adm2: 3,
    lat: 15.5613,
    lon: -85.2294,
    name1: "Col\xF3n",
    name2: "Iriona",
    alerts: 1604
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 4,
    lat: 1.5458,
    lon: 111.3865,
    name1: "Sarawak",
    name2: "Betong",
    alerts: 1603
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 284,
    lat: -28.4927,
    lon: -49.3283,
    name1: "Santa Catarina",
    name2: "Urussanga",
    alerts: 1590
  },
  {
    iso: "COD",
    adm1: 25,
    adm2: 3,
    lat: 1.7417,
    lon: 23.9724,
    name1: "Tshopo",
    name2: "Basoko",
    alerts: 1590
  },
  {
    iso: "NCL",
    adm1: 3,
    adm2: 1,
    lat: -21.8688,
    lon: 166.1444,
    name1: "Sud",
    name2: "Boulouparis",
    alerts: 1590
  },
  {
    iso: "CHN",
    adm1: 8,
    adm2: 1,
    lat: 25.9962,
    lon: 105.9514,
    name1: "Guizhou",
    name2: "Anshun",
    alerts: 1587
  },
  {
    iso: "MYS",
    adm1: 2,
    adm2: 6,
    lat: 5.4013,
    lon: 100.6488,
    name1: "Kedah",
    name2: "Kulim",
    alerts: 1584
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 17,
    lat: -27.3781,
    lon: -54.6345,
    name1: "Misiones",
    name2: "Veinticinco de Mayo",
    alerts: 1580
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 289,
    lat: -24.3031,
    lon: -53.1553,
    name1: "Paran\xE1",
    name2: "Quarto Centen\xE1rio",
    alerts: 1577
  },
  {
    iso: "IDN",
    adm1: 13,
    adm2: 1,
    lat: -2.3133,
    lon: 115.6406,
    name1: "Kalimantan Selatan",
    name2: "Balangan",
    alerts: 1575
  },
  {
    iso: "IDN",
    adm1: 17,
    adm2: 7,
    lat: -4.0238,
    lon: 105.3779,
    name1: "Lampung",
    name2: "Mesuji",
    alerts: 1566
  },
  {
    iso: "IDN",
    adm1: 17,
    adm2: 14,
    lat: -4.5124,
    lon: 104.6198,
    name1: "Lampung",
    name2: "Way Kanan",
    alerts: 1565
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 52,
    lat: 0.3656,
    lon: -68.0018,
    name1: "Amazonas",
    name2: "S\xE3o Gabriel da Cachoeira",
    alerts: 1558
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 17,
    lat: 2.6361,
    lon: 111.7043,
    name1: "Sarawak",
    name2: "Matu",
    alerts: 1555
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 97,
    lat: -2.1846,
    lon: -52.5478,
    name1: "Par\xE1",
    name2: "Porto de Moz",
    alerts: 1552
  },
  {
    iso: "GUY",
    adm1: 1,
    adm2: 4,
    lat: 7.3579,
    lon: -59.9701,
    name1: "Barima-Waini",
    name2: "Rest of Region 1",
    alerts: 1549
  },
  {
    iso: "ARG",
    adm1: 7,
    adm2: 24,
    lat: -28.228,
    lon: -56.2344,
    name1: "Corrientes",
    name2: "Santo Tom\xE9",
    alerts: 1540
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 61,
    lat: -24.4438,
    lon: -49.0853,
    name1: "S\xE3o Paulo",
    name2: "Barra do Chap\xE9u",
    alerts: 1535
  },
  {
    iso: "IDN",
    adm1: 8,
    adm2: 4,
    lat: -2.0369,
    lon: 101.4694,
    name1: "Jambi",
    name2: "Kerinci",
    alerts: 1535
  },
  {
    iso: "BOL",
    adm1: 8,
    adm2: 11,
    lat: -16.7281,
    lon: -63.4587,
    name1: "Santa Cruz",
    name2: "Obispo Santist\xE9ban",
    alerts: 1529
  },
  {
    iso: "HND",
    adm1: 3,
    adm2: 9,
    lat: 15.5828,
    lon: -85.96,
    name1: "Col\xF3n",
    name2: "Tocoa",
    alerts: 1529
  },
  {
    iso: "IND",
    adm1: 4,
    adm2: 10,
    lat: 25.3801,
    lon: 93.034,
    name1: "Assam",
    name2: "Dima Hasao",
    alerts: 1528
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 385,
    lat: -23.8792,
    lon: -52.8435,
    name1: "Paran\xE1",
    name2: "Tuneiras do Oeste",
    alerts: 1520
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 34,
    lat: 28.7686,
    lon: -81.7095,
    name1: "Florida",
    name2: "Lake",
    alerts: 1507
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 9,
    lat: -9.2423,
    lon: -71.9058,
    name1: "Acre",
    name2: "Jord\xE3o",
    alerts: 1504
  },
  {
    iso: "PRY",
    adm1: 10,
    adm2: 2,
    lat: -22.7905,
    lon: -57.2674,
    name1: "Concepci\xF3n",
    name2: "Concepci\xF3n",
    alerts: 1503
  },
  {
    iso: "CHN",
    adm1: 8,
    adm2: 4,
    lat: 26.1373,
    lon: 104.8898,
    name1: "Guizhou",
    name2: "Liupanshui",
    alerts: 1498
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 54,
    lat: -28.6617,
    lon: -54.9714,
    name1: "Rio Grande do Sul",
    name2: "Bossoroca",
    alerts: 1497
  },
  {
    iso: "MYS",
    adm1: 2,
    adm2: 1,
    lat: 5.7113,
    lon: 100.8701,
    name1: "Kedah",
    name2: "Baling",
    alerts: 1496
  },
  {
    iso: "COD",
    adm1: 24,
    adm2: 7,
    lat: -7.2118,
    lon: 27.5692,
    name1: "Tanganyika",
    name2: "Manono",
    alerts: 1493
  },
  {
    iso: "BRA",
    adm1: 23,
    adm2: 14,
    lat: 0.8844,
    lon: -60.1518,
    name1: "Roraima",
    name2: "S\xE3o Luiz",
    alerts: 1492
  },
  {
    iso: "MDG",
    adm1: 5,
    adm2: 3,
    lat: -19.1003,
    lon: 48.687,
    name1: "Toamasina",
    name2: "Atsinanana",
    alerts: 1491
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 161,
    lat: -26.6269,
    lon: -48.9882,
    name1: "Santa Catarina",
    name2: "Massaranduba",
    alerts: 1483
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 61,
    lat: -16.1829,
    lon: -54.9042,
    name1: "Mato Grosso",
    name2: "Juscimeira",
    alerts: 1483
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 381,
    lat: -24.7049,
    lon: -53.7779,
    name1: "Paran\xE1",
    name2: "Toledo",
    alerts: 1472
  },
  {
    iso: "GUY",
    adm1: 1,
    adm2: 1,
    lat: 7.8838,
    lon: -59.8101,
    name1: "Barima-Waini",
    name2: "Barima / Amakura",
    alerts: 1463
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 15,
    lat: -3.082,
    lon: -61.8778,
    name1: "Amazonas",
    name2: "Caapiranga",
    alerts: 1457
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 269,
    lat: -24.2927,
    lon: -47.1281,
    name1: "S\xE3o Paulo",
    name2: "Itariri",
    alerts: 1454
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 80,
    lat: -14.1562,
    lon: -51.8899,
    name1: "Mato Grosso",
    name2: "Nova Nazar\xE9",
    alerts: 1447
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 35,
    lat: -8.3756,
    lon: -65.8117,
    name1: "Amazonas",
    name2: "L\xE1brea",
    alerts: 1438
  },
  {
    iso: "ZAF",
    adm1: 4,
    adm2: 10,
    lat: -28.7002,
    lon: 31.514,
    name1: "KwaZulu-Natal",
    name2: "Uthungulu",
    alerts: 1438
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 73,
    lat: -26.8832,
    lon: -52.7251,
    name1: "Santa Catarina",
    name2: "Coronel Freitas",
    alerts: 1436
  },
  {
    iso: "BOL",
    adm1: 6,
    adm2: 2,
    lat: -10.25,
    lon: -65.9628,
    name1: "Pando",
    name2: "Federico Rom\xE1n",
    alerts: 1429
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 10,
    lat: -7.5064,
    lon: -73.4362,
    name1: "Acre",
    name2: "M\xE2ncio Lima",
    alerts: 1426
  },
  {
    iso: "MDG",
    adm1: 5,
    adm2: 2,
    lat: -16.3874,
    lon: 49.361,
    name1: "Toamasina",
    name2: "Analanjirofo",
    alerts: 1425
  },
  {
    iso: "COD",
    adm1: 25,
    adm2: 10,
    lat: 0.841,
    lon: 23.1432,
    name1: "Tshopo",
    name2: "Yahuma",
    alerts: 1423
  },
  {
    iso: "MYS",
    adm1: 15,
    adm2: 3,
    lat: 3.5561,
    lon: 101.5851,
    name1: "Selangor",
    name2: "Hulu Selangor",
    alerts: 1417
  },
  {
    iso: "IDN",
    adm1: 22,
    adm2: 9,
    lat: -0.7024,
    lon: 132.4765,
    name1: "Papua Barat",
    name2: "Tambrauw",
    alerts: 1409
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 131,
    lat: -2.9078,
    lon: -48.7367,
    name1: "Par\xE1",
    name2: "Tail\xE2ndia",
    alerts: 1408
  },
  {
    iso: "MYS",
    adm1: 1,
    adm2: 5,
    lat: 1.678,
    lon: 103.5582,
    name1: "Johor",
    name2: "Kulaijaya",
    alerts: 1399
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 167,
    lat: -27.1937,
    lon: -50.9275,
    name1: "Santa Catarina",
    name2: "Monte Carlo",
    alerts: 1397
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 198,
    lat: -24.3611,
    lon: -52.5891,
    name1: "Paran\xE1",
    name2: "Mambor\xEA",
    alerts: 1389
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 9,
    lat: -3.3322,
    lon: 140.6833,
    name1: "Papua",
    name2: "Keerom",
    alerts: 1386
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 42,
    lat: -26.739,
    lon: -52.39,
    name1: "Santa Catarina",
    name2: "Bom Jesus",
    alerts: 1380
  },
  {
    iso: "MYS",
    adm1: 13,
    adm2: 22,
    lat: 4.4739,
    lon: 117.6414,
    name1: "Sabah",
    name2: "Tawau",
    alerts: 1376
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 97,
    lat: -15.5168,
    lon: -59.4577,
    name1: "Mato Grosso",
    name2: "Pontes e Lacerda",
    alerts: 1373
  },
  {
    iso: "PNG",
    adm1: 14,
    adm2: 3,
    lat: -7.1196,
    lon: 146.9373,
    name1: "Morobe",
    name2: "Huon",
    alerts: 1372
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 37,
    lat: 29.3204,
    lon: -82.7418,
    name1: "Florida",
    name2: "Levy",
    alerts: 1365
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 56,
    lat: -4.0288,
    lon: -69.6757,
    name1: "Amazonas",
    name2: "Tabatinga",
    alerts: 1364
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 2,
    lat: 28.2284,
    lon: 113.1525,
    name1: "Hunan",
    name2: "Changsha",
    alerts: 1363
  },
  {
    iso: "ARG",
    adm1: 7,
    adm2: 12,
    lat: -27.912,
    lon: -56.7978,
    name1: "Corrientes",
    name2: "Ituzaing\xF3",
    alerts: 1362
  },
  {
    iso: "COD",
    adm1: 5,
    adm2: 2,
    lat: 4.0734,
    lon: 28.41,
    name1: "Haut-Uele",
    name2: "Dungu",
    alerts: 1358
  },
  {
    iso: "MYS",
    adm1: 3,
    adm2: 5,
    lat: 5.4153,
    lon: 102.1663,
    name1: "Kelantan",
    name2: "Kuala Krai",
    alerts: 1358
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 44,
    lat: -27.7016,
    lon: -53.3415,
    name1: "Rio Grande do Sul",
    name2: "Boa Vista das Miss\xF5es",
    alerts: 1356
  },
  {
    iso: "ARG",
    adm1: 3,
    adm2: 1,
    lat: -25.746,
    lon: -61.9866,
    name1: "Chaco",
    name2: "Almirante Brown",
    alerts: 1355
  },
  {
    iso: "PER",
    adm1: 23,
    adm2: 7,
    lat: -6.9059,
    lon: -76.2513,
    name1: "San Mart\xEDn",
    name2: "Picota",
    alerts: 1343
  },
  {
    iso: "IDN",
    adm1: 18,
    adm2: 1,
    lat: 1.3642,
    lon: 127.5897,
    name1: "Maluku Utara",
    name2: "Halmahera Barat",
    alerts: 1341
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 272,
    lat: -25.4724,
    lon: -49.0523,
    name1: "Paran\xE1",
    name2: "Piraquara",
    alerts: 1340
  },
  {
    iso: "BRA",
    adm1: 10,
    adm2: 63,
    lat: -4.1022,
    lon: -44.1453,
    name1: "Maranh\xE3o",
    name2: "Coroat\xE1",
    alerts: 1338
  },
  {
    iso: "IDN",
    adm1: 28,
    adm2: 9,
    lat: -3.3851,
    lon: 121.9969,
    name1: "Sulawesi Tenggara",
    name2: "Konawe Utara",
    alerts: 1328
  },
  {
    iso: "ZAF",
    adm1: 6,
    adm2: 1,
    lat: -25.0487,
    lon: 31.2547,
    name1: "Mpumalanga",
    name2: "Ehlanzeni",
    alerts: 1320
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 51,
    lat: -3.0524,
    lon: -69.0873,
    name1: "Amazonas",
    name2: "Santo Ant\xF4nio do I\xE7\xE1",
    alerts: 1319
  },
  {
    iso: "VEN",
    adm1: 6,
    adm2: 5,
    lat: 5.1151,
    lon: -61.7094,
    name1: "Bol\xEDvar",
    name2: "Gran Sabana",
    alerts: 1319
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 29,
    lat: 1.7688,
    lon: 112.5395,
    name1: "Sarawak",
    name2: "Song",
    alerts: 1311
  },
  {
    iso: "IDN",
    adm1: 28,
    adm2: 6,
    lat: -3.2477,
    lon: 121.1563,
    name1: "Sulawesi Tenggara",
    name2: "Kolaka Utara",
    alerts: 1308
  },
  {
    iso: "PNG",
    adm1: 2,
    adm2: 1,
    lat: -10.0485,
    lon: 148.8787,
    name1: "Central",
    name2: "Abau",
    alerts: 1292
  },
  {
    iso: "ECU",
    adm1: 16,
    adm2: 2,
    lat: -1.1641,
    lon: -77.9396,
    name1: "Napo",
    name2: "Carlos Julio Arosemena Tola",
    alerts: 1283
  },
  {
    iso: "PRY",
    adm1: 17,
    adm2: 2,
    lat: -23.1766,
    lon: -58.8258,
    name1: "Presidente Hayes",
    name2: "Pozo Colorado",
    alerts: 1273
  },
  {
    iso: "ECU",
    adm1: 15,
    adm2: 11,
    lat: -2.4754,
    lon: -77.3909,
    name1: "Morona Santiago",
    name2: "Taisha",
    alerts: 1271
  },
  {
    iso: "IDN",
    adm1: 30,
    adm2: 5,
    lat: -1.8485,
    lon: 99.3362,
    name1: "Sumatera Barat",
    name2: "Kepulauan Mentawai",
    alerts: 1267
  },
  {
    iso: "MEX",
    adm1: 4,
    adm2: 6,
    lat: 19.1342,
    lon: -90.4993,
    name1: "Campeche",
    name2: "Champot\xF3n",
    alerts: 1267
  },
  {
    iso: "IND",
    adm1: 3,
    adm2: 7,
    lat: 27.8565,
    lon: 96.2257,
    name1: "Arunachal Pradesh",
    name2: "Lohit",
    alerts: 1265
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 92,
    lat: -10.1495,
    lon: -53.5939,
    name1: "Mato Grosso",
    name2: "Peixoto de Azevedo",
    alerts: 1257
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 16,
    lat: -3.1298,
    lon: -49.699,
    name1: "Par\xE1",
    name2: "Bai\xE3o",
    alerts: 1244
  },
  {
    iso: "GUY",
    adm1: 8,
    adm2: 8,
    lat: 4.7755,
    lon: -59.2218,
    name1: "Potaro-Siparuni",
    name2: "Rest of Region 8",
    alerts: 1240
  },
  {
    iso: "CHN",
    adm1: 8,
    adm2: 8,
    lat: 27.9695,
    lon: 108.544,
    name1: "Guizhou",
    name2: "Tongren",
    alerts: 1235
  },
  {
    iso: "COD",
    adm1: 20,
    adm2: 2,
    lat: 3.3931,
    lon: 20.9304,
    name1: "Nord-Ubangi",
    name2: "Businga",
    alerts: 1234
  },
  {
    iso: "CIV",
    adm1: 12,
    adm2: 2,
    lat: 8.363,
    lon: -6.0362,
    name1: "Woroba",
    name2: "B\xE9r\xE9",
    alerts: 1234
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 5,
    lat: -4.0169,
    lon: 135.6676,
    name1: "Papua",
    name2: "Dogiyai",
    alerts: 1233
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 266,
    lat: -20.5951,
    lon: -51.4406,
    name1: "S\xE3o Paulo",
    name2: "Itapura",
    alerts: 1225
  },
  {
    iso: "IDN",
    adm1: 23,
    adm2: 27,
    lat: -2.747,
    lon: 136.7282,
    name1: "Papua",
    name2: "Waropen",
    alerts: 1223
  },
  {
    iso: "MYS",
    adm1: 13,
    adm2: 17,
    lat: 5.9151,
    lon: 116.7825,
    name1: "Sabah",
    name2: "Ranau",
    alerts: 1219
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 15,
    lat: -8.4924,
    lon: -72.7349,
    name1: "Acre",
    name2: "Porto Walter",
    alerts: 1216
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 34,
    lat: -12.2623,
    lon: -61.4154,
    name1: "Rond\xF4nia",
    name2: "Parecis",
    alerts: 1212
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 9,
    lat: 28.85,
    lon: -82.4721,
    name1: "Florida",
    name2: "Citrus",
    alerts: 1206
  },
  {
    iso: "PER",
    adm1: 23,
    adm2: 8,
    lat: -5.8646,
    lon: -77.4611,
    name1: "San Mart\xEDn",
    name2: "Rioja",
    alerts: 1205
  },
  {
    iso: "COL",
    adm1: 8,
    adm2: 3,
    lat: 5.2679,
    lon: -75.4753,
    name1: "Caldas",
    name2: "Aranzaz\xFA",
    alerts: 1204
  },
  {
    iso: "IND",
    adm1: 21,
    adm2: 3,
    lat: 24.3038,
    lon: 93.421,
    name1: "Manipur",
    name2: "Churachandpur",
    alerts: 1201
  },
  {
    iso: "IDN",
    adm1: 28,
    adm2: 2,
    lat: -4.7978,
    lon: 121.8508,
    name1: "Sulawesi Tenggara",
    name2: "Bombana",
    alerts: 1199
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 44,
    lat: 25.4032,
    lon: -81.0666,
    name1: "Florida",
    name2: "Monroe",
    alerts: 1198
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 1,
    lat: -12.47,
    lon: -62.2745,
    name1: "Rond\xF4nia",
    name2: "Alta Floresta D'Oeste",
    alerts: 1190
  },
  {
    iso: "IDN",
    adm1: 16,
    adm2: 1,
    lat: 0.9652,
    lon: 104.0638,
    name1: "Kepulauan Riau",
    name2: "Batam",
    alerts: 1189
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 48,
    lat: -1.1592,
    lon: -57.8059,
    name1: "Par\xE1",
    name2: "Faro",
    alerts: 1187
  },
  {
    iso: "NCL",
    adm1: 3,
    adm2: 8,
    lat: -22.2827,
    lon: 166.7396,
    name1: "Sud",
    name2: "Mont-Dore",
    alerts: 1186
  },
  {
    iso: "MMR",
    adm1: 14,
    adm2: 1,
    lat: 14.0916,
    lon: 98.4687,
    name1: "Tanintharyi",
    name2: "Dawei",
    alerts: 1185
  },
  {
    iso: "COD",
    adm1: 5,
    adm2: 4,
    lat: 3.5339,
    lon: 29.9772,
    name1: "Haut-Uele",
    name2: "Faradje",
    alerts: 1181
  },
  {
    iso: "COD",
    adm1: 15,
    adm2: 6,
    lat: -10.7695,
    lon: 25.055,
    name1: "Lualaba",
    name2: "Mutshatsha",
    alerts: 1178
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 9,
    lat: 3.3694,
    lon: 97.6976,
    name1: "Aceh",
    name2: "Aceh Tenggara",
    alerts: 1176
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 3,
    lat: -10.0558,
    lon: -56.3677,
    name1: "Mato Grosso",
    name2: "Alta Floresta",
    alerts: 1166
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 51,
    lat: -10.3843,
    lon: -62.094,
    name1: "Rond\xF4nia",
    name2: "Vale do Para\xEDso",
    alerts: 1160
  },
  {
    iso: "COD",
    adm1: 7,
    adm2: 5,
    lat: -6.4338,
    lon: 21.9796,
    name1: "Kasa\xEF-Central",
    name2: "Kazumba",
    alerts: 1160
  },
  {
    iso: "BRA",
    adm1: 27,
    adm2: 99,
    lat: -6.7386,
    lon: -48.2394,
    name1: "Tocantins",
    name2: "Piraqu\xEA",
    alerts: 1159
  },
  {
    iso: "PNG",
    adm1: 11,
    adm2: 1,
    lat: -4.3835,
    lon: 144.9229,
    name1: "Madang",
    name2: "Bogia",
    alerts: 1158
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 46,
    lat: -4.1866,
    lon: -47.8981,
    name1: "Par\xE1",
    name2: "Dom Eliseu",
    alerts: 1157
  },
  {
    iso: "MDG",
    adm1: 2,
    adm2: 1,
    lat: -13.3435,
    lon: 48.9252,
    name1: "Antsiranana",
    name2: "Diana",
    alerts: 1156
  },
  {
    iso: "IDN",
    adm1: 28,
    adm2: 11,
    lat: -4.9022,
    lon: 122.5953,
    name1: "Sulawesi Tenggara",
    name2: "Muna",
    alerts: 1156
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 88,
    lat: -25.9925,
    lon: -52.5644,
    name1: "Paran\xE1",
    name2: "Coronel Vivida",
    alerts: 1155
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 58,
    lat: -4.4701,
    lon: -65.5315,
    name1: "Amazonas",
    name2: "Tef\xE9",
    alerts: 1154
  },
  {
    iso: "HND",
    adm1: 15,
    adm2: 7,
    lat: 15.1924,
    lon: -86.0766,
    name1: "Olancho",
    name2: "Gualaco",
    alerts: 1150
  },
  {
    iso: "AUS",
    adm1: 7,
    adm2: 62,
    lat: -26.5927,
    lon: 152.9113,
    name1: "Queensland",
    name2: "Sunshine Coast",
    alerts: 1148
  },
  {
    iso: "BRA",
    adm1: 10,
    adm2: 182,
    lat: -5.0402,
    lon: -43.7666,
    name1: "Maranh\xE3o",
    name2: "S\xE3o Jo\xE3o do Soter",
    alerts: 1145
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 271,
    lat: -24.4747,
    lon: -49.9277,
    name1: "Paran\xE1",
    name2: "Pira\xED do Sul",
    alerts: 1144
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 17,
    lat: -12.147,
    lon: -64.0583,
    name1: "Rond\xF4nia",
    name2: "Costa Marques",
    alerts: 1144
  },
  {
    iso: "BOL",
    adm1: 4,
    adm2: 5,
    lat: -15.6195,
    lon: -69.1389,
    name1: "La Paz",
    name2: "Camacho",
    alerts: 1140
  },
  {
    iso: "COD",
    adm1: 26,
    adm2: 6,
    lat: -1.1866,
    lon: 23.2597,
    name1: "Tshuapa",
    name2: "Ikela",
    alerts: 1140
  },
  {
    iso: "GUF",
    adm1: 2,
    adm2: 4,
    lat: 4.9883,
    lon: -53.6496,
    name1: "Saint-Laurent-du-Maroni",
    name2: "Mana",
    alerts: 1136
  },
  {
    iso: "MDG",
    adm1: 4,
    adm2: 4,
    lat: -15.3429,
    lon: 48.2933,
    name1: "Mahajanga",
    name2: "Sofia",
    alerts: 1133
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 313,
    lat: -23.2713,
    lon: -51.4092,
    name1: "Paran\xE1",
    name2: "Rol\xE2ndia",
    alerts: 1125
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 177,
    lat: -28.2799,
    lon: -49.3719,
    name1: "Santa Catarina",
    name2: "Orleans",
    alerts: 1125
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 47,
    lat: -7.7578,
    lon: -68.1971,
    name1: "Amazonas",
    name2: "Pauini",
    alerts: 1119
  },
  {
    iso: "PRY",
    adm1: 5,
    adm2: 2,
    lat: -21.1429,
    lon: -61.0519,
    name1: "Boquer\xF3n",
    name2: "General Eugenio A. Garay",
    alerts: 1119
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 305,
    lat: -23.2617,
    lon: -49.7628,
    name1: "Paran\xE1",
    name2: "Ribeir\xE3o Claro",
    alerts: 1119
  },
  {
    iso: "CAF",
    adm1: 9,
    adm2: 2,
    lat: 5.0653,
    lon: 23.2158,
    name1: "Mbomou",
    name2: "Bangassou",
    alerts: 1118
  },
  {
    iso: "MYS",
    adm1: 13,
    adm2: 24,
    lat: 5.1249,
    lon: 117.0132,
    name1: "Sabah",
    name2: "Tongod",
    alerts: 1118
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 20,
    lat: -16.5378,
    lon: -57.8341,
    name1: "Mato Grosso",
    name2: "C\xE1ceres",
    alerts: 1114
  },
  {
    iso: "CHN",
    adm1: 4,
    adm2: 5,
    lat: 25.4517,
    lon: 118.89,
    name1: "Fujian",
    name2: "Putian",
    alerts: 1113
  },
  {
    iso: "IDN",
    adm1: 25,
    adm2: 5,
    lat: -3.3122,
    lon: 119.1448,
    name1: "Sulawesi Barat",
    name2: "Polewali Mandar",
    alerts: 1112
  },
  {
    iso: "IDN",
    adm1: 19,
    adm2: 6,
    lat: -3.1609,
    lon: 129.2572,
    name1: "Maluku",
    name2: "Maluku Tengah",
    alerts: 1112
  },
  {
    iso: "MYS",
    adm1: 8,
    adm2: 5,
    lat: 3.8555,
    lon: 103.0912,
    name1: "Pahang",
    name2: "Kuantan",
    alerts: 1105
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 112,
    lat: -15.0509,
    lon: -58.0509,
    name1: "Mato Grosso",
    name2: "Salto do C\xE9u",
    alerts: 1102
  },
  {
    iso: "PNG",
    adm1: 14,
    adm2: 4,
    lat: -6.1625,
    lon: 147.0245,
    name1: "Morobe",
    name2: "Kabwum",
    alerts: 1101
  },
  {
    iso: "MYS",
    adm1: 9,
    adm2: 4,
    lat: 5.2224,
    lon: 100.8181,
    name1: "Perak",
    name2: "Kampar",
    alerts: 1100
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 215,
    lat: -26.9091,
    lon: -51.0634,
    name1: "Santa Catarina",
    name2: "Rio das Antas",
    alerts: 1098
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 355,
    lat: -25.3882,
    lon: -54.2618,
    name1: "Paran\xE1",
    name2: "S\xE3o Miguel do Igua\xE7u",
    alerts: 1097
  },
  {
    iso: "ZAF",
    adm1: 4,
    adm2: 6,
    lat: -29.5093,
    lon: 30.1984,
    name1: "KwaZulu-Natal",
    name2: "Umgungundlovu",
    alerts: 1089
  },
  {
    iso: "IDN",
    adm1: 6,
    adm2: 4,
    lat: 0.8859,
    lon: 122.6614,
    name1: "Gorontalo",
    name2: "Gorontalo Utara",
    alerts: 1089
  },
  {
    iso: "IDN",
    adm1: 19,
    adm2: 4,
    lat: -6.1954,
    lon: 134.4501,
    name1: "Maluku",
    name2: "Kepulauan Aru",
    alerts: 1087
  },
  {
    iso: "COD",
    adm1: 1,
    adm2: 1,
    lat: 2.981,
    lon: 23.7544,
    name1: "Bas-Uele",
    name2: "Aketi",
    alerts: 1084
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 17,
    lat: 29.4614,
    lon: -81.3141,
    name1: "Florida",
    name2: "Flagler",
    alerts: 1083
  },
  {
    iso: "USA",
    adm1: 44,
    adm2: 101,
    lat: 29.8586,
    lon: -95.397,
    name1: "Texas",
    name2: "Harris",
    alerts: 1080
  },
  {
    iso: "PRY",
    adm1: 1,
    adm2: 1,
    lat: -20.4084,
    lon: -58.6647,
    name1: "Alto Paraguay",
    name2: "Fuerte Olimpo",
    alerts: 1072
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 62,
    lat: -29.9209,
    lon: -51.0953,
    name1: "Rio Grande do Sul",
    name2: "Cachoeirinha",
    alerts: 1072
  },
  {
    iso: "TWN",
    adm1: 7,
    adm2: 6,
    lat: 23.7479,
    lon: 121.3813,
    name1: "Taiwan",
    name2: "Hualien",
    alerts: 1072
  },
  {
    iso: "AGO",
    adm1: 12,
    adm2: 1,
    lat: -7.9001,
    lon: 21.4959,
    name1: "Lunda Norte",
    name2: "Cambulo",
    alerts: 1072
  },
  {
    iso: "MYS",
    adm1: 9,
    adm2: 3,
    lat: 5.4339,
    lon: 101.296,
    name1: "Perak",
    name2: "Hulu Perak",
    alerts: 1071
  },
  {
    iso: "FJI",
    adm1: 5,
    adm2: 1,
    lat: -17.6334,
    lon: 177.6598,
    name1: "Western",
    name2: "Ba",
    alerts: 1066
  },
  {
    iso: "PNG",
    adm1: 11,
    adm2: 4,
    lat: -5.7012,
    lon: 146.18,
    name1: "Madang",
    name2: "Rai Coast",
    alerts: 1062
  },
  {
    iso: "COL",
    adm1: 8,
    adm2: 13,
    lat: 5.225,
    lon: -75.2876,
    name1: "Caldas",
    name2: "Marulanda",
    alerts: 1062
  },
  {
    iso: "MDG",
    adm1: 4,
    adm2: 3,
    lat: -17.7142,
    lon: 44.8083,
    name1: "Mahajanga",
    name2: "Melaky",
    alerts: 1061
  },
  {
    iso: "PNG",
    adm1: 18,
    adm2: 1,
    lat: -3.3229,
    lon: 142.1629,
    name1: "Sandaun",
    name2: "Aitape-Lumi",
    alerts: 1060
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 62,
    lat: -2.6229,
    lon: -56.2214,
    name1: "Par\xE1",
    name2: "Juruti",
    alerts: 1058
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 7,
    lat: -10.8891,
    lon: -68.6183,
    name1: "Acre",
    name2: "Epitaciol\xE2ndia",
    alerts: 1057
  },
  {
    iso: "MYS",
    adm1: 9,
    adm2: 1,
    lat: 4.0316,
    lon: 101.3716,
    name1: "Perak",
    name2: "Batang Padang",
    alerts: 1057
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 17,
    lat: -5.1185,
    lon: -67.3375,
    name1: "Amazonas",
    name2: "Carauari",
    alerts: 1054
  },
  {
    iso: "NCL",
    adm1: 3,
    adm2: 14,
    lat: -22.0962,
    lon: 166.7512,
    name1: "Sud",
    name2: "Yat\xE9",
    alerts: 1052
  },
  {
    iso: "MDG",
    adm1: 2,
    adm2: 2,
    lat: -14.3123,
    lon: 49.825,
    name1: "Antsiranana",
    name2: "Sava",
    alerts: 1046
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 56,
    lat: -27.847,
    lon: -50.7711,
    name1: "Santa Catarina",
    name2: "Campo Belo do Sul",
    alerts: 1044
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 46,
    lat: -2.6881,
    lon: -56.8546,
    name1: "Amazonas",
    name2: "Parintins",
    alerts: 1035
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 1,
    lat: -3.6947,
    lon: -65.3244,
    name1: "Amazonas",
    name2: "Alvar\xE3es",
    alerts: 1034
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 2,
    lat: -27.1432,
    lon: -54.8023,
    name1: "Misiones",
    name2: "Caingu\xE1s",
    alerts: 1031
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 299,
    lat: -28.3296,
    lon: -53.5074,
    name1: "Rio Grande do Sul",
    name2: "Panambi",
    alerts: 1030
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 55,
    lat: 29.9002,
    lon: -81.4357,
    name1: "Florida",
    name2: "Saint Johns",
    alerts: 1024
  },
  {
    iso: "COD",
    adm1: 20,
    adm2: 6,
    lat: 3.643,
    lon: 22.2295,
    name1: "Nord-Ubangi",
    name2: "Yakoma",
    alerts: 1022
  },
  {
    iso: "IDN",
    adm1: 25,
    adm2: 2,
    lat: -2.9488,
    lon: 119.3119,
    name1: "Sulawesi Barat",
    name2: "Mamasa",
    alerts: 1022
  },
  {
    iso: "COD",
    adm1: 9,
    adm2: 4,
    lat: -6.5421,
    lon: 20.7175,
    name1: "Kasa\xEF",
    name2: "Kamonia",
    alerts: 1015
  },
  {
    iso: "COD",
    adm1: 15,
    adm2: 1,
    lat: -10.5434,
    lon: 23.2318,
    name1: "Lualaba",
    name2: "Dilolo",
    alerts: 1014
  },
  {
    iso: "COL",
    adm1: 22,
    adm2: 35,
    lat: 1.3987,
    lon: -77.5205,
    name1: "Nari\xF1o",
    name2: "Linares",
    alerts: 1013
  },
  {
    iso: "CHN",
    adm1: 30,
    adm2: 11,
    lat: 23.1881,
    lon: 100.722,
    name1: "Yunnan",
    name2: "Pu'er",
    alerts: 1011
  },
  {
    iso: "BRA",
    adm1: 10,
    adm2: 26,
    lat: -5.5041,
    lon: -45.1831,
    name1: "Maranh\xE3o",
    name2: "Barra do Corda",
    alerts: 1010
  },
  {
    iso: "IDN",
    adm1: 22,
    adm2: 11,
    lat: -2.8228,
    lon: 134.3745,
    name1: "Papua Barat",
    name2: "Teluk Wondama",
    alerts: 1010
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 50,
    lat: -0.3183,
    lon: -65.5213,
    name1: "Amazonas",
    name2: "Santa Isabel do Rio Negro",
    alerts: 1005
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 119,
    lat: -16.5358,
    lon: -55.4471,
    name1: "Mato Grosso",
    name2: "Santo Ant\xF4nio do Leverger",
    alerts: 1004
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 6,
    lat: -5.7106,
    lon: -71.7407,
    name1: "Amazonas",
    name2: "Atalaia do Norte",
    alerts: 999
  },
  {
    iso: "MYS",
    adm1: 8,
    adm2: 7,
    lat: 3.6157,
    lon: 102.6661,
    name1: "Pahang",
    name2: "Maran",
    alerts: 999
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 133,
    lat: -1.9515,
    lon: -56.4584,
    name1: "Par\xE1",
    name2: "Terra Santa",
    alerts: 998
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 18,
    lat: -9.4123,
    lon: -70.3964,
    name1: "Acre",
    name2: "Santa Rosa do Purus",
    alerts: 994
  },
  {
    iso: "PNG",
    adm1: 10,
    adm2: 1,
    lat: -6.0403,
    lon: 144.5455,
    name1: "Jiwaka",
    name2: "Anglimp-South Waghi",
    alerts: 989
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 24,
    lat: -13.5798,
    lon: -59.1931,
    name1: "Mato Grosso",
    name2: "Campos de J\xFAlio",
    alerts: 986
  },
  {
    iso: "COL",
    adm1: 20,
    adm2: 23,
    lat: 9.3308,
    lon: -74.3476,
    name1: "Magdalena",
    name2: "San Zen\xF3n",
    alerts: 985
  },
  {
    iso: "MYS",
    adm1: 15,
    adm2: 6,
    lat: 3.3889,
    lon: 101.3268,
    name1: "Selangor",
    name2: "Kuala Selangor",
    alerts: 983
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 86,
    lat: -7.5205,
    lon: -51.4309,
    name1: "Par\xE1",
    name2: "Ouril\xE2ndia do Norte",
    alerts: 981
  },
  {
    iso: "IDN",
    adm1: 17,
    adm2: 13,
    lat: -4.3728,
    lon: 105.5319,
    name1: "Lampung",
    name2: "Tulangbawang",
    alerts: 981
  },
  {
    iso: "BRA",
    adm1: 11,
    adm2: 26,
    lat: -18.7226,
    lon: -56.7233,
    name1: "Mato Grosso do Sul",
    name2: "Corumb\xE1",
    alerts: 979
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 30,
    lat: -6.6828,
    lon: -68.1128,
    name1: "Amazonas",
    name2: "Itamarati",
    alerts: 977
  },
  {
    iso: "BRA",
    adm1: 10,
    adm2: 52,
    lat: -4.8664,
    lon: -43.2992,
    name1: "Maranh\xE3o",
    name2: "Caxias",
    alerts: 974
  },
  {
    iso: "PRY",
    adm1: 1,
    adm2: 3,
    lat: -20.0066,
    lon: -60.2415,
    name1: "Alto Paraguay",
    name2: "Mayor Pablo Lagerenza",
    alerts: 971
  },
  {
    iso: "CHN",
    adm1: 7,
    adm2: 12,
    lat: 22.1828,
    lon: 109.0246,
    name1: "Guangxi",
    name2: "Qinzhou",
    alerts: 971
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 82,
    lat: -0.1696,
    lon: -55.6749,
    name1: "Par\xE1",
    name2: "\xD3bidos",
    alerts: 971
  },
  {
    iso: "MYS",
    adm1: 16,
    adm2: 2,
    lat: 4.6717,
    lon: 103.1418,
    name1: "Trengganu",
    name2: "Dungun",
    alerts: 957
  },
  {
    iso: "COD",
    adm1: 15,
    adm2: 5,
    lat: -10.2253,
    lon: 26.1371,
    name1: "Lualaba",
    name2: "Lubudi",
    alerts: 954
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 184,
    lat: -26.3735,
    lon: -53.3145,
    name1: "Santa Catarina",
    name2: "Palma Sola",
    alerts: 953
  },
  {
    iso: "IDN",
    adm1: 22,
    adm2: 7,
    lat: -1.7162,
    lon: 132.1505,
    name1: "Papua Barat",
    name2: "Sorong Selatan",
    alerts: 952
  },
  {
    iso: "CHN",
    adm1: 29,
    adm2: 6,
    lat: 28.7227,
    lon: 91.6972,
    name1: "Xizang",
    name2: "Shannan",
    alerts: 951
  },
  {
    iso: "GAB",
    adm1: 8,
    adm2: 3,
    lat: -2.3388,
    lon: 10.103,
    name1: "Ogoou\xE9-Maritime",
    name2: "Ndougou",
    alerts: 943
  },
  {
    iso: "ZAF",
    adm1: 6,
    adm2: 2,
    lat: -26.671,
    lon: 29.9295,
    name1: "Mpumalanga",
    name2: "Gert Sibande",
    alerts: 942
  },
  {
    iso: "SSD",
    adm1: 10,
    adm2: 4,
    lat: 5.3174,
    lon: 28.3336,
    name1: "West Equatoria",
    name2: "Yambio",
    alerts: 940
  },
  {
    iso: "MYS",
    adm1: 8,
    adm2: 11,
    lat: 3.5911,
    lon: 102.2656,
    name1: "Pahang",
    name2: "Temerloh",
    alerts: 938
  },
  {
    iso: "MYS",
    adm1: 13,
    adm2: 12,
    lat: 4.6185,
    lon: 116.4628,
    name1: "Sabah",
    name2: "Nabawan",
    alerts: 938
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 13,
    lat: -27.4724,
    lon: -55.0692,
    name1: "Misiones",
    name2: "Ober\xE1",
    alerts: 935
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 44,
    lat: -2.0482,
    lon: -61.7034,
    name1: "Amazonas",
    name2: "Novo Air\xE3o",
    alerts: 933
  },
  {
    iso: "MDG",
    adm1: 4,
    adm2: 2,
    lat: -16.2708,
    lon: 46.2176,
    name1: "Mahajanga",
    name2: "Boeny",
    alerts: 930
  },
  {
    iso: "NCL",
    adm1: 2,
    adm2: 10,
    lat: -20.9478,
    lon: 165.1864,
    name1: "Nord",
    name2: "Poindimi\xE9",
    alerts: 928
  },
  {
    iso: "MEX",
    adm1: 5,
    adm2: 68,
    lat: 17.2208,
    lon: -93.1739,
    name1: "Chiapas",
    name2: "Ocotepec",
    alerts: 928
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 278,
    lat: -25.6365,
    lon: -48.4748,
    name1: "Paran\xE1",
    name2: "Pontal do Paran\xE1",
    alerts: 927
  },
  {
    iso: "IDN",
    adm1: 18,
    adm2: 7,
    lat: 2.3166,
    lon: 128.4484,
    name1: "Maluku Utara",
    name2: "Pulau Morotai",
    alerts: 926
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 88,
    lat: -30.1348,
    lon: -50.5118,
    name1: "Rio Grande do Sul",
    name2: "Capivari do Sul",
    alerts: 926
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 323,
    lat: -27.3357,
    lon: -53.078,
    name1: "Rio Grande do Sul",
    name2: "Planalto",
    alerts: 925
  },
  {
    iso: "IDN",
    adm1: 24,
    adm2: 8,
    lat: 0.5499,
    lon: 101.4644,
    name1: "Riau",
    name2: "Pekanbaru",
    alerts: 925
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 9,
    lat: 28.6949,
    lon: 109.7273,
    name1: "Hunan",
    name2: "Xiangxi Tujia and Miao",
    alerts: 924
  },
  {
    iso: "COL",
    adm1: 2,
    adm2: 82,
    lat: 5.9539,
    lon: -74.6875,
    name1: "Antioquia",
    name2: "Puerto Triunfo",
    alerts: 924
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 359,
    lat: -29.0682,
    lon: -53.2542,
    name1: "Rio Grande do Sul",
    name2: "Salto do Jacu\xED",
    alerts: 923
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 172,
    lat: -26.9082,
    lon: -52.9067,
    name1: "Santa Catarina",
    name2: "Nova Erechim",
    alerts: 919
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 13,
    lat: 29.3919,
    lon: 110.5294,
    name1: "Hunan",
    name2: "Zhangjiajie",
    alerts: 917
  },
  {
    iso: "PNG",
    adm1: 7,
    adm2: 5,
    lat: -5.669,
    lon: 143.8741,
    name1: "Enga",
    name2: "Wapenamanda",
    alerts: 912
  },
  {
    iso: "ZAF",
    adm1: 4,
    adm2: 7,
    lat: -27.6185,
    lon: 32.3003,
    name1: "KwaZulu-Natal",
    name2: "Umkhanyakude",
    alerts: 909
  },
  {
    iso: "LAO",
    adm1: 17,
    adm2: 1,
    lat: 15.382,
    lon: 107.3195,
    name1: "X\xE9kong",
    name2: "Dakcheung",
    alerts: 900
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 138,
    lat: -27.1518,
    lon: -51.5929,
    name1: "Santa Catarina",
    name2: "Joa\xE7aba",
    alerts: 899
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 10,
    lat: 29.9843,
    lon: -81.8521,
    name1: "Florida",
    name2: "Clay",
    alerts: 898
  },
  {
    iso: "THA",
    adm1: 33,
    adm2: 11,
    lat: 6.24,
    lon: 101.9998,
    name1: "Narathiwat",
    name2: "Tak Bai",
    alerts: 897
  },
  {
    iso: "PRY",
    adm1: 8,
    adm2: 2,
    lat: -24.3221,
    lon: -54.661,
    name1: "Canindey\xFA",
    name2: "General Francisco C. Alvarez",
    alerts: 895
  },
  {
    iso: "SUR",
    adm1: 6,
    adm2: 1,
    lat: 5.2665,
    lon: -55.6835,
    name1: "Para",
    name2: "Bigi Poika",
    alerts: 894
  },
  {
    iso: "IDN",
    adm1: 17,
    adm2: 11,
    lat: -5.4359,
    lon: 104.6752,
    name1: "Lampung",
    name2: "Tanggamus",
    alerts: 892
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 346,
    lat: -25.67,
    lon: -50.2867,
    name1: "Paran\xE1",
    name2: "S\xE3o Jo\xE3o do Triunfo",
    alerts: 892
  },
  {
    iso: "COD",
    adm1: 1,
    adm2: 7,
    lat: 2.8178,
    lon: 25.1729,
    name1: "Bas-Uele",
    name2: "Buta",
    alerts: 891
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 132,
    lat: -27.4534,
    lon: -49.5389,
    name1: "Santa Catarina",
    name2: "Ituporanga",
    alerts: 887
  },
  {
    iso: "IDN",
    adm1: 12,
    adm2: 13,
    lat: 0.8985,
    lon: 109.0349,
    name1: "Kalimantan Barat",
    name2: "Singkawang",
    alerts: 886
  },
  {
    iso: "MYS",
    adm1: 14,
    adm2: 8,
    lat: 1.8219,
    lon: 111.9387,
    name1: "Sarawak",
    name2: "Julau",
    alerts: 880
  },
  {
    iso: "AUS",
    adm1: 7,
    adm2: 55,
    lat: -27.5685,
    lon: 153.3608,
    name1: "Queensland",
    name2: "Redland",
    alerts: 880
  },
  {
    iso: "IDN",
    adm1: 26,
    adm2: 5,
    lat: -3.5037,
    lon: 119.872,
    name1: "Sulawesi Selatan",
    name2: "Enrekang",
    alerts: 876
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 142,
    lat: -29.213,
    lon: -51.9218,
    name1: "Rio Grande do Sul",
    name2: "Encantado",
    alerts: 872
  },
  {
    iso: "IDN",
    adm1: 31,
    adm2: 15,
    lat: -3.4528,
    lon: 104.2294,
    name1: "Sumatera Selatan",
    name2: "Prabumulih",
    alerts: 869
  },
  {
    iso: "MYS",
    adm1: 2,
    adm2: 2,
    lat: 5.2061,
    lon: 100.6154,
    name1: "Kedah",
    name2: "Bandar Baharu",
    alerts: 869
  },
  {
    iso: "MDG",
    adm1: 3,
    adm2: 2,
    lat: -23.2954,
    lon: 47.2492,
    name1: "Fianarantsoa",
    name2: "Atsimo-Atsinana",
    alerts: 868
  },
  {
    iso: "CHN",
    adm1: 16,
    adm2: 11,
    lat: 28.2257,
    lon: 117.1066,
    name1: "Jiangxi",
    name2: "Yingtan",
    alerts: 865
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 54,
    lat: -27.0709,
    lon: -48.709,
    name1: "Santa Catarina",
    name2: "Cambori\xFA",
    alerts: 864
  },
  {
    iso: "COL",
    adm1: 20,
    adm2: 12,
    lat: 9.2467,
    lon: -74.1449,
    name1: "Magdalena",
    name2: "Guamal",
    alerts: 861
  },
  {
    iso: "COD",
    adm1: 23,
    adm2: 1,
    lat: 2.5334,
    lon: 19.9603,
    name1: "Sud-Ubangi",
    name2: "Budjala",
    alerts: 860
  },
  {
    iso: "CHN",
    adm1: 1,
    adm2: 11,
    lat: 29.9061,
    lon: 118.0764,
    name1: "Anhui",
    name2: "Huangshan",
    alerts: 858
  },
  {
    iso: "BRA",
    adm1: 5,
    adm2: 42,
    lat: -15.9354,
    lon: -39.1773,
    name1: "Bahia",
    name2: "Belmonte",
    alerts: 856
  },
  {
    iso: "PER",
    adm1: 23,
    adm2: 2,
    lat: -6.561,
    lon: -76.7414,
    name1: "San Mart\xEDn",
    name2: "El Dorado",
    alerts: 852
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 22,
    lat: -5.0543,
    lon: -48.7635,
    name1: "Par\xE1",
    name2: "Bom Jesus do Tocantins",
    alerts: 852
  },
  {
    iso: "AGO",
    adm1: 12,
    adm2: 8,
    lat: -8.4227,
    lon: 20.4768,
    name1: "Lunda Norte",
    name2: "Lucapa",
    alerts: 851
  },
  {
    iso: "MEX",
    adm1: 23,
    adm2: 2,
    lat: 21.0124,
    lon: -87.047,
    name1: "Quintana Roo",
    name2: "Benito Ju\xE1rez",
    alerts: 850
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 37,
    lat: -24.2569,
    lon: -52.7512,
    name1: "Paran\xE1",
    name2: "Boa Esperan\xE7a",
    alerts: 847
  },
  {
    iso: "MYS",
    adm1: 2,
    adm2: 9,
    lat: 5.9874,
    lon: 100.5493,
    name1: "Kedah",
    name2: "Pendang",
    alerts: 847
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 58,
    lat: -27.413,
    lon: -51.2446,
    name1: "Santa Catarina",
    name2: "Campos Novos",
    alerts: 846
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 27,
    lat: 3.3664,
    lon: 99.0575,
    name1: "Sumatera Utara",
    name2: "Serdang Bedagai",
    alerts: 846
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 6,
    lat: -7.9725,
    lon: -72.7553,
    name1: "Acre",
    name2: "Cruzeiro do Sul",
    alerts: 845
  },
  {
    iso: "VEN",
    adm1: 17,
    adm2: 7,
    lat: 8.8604,
    lon: -62.6729,
    name1: "Monagas",
    name2: "Libertador",
    alerts: 843
  },
  {
    iso: "AGO",
    adm1: 14,
    adm2: 10,
    lat: -8.1677,
    lon: 17.1122,
    name1: "Malanje",
    name2: "Marimba",
    alerts: 843
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 267,
    lat: -25.4199,
    lon: -49.1556,
    name1: "Paran\xE1",
    name2: "Pinhais",
    alerts: 841
  },
  {
    iso: "NGA",
    adm1: 35,
    adm2: 2,
    lat: 8.1476,
    lon: 11.0484,
    name1: "Taraba",
    name2: "Bali",
    alerts: 839
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 20,
    lat: -9.9972,
    lon: -67.4276,
    name1: "Acre",
    name2: "Senador Guiomard",
    alerts: 833
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 246,
    lat: -23.4597,
    lon: -52.2313,
    name1: "Paran\xE1",
    name2: "Ourizona",
    alerts: 833
  },
  {
    iso: "TZA",
    adm1: 17,
    adm2: 3,
    lat: -9.2254,
    lon: 34.1472,
    name1: "Njombe",
    name2: "Makete",
    alerts: 830
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 124,
    lat: -26.6276,
    lon: -52.8901,
    name1: "Santa Catarina",
    name2: "Irati",
    alerts: 828
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 371,
    lat: -27.8644,
    lon: -54.497,
    name1: "Rio Grande do Sul",
    name2: "Santa Rosa",
    alerts: 827
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 109,
    lat: -10.4631,
    lon: -61.01,
    name1: "Mato Grosso",
    name2: "Rondol\xE2ndia",
    alerts: 826
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 153,
    lat: -27.0904,
    lon: -51.5056,
    name1: "Santa Catarina",
    name2: "Luzerna",
    alerts: 824
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 150,
    lat: -22.7186,
    lon: -52.2254,
    name1: "Paran\xE1",
    name2: "Inaj\xE1",
    alerts: 824
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 21,
    lat: -3.289,
    lon: -62.9733,
    name1: "Amazonas",
    name2: "Codaj\xE1s",
    alerts: 816
  },
  {
    iso: "PHL",
    adm1: 75,
    adm2: 7,
    lat: 9.3533,
    lon: 125.8544,
    name1: "Surigao del Sur",
    name2: "Carrascal",
    alerts: 812
  },
  {
    iso: "ECU",
    adm1: 16,
    adm2: 5,
    lat: -0.9812,
    lon: -77.7907,
    name1: "Napo",
    name2: "Tena",
    alerts: 811
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 33,
    lat: 29.9853,
    lon: -83.1809,
    name1: "Florida",
    name2: "Lafayette",
    alerts: 811
  },
  {
    iso: "MEX",
    adm1: 23,
    adm2: 4,
    lat: 19.6684,
    lon: -88.0612,
    name1: "Quintana Roo",
    name2: "Felipe Carrillo Puerto",
    alerts: 808
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 17,
    lat: -7.8439,
    lon: -73.2238,
    name1: "Acre",
    name2: "Rodrigues Alves",
    alerts: 804
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 28,
    lat: -3.0952,
    lon: -60.4797,
    name1: "Amazonas",
    name2: "Iranduba",
    alerts: 799
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 92,
    lat: -28.7817,
    lon: -49.4933,
    name1: "Santa Catarina",
    name2: "Forquilhinha",
    alerts: 798
  },
  {
    iso: "MEX",
    adm1: 20,
    adm2: 14,
    lat: 17.318,
    lon: -96.4209,
    name1: "Oaxaca",
    name2: "Capul\xE1lpam de M\xE9ndez",
    alerts: 797
  },
  {
    iso: "CHN",
    adm1: 30,
    adm2: 2,
    lat: 25.3535,
    lon: 101.5702,
    name1: "Yunnan",
    name2: "Chuxiong Yi",
    alerts: 792
  },
  {
    iso: "PER",
    adm1: 8,
    adm2: 11,
    lat: -13.1704,
    lon: -71.5057,
    name1: "Cusco",
    name2: "Paucartambo",
    alerts: 789
  },
  {
    iso: "COD",
    adm1: 21,
    adm2: 7,
    lat: -4.6861,
    lon: 24.3228,
    name1: "Sankuru",
    name2: "Lubefu",
    alerts: 784
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 65,
    lat: -0.8099,
    lon: -47.6352,
    name1: "Par\xE1",
    name2: "Magalh\xE3es Barata",
    alerts: 783
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 9,
    lat: -3.129,
    lon: -57.1724,
    name1: "Amazonas",
    name2: "Barreirinha",
    alerts: 783
  },
  {
    iso: "CHN",
    adm1: 31,
    adm2: 6,
    lat: 29.7096,
    lon: 121.4752,
    name1: "Zhejiang",
    name2: "Ningbo",
    alerts: 779
  },
  {
    iso: "COD",
    adm1: 24,
    adm2: 2,
    lat: -5.9613,
    lon: 28.9357,
    name1: "Tanganyika",
    name2: "Kalemie",
    alerts: 778
  },
  {
    iso: "NCL",
    adm1: 3,
    adm2: 10,
    lat: -22.0487,
    lon: 166.3411,
    name1: "Sud",
    name2: "Pa\xEFta",
    alerts: 777
  },
  {
    iso: "NIC",
    adm1: 9,
    adm2: 1,
    lat: 13.912,
    lon: -85.2549,
    name1: "Jinotega",
    name2: "El Cu\xE1",
    alerts: 774
  },
  {
    iso: "CHN",
    adm1: 30,
    adm2: 15,
    lat: 24.1382,
    lon: 102.199,
    name1: "Yunnan",
    name2: "Yuxi",
    alerts: 774
  },
  {
    iso: "COD",
    adm1: 5,
    adm2: 10,
    lat: 2.7358,
    lon: 29.2578,
    name1: "Haut-Uele",
    name2: "Watsa",
    alerts: 773
  },
  {
    iso: "COL",
    adm1: 20,
    adm2: 21,
    lat: 10.5177,
    lon: -74.7205,
    name1: "Magdalena",
    name2: "Salamina",
    alerts: 772
  },
  {
    iso: "NCL",
    adm1: 2,
    adm2: 3,
    lat: -20.7127,
    lon: 164.8565,
    name1: "Nord",
    name2: "Hiengh\xE8ne",
    alerts: 771
  },
  {
    iso: "USA",
    adm1: 10,
    adm2: 11,
    lat: 26.1156,
    lon: -81.3427,
    name1: "Florida",
    name2: "Collier",
    alerts: 766
  },
  {
    iso: "NIC",
    adm1: 1,
    adm2: 7,
    lat: 14.5755,
    lon: -84.2293,
    name1: "Atl\xE1ntico Norte",
    name2: "Wasp\xE1n",
    alerts: 764
  },
  {
    iso: "PHL",
    adm1: 62,
    adm2: 5,
    lat: 14.9135,
    lon: 121.9897,
    name1: "Quezon",
    name2: "Burdeos",
    alerts: 763
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 181,
    lat: -27.213,
    lon: -52.4879,
    name1: "Santa Catarina",
    name2: "Paial",
    alerts: 758
  },
  {
    iso: "BRA",
    adm1: 23,
    adm2: 4,
    lat: 2.7888,
    lon: -60.1001,
    name1: "Roraima",
    name2: "Bonfim",
    alerts: 757
  },
  {
    iso: "AGO",
    adm1: 13,
    adm2: 4,
    lat: -9.4753,
    lon: 20.6915,
    name1: "Lunda Sul",
    name2: "Saurimo",
    alerts: 752
  },
  {
    iso: "PNG",
    adm1: 21,
    adm2: 3,
    lat: -5.5274,
    lon: 144.1653,
    name1: "Western Highlands",
    name2: "Mul-Baiyer",
    alerts: 752
  },
  {
    iso: "PHL",
    adm1: 18,
    adm2: 5,
    lat: 18.5462,
    lon: 121.5203,
    name1: "Cagayan",
    name2: "Aparri",
    alerts: 749
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 189,
    lat: -29.2672,
    lon: -49.7219,
    name1: "Santa Catarina",
    name2: "Passo de Torres",
    alerts: 748
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 6,
    lat: 27.7403,
    lon: 111.6116,
    name1: "Hunan",
    name2: "Loudi",
    alerts: 747
  },
  {
    iso: "CIV",
    adm1: 10,
    adm2: 1,
    lat: 9.8614,
    lon: -6.4726,
    name1: "Savanes",
    name2: "Bagou\xE9",
    alerts: 744
  },
  {
    iso: "BRA",
    adm1: 23,
    adm2: 8,
    lat: 2.1871,
    lon: -62.4316,
    name1: "Roraima",
    name2: "Iracema",
    alerts: 743
  },
  {
    iso: "BRA",
    adm1: 18,
    adm2: 111,
    lat: -9.0145,
    lon: -43.1656,
    name1: "Piau\xED",
    name2: "Jurema",
    alerts: 739
  },
  {
    iso: "BRA",
    adm1: 27,
    adm2: 65,
    lat: -8.1408,
    lon: -49.1103,
    name1: "Tocantins",
    name2: "Juarina",
    alerts: 739
  },
  {
    iso: "PHL",
    adm1: 1,
    adm2: 25,
    lat: 17.8323,
    lon: 120.9086,
    name1: "Abra",
    name2: "Tineg",
    alerts: 738
  },
  {
    iso: "PNG",
    adm1: 13,
    adm2: 3,
    lat: -9.0694,
    lon: 150.5383,
    name1: "Milne Bay",
    name2: "Kiriwina-Goodenough",
    alerts: 737
  },
  {
    iso: "NPL",
    adm1: 4,
    adm2: 2,
    lat: 29.4955,
    lon: 82.4203,
    name1: "Mid-Western",
    name2: "Karnali",
    alerts: 735
  },
  {
    iso: "CHN",
    adm1: 16,
    adm2: 4,
    lat: 29.308,
    lon: 117.2695,
    name1: "Jiangxi",
    name2: "Jingdezhen",
    alerts: 734
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 69,
    lat: -27.1251,
    lon: -52.6503,
    name1: "Santa Catarina",
    name2: "Chapec\xF3",
    alerts: 734
  },
  {
    iso: "ECU",
    adm1: 18,
    adm2: 4,
    lat: -1.8965,
    lon: -76.8845,
    name1: "Pastaza",
    name2: "Pastaza",
    alerts: 733
  },
  {
    iso: "LAO",
    adm1: 8,
    adm2: 11,
    lat: 19.6159,
    lon: 102.2534,
    name1: "Louangphrabang",
    name2: "Xieng Ngeun",
    alerts: 731
  },
  {
    iso: "PNG",
    adm1: 14,
    adm2: 7,
    lat: -7.2632,
    lon: 146.1757,
    name1: "Morobe",
    name2: "Menyamya",
    alerts: 729
  },
  {
    iso: "GUY",
    adm1: 10,
    adm2: 4,
    lat: 2.7002,
    lon: -58.8079,
    name1: "Upper Takutu-Upper Essequibo",
    name2: "Rest of Region 9",
    alerts: 729
  },
  {
    iso: "ECU",
    adm1: 22,
    adm2: 3,
    lat: 0.0814,
    lon: -77.588,
    name1: "Sucumbios",
    name2: "Gonzalo Pizarro",
    alerts: 728
  },
  {
    iso: "HND",
    adm1: 9,
    adm2: 2,
    lat: 15.4768,
    lon: -84.7071,
    name1: "Gracias a Dios",
    name2: "Brus Laguna",
    alerts: 728
  },
  {
    iso: "IDN",
    adm1: 5,
    adm2: 9,
    lat: -3.4309,
    lon: 102.6987,
    name1: "Bengkulu",
    name2: "Rejang Lebong",
    alerts: 727
  },
  {
    iso: "PHL",
    adm1: 67,
    adm2: 2,
    lat: 5.7887,
    lon: 125.3414,
    name1: "Sarangani",
    name2: "Glan",
    alerts: 726
  },
  {
    iso: "COD",
    adm1: 25,
    adm2: 1,
    lat: 0.7355,
    lon: 26.9969,
    name1: "Tshopo",
    name2: "Bafwasende",
    alerts: 726
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 202,
    lat: -26.0954,
    lon: -53.3453,
    name1: "Paran\xE1",
    name2: "Manfrin\xF3polis",
    alerts: 726
  },
  {
    iso: "PNG",
    adm1: 21,
    adm2: 1,
    lat: -5.6446,
    lon: 144.3518,
    name1: "Western Highlands",
    name2: "Dei",
    alerts: 722
  },
  {
    iso: "PRY",
    adm1: 6,
    adm2: 19,
    lat: -24.8255,
    lon: -55.6447,
    name1: "Caaguaz\xFA",
    name2: "Yh\xFA",
    alerts: 720
  },
  {
    iso: "CHN",
    adm1: 14,
    adm2: 8,
    lat: 27.7337,
    lon: 112.6023,
    name1: "Hunan",
    name2: "Xiangtan",
    alerts: 720
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 34,
    lat: 2.3797,
    lon: 99.2642,
    name1: "Sumatera Utara",
    name2: "Toba Samosir",
    alerts: 719
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 90,
    lat: -6.1517,
    lon: -50.4898,
    name1: "Par\xE1",
    name2: "Parauapebas",
    alerts: 717
  },
  {
    iso: "PNG",
    adm1: 14,
    adm2: 8,
    lat: -6.4939,
    lon: 146.9816,
    name1: "Morobe",
    name2: "Nawae",
    alerts: 716
  },
  {
    iso: "TGO",
    adm1: 1,
    adm2: 2,
    lat: 8.7288,
    lon: 0.5959,
    name1: "Centre",
    name2: "M\xF4",
    alerts: 715
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 57,
    lat: -25.1865,
    lon: -48.8445,
    name1: "Paran\xE1",
    name2: "Campina Grande do Sul",
    alerts: 715
  },
  {
    iso: "CHN",
    adm1: 13,
    adm2: 14,
    lat: 29.6261,
    lon: 114.1747,
    name1: "Hubei",
    name2: "Xianning",
    alerts: 715
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 234,
    lat: -26.6676,
    lon: -50.0053,
    name1: "Santa Catarina",
    name2: "Santa Terezinha",
    alerts: 714
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 159,
    lat: -29.224,
    lon: -53.1783,
    name1: "Rio Grande do Sul",
    name2: "Estrela Velha",
    alerts: 714
  },
  {
    iso: "IDN",
    adm1: 28,
    adm2: 3,
    lat: -4.7015,
    lon: 123.0159,
    name1: "Sulawesi Tenggara",
    name2: "Buton Utara",
    alerts: 713
  },
  {
    iso: "AGO",
    adm1: 15,
    adm2: 9,
    lat: -12.3277,
    lon: 19.7768,
    name1: "Moxico",
    name2: "Moxico",
    alerts: 712
  },
  {
    iso: "NCL",
    adm1: 2,
    adm2: 15,
    lat: -21.2733,
    lon: 165.2016,
    name1: "Nord",
    name2: "Poya (North)",
    alerts: 712
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 58,
    lat: -24.9312,
    lon: -53.0139,
    name1: "Paran\xE1",
    name2: "Campo Bonito",
    alerts: 712
  },
  {
    iso: "BRA",
    adm1: 10,
    adm2: 5,
    lat: -4.5027,
    lon: -43.3835,
    name1: "Maranh\xE3o",
    name2: "Aldeias Altas",
    alerts: 708
  },
  {
    iso: "MYS",
    adm1: 13,
    adm2: 10,
    lat: 4.7097,
    lon: 118.0527,
    name1: "Sabah",
    name2: "Kunak",
    alerts: 707
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 18,
    lat: -9.1711,
    lon: -62.5653,
    name1: "Rond\xF4nia",
    name2: "Cujubim",
    alerts: 702
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 114,
    lat: -23.8762,
    lon: -50.4303,
    name1: "Paran\xE1",
    name2: "Figueira",
    alerts: 699
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 32,
    lat: -2.2531,
    lon: -49.5136,
    name1: "Par\xE1",
    name2: "Camet\xE1",
    alerts: 697
  },
  {
    iso: "CHN",
    adm1: 6,
    adm2: 18,
    lat: 21.0942,
    lon: 110.1644,
    name1: "Guangdong",
    name2: "Zhanjiang",
    alerts: 692
  },
  {
    iso: "AGO",
    adm1: 14,
    adm2: 6,
    lat: -9.038,
    lon: 16.4573,
    name1: "Malanje",
    name2: "Cuaba Nzogo",
    alerts: 692
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 5,
    lat: -27.9296,
    lon: -55.4648,
    name1: "Misiones",
    name2: "Concepci\xF3n",
    alerts: 692
  },
  {
    iso: "BRA",
    adm1: 11,
    adm2: 60,
    lat: -23.71,
    lon: -55.3533,
    name1: "Mato Grosso do Sul",
    name2: "Paranhos",
    alerts: 691
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 60,
    lat: -3.1657,
    lon: -65.4108,
    name1: "Amazonas",
    name2: "Uarini",
    alerts: 690
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 429,
    lat: -23.8564,
    lon: -47.7281,
    name1: "S\xE3o Paulo",
    name2: "Pilar do Sul",
    alerts: 688
  },
  {
    iso: "BRA",
    adm1: 11,
    adm2: 61,
    lat: -17.9143,
    lon: -54.3392,
    name1: "Mato Grosso do Sul",
    name2: "Pedro Gomes",
    alerts: 684
  },
  {
    iso: "BRA",
    adm1: 22,
    adm2: 19,
    lat: -11.3513,
    lon: -60.7848,
    name1: "Rond\xF4nia",
    name2: "Espig\xE3o D'Oeste",
    alerts: 684
  },
  {
    iso: "PHL",
    adm1: 75,
    adm2: 17,
    lat: 8.4373,
    lon: 126.182,
    name1: "Surigao del Sur",
    name2: "Tagbina",
    alerts: 684
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 164,
    lat: -24.9872,
    lon: -50.8599,
    name1: "Paran\xE1",
    name2: "Iva\xED",
    alerts: 683
  },
  {
    iso: "MEX",
    adm1: 10,
    adm2: 23,
    lat: 24.0036,
    lon: -104.0219,
    name1: "Durango",
    name2: "Poanas",
    alerts: 683
  },
  {
    iso: "PER",
    adm1: 12,
    adm2: 1,
    lat: -11.0264,
    lon: -75.1305,
    name1: "Jun\xEDn",
    name2: "Chanchamayo",
    alerts: 682
  },
  {
    iso: "IDN",
    adm1: 18,
    adm2: 3,
    lat: 0.4027,
    lon: 128.2822,
    name1: "Maluku Utara",
    name2: "Halmahera Tengah",
    alerts: 679
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 10,
    lat: -27.6256,
    lon: -55.386,
    name1: "Misiones",
    name2: "Leandro N. Alem",
    alerts: 679
  },
  {
    iso: "IDN",
    adm1: 32,
    adm2: 4,
    lat: 3.4801,
    lon: 98.6941,
    name1: "Sumatera Utara",
    name2: "Deli Serdang",
    alerts: 679
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 281,
    lat: -29.001,
    lon: -51.3159,
    name1: "Rio Grande do Sul",
    name2: "Nova P\xE1dua",
    alerts: 677
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 49,
    lat: -2.5298,
    lon: -59.6384,
    name1: "Amazonas",
    name2: "Rio Preto da Eva",
    alerts: 675
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 36,
    lat: -3.2915,
    lon: -60.9588,
    name1: "Amazonas",
    name2: "Manacapuru",
    alerts: 675
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 258,
    lat: -27.5916,
    lon: -48.8371,
    name1: "Santa Catarina",
    name2: "S\xE3o Pedro de Alc\xE2ntara",
    alerts: 675
  },
  {
    iso: "MYS",
    adm1: 16,
    adm2: 1,
    lat: 5.5925,
    lon: 102.5166,
    name1: "Trengganu",
    name2: "Besut",
    alerts: 675
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 16,
    lat: -27.1245,
    lon: -49.3642,
    name1: "Santa Catarina",
    name2: "Api\xFAna",
    alerts: 672
  },
  {
    iso: "PHL",
    adm1: 74,
    adm2: 4,
    lat: 9.4747,
    lon: 125.7847,
    name1: "Surigao del Norte",
    name2: "Claver",
    alerts: 669
  },
  {
    iso: "IDN",
    adm1: 13,
    adm2: 7,
    lat: -2.6031,
    lon: 115.5086,
    name1: "Kalimantan Selatan",
    name2: "Hulu Sungai Tengah",
    alerts: 669
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 122,
    lat: -26.8453,
    lon: -53.3245,
    name1: "Santa Catarina",
    name2: "Iraceminha",
    alerts: 668
  },
  {
    iso: "BRA",
    adm1: 25,
    adm2: 563,
    lat: -22.6823,
    lon: -48.5404,
    name1: "S\xE3o Paulo",
    name2: "S\xE3o Manuel",
    alerts: 668
  },
  {
    iso: "GTM",
    adm1: 12,
    adm2: 5,
    lat: 16.3391,
    lon: -89.5207,
    name1: "Pet\xE9n",
    name2: "Popt\xFAn",
    alerts: 667
  },
  {
    iso: "SUR",
    adm1: 1,
    adm2: 2,
    lat: 5.0744,
    lon: -54.9589,
    name1: "Brokopondo",
    name2: "Centrum",
    alerts: 667
  },
  {
    iso: "COD",
    adm1: 7,
    adm2: 3,
    lat: -5.1329,
    lon: 22.9485,
    name1: "Kasa\xEF-Central",
    name2: "Dimbelenge",
    alerts: 666
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 40,
    lat: -14.7301,
    lon: -56.9616,
    name1: "Mato Grosso",
    name2: "Denise",
    alerts: 665
  },
  {
    iso: "NCL",
    adm1: 3,
    adm2: 13,
    lat: -21.7232,
    lon: 166.2638,
    name1: "Sud",
    name2: "Thio",
    alerts: 663
  },
  {
    iso: "VNM",
    adm1: 45,
    adm2: 5,
    lat: 13.5241,
    lon: 109.1968,
    name1: "Ph\xFA Y\xEAn",
    name2: "S\xF4ng C\u1EA7u",
    alerts: 662
  },
  {
    iso: "IDN",
    adm1: 1,
    adm2: 22,
    lat: 2.6133,
    lon: 96.0857,
    name1: "Aceh",
    name2: "Simeulue",
    alerts: 659
  },
  {
    iso: "IDN",
    adm1: 13,
    adm2: 6,
    lat: -2.7287,
    lon: 115.2068,
    name1: "Kalimantan Selatan",
    name2: "Hulu Sungai Selatan",
    alerts: 659
  },
  {
    iso: "BRA",
    adm1: 4,
    adm2: 59,
    lat: -2.6918,
    lon: -67.826,
    name1: "Amazonas",
    name2: "Tonantins",
    alerts: 655
  },
  {
    iso: "COD",
    adm1: 15,
    adm2: 7,
    lat: -9.445,
    lon: 23.0274,
    name1: "Lualaba",
    name2: "Sandoa",
    alerts: 654
  },
  {
    iso: "GUF",
    adm1: 1,
    adm2: 10,
    lat: 4.4655,
    lon: -52.5101,
    name1: "Cayenne",
    name2: "Roura",
    alerts: 654
  },
  {
    iso: "BRA",
    adm1: 23,
    adm2: 5,
    lat: 2.2728,
    lon: -60.5423,
    name1: "Roraima",
    name2: "Cant\xE1",
    alerts: 653
  },
  {
    iso: "BRA",
    adm1: 5,
    adm2: 339,
    lat: -16.2292,
    lon: -39.201,
    name1: "Bahia",
    name2: "Santa Cruz Cabr\xE1lia",
    alerts: 652
  },
  {
    iso: "PRY",
    adm1: 18,
    adm2: 9,
    lat: -24.5359,
    lon: -56.2521,
    name1: "San Pedro",
    name2: "San Estanislao",
    alerts: 651
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 406,
    lat: -29.4748,
    lon: -53.8863,
    name1: "Rio Grande do Sul",
    name2: "S\xE3o Martinho da Serra",
    alerts: 651
  },
  {
    iso: "COD",
    adm1: 3,
    adm2: 7,
    lat: -9.2027,
    lon: 27.1939,
    name1: "Haut-Katanga",
    name2: "Mitwaba",
    alerts: 650
  },
  {
    iso: "PNG",
    adm1: 14,
    adm2: 6,
    lat: -6.3558,
    lon: 146.2637,
    name1: "Morobe",
    name2: "Markham",
    alerts: 645
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 288,
    lat: -27.3879,
    lon: -49.3478,
    name1: "Santa Catarina",
    name2: "Vidal Ramos",
    alerts: 640
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 210,
    lat: -27.2493,
    lon: -53.2416,
    name1: "Rio Grande do Sul",
    name2: "Ira\xED",
    alerts: 640
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 186,
    lat: -27.0926,
    lon: -53.1798,
    name1: "Santa Catarina",
    name2: "Palmitos",
    alerts: 636
  },
  {
    iso: "BRA",
    adm1: 1,
    adm2: 5,
    lat: -10.4734,
    lon: -67.8365,
    name1: "Acre",
    name2: "Capixaba",
    alerts: 635
  },
  {
    iso: "BOL",
    adm1: 4,
    adm2: 9,
    lat: -17.6454,
    lon: -67.8935,
    name1: "La Paz",
    name2: "Gualberto Villarroel",
    alerts: 635
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 34,
    lat: -13.1928,
    lon: -59.7504,
    name1: "Mato Grosso",
    name2: "Comodoro",
    alerts: 633
  },
  {
    iso: "IDN",
    adm1: 19,
    adm2: 9,
    lat: -3.1271,
    lon: 128.3196,
    name1: "Maluku",
    name2: "Seram Bagian Barat",
    alerts: 632
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 53,
    lat: -29.3057,
    lon: -52.4139,
    name1: "Rio Grande do Sul",
    name2: "Boqueir\xE3o do Le\xE3o",
    alerts: 629
  },
  {
    iso: "BRA",
    adm1: 5,
    adm2: 352,
    lat: -12.8024,
    lon: -45.4926,
    name1: "Bahia",
    name2: "S\xE3o Desid\xE9rio",
    alerts: 629
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 66,
    lat: -25.6105,
    lon: -53.7606,
    name1: "Paran\xE1",
    name2: "Capanema",
    alerts: 628
  },
  {
    iso: "MYS",
    adm1: 8,
    adm2: 9,
    lat: 3.855,
    lon: 101.8348,
    name1: "Pahang",
    name2: "Raub",
    alerts: 628
  },
  {
    iso: "MDG",
    adm1: 3,
    adm2: 5,
    lat: -21.3489,
    lon: 47.8643,
    name1: "Fianarantsoa",
    name2: "Vatovavy Fitovinany",
    alerts: 625
  },
  {
    iso: "IND",
    adm1: 23,
    adm2: 2,
    lat: 23.5402,
    lon: 93.2373,
    name1: "Mizoram",
    name2: "Champhai",
    alerts: 624
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 20,
    lat: -3.1911,
    lon: -54.9928,
    name1: "Par\xE1",
    name2: "Belterra",
    alerts: 624
  },
  {
    iso: "COL",
    adm1: 23,
    adm2: 6,
    lat: 7.2705,
    lon: -72.652,
    name1: "Norte de Santander",
    name2: "C\xE1cota",
    alerts: 623
  },
  {
    iso: "ECU",
    adm1: 22,
    adm2: 2,
    lat: -0.2786,
    lon: -75.8796,
    name1: "Sucumbios",
    name2: "Cuyabeno",
    alerts: 622
  },
  {
    iso: "IDN",
    adm1: 11,
    adm2: 22,
    lat: -8.1268,
    lon: 112.641,
    name1: "Jawa Timur",
    name2: "Malang",
    alerts: 621
  },
  {
    iso: "PNG",
    adm1: 5,
    adm2: 1,
    lat: -4.3449,
    lon: 142.3078,
    name1: "East Sepik",
    name2: "Ambunti-Dreikikir",
    alerts: 621
  },
  {
    iso: "VNM",
    adm1: 39,
    adm2: 13,
    lat: 10.6738,
    lon: 106.1751,
    name1: "Long An",
    name2: "Th\u1EA1nh H\xF3a",
    alerts: 619
  },
  {
    iso: "AGO",
    adm1: 7,
    adm2: 8,
    lat: -10.699,
    lon: 15.3357,
    name1: "Cuanza Sul",
    name2: "Quibala",
    alerts: 615
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 4,
    lat: -27.5525,
    lon: -55.8577,
    name1: "Misiones",
    name2: "Capital",
    alerts: 613
  },
  {
    iso: "BRA",
    adm1: 13,
    adm2: 525,
    lat: -19.8523,
    lon: -44.974,
    name1: "Minas Gerais",
    name2: "Nova Serrana",
    alerts: 608
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 113,
    lat: -9.3641,
    lon: -50.6113,
    name1: "Par\xE1",
    name2: "Santana do Araguaia",
    alerts: 606
  },
  {
    iso: "IDN",
    adm1: 29,
    adm2: 6,
    lat: 3.5507,
    lon: 125.5413,
    name1: "Sulawesi Utara",
    name2: "Kepulauan Sangihe",
    alerts: 606
  },
  {
    iso: "GUY",
    adm1: 7,
    adm2: 2,
    lat: 7.3544,
    lon: -58.8022,
    name1: "Pomeroon-Supenaam",
    name2: "Charity / Urasara",
    alerts: 606
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 157,
    lat: -22.6597,
    lon: -51.9677,
    name1: "Paran\xE1",
    name2: "Itaguaj\xE9",
    alerts: 603
  },
  {
    iso: "BRA",
    adm1: 13,
    adm2: 621,
    lat: -20.7704,
    lon: -43.1548,
    name1: "Minas Gerais",
    name2: "Presidente Bernardes",
    alerts: 601
  },
  {
    iso: "MYS",
    adm1: 9,
    adm2: 10,
    lat: 4.2643,
    lon: 100.9097,
    name1: "Perak",
    name2: "Perak Tengah",
    alerts: 601
  },
  {
    iso: "AUS",
    adm1: 7,
    adm2: 30,
    lat: -26.147,
    lon: 152.4636,
    name1: "Queensland",
    name2: "Gympie",
    alerts: 600
  },
  {
    iso: "BOL",
    adm1: 4,
    adm2: 18,
    lat: -16.1805,
    lon: -67.6929,
    name1: "La Paz",
    name2: "Nor Yungas",
    alerts: 599
  },
  {
    iso: "CHN",
    adm1: 26,
    adm2: 14,
    lat: 29.672,
    lon: 104.9328,
    name1: "Sichuan",
    name2: "Neijiang]]",
    alerts: 599
  },
  {
    iso: "COD",
    adm1: 4,
    adm2: 6,
    lat: -7.9205,
    lon: 26.8683,
    name1: "Haut-Lomami",
    name2: "Malemba-Nkulu",
    alerts: 597
  },
  {
    iso: "PNG",
    adm1: 21,
    adm2: 4,
    lat: -5.9885,
    lon: 144.1486,
    name1: "Western Highlands",
    name2: "Tambul-Nebilyer",
    alerts: 594
  },
  {
    iso: "SLB",
    adm1: 10,
    adm2: 20,
    lat: -8.2833,
    lon: 157.5129,
    name1: "Western",
    name2: "Roviana Lagoon",
    alerts: 593
  },
  {
    iso: "IDN",
    adm1: 17,
    adm2: 6,
    lat: -4.8093,
    lon: 104.8058,
    name1: "Lampung",
    name2: "Lampung Utara",
    alerts: 592
  },
  {
    iso: "IDN",
    adm1: 4,
    adm2: 4,
    lat: -6.6421,
    lon: 106.2128,
    name1: "Banten",
    name2: "Lebak",
    alerts: 591
  },
  {
    iso: "HND",
    adm1: 15,
    adm2: 18,
    lat: 15.3055,
    lon: -85.7145,
    name1: "Olancho",
    name2: "San Esteban",
    alerts: 590
  },
  {
    iso: "VNM",
    adm1: 52,
    adm2: 10,
    lat: 21.4191,
    lon: 103.6545,
    name1: "S\u01A1n La",
    name2: "Thu\u1EADn Ch\xE2u",
    alerts: 590
  },
  {
    iso: "BRA",
    adm1: 23,
    adm2: 1,
    lat: 3.0756,
    lon: -62.76,
    name1: "Roraima",
    name2: "Alto Alegre",
    alerts: 588
  },
  {
    iso: "IDN",
    adm1: 29,
    adm2: 10,
    lat: 1.0842,
    lon: 124.5211,
    name1: "Sulawesi Utara",
    name2: "Minahasa Selatan",
    alerts: 586
  },
  {
    iso: "PER",
    adm1: 5,
    adm2: 5,
    lat: -13.044,
    lon: -73.7259,
    name1: "Ayacucho",
    name2: "La Mar",
    alerts: 584
  },
  {
    iso: "BRA",
    adm1: 21,
    adm2: 412,
    lat: -28.1235,
    lon: -54.8949,
    name1: "Rio Grande do Sul",
    name2: "S\xE3o Pedro do Buti\xE1",
    alerts: 582
  },
  {
    iso: "PRY",
    adm1: 7,
    adm2: 9,
    lat: -26.1854,
    lon: -55.5281,
    name1: "Caazap\xE1",
    name2: "Taba\xED",
    alerts: 581
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 59,
    lat: -5.1148,
    lon: -49.8697,
    name1: "Par\xE1",
    name2: "Itupiranga",
    alerts: 580
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 71,
    lat: -24.7996,
    lon: -49.8417,
    name1: "Paran\xE1",
    name2: "Castro",
    alerts: 580
  },
  {
    iso: "COD",
    adm1: 13,
    adm2: 8,
    lat: -4.6742,
    lon: 19.527,
    name1: "Kwilu",
    name2: "Idiofa",
    alerts: 580
  },
  {
    iso: "PAN",
    adm1: 6,
    adm2: 1,
    lat: 8.4198,
    lon: -77.5971,
    name1: "Ember\xE1",
    name2: "C\xE9maco",
    alerts: 576
  },
  {
    iso: "BRA",
    adm1: 14,
    adm2: 54,
    lat: -2.0627,
    lon: -49.1307,
    name1: "Par\xE1",
    name2: "Igarap\xE9-Miri",
    alerts: 576
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 42,
    lat: -15.6563,
    lon: -54.7765,
    name1: "Mato Grosso",
    name2: "Dom Aquino",
    alerts: 575
  },
  {
    iso: "ECU",
    adm1: 18,
    adm2: 1,
    lat: -1.3466,
    lon: -76.7639,
    name1: "Pastaza",
    name2: "Arajuno",
    alerts: 573
  },
  {
    iso: "COD",
    adm1: 3,
    adm2: 1,
    lat: -11.2962,
    lon: 26.5902,
    name1: "Haut-Katanga",
    name2: "Kambove",
    alerts: 572
  },
  {
    iso: "BRA",
    adm1: 12,
    adm2: 31,
    lat: -13.8698,
    lon: -51.1484,
    name1: "Mato Grosso",
    name2: "Cocalinho",
    alerts: 571
  },
  {
    iso: "BOL",
    adm1: 8,
    adm2: 1,
    lat: -17.8882,
    lon: -63.2012,
    name1: "Santa Cruz",
    name2: "Andr\xE9s Ib\xE1\xF1ez",
    alerts: 571
  },
  {
    iso: "BRA",
    adm1: 24,
    adm2: 71,
    lat: -27.2391,
    lon: -52.0074,
    name1: "Santa Catarina",
    name2: "Conc\xF3rdia",
    alerts: 571
  },
  {
    iso: "BRA",
    adm1: 16,
    adm2: 166,
    lat: -23.3539,
    lon: -53.4243,
    name1: "Paran\xE1",
    name2: "Ivat\xE9",
    alerts: 571
  },
  {
    iso: "IDN",
    adm1: 18,
    adm2: 9,
    lat: 0.4852,
    lon: 127.6737,
    name1: "Maluku Utara",
    name2: "Tidore Kepulauan",
    alerts: 570
  },
  {
    iso: "ARG",
    adm1: 14,
    adm2: 15,
    lat: -27.7742,
    lon: -55.1643,
    name1: "Misiones",
    name2: "San Javier",
    alerts: 569
  }
];

// src/centroids.ts
var centroidIndex = /* @__PURE__ */ new Map();
for (const entry of glad_adm2_centroids_default) {
  centroidIndex.set(`${entry.iso}.${entry.adm1}.${entry.adm2}`, entry);
}
var ADM2_CENTROID_CAP = centroidIndex.size;
function lookupAdm2Centroid(iso, adm1, adm2) {
  const entry = centroidIndex.get(`${iso}.${adm1}.${adm2}`);
  if (!entry || !Number.isFinite(entry.lat) || !Number.isFinite(entry.lon)) return null;
  return { lat: entry.lat, lon: entry.lon };
}

// src/index.ts
var PLUGIN_ID = "deforestation-gfw";
var GFW_API_URL = "https://data-api.globalforestwatch.org";
var FIRES_DATASET = "nasa_viirs_fire_alerts";
var GLAD_DATASET = "gadm__glad__adm2_daily_alerts";
var POLL_INTERVAL_MS = 25 * 60 * 1e3;
var SNAPSHOT_TTL_SECONDS = Math.max(300, Math.floor(POLL_INTERVAL_MS * 3 / 1e3));
var FIRE_ZOOM = 6;
var MAX_FIRE_TILES = 36;
var GLAD_RECENT_DAYS = 45;
var GLAD_TOP_REGIONS = 500;
var FIRE_GRID = [
  { lat: -12, lon: -62 },
  // Brazil - Amazon
  { lat: -14, lon: -52 },
  // Brazil - Cerrado
  { lat: -8, lon: -42 },
  // Brazil - northeast
  { lat: -17, lon: -60 },
  // Bolivia - Chiquitania
  { lat: -23, lon: -59 },
  // Paraguay
  { lat: -10, lon: -74 },
  // Peru
  { lat: 4, lon: -73 },
  // Colombia
  { lat: -32, lon: -63 },
  // Argentina
  { lat: 24, lon: -104 },
  // Mexico
  { lat: 15, lon: -91 },
  // Guatemala / Honduras
  { lat: 38, lon: -121 },
  // California
  { lat: 31, lon: -92 },
  // US southeast
  { lat: 55, lon: -110 },
  // Canada boreal
  { lat: -12, lon: 20 },
  // Angola
  { lat: -14, lon: 27 },
  // Zambia / southern DRC
  { lat: -4, lon: 24 },
  // DRC
  { lat: -2, lon: 15 },
  // Congo basin north
  { lat: -6, lon: 34 },
  // Tanzania
  { lat: -16, lon: 35 },
  // Mozambique
  { lat: -19, lon: 47 },
  // Madagascar
  { lat: 9, lon: 9 },
  // Nigeria / Cameroon
  { lat: 7, lon: -2 },
  // Ghana / Ivory Coast
  { lat: 12, lon: 30 },
  // Sudan / South Sudan
  { lat: -1, lon: 37 },
  // Kenya
  { lat: -13, lon: 34 },
  // Malawi
  { lat: -22, lon: 24 },
  // Botswana
  { lat: 0, lon: 112 },
  // Indonesia - Kalimantan
  { lat: 0, lon: 105 },
  // Indonesia - Sumatra
  { lat: -4, lon: 138 },
  // Papua
  { lat: 21, lon: 96 },
  // Myanmar
  { lat: 16, lon: 101 },
  // Thailand / Laos
  { lat: 26, lon: 92 },
  // India - northeast
  { lat: 47, lon: 105 },
  // Mongolia
  { lat: 57, lon: 95 },
  // Siberia
  { lat: -16, lon: 133 },
  // Australia - Northern Territory
  { lat: 39, lon: 22 }
  // Greece / Mediterranean
];
try {
  db.prepare(
    "CREATE TABLE IF NOT EXISTS deforestation_alerts (id TEXT PRIMARY KEY, payload TEXT NOT NULL, alert_type TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)"
  ).run();
} catch (err) {
  console.error("[DeforestationGfw] could not ensure SQLite table:", err instanceof Error ? err.message : err);
}
var insertAlert = db.prepare(
  "INSERT OR IGNORE INTO deforestation_alerts (id, payload, alert_type, source_ts, fetched_at) VALUES (@id, @payload, @alert_type, @source_ts, @fetched_at)"
);
async function fetchFireTile(tile) {
  const url = `${GFW_API_URL}/dataset/${FIRES_DATASET}/latest/features?lat=${tile.lat}&lng=${tile.lon}&z=${FIRE_ZOOM}`;
  const res = await withRetry(() => fetchWithTimeout(url));
  const payload = await res.json();
  return extractFireFeatures(payload);
}
async function fetchGladRows(apiKey) {
  const cutoff = new Date(Date.now() - GLAD_RECENT_DAYS * 864e5).toISOString().slice(0, 10);
  const sql = `SELECT iso, adm1, adm2, umd_glad_landsat_alerts__date, umd_glad_landsat_alerts__confidence, alert__count FROM ${GLAD_DATASET} WHERE umd_glad_landsat_alerts__date >= '${cutoff}' ORDER BY alert__count DESC LIMIT ${GLAD_TOP_REGIONS}`;
  const res = await withRetry(
    () => fetchWithTimeout(`${GFW_API_URL}/dataset/${GLAD_DATASET}/latest/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": apiKey },
      body: JSON.stringify({ sql })
    })
  );
  if (!res.ok) throw new Error(`GLAD query failed with HTTP ${res.status}`);
  return await res.json();
}
async function seedDeforestationGfw() {
  const items = [];
  const startedAt = Date.now();
  console.log(`[DeforestationGfw] Polling GFW fires (${FIRE_GRID.length} tiles) + GLAD adm2 alerts...`);
  const tiles = FIRE_GRID.slice(0, MAX_FIRE_TILES);
  let tilesOk = 0;
  for (const tile of tiles) {
    try {
      const features = await fetchFireTile(tile);
      let placed = 0;
      for (const feature of features) {
        const item = mapFireFeature(feature);
        if (item) {
          items.push(item);
          placed++;
        }
      }
      tilesOk++;
      console.log(`[DeforestationGfw] tile (${tile.lat},${tile.lon}): ${placed} fire features`);
    } catch (err) {
      console.warn(
        `[DeforestationGfw] fire tile (${tile.lat},${tile.lon}) failed: ${err instanceof Error ? err.message : String(err)}`
      );
    }
  }
  console.log(`[DeforestationGfw] FIRES: ${tilesOk}/${tiles.length} tiles OK`);
  const apiKey = process.env.GFW_GLAD_API_KEY;
  if (!apiKey) {
    console.warn("[DeforestationGfw] GFW_GLAD_API_KEY not set \u2014 skipping GLAD deforestation layer.");
  } else {
    try {
      const payload = await fetchGladRows(apiKey);
      const rows = dedupeGladRows(extractGladRows(payload));
      let placed = 0;
      for (const row of rows) {
        const iso = typeof row.iso === "string" ? row.iso : "";
        const adm1 = Number(row.adm1);
        const adm2 = Number(row.adm2);
        if (!Number.isFinite(adm1) || !Number.isFinite(adm2)) continue;
        const item = mapGladRow(row, lookupAdm2Centroid(iso, adm1, adm2));
        if (item) {
          items.push(item);
          placed++;
        }
      }
      console.log(
        `[DeforestationGfw] GLAD: ${rows.length} regions, ${placed} placed (centroid cap ${ADM2_CENTROID_CAP})`
      );
    } catch (err) {
      console.warn(`[DeforestationGfw] GLAD layer failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  let inserted = 0;
  for (const item of items) {
    const sourceTs = item.date ? Date.parse(item.date) : NaN;
    const result = insertAlert.run({
      id: item.id,
      payload: JSON.stringify(item),
      alert_type: item.alertType,
      source_ts: Number.isFinite(sourceTs) ? sourceTs : startedAt,
      fetched_at: startedAt
    });
    if (result.changes > 0) inserted++;
  }
  console.log(`[DeforestationGfw] SQLite: ${inserted} new rows (${items.length} items total)`);
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
  return items;
}
var index_default = {
  name: PLUGIN_ID,
  interval: POLL_INTERVAL_MS,
  fetch: seedDeforestationGfw
};
export {
  index_default as default,
  seedDeforestationGfw
};
