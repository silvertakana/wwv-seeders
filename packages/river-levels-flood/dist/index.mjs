var __defProp = Object.defineProperty;
var __defProps = Object.defineProperties;
var __getOwnPropDescs = Object.getOwnPropertyDescriptors;
var __getOwnPropSymbols = Object.getOwnPropertySymbols;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __propIsEnum = Object.prototype.propertyIsEnumerable;
var __defNormalProp = (obj, key, value) => key in obj ? __defProp(obj, key, { enumerable: true, configurable: true, writable: true, value }) : obj[key] = value;
var __spreadValues = (a, b) => {
  for (var prop in b || (b = {}))
    if (__hasOwnProp.call(b, prop))
      __defNormalProp(a, prop, b[prop]);
  if (__getOwnPropSymbols)
    for (var prop of __getOwnPropSymbols(b)) {
      if (__propIsEnum.call(b, prop))
        __defNormalProp(a, prop, b[prop]);
    }
  return a;
};
var __spreadProps = (a, b) => __defProps(a, __getOwnPropDescs(b));

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
    const response = await fetch(url, __spreadProps(__spreadValues({}, options), {
      signal: controller.signal
    }));
    if (!response.ok) {
      throw new Error(`HTTP ${response.status} - ${response.statusText}`);
    }
    return response;
  } finally {
    clearTimeout(id);
  }
}

// src/gauges.ts
var GAUGES = [
  { code: "01100500", name: "MERRIMACK RIVER AT LAWRENCE, MA" },
  { code: "01184000", name: "CONNECTICUT RIVER AT THOMPSONVILLE, CT" },
  { code: "01357500", name: "MOHAWK RIVER AT COHOES NY" },
  { code: "01358000", name: "HUDSON RIVER AT GREEN ISLAND NY" },
  { code: "01427510", name: "DELAWARE RIVER AT CALLICOON NY" },
  { code: "01463500", name: "Delaware River at Trenton NJ" },
  { code: "01474500", name: "Schuylkill River at Philadelphia, PA" },
  { code: "01540500", name: "Susquehanna River at Danville, PA" },
  { code: "01576000", name: "Susquehanna River at Marietta, PA" },
  { code: "01636500", name: "SHENANDOAH RIVER AT MILLVILLE, WV" },
  { code: "01638500", name: "POTOMAC RIVER AT POINT OF ROCKS, MD" },
  { code: "01646500", name: "POTOMAC RIVER NEAR WASH, DC LITTLE FALLS PUMP STA" },
  { code: "01664000", name: "RAPPAHANNOCK RIVER AT REMINGTON, VA" },
  { code: "02035000", name: "JAMES RIVER AT CARTERSVILLE, VA" },
  { code: "02037500", name: "JAMES RIVER NEAR RICHMOND, VA" },
  { code: "02080500", name: "ROANOKE RIVER AT ROANOKE RAPIDS, NC" },
  { code: "02083500", name: "TAR RIVER AT TARBORO, NC" },
  { code: "02088500", name: "LITTLE RIVER NEAR PRINCETON, NC" },
  { code: "02089500", name: "NEUSE RIVER AT KINSTON, NC" },
  { code: "02105500", name: "CAPE FEAR R AT WILM O HUSKE LOCK NR TARHEEL, NC" },
  { code: "02131000", name: "PEE DEE RIVER AT PEEDEE, SC" },
  { code: "02146000", name: "CATAWBA RIVER NEAR ROCK HILL, SC" },
  { code: "02197000", name: "SAVANNAH RIVER AT AUGUSTA, GA" },
  { code: "02198500", name: "SAVANNAH RIVER NEAR CLYO, GA" },
  { code: "02213000", name: "OCMULGEE RIVER AT MACON, GA" },
  { code: "02225000", name: "ALTAMAHA RIVER NEAR BAXLEY, GA" },
  { code: "02225500", name: "OHOOPEE RIVER NEAR REIDSVILLE, GA" },
  { code: "02226000", name: "ALTAMAHA RIVER AT DOCTORTOWN, GA" },
  { code: "02319000", name: "WITHLACOOCHEE RIVER NEAR PINETTA, FLA." },
  { code: "02323500", name: "SUWANNEE RIVER NEAR WILCOX, FLA." },
  { code: "02336000", name: "CHATTAHOOCHEE RIVER AT ATLANTA, GA" },
  { code: "02352500", name: "FLINT RIVER AT ALBANY, GA" },
  { code: "02412000", name: "TALLAPOOSA RIVER NEAR HEFLIN, AL" },
  { code: "02428400", name: "ALABAMA RIVER AT CLAIBORNE L&D NEAR MONROEVILLE" },
  { code: "03015500", name: "Brokenstraw Creek at Youngsville, PA" },
  { code: "03049500", name: "Allegheny River at Natrona, PA" },
  { code: "03081500", name: "Youghiogheny River at Ohiopyle, PA" },
  { code: "03085000", name: "Monongahela River at Braddock, PA" },
  { code: "03150000", name: "Muskingum River at McConnelsville OH" },
  { code: "03193000", name: "KANAWHA RIVER AT KANAWHA FALLS, WV" },
  { code: "03201500", name: "OHIO RIVER AT POINT PLEASANT, WV" },
  { code: "03216600", name: "OHIO RIVER AT GREENUP DAM NEAR GREENUP, KY" },
  { code: "03230500", name: "Big Darby Creek at Darbyville OH" },
  { code: "03270500", name: "Great Miami River at Dayton OH" },
  { code: "03274000", name: "Great Miami River at Hamilton OH" },
  { code: "03294500", name: "OHIO RIVER AT LOUISVILLE, KY" },
  { code: "03303280", name: "OHIO RIVER AT CANNELTON DAM AT CANNELTON, IN" },
  { code: "03319000", name: "ROUGH RIVER NEAR DUNDEE, KY" },
  { code: "03331500", name: "TIPPECANOE RIVER NEAR ORA, IN" },
  { code: "03335500", name: "WABASH RIVER AT LAFAYETTE, IN" },
  { code: "03377500", name: "WABASH RIVER AT MT. CARMEL, IL" },
  { code: "03378500", name: "WABASH RIVER AT NEW HARMONY, IN" },
  { code: "03431500", name: "CUMBERLAND RIVER AT NASHVILLE, TN" },
  { code: "03504000", name: "NANTAHALA RIVER NEAR RAINBOW SPRINGS, NC" },
  { code: "03571000", name: "SEQUATCHIE RIVER NEAR WHITWELL, TN" },
  { code: "03610000", name: "CLARKS RIVER AT MURRAY, KY" },
  { code: "03612600", name: "OHIO RIVER AT OLMSTED, IL" },
  { code: "04193500", name: "Maumee River at Waterville OH" },
  { code: "05331000", name: "MISSISSIPPI RIVER AT ST. PAUL, MN" },
  { code: "05587450", name: "Mississippi River at Grafton, IL" },
  { code: "06185500", name: "Missouri River near Culbertson MT" },
  { code: "06287000", name: "Bighorn R bl Yellowtail Afterbay Dam nr St. Xavier" },
  { code: "06307500", name: "Tongue River at Tongue R Dam nr Decker MT" },
  { code: "06329500", name: "Yellowstone River near Sidney MT" },
  { code: "06342500", name: "MISSOURI RIVER AT BISMARCK, ND" },
  { code: "06440000", name: "MISSOURI RIVER AT PIERRE, SD" },
  { code: "06478500", name: "JAMES RIVER NEAR SCOTLAND, SD" },
  { code: "06610000", name: "Missouri River at Omaha, NE" },
  { code: "06805500", name: "Platte River at Louisville, Nebr." },
  { code: "06818000", name: "Missouri River at St. Joseph, MO" },
  { code: "06889000", name: "KANSAS R AT TOPEKA, KS" },
  { code: "07010000", name: "Mississippi River at St. Louis, MO" },
  { code: "07022000", name: "Mississippi River at Thebes, IL" },
  { code: "07246500", name: "Arkansas River near Sallisaw, OK" },
  { code: "07258000", name: "Arkansas River at Dardanelle, AR" },
  { code: "07263500", name: "Arkansas River at Little Rock, AR" },
  { code: "07344500", name: "Big Cypress Ck nr Pittsburg, TX" },
  { code: "07355500", name: "Red River at Alexandria, LA" },
  { code: "07374000", name: "Mississippi River at Baton Rouge, LA" },
  { code: "07380500", name: "Bayou Lafourche at Napoleonville, LA" },
  { code: "08057000", name: "Trinity Rv at Dallas, TX" },
  { code: "08066500", name: "Trinity Rv at Romayor, TX" },
  { code: "08096500", name: "Brazos Rv at Waco, TX" },
  { code: "08114000", name: "Brazos Rv at Richmond, TX" },
  { code: "08158000", name: "Colorado Rv at Austin, TX" },
  { code: "08176500", name: "Guadalupe Rv at Victoria, TX" },
  { code: "08330000", name: "RIO GRANDE AT ALBUQUERQUE, NM" },
  { code: "09095500", name: "COLORADO RIVER NEAR CAMEO, CO." },
  { code: "09152500", name: "GUNNISON RIVER NEAR GRAND JUNCTION, CO." },
  { code: "09180500", name: "COLORADO RIVER NEAR CISCO, UT" },
  { code: "09315000", name: "GREEN RIVER AT GREEN RIVER, UT" },
  { code: "09367000", name: "LA PLATA RIVER AT LA PLATA, NM" },
  { code: "09380000", name: "COLORADO RIVER AT LEES FERRY, AZ" },
  { code: "09402000", name: "LITTLE COLORADO RIVER NEAR CAMERON, AZ" },
  { code: "11303500", name: "SAN JOAQUIN R NR VERNALIS CA" },
  { code: "11377100", name: "SACRAMENTO R AB BEND BRIDGE NR RED BLUFF CA" },
  { code: "11446500", name: "AMERICAN R A FAIR OAKS CA" },
  { code: "13334300", name: "SNAKE RIVER NEAR ANATONE, WA" },
  { code: "14105700", name: "COLUMBIA RIVER AT THE DALLES, OR" },
  { code: "14174000", name: "WILLAMETTE RIVER AT ALBANY, OR" },
  { code: "14191000", name: "WILLAMETTE RIVER AT SALEM, OR" },
  { code: "14211720", name: "WILLAMETTE RIVER AT PORTLAND, OR" },
  { code: "14361500", name: "ROGUE RIVER AT GRANTS PASS, OR" }
];

// src/index.ts
var USGS_IV_URL = "https://waterservices.usgs.gov/nwis/iv/";
var USGS_USER_AGENT = "wwv-plugin-river-levels (contact: batch@worldwideview.dev)";
var PLUGIN_ID = "river-levels-flood";
var SNAPSHOT_TTL_SECONDS = 5400;
var BATCH_SIZE = 50;
function chunkArray(items, size) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}
function parseIvResponse(payload) {
  var _a, _b, _c, _d, _e, _f, _g, _h, _i, _j, _k, _l, _m;
  if (typeof payload !== "object" || payload === null) return [];
  const envelope = payload;
  const series = (_a = envelope.value) == null ? void 0 : _a.timeSeries;
  if (!Array.isArray(series)) return [];
  const items = [];
  for (const entry of series) {
    const info = (_b = entry.sourceInfo) != null ? _b : {};
    const siteCode = (_d = (_c = info.siteCode) == null ? void 0 : _c[0]) == null ? void 0 : _d.value;
    const latitude = Number((_f = (_e = info.geoLocation) == null ? void 0 : _e.geogLocation) == null ? void 0 : _f.latitude);
    const longitude = Number((_h = (_g = info.geoLocation) == null ? void 0 : _g.geogLocation) == null ? void 0 : _h.longitude);
    if (!siteCode || !Number.isFinite(latitude) || !Number.isFinite(longitude)) continue;
    const readings = (_k = (_j = (_i = entry.values) == null ? void 0 : _i[0]) == null ? void 0 : _j.value) != null ? _k : [];
    if (readings.length === 0) continue;
    const latest = readings[readings.length - 1];
    const stageFt = Number(latest.value);
    if (!Number.isFinite(stageFt)) continue;
    items.push({
      id: siteCode,
      lat: latitude,
      lon: longitude,
      name: (_l = info.siteName) != null ? _l : siteCode,
      stage_ft: stageFt,
      dateTime: (_m = latest.dateTime) != null ? _m : (/* @__PURE__ */ new Date()).toISOString()
    });
  }
  return items;
}
function itemEpochMs(item) {
  const parsed = new Date(item.dateTime);
  return Number.isNaN(parsed.getTime()) ? Date.now() : parsed.getTime();
}
try {
  db.prepare(
    "CREATE TABLE IF NOT EXISTS river_levels_flood (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)"
  ).run();
} catch (err) {
  console.error("[RiverLevelsFlood] could not ensure SQLite table:", err instanceof Error ? err.message : err);
}
var insertGauge = db.prepare(
  "INSERT OR IGNORE INTO river_levels_flood (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)"
);
async function seedRiverLevels() {
  try {
    console.log(`[RiverLevelsFlood] Polling USGS NWIS for ${GAUGES.length} gauges in batches of ${BATCH_SIZE}...`);
    const byId = /* @__PURE__ */ new Map();
    const fetchedAt = Date.now();
    for (const chunk of chunkArray(GAUGES, BATCH_SIZE)) {
      const url = `${USGS_IV_URL}?format=json&sites=${chunk.map((g) => g.code).join(",")}&parameterCd=00065`;
      const res = await withRetry(
        () => fetchWithTimeout(url, { headers: { "User-Agent": USGS_USER_AGENT } })
      );
      const data = await res.json();
      const items2 = parseIvResponse(data);
      for (const item of items2) byId.set(item.id, item);
      console.log(`[RiverLevelsFlood] Batch of ${chunk.length} requested -> ${items2.length} with data`);
    }
    const items = [...byId.values()];
    let insertedCount = 0;
    for (const item of items) {
      const result = insertGauge.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: itemEpochMs(item),
        fetched_at: fetchedAt
      });
      if (result.changes > 0) insertedCount++;
    }
    console.log(`[RiverLevelsFlood] Parsed ${items.length} river gauges. Saved ${insertedCount} new to SQLite.`);
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
    console.error("[RiverLevelsFlood] seeder failed:", err instanceof Error ? err.message : err);
  }
}
var index_default = {
  name: PLUGIN_ID,
  cron: "*/15 * * * *",
  // Every 15 minutes
  fn: seedRiverLevels
};
export {
  chunkArray,
  index_default as default,
  parseIvResponse,
  seedRiverLevels
};
