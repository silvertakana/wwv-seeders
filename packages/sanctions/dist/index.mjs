// src/index.ts
import sax from "sax";

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
var CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36";

// src/index.ts
import * as Sentry from "@sentry/node";
var OFAC_SOURCES = [
  { label: "SDN", url: "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/sdn_advanced.xml" },
  { label: "CONSOLIDATED", url: "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/cons_advanced.xml" }
];
function local(name) {
  const colon = name.indexOf(":");
  return colon === -1 ? name : name.slice(colon + 1);
}
async function fetchSource(source) {
  console.log(`[Sanctions] Fetching OFAC ${source.label}...`);
  const t0 = Date.now();
  const response = await fetchWithTimeout(source.url, {
    headers: { "User-Agent": CHROME_UA }
  }, 45e3);
  return new Promise((resolve, reject) => {
    const parser = sax.parser(true, { trim: false, normalize: false });
    const areaCodes = /* @__PURE__ */ new Map();
    const featureTypes = /* @__PURE__ */ new Map();
    const legalBasis = /* @__PURE__ */ new Map();
    const locations = /* @__PURE__ */ new Map();
    const parties = /* @__PURE__ */ new Map();
    const entries = [];
    let bytesReceived = 0;
    const stack = [];
    let text = "";
    let inAreaCodeValues = false;
    let inFeatureTypeValues = false;
    let inLegalBasisValues = false;
    let inLocations = false;
    let inDistinctParties = false;
    let inSanctionsEntries = false;
    let refId = "", refShortRef = "", refDescription = "";
    let locId = "";
    let locAreaCodeIds = null;
    let partyFixedRef = "";
    let profileId = "", profileSubTypeId = "";
    let aliases = null;
    let curAlias = null;
    let inDocumentedName = false;
    let namePartsBuf = null;
    let profileFeatures = null;
    let curFeature = null;
    let entryId = "", entryProfileId = "";
    function resolveLocation(locId2) {
      if (!locAreaCodeIds) return { codes: [], names: [] };
      const ids = locAreaCodeIds;
      const mapped = ids.map((id) => areaCodes.get(id)).filter(Boolean);
      const pairs = [...new Map(mapped.map((item) => [item.code, item.name])).entries()].filter(([code]) => code.length > 0);
      return { codes: pairs.map(([c]) => c), names: pairs.map(([, n]) => n) };
    }
    function finalizeParty() {
      const seen = /* @__PURE__ */ new Map();
      for (const feat of profileFeatures ?? []) {
        if (!/location/i.test(featureTypes.get(feat.featureTypeId) || "")) continue;
        for (const lid of feat.locationIds) {
          const loc = locations.get(lid);
          if (!loc) continue;
          loc.codes.forEach((code, i) => {
            if (code && !seen.has(code)) seen.set(code, loc.names[i] ?? "");
          });
        }
      }
      parties.set(profileId, { countryCodes: [...seen.keys()] });
    }
    function finalizeEntry() {
      const party = parties.get(entryProfileId);
      entries.push({ countryCodes: (party == null ? void 0 : party.countryCodes) ?? [] });
    }
    parser.onopentag = (node) => {
      const name = local(node.name);
      const attrs = node.attributes;
      stack.push(name);
      text = "";
      switch (name) {
        case "AreaCodeValues":
          inAreaCodeValues = true;
          break;
        case "FeatureTypeValues":
          inFeatureTypeValues = true;
          break;
        case "LegalBasisValues":
          inLegalBasisValues = true;
          break;
        case "Locations":
          inLocations = true;
          break;
        case "DistinctParties":
          inDistinctParties = true;
          break;
        case "SanctionsEntries":
          inSanctionsEntries = true;
          break;
        case "AreaCode":
          if (inAreaCodeValues) {
            refId = attrs.ID || "";
            refDescription = attrs.Description || "";
          }
          break;
        case "FeatureType":
          if (inFeatureTypeValues) refId = attrs.ID || "";
          break;
        case "Location":
          if (inLocations) {
            locId = attrs.ID || "";
            locAreaCodeIds = [];
          }
          break;
        case "LocationAreaCode":
          if (locAreaCodeIds && attrs.AreaCodeID) locAreaCodeIds.push(attrs.AreaCodeID);
          break;
        case "DistinctParty":
          if (inDistinctParties) {
            partyFixedRef = attrs.FixedRef || "";
            aliases = [];
            profileFeatures = [];
          }
          break;
        case "Profile":
          if (inDistinctParties) {
            profileId = attrs.ID || partyFixedRef;
            profileSubTypeId = attrs.PartySubTypeID || "";
          }
          break;
        case "Feature":
          if (inDistinctParties) curFeature = { featureTypeId: attrs.FeatureTypeID || "", locationIds: [] };
          break;
        case "VersionLocation":
          if (curFeature && attrs.LocationID) curFeature.locationIds.push(attrs.LocationID);
          break;
        case "SanctionsEntry":
          if (inSanctionsEntries) {
            entryId = attrs.ID || "";
            entryProfileId = attrs.ProfileID || "";
          }
          break;
      }
    };
    parser.onclosetag = (rawName) => {
      const name = local(rawName);
      const t = text.trim();
      text = "";
      stack.pop();
      switch (name) {
        case "AreaCodeValues":
          inAreaCodeValues = false;
          break;
        case "FeatureTypeValues":
          inFeatureTypeValues = false;
          break;
        case "Locations":
          inLocations = false;
          break;
        case "DistinctParties":
          inDistinctParties = false;
          break;
        case "SanctionsEntries":
          inSanctionsEntries = false;
          break;
        case "AreaCode":
          if (inAreaCodeValues && refId) areaCodes.set(refId, { code: t, name: refDescription });
          break;
        case "FeatureType":
          if (inFeatureTypeValues && refId) featureTypes.set(refId, t);
          break;
        case "Location":
          if (locAreaCodeIds !== null) {
            locations.set(locId, resolveLocation(locId));
            locId = "";
            locAreaCodeIds = null;
          }
          break;
        case "Feature":
          if (curFeature) {
            profileFeatures.push(curFeature);
            curFeature = null;
          }
          break;
        case "Profile":
          if (inDistinctParties && profileId) finalizeParty();
          profileId = "";
          profileSubTypeId = "";
          aliases = [];
          profileFeatures = [];
          break;
        case "SanctionsEntry":
          finalizeEntry();
          entryId = "";
          entryProfileId = "";
          break;
      }
    };
    parser.ontext = (chunk) => {
      text += chunk;
    };
    parser.oncdata = (chunk) => {
      text += chunk;
    };
    parser.onerror = (err) => {
      parser.resume();
    };
    parser.onend = () => {
      resolve({ entries });
    };
    if (!response.body) return reject(new Error("No response body"));
    (async () => {
      try {
        for await (const chunk of response.body) {
          parser.write(Buffer.from(chunk).toString("utf-8"));
        }
        parser.close();
      } catch (err) {
        reject(err);
      }
    })();
  });
}
async function fetchSanctionsData() {
  try {
    console.log("[Sanctions] Fetching OFAC feeds using SAX streaming...");
    let allEntries = [];
    for (const source of OFAC_SOURCES) {
      const result = await fetchSource(source);
      allEntries = allEntries.concat(result.entries);
    }
    const countryCounts = {};
    for (const entry of allEntries) {
      for (const code of entry.countryCodes) {
        if (code && code !== "XX") {
          countryCounts[code] = (countryCounts[code] || 0) + 1;
        }
      }
    }
    const items = Object.entries(countryCounts).map(([code, count]) => {
      let level = "low";
      if (count > 500) level = "high";
      else if (count > 50) level = "medium";
      return {
        id: `sanction-${code}`,
        countryCode: code,
        count,
        level,
        // Assign a placeholder coordinate, though frontend rendering will extract
        // 3D bounds from /borders.geojson using countryCode instead.
        latitude: 0,
        longitude: 0,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      };
    });
    const sanctionsObj = {
      id: "sanctions-live",
      timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      items
    };
    await setLiveSnapshot("international-sanctions", sanctionsObj, 86400);
    console.log(`[Sanctions] Published ${items.length} sanctioned countries.`);
  } catch (err) {
    console.error("[Sanctions] Error:", err);
    Sentry.captureException(err, { extra: { context: "sanctions" } });
  }
}
var index_default = {
  name: "international-sanctions",
  cron: "0 * * * *",
  // run hourly
  fn: fetchSanctionsData
};
export {
  index_default as default,
  fetchSanctionsData
};
