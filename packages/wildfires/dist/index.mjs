// src/index.ts
import { db } from "@worldwideview/seeder-sdk";
import { setLiveSnapshot } from "@worldwideview/seeder-sdk";
import { fetchWithTimeout, withRetry } from "@worldwideview/seeder-sdk";
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
  const tiers = [
    { level: 1, size: 2 },
    // Macro: ~220km
    { level: 2, size: 0.5 },
    // Meso: ~55km
    { level: 3, size: 0.05 }
    // Micro: ~5.5km
  ];
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
  let insertedCount = 0;
  const insertMany = db.transaction((firesList) => {
    for (const f of firesList) {
      const timeStr = f.acq_time.toString().padStart(4, "0");
      const tsStr = `${f.acq_date}T${timeStr.substring(0, 2)}:${timeStr.substring(2, 4)}:00Z`;
      let sourceTs = 0;
      try {
        sourceTs = new Date(tsStr).getTime();
        if (isNaN(sourceTs)) sourceTs = fetchedAt;
      } catch (e) {
        sourceTs = fetchedAt;
      }
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
  index_default as default,
  seedWildfires
};
