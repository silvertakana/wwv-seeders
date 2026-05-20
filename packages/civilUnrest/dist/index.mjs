// src/index.ts
import { db } from "@worldwideview/seeder-sdk";
import { setLiveSnapshot } from "@worldwideview/seeder-sdk";
import { fetchWithTimeout, withRetry } from "@worldwideview/seeder-sdk";
var GDELT_URL = "http://api.gdeltproject.org/api/v1/gkg_geojson?query=protest OR riot OR demonstration OR strike OR clash&maxrows=2500";
function classifyGdeltEventType(name) {
  const lowerName = name.toLowerCase();
  if (lowerName.includes("riot")) return "Riots";
  if (lowerName.includes("clash")) return "Riots";
  if (lowerName.includes("strike")) return "Strikes";
  if (lowerName.includes("demonstration")) return "Demonstrations";
  return "Protests";
}
function classifyGdeltSubType(name, count) {
  const lowerName = name.toLowerCase();
  if (count > 50 || lowerName.includes("riot") || lowerName.includes("clash")) return "Violent demonstration";
  if (lowerName.includes("strike")) return "Labor strike";
  return "Peaceful protest";
}
var insertUnrest = db.prepare(
  "INSERT OR REPLACE INTO civil_unrest (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)"
);
async function seedCivilUnrest() {
  var _a, _b, _c;
  console.log("[CivilUnrest] Fetching from GDELT API...");
  const res = await withRetry(() => fetchWithTimeout(GDELT_URL, { headers: { "User-Agent": "WWV-Data-Engine" } }, 25e3), 3, 5e3);
  if (!res.ok) {
    console.warn(`[CivilUnrest] Failed to fetch. HTTP ${res.status}`);
    return;
  }
  const json = await res.json();
  const features = json.features;
  if (!features || features.length === 0) {
    console.log("[CivilUnrest] No events returned from GDELT.");
    return;
  }
  const locationMap = /* @__PURE__ */ new Map();
  for (const feature of features) {
    const name = ((_a = feature.properties) == null ? void 0 : _a.name) || "";
    if (!name) continue;
    const coords = (_b = feature.geometry) == null ? void 0 : _b.coordinates;
    if (!Array.isArray(coords) || coords.length < 2) continue;
    const [lon, lat] = coords;
    if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;
    const key = `${lat.toFixed(1)}:${lon.toFixed(1)}`;
    const existing = locationMap.get(key);
    if (existing) {
      existing.count++;
      existing.urls.push(feature.properties.url);
      if (feature.properties.urltone < existing.worstTone) {
        existing.worstTone = feature.properties.urltone;
      }
    } else {
      locationMap.set(key, {
        name,
        lat,
        lon,
        count: 1,
        worstTone: feature.properties.urltone ?? 0,
        date: feature.properties.urlpubtimedate,
        urls: [feature.properties.url]
      });
    }
  }
  const fetchedAt = Date.now();
  const items = [];
  for (const [, loc] of locationMap) {
    if (loc.count < 3) continue;
    const country = ((_c = loc.name.split(",").pop()) == null ? void 0 : _c.trim()) || "Unknown";
    const eventType = classifyGdeltEventType(loc.name);
    const item = {
      id: `gdelt-${loc.lat.toFixed(2)}-${loc.lon.toFixed(2)}`,
      lat: loc.lat,
      lon: loc.lon,
      type: eventType,
      subType: classifyGdeltSubType(loc.name, loc.count),
      actor1: "General Public",
      actor2: "N/A",
      fatalities: 0,
      country,
      location: loc.name,
      date: loc.date,
      source: "GDELT",
      notes: `${loc.count} clustered reports. Worst Tone: ${loc.worstTone.toFixed(1)}`,
      reportCount: loc.count
    };
    items.push(item);
    insertUnrest.run({
      id: item.id,
      payload: JSON.stringify(item),
      source_ts: new Date(item.date).getTime(),
      fetched_at: fetchedAt
    });
  }
  console.log(`[CivilUnrest] Clustered ${features.length} mentions into ${items.length} confirmed unrest events.`);
  await setLiveSnapshot(
    "civil-unrest",
    {
      source: "gdelt",
      fetchedAt: (/* @__PURE__ */ new Date()).toISOString(),
      items,
      totalCount: items.length
    },
    86400
  );
}
var index_default = {
  name: "civil-unrest",
  cron: "*/15 * * * *",
  fn: seedCivilUnrest
};
export {
  index_default as default,
  seedCivilUnrest
};
