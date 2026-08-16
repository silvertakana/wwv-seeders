// src/index.ts
import { db } from "@worldwideview/seeder-sdk";
import { setLiveSnapshot } from "@worldwideview/seeder-sdk";
import { fetchWithTimeout, withRetry } from "@worldwideview/seeder-sdk";
import { geolocateIp } from "@worldwideview/seeder-sdk";
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
