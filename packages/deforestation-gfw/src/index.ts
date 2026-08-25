// Backend seeder for the WorldWideView `deforestation-gfw` plugin.
//
// Two data layers from the Global Forest Watch (GFW) Data API:
//
//  1. FIRES — NASA VIIRS fire alerts point feed. The /features endpoint is
//     TILE-SCOPED and current-only (fires drop off as the satellite pass
//     rotates), so one poll fans out over a bounded grid of tile anchors at
//     zoom 6 and merges the point features. A failed tile must never kill the
//     poll, so each tile call is wrapped in its own try/catch.
//
//  2. DEFORESTATION — GLAD-Landsat daily alerts aggregated to GADM adm2. There
//     is no point feed for this layer (the /features endpoint 501s), so it is
//     queried as a table: POST /dataset/gadm__glad__adm2_daily_alerts/latest/
//     query with { sql } (verified live 2026-08-25; the OpenAPI schema
//     QueryRequestIn requires a `sql` string). Each (iso,adm1,adm2) region is
//     emitted ONCE at its admin centroid, resolved via the bundled offline
//     centroid lookup (capped — see src/centroids.ts).
//
// The GFW API publishes no rate limits; the poll cadence is kept modest and
// the fire fan-out bounded (max 36 tile calls per poll).
import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';
import {
  extractFireFeatures,
  extractGladRows,
  dedupeGladRows,
  mapFireFeature,
  mapGladRow,
} from './parse';
import type { DeforestationAlertItem, FireFeature } from './parse';
import { lookupAdm2Centroid, ADM2_CENTROID_CAP } from './centroids';

const PLUGIN_ID = 'deforestation-gfw';
const GFW_API_URL = 'https://data-api.globalforestwatch.org';
const FIRES_DATASET = 'nasa_viirs_fire_alerts';
const GLAD_DATASET = 'gadm__glad__adm2_daily_alerts';

const POLL_INTERVAL_MS = 25 * 60 * 1000; // modest cadence (GFW publishes no rate limits)
const SNAPSHOT_TTL_SECONDS = Math.max(300, Math.floor((POLL_INTERVAL_MS * 3) / 1000));

const FIRE_ZOOM = 6; // zoom the tile-scoped /features endpoint expects
const MAX_FIRE_TILES = 36; // hard cap on the per-poll fan-out
const GLAD_RECENT_DAYS = 45; // GLAD daily alerts ingest lags ~3-4 weeks
const GLAD_TOP_REGIONS = 500; // rows fetched per poll (bounded query)

// Tile anchors for the fires grid. Each call returns the active-fire features
// inside ONE zoom-6 tile containing (lat, lon). These 36 anchors cover the
// historically high-burn land belt: African savanna, Amazon/Cerrado, Southeast
// Asia, Indian subcontinent, Mediterranean, and boreal North America/Eurasia.
const FIRE_GRID: Array<{ lat: number; lon: number }> = [
  { lat: -12, lon: -62 }, // Brazil - Amazon
  { lat: -14, lon: -52 }, // Brazil - Cerrado
  { lat: -8, lon: -42 }, // Brazil - northeast
  { lat: -17, lon: -60 }, // Bolivia - Chiquitania
  { lat: -23, lon: -59 }, // Paraguay
  { lat: -10, lon: -74 }, // Peru
  { lat: 4, lon: -73 }, // Colombia
  { lat: -32, lon: -63 }, // Argentina
  { lat: 24, lon: -104 }, // Mexico
  { lat: 15, lon: -91 }, // Guatemala / Honduras
  { lat: 38, lon: -121 }, // California
  { lat: 31, lon: -92 }, // US southeast
  { lat: 55, lon: -110 }, // Canada boreal
  { lat: -12, lon: 20 }, // Angola
  { lat: -14, lon: 27 }, // Zambia / southern DRC
  { lat: -4, lon: 24 }, // DRC
  { lat: -2, lon: 15 }, // Congo basin north
  { lat: -6, lon: 34 }, // Tanzania
  { lat: -16, lon: 35 }, // Mozambique
  { lat: -19, lon: 47 }, // Madagascar
  { lat: 9, lon: 9 }, // Nigeria / Cameroon
  { lat: 7, lon: -2 }, // Ghana / Ivory Coast
  { lat: 12, lon: 30 }, // Sudan / South Sudan
  { lat: -1, lon: 37 }, // Kenya
  { lat: -13, lon: 34 }, // Malawi
  { lat: -22, lon: 24 }, // Botswana
  { lat: 0, lon: 112 }, // Indonesia - Kalimantan
  { lat: 0, lon: 105 }, // Indonesia - Sumatra
  { lat: -4, lon: 138 }, // Papua
  { lat: 21, lon: 96 }, // Myanmar
  { lat: 16, lon: 101 }, // Thailand / Laos
  { lat: 26, lon: 92 }, // India - northeast
  { lat: 47, lon: 105 }, // Mongolia
  { lat: 57, lon: 95 }, // Siberia
  { lat: -16, lon: 133 }, // Australia - Northern Territory
  { lat: 39, lon: 22 }, // Greece / Mediterranean
];

// Ensure the per-plugin SQLite table exists (INSERT OR IGNORE below requires
// it). The engine may pre-create tables, so IF NOT EXISTS keeps this safe.
try {
  db.prepare(
    'CREATE TABLE IF NOT EXISTS deforestation_alerts (id TEXT PRIMARY KEY, payload TEXT NOT NULL, alert_type TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)'
  ).run();
} catch (err) {
  console.error('[DeforestationGfw] could not ensure SQLite table:', err instanceof Error ? err.message : err);
}

const insertAlert = db.prepare(
  'INSERT OR IGNORE INTO deforestation_alerts (id, payload, alert_type, source_ts, fetched_at) VALUES (@id, @payload, @alert_type, @source_ts, @fetched_at)'
);

async function fetchFireTile(tile: { lat: number; lon: number }): Promise<FireFeature[]> {
  const url = `${GFW_API_URL}/dataset/${FIRES_DATASET}/latest/features?lat=${tile.lat}&lng=${tile.lon}&z=${FIRE_ZOOM}`;
  const res = await withRetry(() => fetchWithTimeout(url));
  const payload = (await res.json()) as unknown;
  return extractFireFeatures(payload);
}

async function fetchGladRows(apiKey: string): Promise<unknown> {
  const cutoff = new Date(Date.now() - GLAD_RECENT_DAYS * 86_400_000).toISOString().slice(0, 10);
  const sql =
    'SELECT iso, adm1, adm2, umd_glad_landsat_alerts__date, umd_glad_landsat_alerts__confidence, alert__count ' +
    `FROM ${GLAD_DATASET} WHERE umd_glad_landsat_alerts__date >= '${cutoff}' ` +
    `ORDER BY alert__count DESC LIMIT ${GLAD_TOP_REGIONS}`;
  const res = await withRetry(() =>
    fetchWithTimeout(`${GFW_API_URL}/dataset/${GLAD_DATASET}/latest/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey },
      body: JSON.stringify({ sql }),
    })
  );
  if (!res.ok) throw new Error(`GLAD query failed with HTTP ${res.status}`);
  return (await res.json()) as unknown;
}

export async function seedDeforestationGfw(): Promise<DeforestationAlertItem[]> {
  const items: DeforestationAlertItem[] = [];
  const startedAt = Date.now();
  console.log(`[DeforestationGfw] Polling GFW fires (${FIRE_GRID.length} tiles) + GLAD adm2 alerts...`);

  // 1. FIRES: bounded tile fan-out, each tile individually guarded.
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

  // 2. DEFORESTATION: one bounded SQL query. Requires the GFW API key, which
  //    the engine injects from the marketplace credentials (GFW_GLAD_API_KEY).
  const apiKey = process.env.GFW_GLAD_API_KEY;
  if (!apiKey) {
    console.warn('[DeforestationGfw] GFW_GLAD_API_KEY not set — skipping GLAD deforestation layer.');
  } else {
    try {
      const payload = await fetchGladRows(apiKey);
      const rows = dedupeGladRows(extractGladRows(payload));
      let placed = 0;
      for (const row of rows) {
        const iso = typeof row.iso === 'string' ? row.iso : '';
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

  // 3. Persist to the self-guarded SQLite table (INSERT OR IGNORE).
  let inserted = 0;
  for (const item of items) {
    const sourceTs = item.date ? Date.parse(item.date) : NaN;
    const result = insertAlert.run({
      id: item.id,
      payload: JSON.stringify(item),
      alert_type: item.alertType,
      source_ts: Number.isFinite(sourceTs) ? sourceTs : startedAt,
      fetched_at: startedAt,
    });
    if (result.changes > 0) inserted++;
  }
  console.log(`[DeforestationGfw] SQLite: ${inserted} new rows (${items.length} items total)`);

  // 4. Publish the live snapshot. Returning the items ALSO lets the engine's
  //    interval scheduler wrap and broadcast them, so both invocation paths
  //    (scheduled fetch, manual fn call) are idempotent and self-sufficient.
  await setLiveSnapshot(
    PLUGIN_ID,
    {
      source: PLUGIN_ID,
      fetchedAt: new Date().toISOString(),
      items,
      totalCount: items.length,
    },
    SNAPSHOT_TTL_SECONDS
  );

  return items;
}

export default {
  name: PLUGIN_ID,
  interval: POLL_INTERVAL_MS,
  fetch: seedDeforestationGfw,
};