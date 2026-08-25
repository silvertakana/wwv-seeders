// Pure mapping helpers for the Global Forest Watch (GFW) Data API feeds.
// Kept separate from index.ts so the item-shape contract is unit-testable
// without touching SQLite, Redis, or the network.

export const GFW_HOME_URL = 'https://www.globalforestwatch.org';

export type AlertType = 'fire' | 'deforestation';

export interface DeforestationAlertItem {
  id: string;
  lat: number;
  lon: number;
  alertType: AlertType;
  confidence?: string | number;
  date?: string;
  url: string;
}

export interface FireFeature {
  latitude?: unknown;
  longitude?: unknown;
  alert__date?: unknown;
  alert__time_utc?: unknown;
  confidence__cat?: unknown;
  [key: string]: unknown;
}

export interface GladAlertRow {
  iso?: unknown;
  adm1?: unknown;
  adm2?: unknown;
  umd_glad_landsat_alerts__date?: unknown;
  umd_glad_landsat_alerts__confidence?: unknown;
  alert__count?: unknown;
  [key: string]: unknown;
}

export interface Point {
  lat: number;
  lon: number;
}

function finiteNumber(value: unknown): number | null {
  // Number(null) is 0 and Number('') is 0, which would fabricate a position at
  // the origin; missing/empty values must be treated as absent, not zero.
  if (value === null || value === undefined) return null;
  if (typeof value === 'string' && value.trim() === '') return null;
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

// ---------------------------------------------------------------------------
// FIRES — NASA VIIRS fire alerts point feed.
// ---------------------------------------------------------------------------

export function mapFireFeature(feature: FireFeature): DeforestationAlertItem | null {
  const lat = finiteNumber(feature.latitude);
  const lon = finiteNumber(feature.longitude);
  // The globe drops items without a finite position; skip them here.
  if (lat === null || lon === null) return null;

  const date = optionalString(feature.alert__date);
  const time = optionalString(feature.alert__time_utc);
  const confidence = optionalString(feature.confidence__cat);

  return {
    id: ['fire', lat.toFixed(4), lon.toFixed(4), date ?? 'unknown', time ?? '']
      .filter((s) => s.length > 0)
      .join('-'),
    lat,
    lon,
    alertType: 'fire',
    confidence,
    date,
    url: GFW_HOME_URL,
  };
}

/**
 * The fires feed returns { data: FireFeature[], status: 'success' }. Accept
 * that envelope, a bare array, or a GeoJSON-ish { features: [...] } defensively.
 */
export function extractFireFeatures(payload: unknown): FireFeature[] {
  if (payload === null || typeof payload !== 'object') return [];
  if (Array.isArray(payload)) return payload as FireFeature[];
  const obj = payload as Record<string, unknown>;
  if (Array.isArray(obj.data)) return obj.data as FireFeature[];
  if (Array.isArray(obj.features)) return obj.features as FireFeature[];
  return [];
}

export function parseFiresResponse(payload: unknown): DeforestationAlertItem[] {
  const items: DeforestationAlertItem[] = [];
  for (const feature of extractFireFeatures(payload)) {
    const item = mapFireFeature(feature);
    if (item) items.push(item);
  }
  return items;
}

// ---------------------------------------------------------------------------
// DEFORESTATION — GLAD adm2 daily alerts (admin-level aggregation).
// ---------------------------------------------------------------------------

export function mapGladRow(row: GladAlertRow, centroid: Point | null): DeforestationAlertItem | null {
  const iso = optionalString(row.iso);
  const adm1 = finiteNumber(row.adm1);
  const adm2 = finiteNumber(row.adm2);
  if (iso === undefined || adm1 === null || adm2 === null) return null;
  if (!centroid || !Number.isFinite(centroid.lat) || !Number.isFinite(centroid.lon)) return null;

  const date = optionalString(row.umd_glad_landsat_alerts__date);
  const confidence = optionalString(row.umd_glad_landsat_alerts__confidence);

  return {
    id: `glad-${iso}-${adm1}-${adm2}`,
    lat: centroid.lat,
    lon: centroid.lon,
    alertType: 'deforestation',
    confidence: confidence ?? finiteNumber(row.alert__count) ?? undefined,
    date,
    url: GFW_HOME_URL,
  };
}

export function extractGladRows(payload: unknown): GladAlertRow[] {
  if (payload === null || typeof payload !== 'object') return [];
  if (Array.isArray(payload)) return payload as GladAlertRow[];
  const obj = payload as Record<string, unknown>;
  if (Array.isArray(obj.data)) return obj.data as GladAlertRow[];
  return [];
}

/**
 * The adm2 daily-alerts table carries one row per (region, alert date). The
 * frontend wants ONE item per region, so dedupe by (iso,adm1,adm2) keeping the
 * first row — the API is queried with ORDER BY alert__count DESC, so that row
 * is the region's most active alert.
 */
export function dedupeGladRows(rows: GladAlertRow[]): GladAlertRow[] {
  const seen = new Set<string>();
  const out: GladAlertRow[] = [];
  for (const row of rows) {
    const iso = typeof row.iso === 'string' ? row.iso : '';
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

export function parseGladQueryResponse(
  payload: unknown,
  lookup: (iso: string, adm1: number, adm2: number) => Point | null
): DeforestationAlertItem[] {
  const items: DeforestationAlertItem[] = [];
  for (const row of dedupeGladRows(extractGladRows(payload))) {
    const iso = typeof row.iso === 'string' ? row.iso : '';
    const adm1 = finiteNumber(row.adm1);
    const adm2 = finiteNumber(row.adm2);
    if (adm1 === null || adm2 === null) continue;
    const item = mapGladRow(row, lookup(iso, adm1, adm2));
    if (item) items.push(item);
  }
  return items;
}