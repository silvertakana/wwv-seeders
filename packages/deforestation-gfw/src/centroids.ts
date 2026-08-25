// Offline adm2 -> centroid lookup for the GLAD deforestation layer.
//
// The GLAD adm2 daily-alerts table carries GADM admin ids (iso, adm1, adm2) but
// no geometry, and the GFW API exposes no vector features for it, so each alert
// region is placed at its administrative centroid.
//
// The table is a CAP (see ADM2_CENTROID_CAP below): it contains the top ~1000
// alert regions by summed GLAD alert count in the recent window, generated
// offline (2026-08-25) from the live API — adm2 geometry (name_1/name_2 via
// gadm_geotrellis_features v4.1) joined to gadm__glad__adm2_daily_alerts, with
// ST_Centroid computed server-side. Regions outside the cap are dropped by
// mapGladRow (they cannot be placed on the globe), which is an accepted
// precision-vs-bundle-size tradeoff.
import rawCentroids from './data/glad-adm2-centroids.json';
import type { Point } from './parse';

export interface Adm2CentroidEntry {
  iso: string;
  adm1: number;
  adm2: number;
  lat: number;
  lon: number;
  name1: string | null;
  name2: string | null;
}

const centroidIndex = new Map<string, Adm2CentroidEntry>();
for (const entry of rawCentroids) {
  centroidIndex.set(`${entry.iso}.${entry.adm1}.${entry.adm2}`, entry);
}

/** Number of (iso,adm1,adm2) regions bundled in the offline lookup. */
export const ADM2_CENTROID_CAP = centroidIndex.size;

export function lookupAdm2Centroid(iso: string, adm1: number, adm2: number): Point | null {
  const entry = centroidIndex.get(`${iso}.${adm1}.${adm2}`);
  if (!entry || !Number.isFinite(entry.lat) || !Number.isFinite(entry.lon)) return null;
  return { lat: entry.lat, lon: entry.lon };
}