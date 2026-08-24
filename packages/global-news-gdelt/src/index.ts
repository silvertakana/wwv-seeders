// Global news / event stream seeder backed by the GDELT GKG GEOJSON API.
//
// SOURCE SUBSTITUTION (2026-08-24, contract data-source policy): the original
// design used the GDELT DOC 2.0 artlist API (api.gdeltproject.org/api/v2/doc).
// That endpoint returns HTTP 429 / "fetch failed" from the WWV engine container
// (the server IP is throttled on v2 DOC; verified live: v2 doc FAILS with
// "fetch failed" while v1 gkg_geojson returns HTTP 200, 1.3MB GeoJSON from the
// same container). Per the batch contract ("source substitution: substitute a
// verified zero-key alternative"), this seeder now uses the GDELT v1
// gkg_geojson endpoint, which returns GeoJSON FeatureCollection with Point
// geometry and rich properties DIRECTLY - no country-centroid approximation
// needed (the v2 artlist approach's main weakness).
//
// URL: http://api.gdeltproject.org/api/v1/gkg_geojson?query=<q>&maxrows=<n>
// Query syntax: GKG supports keyword queries (space = AND, OR with parens).
import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

const GKG_GEOJSON_URL = 'http://api.gdeltproject.org/api/v1/gkg_geojson';
const PLUGIN_ID = 'global-news-gdelt';
const SNAPSHOT_TTL_SECONDS = 1800; // 30 min for 15-min cadence
const MAX_RECORDS = 500;

// A few fixed queries; results are merged and deduped by URL.
const QUERIES = [
  'conflict OR protest OR election',
  'earthquake OR flood OR hurricane',
  'strike OR riot OR unrest',
];

export interface NewsArticleItem {
  id: string; // canonical article URL (unique per article)
  url: string;
  title: string;
  domain: string | null;
  language: string | null;
  lat: number;
  lon: number;
  tone: number | null;
  mentions: number | null;
  mentionedThemes: string | null;
  publishedAt: string | null;
  sourceCountry: string | null;
}

function numeric(value: unknown): number | null {
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

function str(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

/**
 * Extract GeoJSON features from the GKG response envelope. Accepts a bare
 * FeatureCollection, {features: [...]}, {results: [...]}, or an array.
 */
export function extractFeatures(payload: unknown): Array<Record<string, unknown>> {
  if (payload === null || typeof payload !== 'object') return [];
  const obj = payload as Record<string, unknown>;
  if (Array.isArray(payload)) return payload as Array<Record<string, unknown>>;
  if (Array.isArray(obj.features)) return obj.features as Array<Record<string, unknown>>;
  if (Array.isArray(obj.results)) return obj.results as Array<Record<string, unknown>>;
  return [];
}

/**
 * Map a GKG GeoJSON feature to a NewsArticleItem. Requires a Point geometry
 * with [lon, lat] and a URL in properties. Features without a URL or without
 * coordinates are dropped (they cannot be placed on the globe).
 */
export function mapFeatureToItem(feature: Record<string, unknown>): NewsArticleItem | null {
  const properties = (feature.properties ?? {}) as Record<string, unknown>;
  const url = str(properties.url);
  if (!url) return null;

  const geometry = feature.geometry as Record<string, unknown> | null;
  const coords = Array.isArray(geometry?.coordinates) ? (geometry.coordinates as number[]) : null;
  if (!coords || coords.length < 2) return null;
  const lon = numeric(coords[0]);
  const lat = numeric(coords[1]);
  if (lon === null || lat === null) return null;

  const name = str(properties.name) ?? url;
  const domain = url.match(/^https?:\/\/([^/]+)/)?.[1] ?? null;

  return {
    id: url,
    url,
    title: name,
    domain,
    language: str(properties.language),
    lat,
    lon,
    tone: numeric(properties.urltone),
    mentions: numeric(properties.nummentions),
    mentionedThemes: str(properties.mentionedthemes),
    publishedAt: str(properties.urlpubtimedate),
    sourceCountry: str(properties.sourcecountry),
  };
}

export function parseGkgResponse(payload: unknown): NewsArticleItem[] {
  const items: NewsArticleItem[] = [];
  for (const feature of extractFeatures(payload)) {
    const item = mapFeatureToItem(feature);
    if (item) items.push(item);
  }
  return items;
}

export function buildQueryUrl(query: string): string {
  return `${GKG_GEOJSON_URL}?query=${encodeURIComponent(query)}&maxrows=${MAX_RECORDS}`;
}

// Ensure the per-plugin SQLite table exists (INSERT OR IGNORE below requires
// it). The engine may pre-create tables, so IF NOT EXISTS keeps this safe.
try {
  db.prepare(
    'CREATE TABLE IF NOT EXISTS global_news_gdelt (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)'
  ).run();
} catch (err) {
  console.error('[GlobalNewsGdelt] could not ensure SQLite table:', err instanceof Error ? err.message : err);
}

const insertArticle = db.prepare(
  'INSERT OR IGNORE INTO global_news_gdelt (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)'
);

export async function seedGlobalNews(): Promise<void> {
  try {
    console.log(`[GlobalNewsGdelt] Polling GKG GEOJSON with ${QUERIES.length} queries...`);
    const byUrl = new Map<string, NewsArticleItem>();

    for (const query of QUERIES) {
      try {
        const res = await withRetry(() => fetchWithTimeout(buildQueryUrl(query)));
        const data = (await res.json()) as unknown;
        const items = parseGkgResponse(data);
        for (const item of items) byUrl.set(item.id, item);
        console.log(`[GlobalNewsGdelt] Query "${query}" -> ${items.length} articles`);
      } catch (err) {
        console.warn(
          `[GlobalNewsGdelt] Query "${query}" failed: ${err instanceof Error ? err.message : String(err)}`
        );
      }
    }

    if (byUrl.size === 0) {
      console.warn('[GlobalNewsGdelt] No articles after all queries; skipping snapshot.');
      return;
    }

    const items = Array.from(byUrl.values());
    for (const item of items) {
      insertArticle.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: item.publishedAt ? Date.parse(item.publishedAt) : Date.now(),
        fetched_at: Date.now(),
      });
    }

    setLiveSnapshot(
      PLUGIN_ID,
      {
        source: PLUGIN_ID,
        fetchedAt: new Date().toISOString(),
        items,
        totalCount: items.length,
      },
      SNAPSHOT_TTL_SECONDS
    );
    console.log(`[GlobalNewsGdelt] Snapshot saved: ${items.length} articles.`);
  } catch (err) {
    console.error('[GlobalNewsGdelt] seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: PLUGIN_ID,
  cron: '*/15 * * * *', // Every 15 minutes
  fn: seedGlobalNews,
};