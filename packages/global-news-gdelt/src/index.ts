import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

// GDELT DOC 2.0 full-text news search. STRICT rate limiting: at most one
// request per 5 seconds per IP (the API returns a plain-text "Please limit
// requests..." page when throttled, and bursts trigger a sustained block).
// We therefore poll modestly (every 15 min) with a few fixed queries spaced
// 6 seconds apart, and treat a throttled single query as non-fatal.
//
// Mode: artlist + format=json. The DOC API has NO geojson format (checked
// 2026-08-24: format=geojson -> "Invalid format.") and article-list records
// carry NO coordinates. DESIGN DECISION (documented): articles are placed on
// the globe by mapping their 2-letter source country to an embedded country
// centroid (~top countries by news volume). Articles with an unknown or
// missing country code are dropped (they cannot be geolocated).
const GDELT_DOC_URL = 'https://api.gdeltproject.org/api/v2/doc/doc';
const PLUGIN_ID = 'global-news-gdelt';
const SNAPSHOT_TTL_SECONDS = 5400; // 90 min for a 15-minute cadence
const MAX_RECORDS = 50;
// >5s between requests to respect the rate limit; overridable so unit tests
// do not sleep (GDELT_QUERY_GAP_MS=0).
const QUERY_GAP_MS = Number(process.env.GDELT_QUERY_GAP_MS ?? '6000');

// Parenthesized OR terms are REQUIRED by GDELT ("Queries containing OR'd terms
// must be surrounded by ()").
const QUERIES = [
  '(conflict OR protest OR election)',
  '(earthquake OR flood OR hurricane)',
  '(strike OR riot OR unrest)',
];

export interface NewsArticleItem {
  id: string; // canonical article URL (unique per article)
  url: string;
  title: string;
  domain: string | null;
  language: string | null;
  sourcecountry: string | null;
  lat: number;
  lon: number;
  seendate: string | null; // compact GDELT datetime, e.g. 20260824120000
}

// Approximate country centroids (lat, lon) keyed by the two-letter country
// code GDELT emits (FIPS-style; ISO aliases like GB/UK and DE/GM included).
// Values are representative national centroids, good enough to anchor a news
// marker on the globe.
export const COUNTRY_CENTROIDS: Record<string, { lat: number; lon: number }> = {
  AF: { lat: 33.0, lon: 65.0 },
  AL: { lat: 41.2, lon: 20.1 },
  AR: { lat: -34.6, lon: -64.0 },
  AU: { lat: -25.0, lon: 134.0 },
  AT: { lat: 47.2, lon: 13.2 },
  AZ: { lat: 40.4, lon: 47.6 },
  BD: { lat: 23.7, lon: 90.4 },
  BY: { lat: 53.5, lon: 28.0 },
  BE: { lat: 50.6, lon: 4.6 },
  BR: { lat: -10.0, lon: -52.0 },
  BG: { lat: 42.7, lon: 25.5 },
  CA: { lat: 56.1, lon: -106.3 },
  CL: { lat: -35.7, lon: -71.5 },
  CN: { lat: 35.9, lon: 104.2 },
  CO: { lat: 4.6, lon: -74.3 },
  HR: { lat: 45.1, lon: 15.2 },
  CZ: { lat: 49.8, lon: 15.5 },
  DK: { lat: 56.3, lon: 9.5 },
  EG: { lat: 26.8, lon: 30.8 },
  EE: { lat: 58.6, lon: 25.0 },
  FI: { lat: 62.0, lon: 25.7 },
  FR: { lat: 46.2, lon: 2.2 },
  DE: { lat: 51.1, lon: 10.4 },
  GM: { lat: 51.1, lon: 10.4 }, // FIPS alias for Germany
  GE: { lat: 42.3, lon: 43.4 },
  GR: { lat: 39.1, lon: 21.8 },
  HU: { lat: 47.2, lon: 19.5 },
  IN: { lat: 20.6, lon: 78.9 },
  ID: { lat: -0.8, lon: 113.9 },
  IR: { lat: 32.4, lon: 53.7 },
  IQ: { lat: 33.2, lon: 43.7 },
  IE: { lat: 53.1, lon: -8.2 },
  IL: { lat: 31.0, lon: 34.9 },
  IT: { lat: 42.8, lon: 12.8 },
  JP: { lat: 36.2, lon: 138.3 },
  JO: { lat: 31.3, lon: 36.3 },
  KZ: { lat: 48.0, lon: 67.0 },
  KE: { lat: 0.0, lon: 37.9 },
  KP: { lat: 40.3, lon: 127.5 },
  KR: { lat: 36.5, lon: 127.8 },
  KW: { lat: 29.3, lon: 47.5 },
  LB: { lat: 33.9, lon: 35.8 },
  LY: { lat: 26.3, lon: 17.2 },
  LT: { lat: 55.2, lon: 23.9 },
  LV: { lat: 56.9, lon: 24.6 },
  MY: { lat: 4.2, lon: 102.0 },
  MX: { lat: 23.6, lon: -102.5 },
  MD: { lat: 47.4, lon: 28.4 },
  MA: { lat: 31.8, lon: -7.1 },
  MM: { lat: 21.9, lon: 95.9 },
  NP: { lat: 28.4, lon: 84.1 },
  NL: { lat: 52.1, lon: 5.3 },
  NZ: { lat: -41.3, lon: 174.8 },
  NG: { lat: 9.6, lon: 8.1 },
  NO: { lat: 60.5, lon: 8.5 },
  PK: { lat: 30.4, lon: 69.3 },
  PS: { lat: 31.9, lon: 35.2 },
  PE: { lat: -9.2, lon: -75.0 },
  PH: { lat: 12.9, lon: 121.8 },
  PL: { lat: 52.1, lon: 19.4 },
  PT: { lat: 39.4, lon: -8.2 },
  QA: { lat: 25.3, lon: 51.2 },
  RO: { lat: 45.9, lon: 25.0 },
  RU: { lat: 61.5, lon: 105.3 },
  SA: { lat: 24.0, lon: 45.0 },
  RS: { lat: 44.2, lon: 20.9 },
  SG: { lat: 1.35, lon: 103.8 },
  SK: { lat: 48.7, lon: 19.5 },
  SI: { lat: 46.1, lon: 14.8 },
  SO: { lat: 5.2, lon: 46.2 },
  ZA: { lat: -30.6, lon: 22.9 },
  ES: { lat: 40.2, lon: -3.6 },
  SE: { lat: 60.1, lon: 18.6 },
  CH: { lat: 46.8, lon: 8.2 },
  SY: { lat: 35.0, lon: 38.0 },
  TW: { lat: 23.7, lon: 121.0 },
  TH: { lat: 15.1, lon: 101.0 },
  TR: { lat: 39.0, lon: 35.0 },
  UA: { lat: 49.0, lon: 31.4 },
  AE: { lat: 23.9, lon: 54.3 },
  GB: { lat: 55.4, lon: -3.4 },
  UK: { lat: 55.4, lon: -3.4 }, // FIPS alias for the United Kingdom
  US: { lat: 39.8, lon: -98.6 },
  UZ: { lat: 41.4, lon: 64.6 },
  VE: { lat: 6.4, lon: -66.6 },
  VN: { lat: 14.1, lon: 108.3 },
  YE: { lat: 15.6, lon: 48.5 },
  ZM: { lat: -13.1, lon: 27.8 },
  ZW: { lat: -19.0, lon: 29.9 },
};

export interface GdeltArticle {
  url?: string;
  url_mobile?: string;
  title?: string;
  seendate?: string;
  domain?: string;
  language?: string;
  sourcecountry?: string;
  country?: string;
}

/**
 * Extract the article array from a DOC API response. The response envelope has
 * varied across GDELT versions ('articles' key, 'results' key, bare array), so
 * all three are accepted defensively.
 */
export function extractArticles(payload: unknown): GdeltArticle[] {
  if (Array.isArray(payload)) return payload as GdeltArticle[];
  if (typeof payload !== 'object' || payload === null) return [];
  const record = payload as Record<string, unknown>;
  for (const key of ['articles', 'results'] as const) {
    const value = record[key];
    if (Array.isArray(value)) return value as GdeltArticle[];
  }
  // Some modes nest the list under one key with an unknown name; GDELT's
  // artlist responses use 'articles', so the explicit keys above cover it.
  return [];
}

export function parseGdeltResponse(payload: unknown): NewsArticleItem[] {
  const items: NewsArticleItem[] = [];
  for (const article of extractArticles(payload)) {
    if (typeof article !== 'object' || article === null) continue;

    const url = article.url ?? article.url_mobile;
    if (!url) continue;

    const countryCode = (article.sourcecountry ?? article.country ?? '').toUpperCase();
    const centroid = COUNTRY_CENTROIDS[countryCode];
    if (!centroid) continue; // unknown country -> cannot place on the globe

    items.push({
      id: url,
      url,
      title: article.title ?? '(untitled)',
      domain: article.domain ?? null,
      language: article.language ?? null,
      sourcecountry: countryCode,
      lat: centroid.lat,
      lon: centroid.lon,
      seendate: article.seendate ?? null,
    });
  }
  return items;
}

// GDELT seendate is a compact UTC datetime: YYYYMMDDHHMMSS.
export function seendateEpochMs(seendate: string | null): number {
  if (seendate && /^\d{14}$/.test(seendate)) {
    const year = Number(seendate.slice(0, 4));
    const month = Number(seendate.slice(4, 6));
    const day = Number(seendate.slice(6, 8));
    const hour = Number(seendate.slice(8, 10));
    const minute = Number(seendate.slice(10, 12));
    const second = Number(seendate.slice(12, 14));
    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      return Date.UTC(year, month - 1, day, hour, minute, second);
    }
  }
  return Date.now();
}

export function buildQueryUrl(query: string): string {
  return `${GDELT_DOC_URL}?query=${encodeURIComponent(query)}&mode=artlist&format=json&maxrecords=${MAX_RECORDS}`;
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

export async function seedGlobalNews() {
  try {
    console.log(`[GlobalNewsGdelt] Polling GDELT DOC API with ${QUERIES.length} queries...`);

    // Dedupe across queries by article URL.
    const byUrl = new Map<string, NewsArticleItem>();
    const fetchedAt = Date.now();
    let queriesSucceeded = 0;

    for (const query of QUERIES) {
      try {
        const res = await withRetry(() => fetchWithTimeout(buildQueryUrl(query)));
        const data = (await res.json()) as unknown;
        const items = parseGdeltResponse(data);
        for (const item of items) byUrl.set(item.id, item);
        queriesSucceeded += 1;
        console.log(`[GlobalNewsGdelt] Query "${query}" -> ${items.length} mappable articles`);
      } catch (err) {
        // A throttled or failed single query must not kill the whole run.
        console.error(`[GlobalNewsGdelt] query "${query}" failed:`, err instanceof Error ? err.message : err);
      }
      // Respect the strict 5-second-per-request limit between queries.
      await new Promise((resolve) => setTimeout(resolve, QUERY_GAP_MS));
    }

    if (queriesSucceeded === 0) {
      console.error('[GlobalNewsGdelt] all queries failed; skipping snapshot');
      return;
    }

    const items = [...byUrl.values()];
    let insertedCount = 0;
    for (const item of items) {
      const result = insertArticle.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: seendateEpochMs(item.seendate),
        fetched_at: fetchedAt,
      });
      if (result.changes > 0) insertedCount++;
    }

    console.log(`[GlobalNewsGdelt] Parsed ${items.length} unique articles. Saved ${insertedCount} new to SQLite.`);

    // Save to Redis Live Cache
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
  } catch (err) {
    console.error('[GlobalNewsGdelt] seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: PLUGIN_ID,
  cron: '*/15 * * * *', // Every 15 minutes (modest polling for a strict rate limit)
  fn: seedGlobalNews,
};