import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

const GDACS_RSS_URL = 'https://www.gdacs.org/xml/rss_24h.xml';
const PLUGIN_ID = 'live-disasters';
const SNAPSHOT_TTL_SECONDS = 3600;

export interface LiveDisasterItem {
  id: string;
  title: string;
  description: string;
  link: string;
  lat: number;
  lon: number;
  alertlevel: string | null; // 'green' | 'orange' | 'red' | null
  severity: number | null; // numeric severity attribute (e.g. EQ magnitude, TC wind speed)
  severityText: string | null;
  eventtype: string | null; // GDACS event type code: EQ, TC, FL, DR, VO, ...
  iso3: string | null;
  country: string | null;
  pubDate: string; // ISO 8601
  occurredAt: number; // epoch ms
}

// Decode the XML entities GDACS uses inside text nodes.
export function unescapeXml(value: string): string {
  return value
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, '&');
}

// The RSS is a flat list of <item> blocks; regex extraction is enough and
// avoids pulling an XML parser into the bundler.
export function extractItemBlocks(xml: string): string[] {
  return xml.match(/<item>[\s\S]*?<\/item>/g) ?? [];
}

function stripCdata(text: string): string {
  const match = text.match(/^<!\[CDATA\[([\s\S]*)\]\]>$/);
  return match ? match[1] : text;
}

// Read the text of a (possibly namespaced) child tag, e.g. 'geo:lat' or
// 'gdacs:eventtype'. Returns null when the tag is absent or empty.
export function childText(block: string, tagName: string): string | null {
  const escaped = tagName.replace(/:/g, '\\:');
  const match = block.match(new RegExp(`<${escaped}(?:\\s[^>]*)?>([\\s\\S]*?)</${escaped}>`));
  if (!match) return null;
  const text = stripCdata(match[1]).trim();
  return text.length > 0 ? unescapeXml(text) : null;
}

// Read an attribute value from a (possibly namespaced) child tag, e.g.
// <gdacs:severity value="4.6" unit="M"> -> '4.6'.
export function childAttr(block: string, tagName: string, attrName: string): string | null {
  const escaped = tagName.replace(/:/g, '\\:');
  const match = block.match(new RegExp(`<${escaped}\\b[^>]*\\b${attrName}="([^"]*)"`));
  return match ? unescapeXml(match[1]) : null;
}

export function parseDate(value: string | null): { iso: string; epochMs: number } | null {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return { iso: date.toISOString(), epochMs: date.getTime() };
}

export function parseItemBlock(block: string, index: number): LiveDisasterItem | null {
  const guid = childText(block, 'guid');
  const title = childText(block, 'title');
  if (guid === null && title === null) return null;

  const lat = Number(childText(block, 'geo:lat'));
  const lon = Number(childText(block, 'geo:long'));
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

  // Only surface current, non-temporary alerts.
  if (childText(block, 'gdacs:temporary') === 'true') return null;
  const isCurrent = childText(block, 'gdacs:iscurrent');
  if (isCurrent !== null && isCurrent !== 'true') return null;

  const severityAttr = childAttr(block, 'gdacs:severity', 'value');
  const severityNumber = severityAttr !== null ? Number(severityAttr) : NaN;
  const pubDateRaw = childText(block, 'gdacs:fromdate') ?? childText(block, 'pubDate');
  const parsed = parseDate(pubDateRaw);
  const nowMs = Date.now();

  return {
    id: guid ?? `gdacs-${index}`,
    title: title ?? 'GDACS event',
    description: childText(block, 'description') ?? '',
    link: childText(block, 'link') ?? '',
    lat,
    lon,
    alertlevel: childText(block, 'gdacs:alertlevel')?.toLowerCase() ?? null,
    severity: Number.isFinite(severityNumber) ? severityNumber : null,
    severityText: childText(block, 'gdacs:severity'),
    eventtype: childText(block, 'gdacs:eventtype'),
    iso3: childText(block, 'gdacs:iso3'),
    country: childText(block, 'gdacs:country'),
    pubDate: parsed?.iso ?? new Date(nowMs).toISOString(),
    occurredAt: parsed?.epochMs ?? nowMs,
  };
}

// Ensure the per-plugin SQLite table exists (INSERT OR IGNORE below requires
// it). The engine may pre-create tables, so IF NOT EXISTS keeps this safe.
try {
  db.prepare(
    'CREATE TABLE IF NOT EXISTS live_disasters (id TEXT PRIMARY KEY, payload TEXT NOT NULL, source_ts INTEGER, fetched_at INTEGER)'
  ).run();
} catch (err) {
  console.error('[LiveDisasters] could not ensure SQLite table:', err instanceof Error ? err.message : err);
}

const insertLiveDisaster = db.prepare(
  'INSERT OR IGNORE INTO live_disasters (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)'
);

export async function seedLiveDisasters() {
  try {
    console.log('[LiveDisasters] Polling GDACS RSS...');

    const res = await withRetry(() => fetchWithTimeout(GDACS_RSS_URL));
    const xml = await res.text();
    const fetchedAt = Date.now();

    const items = extractItemBlocks(xml)
      .map((block, i) => parseItemBlock(block, i))
      .filter((item): item is LiveDisasterItem => item !== null);

    let insertedCount = 0;
    for (const item of items) {
      const result = insertLiveDisaster.run({
        id: item.id,
        payload: JSON.stringify(item),
        source_ts: item.occurredAt,
        fetched_at: fetchedAt,
      });
      if (result.changes > 0) insertedCount++;
    }

    console.log(`[LiveDisasters] Parsed ${items.length} events. Saved ${insertedCount} new to SQLite.`);

    // Save to Redis Live Cache
    await setLiveSnapshot(
      PLUGIN_ID,
      {
        source: PLUGIN_ID,
        fetchedAt: new Date().toISOString(),
        items,
        totalCount: items.length,
      },
      SNAPSHOT_TTL_SECONDS // 1 hour TTL
    );
  } catch (err) {
    console.error('[LiveDisasters] seeder failed:', err instanceof Error ? err.message : err);
  }
}

export default {
  name: PLUGIN_ID,
  cron: '0 * * * *', // Every hour
  fn: seedLiveDisasters,
};