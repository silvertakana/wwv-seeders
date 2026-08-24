// Unit tests for the live-disasters seeder's pure logic (GDACS RSS parsing,
// item mapping, and end-to-end snapshot wiring).
//
// The @worldwideview/seeder-sdk is fully mocked so better-sqlite3 native
// bindings never load in the test environment (same pattern as
// packages/earthquakes/src/__tests__/index.test.ts).
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn((fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(async () => ({ text: async () => '' })),
  db: {
    prepare: vi.fn(() => ({
      run: vi.fn(() => ({ changes: 0 })),
    })),
  },
}));

import {
  unescapeXml,
  extractItemBlocks,
  childText,
  childAttr,
  parseItemBlock,
  seedLiveDisasters,
  type LiveDisasterItem,
} from '../index';
import { setLiveSnapshot, fetchWithTimeout, withRetry, db } from '@worldwideview/seeder-sdk';

// db.prepare is invoked once per top-level statement at module load
// (index 0 = CREATE TABLE IF NOT EXISTS guard, index 1 = INSERT OR IGNORE).
// Capture the run mock for the INSERT so tests can assert per-item inserts.
const prepareMock = vi.mocked(db.prepare);
const createTableSql = prepareMock.mock.calls[0][0] as string;
const insertSql = prepareMock.mock.calls[1][0] as string;
const insertRunMock = prepareMock.mock.results[1].value.run as ReturnType<typeof vi.fn>;

const EQ_FIXTURE = `<?xml version="1.0" encoding="UTF-8"?>
<rss><channel>
<item>
  <title>Green earthquake (Magnitude 4.6M, Depth:10km) in Indonesia 24/08/2026 01:12 UTC, 1.2 million in 100km.</title>
  <description>On 8/24/2026 1:12:59 AM, an earthquake occurred in Indonesia potentially affecting 1.2 million in 100km.</description>
  <link>https://www.gdacs.org/report.aspx?eventtype=EQ&amp;eventid=1561430</link>
  <pubDate>Mon, 24 Aug 2026 01:58:31 GMT</pubDate>
  <gdacs:fromdate>Mon, 24 Aug 2026 01:12:59 GMT</gdacs:fromdate>
  <gdacs:iscurrent>true</gdacs:iscurrent>
  <gdacs:temporary>false</gdacs:temporary>
  <guid isPermaLink="false">EQ1561430</guid>
  <geo:Point><geo:lat>-8.2203</geo:lat><geo:long>120.4857</geo:long></geo:Point>
  <gdacs:eventtype>EQ</gdacs:eventtype>
  <gdacs:alertlevel>Green</gdacs:alertlevel>
  <gdacs:severity value="4.6" unit="M">Magnitude 4.6M, Depth:10km</gdacs:severity>
  <gdacs:iso3>IDN</gdacs:iso3>
  <gdacs:country>Indonesia</gdacs:country>
</item>
<item>
  <title>Orange Tropical Storm ISELLE-26 (maximum wind speed of 120 km/h)</title>
  <description>A tropical storm is active near Fiji.</description>
  <link>https://www.gdacs.org/report.aspx?eventtype=TC&amp;eventid=1001309</link>
  <gdacs:fromdate>Sun, 23 Aug 2026 20:00:00 GMT</gdacs:fromdate>
  <gdacs:iscurrent>true</gdacs:iscurrent>
  <gdacs:temporary>false</gdacs:temporary>
  <guid isPermaLink="false">TC1001309</guid>
  <geo:Point><geo:lat>-17.5</geo:lat><geo:long>178.2</geo:long></geo:Point>
  <gdacs:eventtype>TC</gdacs:eventtype>
  <gdacs:alertlevel>Orange</gdacs:alertlevel>
  <gdacs:severity value="120" unit="km/h">Tropical Storm</gdacs:severity>
  <gdacs:iso3>FJI</gdacs:iso3>
  <gdacs:country>Fiji</gdacs:country>
</item>
</channel></rss>`;

beforeEach(() => {
  vi.clearAllMocks();
});

describe('unescapeXml', () => {
  it('decodes XML entities used in RSS text nodes', () => {
    expect(unescapeXml('EQ &amp; TC &lt;drill&gt; &quot;q&quot; &#39;a&#39;')).toBe(
      'EQ & TC <drill> "q" \'a\''
    );
  });
});

describe('extractItemBlocks', () => {
  it('extracts every <item> block', () => {
    const blocks = extractItemBlocks(EQ_FIXTURE);
    expect(blocks).toHaveLength(2);
    expect(blocks[0]).toContain('EQ1561430');
  });
});

describe('childText / childAttr', () => {
  const block = extractItemBlocks(EQ_FIXTURE)[0];

  it('reads plain and namespaced child tags', () => {
    expect(childText(block, 'title')).toContain('Green earthquake');
    expect(childText(block, 'gdacs:eventtype')).toBe('EQ');
    expect(childText(block, 'gdacs:alertlevel')).toBe('Green');
  });

  it('reads geo:lat inside the geo:Point wrapper', () => {
    expect(childText(block, 'geo:lat')).toBe('-8.2203');
    expect(childText(block, 'geo:long')).toBe('120.4857');
  });

  it('decodes entities inside tag text', () => {
    expect(childText(block, 'link')).toBe(
      'https://www.gdacs.org/report.aspx?eventtype=EQ&eventid=1561430'
    );
  });

  it('returns null for a missing tag', () => {
    expect(childText(block, 'gdacs:eventname')).toBeNull();
  });

  it('reads an attribute from a namespaced tag', () => {
    expect(childAttr(block, 'gdacs:severity', 'value')).toBe('4.6');
    expect(childAttr(block, 'gdacs:severity', 'unit')).toBe('M');
  });
});

describe('parseItemBlock', () => {
  it('maps a full GDACS item (EQ)', () => {
    const item = parseItemBlock(extractItemBlocks(EQ_FIXTURE)[0], 0);

    expect(item).not.toBeNull();
    expect(item).toMatchObject({
      id: 'EQ1561430',
      lat: -8.2203,
      lon: 120.4857,
      alertlevel: 'green',
      severity: 4.6,
      eventtype: 'EQ',
      iso3: 'IDN',
      country: 'Indonesia',
    });
    expect(item?.link).toBe('https://www.gdacs.org/report.aspx?eventtype=EQ&eventid=1561430');
    expect(item?.pubDate).toBe('2026-08-24T01:12:59.000Z');
    expect(item?.occurredAt).toBe(new Date('2026-08-24T01:12:59.000Z').getTime());
  });

  it('maps a tropical cyclone item (alertlevel stays lowercase)', () => {
    const item = parseItemBlock(extractItemBlocks(EQ_FIXTURE)[1], 1) as LiveDisasterItem;

    expect(item.id).toBe('TC1001309');
    expect(item.eventtype).toBe('TC');
    expect(item.alertlevel).toBe('orange');
    expect(item.severity).toBe(120);
    expect(item.lat).toBe(-17.5);
    expect(item.lon).toBe(178.2);
  });

  it('rejects an item with non-finite coordinates', () => {
    const block = extractItemBlocks(EQ_FIXTURE)[0].replace('<geo:lat>-8.2203</geo:lat>', '<geo:lat>abc</geo:lat>');
    expect(parseItemBlock(block, 0)).toBeNull();
  });

  it('skips temporary alerts', () => {
    const block = extractItemBlocks(EQ_FIXTURE)[0].replace(
      '<gdacs:temporary>false</gdacs:temporary>',
      '<gdacs:temporary>true</gdacs:temporary>'
    );
    expect(parseItemBlock(block, 0)).toBeNull();
  });

  it('skips non-current alerts', () => {
    const block = extractItemBlocks(EQ_FIXTURE)[0].replace(
      '<gdacs:iscurrent>true</gdacs:iscurrent>',
      '<gdacs:iscurrent>false</gdacs:iscurrent>'
    );
    expect(parseItemBlock(block, 0)).toBeNull();
  });

  it('falls back to an index-based id when guid is missing', () => {
    const block = extractItemBlocks(EQ_FIXTURE)[0].replace(
      '<guid isPermaLink="false">EQ1561430</guid>',
      ''
    );
    const item = parseItemBlock(block, 7);
    expect(item?.id).toBe('gdacs-7');
    expect(item?.title).toContain('Green earthquake');
  });

  it('returns null for a block with neither title nor guid', () => {
    expect(parseItemBlock('<item><link>https://x</link></item>', 0)).toBeNull();
  });
});

describe('seedLiveDisasters integration', () => {
  it('fetches, parses, persists, and snapshots end-to-end with mocked IO', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      text: async () => EQ_FIXTURE,
    } as never);

    await seedLiveDisasters();

    expect(withRetry).toHaveBeenCalledTimes(1);
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'live-disasters',
      expect.objectContaining({
        source: 'live-disasters',
        totalCount: 2,
        items: expect.any(Array),
      }),
      3600
    );
    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as {
      items: LiveDisasterItem[];
      totalCount: number;
    };
    expect(snapshot.items).toHaveLength(snapshot.totalCount);
    expect(snapshot.items[0]).toMatchObject({
      id: 'EQ1561430',
      lat: -8.2203,
      lon: 120.4857,
      alertlevel: 'green',
      eventtype: 'EQ',
    });
    expect(snapshot.items[1]).toMatchObject({ id: 'TC1001309', eventtype: 'TC' });
  });

  it('persists each item with id, JSON payload, source_ts, and fetched_at', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      text: async () => EQ_FIXTURE,
    } as never);

    await seedLiveDisasters();

    expect(insertRunMock).toHaveBeenCalledTimes(2);
    const args = insertRunMock.mock.calls[0][0] as {
      id: string;
      payload: string;
      source_ts: number;
      fetched_at: number;
    };
    expect(args.id).toBe('EQ1561430');
    expect(args.source_ts).toBe(new Date('2026-08-24T01:12:59.000Z').getTime());
    expect(typeof args.fetched_at).toBe('number');
    const payload = JSON.parse(args.payload) as LiveDisasterItem;
    expect(payload).toMatchObject({ id: 'EQ1561430', eventtype: 'EQ', alertlevel: 'green' });
  });

  it('logs and skips the snapshot when the fetch fails', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('network down'));

    await expect(seedLiveDisasters()).resolves.toBeUndefined();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});

describe('module-level SQLite wiring', () => {
  it('ensures the live_disasters table with a CREATE TABLE IF NOT EXISTS guard', () => {
    expect(createTableSql).toContain('CREATE TABLE IF NOT EXISTS live_disasters');
    expect(createTableSql).toContain('id TEXT PRIMARY KEY');
  });

  it('prepares an INSERT OR IGNORE against the live_disasters table', () => {
    expect(insertSql).toContain('INSERT OR IGNORE INTO live_disasters');
  });
});

describe('default export contract', () => {
  it('registers as "live-disasters" on an hourly cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('live-disasters');
    expect(seeder.cron).toBe('0 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});