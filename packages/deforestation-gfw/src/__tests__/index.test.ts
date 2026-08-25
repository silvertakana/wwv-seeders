// Unit tests for the deforestation-gfw seeder.
//
// The @worldwideview/seeder-sdk is mocked so better-sqlite3 native bindings and
// Redis are never loaded in the test environment. Fixtures are REAL responses
// captured from the live GFW API on 2026-08-25 (fires tile GET + GLAD adm2
// query POST), sanitized to strip SDK/transport fields.
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { readFileSync } from 'node:fs';

import type { FireFeature, GladAlertRow } from '../parse';
import { GFW_HOME_URL } from '../parse';

const fireFixture = JSON.parse(
  readFileSync(new URL('./fixtures/fire-tile.json', import.meta.url), 'utf8')
) as { data: FireFeature[]; status: string };
const gladFixture = JSON.parse(
  readFileSync(new URL('./fixtures/glad-query.json', import.meta.url), 'utf8')
) as { data: GladAlertRow[]; status: string };

vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(),
  db: {
    prepare: vi.fn(() => ({
      run: vi.fn(() => ({ changes: 1 })),
      get: vi.fn(),
      all: vi.fn(),
    })),
  },
}));

import { db, setLiveSnapshot, fetchWithTimeout } from '@worldwideview/seeder-sdk';
import {
  mapFireFeature,
  mapGladRow,
  parseFiresResponse,
  parseGladQueryResponse,
  dedupeGladRows,
} from '../parse';
import { lookupAdm2Centroid, ADM2_CENTROID_CAP } from '../centroids';
import seeder, { seedDeforestationGfw } from '../index';

const mockedSetLiveSnapshot = vi.mocked(setLiveSnapshot);
const mockedFetch = vi.mocked(fetchWithTimeout);

describe('deforestation-gfw parser: FIRES layer', () => {
  it('maps a real VIIRS fire feature to a globe-ready item', () => {
    const item = mapFireFeature(fireFixture.data[0]);
    expect(item).not.toBeNull();
    expect(Number.isFinite(item!.lat)).toBe(true);
    expect(Number.isFinite(item!.lon)).toBe(true);
    expect(item!.alertType).toBe('fire');
    expect(item!.confidence).toBe(fireFixture.data[0].confidence__cat);
    expect(item!.date).toBe(fireFixture.data[0].alert__date);
    expect(item!.url).toBe(GFW_HOME_URL);
    expect(item!.id).toMatch(/^fire-/);
  });

  it('parses every feature in the real tile fixture', () => {
    const items = parseFiresResponse(fireFixture);
    expect(items.length).toBe(fireFixture.data.length);
    for (const item of items) {
      expect(Number.isFinite(item.lat)).toBe(true);
      expect(Number.isFinite(item.lon)).toBe(true);
      expect(item.alertType).toBe('fire');
    }
  });

  it('drops fire features without finite coordinates', () => {
    expect(mapFireFeature({ latitude: 'n/a', longitude: 20 })).toBeNull();
    expect(mapFireFeature({ latitude: null, longitude: null })).toBeNull();
    expect(mapFireFeature({})).toBeNull();
    expect(mapFireFeature({ latitude: 12, longitude: Number.NaN })).toBeNull();
  });
});

describe('deforestation-gfw parser: DEFORESTATION layer', () => {
  it('maps a real GLAD row at its known centroid', () => {
    const row = gladFixture.data[0];
    const centroid = lookupAdm2Centroid(String(row.iso), Number(row.adm1), Number(row.adm2));
    expect(centroid).not.toBeNull();
    const item = mapGladRow(row, centroid);
    expect(item).not.toBeNull();
    expect(Number.isFinite(item!.lat)).toBe(true);
    expect(Number.isFinite(item!.lon)).toBe(true);
    expect(item!.alertType).toBe('deforestation');
    expect(item!.date).toBe(row.umd_glad_landsat_alerts__date);
    expect(item!.confidence).toBe(row.umd_glad_landsat_alerts__confidence);
    expect(item!.url).toBe(GFW_HOME_URL);
    expect(item!.id).toBe(`glad-${row.iso}-${row.adm1}-${row.adm2}`);
  });

  it('places every real GLAD fixture row (all are within the centroid cap)', () => {
    const items = parseGladQueryResponse(gladFixture, lookupAdm2Centroid);
    expect(items.length).toBe(gladFixture.data.length);
    for (const item of items) {
      expect(Number.isFinite(item.lat)).toBe(true);
      expect(Number.isFinite(item.lon)).toBe(true);
      expect(item.alertType).toBe('deforestation');
    }
    expect(ADM2_CENTROID_CAP).toBeGreaterThan(0);
    expect(ADM2_CENTROID_CAP).toBeLessThanOrEqual(1000);
  });

  it('drops GLAD rows that cannot be placed (outside the centroid cap)', () => {
    const row: GladAlertRow = { iso: 'ATA', adm1: 1, adm2: 1, umd_glad_landsat_alerts__date: '2026-07-01' };
    expect(lookupAdm2Centroid('ATA', 1, 1)).toBeNull();
    expect(mapGladRow(row, lookupAdm2Centroid('ATA', 1, 1))).toBeNull();
    expect(mapGladRow(row, null)).toBeNull();
  });

  it('drops malformed GLAD rows (missing iso / non-numeric adm ids)', () => {
    expect(mapGladRow({ adm1: 1, adm2: 2 }, lookupAdm2Centroid('X', 1, 2))).toBeNull();
    expect(mapGladRow({ iso: 'IDN', adm1: 'x', adm2: 2 }, lookupAdm2Centroid('IDN', 1, 2))).toBeNull();
  });

  it('dedupes GLAD rows by region, keeping the first occurrence', () => {
    const a: GladAlertRow = { iso: 'AGO', adm1: 1, adm2: 1, umd_glad_landsat_alerts__date: '2026-07-01' };
    const b: GladAlertRow = { iso: 'AGO', adm1: 1, adm2: 1, umd_glad_landsat_alerts__date: '2026-07-02' };
    const c: GladAlertRow = { iso: 'AGO', adm1: 2, adm2: 1, umd_glad_landsat_alerts__date: '2026-07-03' };
    const rows = dedupeGladRows([a, b, c, { iso: 'AGO', adm1: 2, adm2: 1, umd_glad_landsat_alerts__date: '2026-07-04' }]);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toBe(a);
    expect(rows[1]).toBe(c);
  });
});

describe('deforestation-gfw seeder contract', () => {
  it('default export has name === "deforestation-gfw"', () => {
    expect(seeder.name).toBe('deforestation-gfw');
  });

  it('default export has a modest interval (25 min)', () => {
    expect(seeder.interval).toBe(25 * 60 * 1000);
  });

  it('default export has fetch as a function (engine interval scheduler shape)', () => {
    expect(typeof seeder.fetch).toBe('function');
  });

  it('creates the self-guarded SQLite table at module scope', () => {
    const createCalls = vi.mocked(db.prepare).mock.calls.map((args) => args[0]);
    expect(createCalls.some((sql) => String(sql).includes('CREATE TABLE IF NOT EXISTS deforestation_alerts'))).toBe(true);
  });
});

describe('deforestation-gfw end-to-end fetch (mocked SDK)', () => {
  beforeEach(() => {
    mockedSetLiveSnapshot.mockClear();
    mockedFetch.mockReset();
    mockedFetch.mockImplementation(async (url: string | URL) => {
      const href = String(url);
      return {
        ok: true,
        status: 200,
        json: async () => (href.includes('/query') ? gladFixture : fireFixture),
      } as unknown as Response;
    });
    vi.stubEnv('GFW_GLAD_API_KEY', 'fixture-key');
  });

  it('collects fire + deforestation items, persists rows, and publishes a snapshot', async () => {
    const items = await seedDeforestationGfw();
    expect(items.length).toBeGreaterThan(0);
    const alertTypes = new Set(items.map((i) => i.alertType));
    expect(alertTypes.has('fire')).toBe(true);
    expect(alertTypes.has('deforestation')).toBe(true);

    const insertCalls = vi.mocked(db.prepare).mock.calls;
    expect(insertCalls.length).toBeGreaterThanOrEqual(2); // CREATE TABLE + INSERT
    const insertSql = insertCalls.map((args) => String(args[0])).find((s) => s.includes('INSERT OR IGNORE'));
    expect(insertSql).toBeTruthy();

    expect(mockedSetLiveSnapshot).toHaveBeenCalledTimes(1);
    const [pluginId, snapshot] = mockedSetLiveSnapshot.mock.calls[0];
    expect(pluginId).toBe('deforestation-gfw');
    expect((snapshot as { items: unknown[] }).items).toHaveLength(items.length);
  });
});