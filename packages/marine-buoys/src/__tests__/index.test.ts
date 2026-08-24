// Unit tests for the marine-buoys seeder's pure logic (NDBC latest_obs.txt
// parsing) and the seeder wiring (SQLite insert + live snapshot).
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

import { parseLatestObs, parseObsValue, seedMarineBuoys, type BuoyObservation } from '../index';
import { setLiveSnapshot, fetchWithTimeout, withRetry, db } from '@worldwideview/seeder-sdk';

// db.prepare is invoked once at module load (top-level insertBuoy statement).
const prepareMock = vi.mocked(db.prepare);
const insertRunMock = prepareMock.mock.results[0].value.run as ReturnType<typeof vi.fn>;

// A slice of the REAL NOAA NDBC latest_obs.txt feed (fetched and verified live
// 2026-08-24): the #STN header line, the #text units line, data rows with
// numeric wave heights, "MM" missing markers, an alphanumeric station (AAMC1),
// a "+" prefixed pressure value, and one row with non-numeric coordinates.
const SAMPLE_OBS = [
  '#STN       LAT      LON  YYYY MM DD hh mm WDIR WSPD   GST WVHT  DPD APD MWD   PRES  PTDY  ATMP  WTMP  DEWP  VIS   TIDE',
  '#text      deg      deg   yr mo day hr mn degT  m/s   m/s   m   sec sec degT   hPa   hPa  degC  degC  degC  nmi     ft',
  '14049   -12.000   65.000 2026 08 24 01 00 123   6.8   8.4   MM  MM   MM  MM 1016.8    MM  20.1  26.7    MM   MM     MM',
  '22101    37.24   126.02  2026 08 24 02 00 170   2.0    MM  0.5   0   MM  MM     MM    MM  26.1  26.5    MM   MM     MM',
  'AAMC1    37.772 -122.300 2026 08 24 02 00 260   5.7   6.7   MM  MM   MM  MM 1012.2    MM  17.5  21.5    MM   MM     MM',
  'ACYN4    39.357  -74.418 2026 08 24 02 00  MM    MM    MM   MM  MM   MM  MM 1013.0  +2.2  25.2  24.9    MM   MM     MM',
  '60000       MM   -5.000 2026 08 24 02 00 100   1.0    MM   MJ  1   MM  MM     MM    MM  10.0  11.0    MM   MM     MM',
].join('\n');

beforeEach(() => {
  vi.clearAllMocks();
});

describe('parseObsValue', () => {
  it('parses finite numbers', () => {
    expect(parseObsValue('6.8')).toBe(6.8);
    expect(parseObsValue('-12.000')).toBe(-12);
    expect(parseObsValue('+2.2')).toBe(2.2);
    expect(parseObsValue('0')).toBe(0);
  });

  it('maps MM, empty, and undefined to null', () => {
    expect(parseObsValue('MM')).toBeNull();
    expect(parseObsValue('')).toBeNull();
    expect(parseObsValue(undefined)).toBeNull();
  });

  it('maps non-numeric tokens to null', () => {
    expect(parseObsValue('MJ')).toBeNull();
    expect(parseObsValue('abc')).toBeNull();
  });
});

describe('parseLatestObs', () => {
  it('parses the header line and real data rows into buoy observations', () => {
    const rows = parseLatestObs(SAMPLE_OBS);

    expect(rows).toHaveLength(4); // 60000 row has MM LAT -> skipped

    expect(rows[0]).toMatchObject<Partial<BuoyObservation>>({
      stn: '14049',
      lat: -12,
      lon: 65,
      year: 2026,
      month: 8,
      day: 24,
      hour: 1,
      minute: 0,
      wdir: 123,
      wspd: 6.8,
      gst: 8.4,
      wvht: null, // MM
      dpd: null, // MM
      pres: 1016.8,
      ptdy: null, // MM
      atmp: 20.1,
      wtmp: 26.7,
    });
  });

  it('keeps numeric wave-height and wind values', () => {
    const rows = parseLatestObs(SAMPLE_OBS);

    const korea = rows.find((r) => r.stn === '22101');
    expect(korea).toBeDefined();
    expect(korea!.wvht).toBe(0.5);
    expect(korea!.dpd).toBe(0);
    expect(korea!.wspd).toBe(2.0);
    expect(korea!.gst).toBeNull(); // MM
    expect(korea!.lat).toBe(37.24);
    expect(korea!.lon).toBe(126.02);
  });

  it('handles alphanumeric station ids (C-MAN stations)', () => {
    const rows = parseLatestObs(SAMPLE_OBS);

    const coastGuard = rows.find((r) => r.stn === 'AAMC1');
    expect(coastGuard).toBeDefined();
    expect(coastGuard!.lat).toBe(37.772);
    expect(coastGuard!.lon).toBe(-122.3);
    expect(coastGuard!.wdir).toBe(260);
    expect(coastGuard!.wspd).toBe(5.7);
    expect(coastGuard!.pres).toBe(1012.2);
  });

  it('parses "+"-prefixed values (NDBC pressure trend)', () => {
    const rows = parseLatestObs(SAMPLE_OBS);

    const delaware = rows.find((r) => r.stn === 'ACYN4');
    expect(delaware).toBeDefined();
    expect(delaware!.ptdy).toBe(2.2);
    expect(delaware!.wdir).toBeNull(); // MM wind direction
    expect(delaware!.wspd).toBeNull(); // MM wind speed
  });

  it('skips rows with non-finite lat/lon (MM LAT)', () => {
    const rows = parseLatestObs(SAMPLE_OBS);

    expect(rows.some((r) => r.stn === '60000')).toBe(false);
  });

  it('skips empty lines and unit/comment lines', () => {
    const withComment = `${SAMPLE_OBS}\n# some trailing comment\n\n`;
    const rows = parseLatestObs(withComment);

    expect(rows).toHaveLength(4);
  });
});

describe('seedMarineBuoys integration', () => {
  it('fetches, parses, persists, and snapshots end-to-end with mocked IO', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      text: async () => SAMPLE_OBS,
    } as never);

    await seedMarineBuoys();

    expect(withRetry).toHaveBeenCalledTimes(1);
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'marine-buoys',
      expect.objectContaining({
        source: 'marine-buoys',
        totalCount: 4,
        items: expect.any(Array),
      }),
      3600
    );
    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as {
      items: BuoyObservation[];
      totalCount: number;
    };
    expect(snapshot.items).toHaveLength(snapshot.totalCount);
    expect(snapshot.items[0]).toMatchObject({
      stn: '14049',
      lat: -12,
      lon: 65,
    });
  });

  it('persists each row with stn, JSON payload, source_ts, and fetched_at', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      text: async () => SAMPLE_OBS,
    } as never);

    await seedMarineBuoys();

    expect(insertRunMock).toHaveBeenCalledTimes(4);
    const args = insertRunMock.mock.calls[0][0] as {
      stn: string;
      payload: string;
      source_ts: number;
      fetched_at: number;
    };
    expect(args.stn).toBe('14049');
    expect(typeof args.source_ts).toBe('number');
    expect(typeof args.fetched_at).toBe('number');
    const payload = JSON.parse(args.payload) as BuoyObservation;
    expect(payload).toMatchObject({
      stn: '14049',
      lat: -12,
      lon: 65,
      atmp: 20.1,
    });
  });

  it('survives a fetch failure and does not snapshot', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('HTTP 500'));

    await seedMarineBuoys();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});

describe('default export contract', () => {
  it('registers as "marine-buoys" on an hourly cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('marine-buoys');
    expect(seeder.cron).toBe('0 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});