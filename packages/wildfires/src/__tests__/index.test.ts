// Unit tests for the wildfires seeder's pure logic (CSV parsing, multi-tier
// clustering, id stability, and timestamp building).
//
// The @worldwideview/seeder-sdk is fully mocked so better-sqlite3 native
// bindings never load in the test environment (same pattern as
// packages/market-tracker/src/__tests__/seederContract.test.ts).
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn((fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(async () => ({ text: async () => '' })),
  db: {
    prepare: vi.fn(() => ({
      run: vi.fn(() => ({ changes: 0 })),
    })),
    transaction: vi.fn((fn: (list: unknown[]) => unknown) => fn),
  },
}));

import {
  buildSourceTs,
  clusterFires,
  parseCSV,
  DEFAULT_TIERS,
  type FIRMSRecord,
} from '../index';
import { seedWildfires } from '../index';
import {
  setLiveSnapshot,
  fetchWithTimeout,
  withRetry,
} from '@worldwideview/seeder-sdk';

const CSV_HEADER =
  'latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,confidence,version,bright_ti5,frp,daynight';

function makeFire(partial: Partial<FIRMSRecord> = {}): FIRMSRecord {
  return {
    latitude: 34.123,
    longitude: -118.456,
    bright_ti4: 300,
    scan: 0.5,
    track: 0.5,
    acq_date: '2024-04-01',
    acq_time: '1430',
    satellite: 'NPP',
    confidence: 'nominal',
    version: '2.0',
    bright_ti5: 250,
    frp: 10,
    daynight: 'D',
    ...partial,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe('parseCSV', () => {
  it('returns [] for empty input', () => {
    expect(parseCSV('')).toEqual([]);
  });

  it('returns [] for whitespace-only input', () => {
    expect(parseCSV('   \n  \n ')).toEqual([]);
  });

  it('returns [] for header-only input', () => {
    expect(parseCSV(CSV_HEADER)).toEqual([]);
  });

  it('parses a well-formed row with numeric conversion and trimmed values', () => {
    const csv =
      `${CSV_HEADER}\n` +
      '34.5,-118.2,320.1,0.4,0.6,2024-04-01,1430,NPP,high,2.0,280.7,12.5,D';
    const rows = parseCSV(csv);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      latitude: 34.5,
      longitude: -118.2,
      bright_ti4: 320.1,
      scan: 0.4,
      track: 0.6,
      acq_date: '2024-04-01',
      acq_time: '1430',
      satellite: 'NPP',
      confidence: 'high',
      version: '2.0',
      bright_ti5: 280.7,
      frp: 12.5,
      daynight: 'D',
    });
  });

  it('skips rows with fewer values than headers (short rows)', () => {
    const csv =
      `${CSV_HEADER}\n` +
      '34.5,-118.2,320.1,0.4,0.6,2024-04-01,1430,NPP,high\n' +
      '34.6,-118.3,300.0,0.5,0.5,2024-04-01,1430,NPP,high,2.0,250.0,8.0,D';
    const rows = parseCSV(csv);

    expect(rows).toHaveLength(1);
    expect(rows[0].latitude).toBe(34.6);
  });

  it('skips rows with non-numeric latitude or longitude', () => {
    const csv =
      `${CSV_HEADER}\n` +
      'abc,-118.2,320.1,0.4,0.6,2024-04-01,1430,NPP,high,2.0,280.7,12.5,D\n' +
      '34.5,xyz,320.1,0.4,0.6,2024-04-01,1430,NPP,high,2.0,280.7,12.5,D';
    expect(parseCSV(csv)).toEqual([]);
  });

  it('defaults missing numeric fields to 0', () => {
    const csv =
      `${CSV_HEADER}\n` +
      '34.5,-118.2,,,,2024-04-01,1430,NPP,high,2.0,,,D';
    const rows = parseCSV(csv);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      bright_ti4: 0,
      scan: 0,
      track: 0,
      bright_ti5: 0,
      frp: 0,
    });
  });

  it('defaults missing string fields to empty string', () => {
    const csv = `${CSV_HEADER}\n34.5,-118.2,300,0.5,0.5,,,,,,,,`;
    const rows = parseCSV(csv);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      acq_date: '',
      acq_time: '',
      satellite: '',
      confidence: '',
      version: '',
      daynight: '',
    });
  });

  it('tolerates extra columns beyond the header (extras are dropped)', () => {
    const csv =
      `${CSV_HEADER}\n` +
      '34.5,-118.2,320.1,0.4,0.6,2024-04-01,1430,NPP,high,2.0,280.7,12.5,D,EXTRA1,EXTRA2';
    const rows = parseCSV(csv);

    expect(rows).toHaveLength(1);
    expect(rows[0].daynight).toBe('D');
  });

  it('trims header names and cell values', () => {
    const csv =
      ` latitude , longitude , bright_ti4 ,scan,track,acq_date,acq_time,satellite,confidence,version,bright_ti5,frp,daynight\n` +
      ' 34.5 , -118.2 ,320.1,0.4,0.6,2024-04-01,1430,NPP,high,2.0,280.7,12.5,D';
    const rows = parseCSV(csv);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ latitude: 34.5, longitude: -118.2 });
  });

  it('handles CRLF line endings', () => {
    const csv =
      `${CSV_HEADER}\r\n` +
      '34.5,-118.2,320.1,0.4,0.6,2024-04-01,1430,NPP,high,2.0,280.7,12.5,D\r\n' +
      '34.6,-118.3,300.0,0.5,0.5,2024-04-01,1430,NPP,low,2.0,250.0,8.0,D';
    const rows = parseCSV(csv);

    expect(rows).toHaveLength(2);
    expect(rows[0].confidence).toBe('high');
    expect(rows[1].confidence).toBe('low');
  });
});

describe('clusterFires', () => {
  it('emits one entry per default tier for a single fire', () => {
    const out = clusterFires([makeFire()]);

    expect(out).toHaveLength(DEFAULT_TIERS.length);
    expect(out.map((f) => f.tier)).toEqual([1, 2, 3]);
  });

  it('merges same-cell fires at coarse tiers and splits them at fine tiers', () => {
    const a = makeFire({ latitude: 1.0, longitude: 2.0, frp: 10, confidence: 'low' });
    const b = makeFire({ latitude: 1.1, longitude: 2.1, frp: 20, confidence: 'nominal' });
    const out = clusterFires([a, b]);

    // tier 1 (2.0 deg): same cell -> merged (1 entry)
    // tier 2 (0.5 deg): same cell -> merged (1 entry)
    // tier 3 (0.05 deg): different cells -> split (2 entries)
    expect(out).toHaveLength(4);
    expect(out.filter((f) => f.tier === 1)).toHaveLength(1);
    expect(out.filter((f) => f.tier === 2)).toHaveLength(1);
    expect(out.filter((f) => f.tier === 3)).toHaveLength(2);
  });

  it('sums frp when merging same-cell fires', () => {
    const a = makeFire({ latitude: 1.0, longitude: 2.0, frp: 10 });
    const b = makeFire({ latitude: 1.1, longitude: 2.1, frp: 20 });
    const out = clusterFires([a, b]);

    const merged = out.find((f) => f.tier === 1);
    expect(merged?.frp).toBe(30);
  });

  it('keeps the first fire id when merging same-cell fires', () => {
    const a = makeFire({ latitude: 1.0, longitude: 2.0 });
    const b = makeFire({ latitude: 1.1, longitude: 2.1 });
    const out = clusterFires([a, b]);

    const merged = out.find((f) => f.tier === 1);
    expect(merged?.id).toBe(
      `firm_${a.acq_date}_${a.acq_time}_${Math.round(a.latitude * 1000)}_${Math.round(a.longitude * 1000)}_t1`
    );
  });

  it('escalates confidence low -> nominal -> high on merge', () => {
    const base = { latitude: 1.0, longitude: 2.0 } as const;
    const low = makeFire({ ...base, confidence: 'low' });
    const nominal = makeFire({ ...base, confidence: 'nominal' });
    const high = makeFire({ ...base, confidence: 'high' });

    const cases: Array<[FIRMSRecord, FIRMSRecord, string]> = [
      [low, nominal, 'nominal'],
      [low, high, 'high'],
      [nominal, high, 'high'],
      [nominal, low, 'nominal'],
      [high, nominal, 'high'],
      [high, low, 'high'],
    ];

    for (const [first, second, expected] of cases) {
      const out = clusterFires([first, second]);
      const merged = out.find((f) => f.tier === 1);
      expect(merged?.confidence).toBe(expected);
    }
  });

  it('splits fires on opposite sides of a tier-1 grid boundary', () => {
    const a = makeFire({ latitude: 1.99, longitude: 2.0 });
    const b = makeFire({ latitude: 2.01, longitude: 2.0 });
    const tiers = [{ level: 1, size: 2.0 }];
    const out = clusterFires([a, b], tiers);

    expect(out).toHaveLength(2);
  });

  it('splits fires on opposite sides of a negative-coordinate grid boundary', () => {
    const a = makeFire({ latitude: -0.049, longitude: 2.0 });
    const b = makeFire({ latitude: -0.051, longitude: 2.0 });
    const tiers = [{ level: 3, size: 0.05 }];
    const out = clusterFires([a, b], tiers);

    expect(out).toHaveLength(2);
  });

  it('merges fires within the same negative-coordinate grid cell', () => {
    const a = makeFire({ latitude: -0.01, longitude: 2.0 });
    const b = makeFire({ latitude: -0.049, longitude: 2.0 });
    const tiers = [{ level: 3, size: 0.05 }];
    const out = clusterFires([a, b], tiers);

    expect(out).toHaveLength(1);
  });

  it('respects a custom tier list', () => {
    const out = clusterFires([makeFire()], [{ level: 9, size: 1.0 }]);

    expect(out).toHaveLength(1);
    expect(out[0].tier).toBe(9);
  });

  it('returns [] for an empty fire list', () => {
    expect(clusterFires([])).toEqual([]);
  });
});

describe('clusterFires id stability', () => {
  it('builds the documented deterministic id format', () => {
    const out = clusterFires([makeFire()]);
    const t1 = out.find((f) => f.tier === 1);

    expect(t1?.id).toBe('firm_2024-04-01_1430_34123_-118456_t1');
  });

  it('produces identical ids for identical inputs across calls', () => {
    const a = clusterFires([makeFire({ latitude: 34.5, longitude: -118.2 })]);
    const b = clusterFires([makeFire({ latitude: 34.5, longitude: -118.2 })]);

    expect(a.map((f) => f.id)).toEqual(b.map((f) => f.id));
  });

  it('produces different ids for different coordinates', () => {
    const a = clusterFires([makeFire({ latitude: 34.5, longitude: -118.2 })]);
    const b = clusterFires([makeFire({ latitude: 34.6, longitude: -118.3 })]);

    expect(a[0].id).not.toBe(b[0].id);
  });

  it('appends the tier level to the id, keeping tiers distinct', () => {
    const out = clusterFires([makeFire()]);
    const t1 = out.find((f) => f.tier === 1);
    const t2 = out.find((f) => f.tier === 2);

    expect(t1?.id.endsWith('_t1')).toBe(true);
    expect(t2?.id.endsWith('_t2')).toBe(true);
    expect(t1?.id).not.toBe(t2?.id);
  });

  it('uses the raw (unpadded) acq_time inside the id', () => {
    const out = clusterFires([makeFire({ acq_time: '5' })]);
    const t1 = out.find((f) => f.tier === 1);

    expect(t1?.id).toBe('firm_2024-04-01_5_34123_-118456_t1');
  });
});

describe('buildSourceTs', () => {
  const fetchedAt = 1712000000000;

  it('builds a UTC timestamp from acq_date and 4-digit acq_time', () => {
    expect(buildSourceTs('2024-04-01', '1430', fetchedAt)).toBe(
      new Date('2024-04-01T14:30:00Z').getTime()
    );
  });

  it('zero-pads a 1-digit acq_time (0005 -> 00:05)', () => {
    expect(buildSourceTs('2024-04-01', '5', fetchedAt)).toBe(
      new Date('2024-04-01T00:05:00Z').getTime()
    );
  });

  it('zero-pads a 3-digit acq_time', () => {
    expect(buildSourceTs('2024-04-01', '700', fetchedAt)).toBe(
      new Date('2024-04-01T07:00:00Z').getTime()
    );
  });

  it('truncates a 5-digit acq_time to hh:mm', () => {
    expect(buildSourceTs('2024-04-01', '12345', fetchedAt)).toBe(
      new Date('2024-04-01T12:34:00Z').getTime()
    );
  });

  it('falls back to fetchedAt for an invalid acq_date', () => {
    expect(buildSourceTs('not-a-date', '1430', fetchedAt)).toBe(fetchedAt);
  });

  it('falls back to fetchedAt for an empty acq_date', () => {
    expect(buildSourceTs('', '1430', fetchedAt)).toBe(fetchedAt);
  });

  it('falls back to fetchedAt for a non-numeric acq_time', () => {
    expect(buildSourceTs('2024-04-01', 'ab', fetchedAt)).toBe(fetchedAt);
  });
});

describe('seedWildfires integration', () => {
  it('parses, clusters, persists, and snapshots end-to-end with mocked IO', async () => {
    const csv =
      `${CSV_HEADER}\n` +
      '34.5,-118.2,320.1,0.4,0.6,2024-04-01,1430,NPP,high,2.0,280.7,12.5,D\n' +
      '34.6,-118.3,300.0,0.5,0.5,2024-04-01,1430,NPP,low,2.0,250.0,8.0,D';
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      text: async () => csv,
    });

    await seedWildfires();

    // tier 1 (2.0): merged (1) + tier 2 (0.5): merged (1) + tier 3 (0.05): split (2)
    expect(withRetry).toHaveBeenCalledTimes(1);
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'wildfire',
      expect.objectContaining({
        source: 'wildfire',
        totalCount: 4,
        items: expect.any(Array),
      }),
      1800
    );
    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as {
      items: unknown[];
      totalCount: number;
    };
    expect(snapshot.items).toHaveLength(snapshot.totalCount);
  });
});

describe('default export contract', () => {
  it('registers as "wildfire" on a 15-minute cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('wildfire');
    expect(seeder.cron).toBe('*/15 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});
