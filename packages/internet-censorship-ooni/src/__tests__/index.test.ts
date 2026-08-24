// Unit tests for the internet-censorship-ooni seeder's pure logic (OONI
// measurements parsing, censorship-event filtering, and end-to-end snapshot
// wiring).
//
// The @worldwideview/seeder-sdk is fully mocked so better-sqlite3 native
// bindings never load in the test environment (same pattern as
// packages/live-disasters/src/__tests__/index.test.ts).
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn((fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(async () => ({ json: async () => ({}), text: async () => '' })),
  db: {
    prepare: vi.fn(() => ({
      run: vi.fn(() => ({ changes: 0 })),
    })),
  },
}));

import {
  isCensorshipEvent,
  mapMeasurementToItem,
  parseOoniResponse,
  seedInternetCensorshipOoni,
  type CensorshipEventItem,
  type OoniMeasurement,
  type OoniResponse,
} from '../index';
import { setLiveSnapshot, fetchWithTimeout, withRetry, db } from '@worldwideview/seeder-sdk';

// db.prepare is invoked once per top-level statement at module load
// (index 0 = CREATE TABLE IF NOT EXISTS guard, index 1 = INSERT OR IGNORE).
const prepareMock = vi.mocked(db.prepare);
const createTableSql = prepareMock.mock.calls[0][0] as string;
const insertSql = prepareMock.mock.calls[1][0] as string;
const insertRunMock = prepareMock.mock.results[1].value.run as ReturnType<typeof vi.fn>;

// Real response body of https://api.ooni.io/api/v1/measurements?probe_cc=IR&limit=10
// captured 2026-08-24T13:48Z. Top-level shape is { metadata, results } and the
// 10 results include 5 censorship events (anomaly/confirmed/blocking > 0).
const OONI_FIXTURE: OoniResponse = {"metadata":{"count":-1,"current_page":1,"limit":10,"next_url":"https://api.ooni.io/api/v1/measurements?probe_cc=IR&limit=10&offset=10","offset":0,"pages":-1,"query_time":0.07098817825317383},"results":[{"anomaly":true,"confirmed":true,"failure":false,"input":"https://strongvpn.com/","probe_asn":"AS42337","probe_cc":"IR","report_id":"20260824T134903Z_webconnectivity_IR_42337_n4_LQ7SSqCGNI2wawEo","scores":{"blocking_general":2,"blocking_global":0,"blocking_country":1,"blocking_isp":0,"blocking_local":0,"fingerprints":[{"name":"ooni.ir_10dot10_ipv4_1","scope":"nat","location_found":"dns","confidence_no_fp":10,"expected_countries":["IR"]},{"name":"ooni.ir_10dot10_ipv4_1","scope":"nat","location_found":"dns","confidence_no_fp":10,"expected_countries":["IR"]}],"confirmed":true,"analysis":{"blocking_type":"dns"}},"test_name":"web_connectivity","measurement_url":"https://api.ooni.io/api/v1/raw_measurement?measurement_uid=20260824134903.656727_IR_webconnectivity_6e8d557c6ae8fd7a","measurement_start_time":"2026-08-24T13:48:47.000000Z","measurement_uid":"20260824134903.656727_IR_webconnectivity_6e8d557c6ae8fd7a","verification_status":"verified"},{"anomaly":false,"confirmed":false,"failure":false,"input":"https://www.hootsuite.com/","probe_asn":"AS58224","probe_cc":"IR","report_id":"20260824T134852Z_webconnectivity_IR_58224_n4_Zj4hJyuO7sUtBmvB","scores":{"blocking_general":0,"blocking_global":0,"blocking_country":0,"blocking_isp":0,"blocking_local":0},"test_name":"web_connectivity","measurement_url":"https://api.ooni.io/api/v1/raw_measurement?measurement_uid=20260824134852.434872_IR_webconnectivity_ed56e033588b0ba8","measurement_start_time":"2026-08-24T13:48:45.000000Z","measurement_uid":"20260824134852.434872_IR_webconnectivity_ed56e033588b0ba8","verification_status":"verified"},{"anomaly":false,"confirmed":false,"failure":false,"input":"https://ssd.eff.org/","probe_asn":"AS42337","probe_cc":"IR","report_id":"20260824T134847Z_webconnectivity_IR_42337_n4_O0zYdDkvoyOoVXH6","scores":{"blocking_general":0,"blocking_global":0,"blocking_country":0,"blocking_isp":0,"blocking_local":0},"test_name":"web_connectivity","measurement_url":"https://api.ooni.io/api/v1/raw_measurement?measurement_uid=20260824134847.820000_IR_webconnectivity_48408c74e6bc1b90","measurement_start_time":"2026-08-24T13:48:45.000000Z","measurement_uid":"20260824134847.820000_IR_webconnectivity_48408c74e6bc1b90","verification_status":"verified"},{"anomaly":true,"confirmed":false,"failure":false,"input":"https://www.douyin.com/","probe_asn":"AS58224","probe_cc":"IR","report_id":"20260824T134848Z_webconnectivity_IR_58224_n4_LBWtbFoQa0LZBIVP","scores":{"blocking_general":1,"blocking_global":0,"blocking_country":0,"blocking_isp":0,"blocking_local":0,"analysis":{"blocking_type":"http-failure"}},"test_name":"web_connectivity","measurement_url":"https://api.ooni.io/api/v1/raw_measurement?measurement_uid=20260824134848.352844_IR_webconnectivity_bcea6655c0c818da","measurement_start_time":"2026-08-24T13:48:41.000000Z","measurement_uid":"20260824134848.352844_IR_webconnectivity_bcea6655c0c818da","verification_status":"verified"},{"anomaly":false,"confirmed":false,"failure":false,"input":"https://www.lilithfund.org/","probe_asn":"AS197207","probe_cc":"IR","report_id":"20260824T134851Z_webconnectivity_IR_197207_n4_bRpXfio7QEKrUohi","scores":{"blocking_general":0,"blocking_global":0,"blocking_country":0,"blocking_isp":0,"blocking_local":0,"fingerprints":[{"name":"cp.fp_x_redirect_just","scope":"fp","location_found":"body","confidence_no_fp":5,"expected_countries":[]}]},"test_name":"web_connectivity","measurement_url":"https://api.ooni.io/api/v1/raw_measurement?measurement_uid=20260824134852.315335_IR_webconnectivity_6d66766448c5eb89","measurement_start_time":"2026-08-24T13:48:40.000000Z","measurement_uid":"20260824134852.315335_IR_webconnectivity_6d66766448c5eb89","verification_status":"unverified"},{"anomaly":true,"confirmed":true,"failure":false,"input":"https://spys.one/","probe_asn":"AS42337","probe_cc":"IR","report_id":"20260824T134845Z_webconnectivity_IR_42337_n4_c2ozWkSuSZFI1zwK","scores":{"blocking_general":2,"blocking_global":0,"blocking_country":1,"blocking_isp":0,"blocking_local":0,"fingerprints":[{"name":"ooni.ir_10dot10_ipv4_1","scope":"nat","location_found":"dns","confidence_no_fp":10,"expected_countries":["IR"]},{"name":"ooni.ir_10dot10_ipv4_1","scope":"nat","location_found":"dns","confidence_no_fp":10,"expected_countries":["IR"]}],"confirmed":true,"analysis":{"blocking_type":"dns"}},"test_name":"web_connectivity","measurement_url":"https://api.ooni.io/api/v1/raw_measurement?measurement_uid=20260824134845.874973_IR_webconnectivity_8a93d975a65c31c5","measurement_start_time":"2026-08-24T13:48:29.000000Z","measurement_uid":"20260824134845.874973_IR_webconnectivity_8a93d975a65c31c5","verification_status":"verified"},{"anomaly":false,"confirmed":false,"failure":false,"input":"https://sputniknews.com/","probe_asn":"AS42337","probe_cc":"IR","report_id":"20260824T134829Z_webconnectivity_IR_42337_n4_U3LC61FqYYRWcLcN","scores":{"blocking_general":0,"blocking_global":0,"blocking_country":0,"blocking_isp":0,"blocking_local":0,"fingerprints":[{"name":"cp.fp_x_news","scope":"fp","location_found":"body","confidence_no_fp":5,"expected_countries":[]},{"name":"cp.fp_r_fp_1","scope":"fp","location_found":"body","confidence_no_fp":5,"expected_countries":[]}]},"test_name":"web_connectivity","measurement_url":"https://api.ooni.io/api/v1/raw_measurement?measurement_uid=20260824134829.303154_IR_webconnectivity_b6ebbefc6120cd47","measurement_start_time":"2026-08-24T13:48:27.000000Z","measurement_uid":"20260824134829.303154_IR_webconnectivity_b6ebbefc6120cd47","verification_status":"verified"},{"anomaly":false,"confirmed":false,"failure":false,"input":"https://sputniknews.cn/","probe_asn":"AS42337","probe_cc":"IR","report_id":"20260824T134827Z_webconnectivity_IR_42337_n4_oYiH84yS9Rd8gBEg","scores":{"blocking_general":0,"blocking_global":0,"blocking_country":0,"blocking_isp":0,"blocking_local":0},"test_name":"web_connectivity","measurement_url":"https://api.ooni.io/api/v1/raw_measurement?measurement_uid=20260824134827.350945_IR_webconnectivity_29202645028a48bb","measurement_start_time":"2026-08-24T13:48:20.000000Z","measurement_uid":"20260824134827.350945_IR_webconnectivity_29202645028a48bb","verification_status":"verified"},{"anomaly":true,"confirmed":false,"failure":false,"input":"https://www.clubhouseapi.com/","probe_asn":"AS58224","probe_cc":"IR","report_id":"20260824T134843Z_webconnectivity_IR_58224_n4_3ZJ7mFzWF4nl8ywV","scores":{"blocking_general":1,"blocking_global":0,"blocking_country":0,"blocking_isp":0,"blocking_local":0,"analysis":{"blocking_type":"http-failure"}},"test_name":"web_connectivity","measurement_url":"https://api.ooni.io/api/v1/raw_measurement?measurement_uid=20260824134843.816212_IR_webconnectivity_161a3a5960b91cd1","measurement_start_time":"2026-08-24T13:48:19.000000Z","measurement_uid":"20260824134843.816212_IR_webconnectivity_161a3a5960b91cd1","verification_status":"verified"},{"anomaly":true,"confirmed":false,"failure":false,"input":"https://www.clubhouse.com/","probe_asn":"AS58224","probe_cc":"IR","report_id":"20260824T134822Z_webconnectivity_IR_58224_n4_XGphNlSHkOjg7I5m","scores":{"blocking_general":1,"blocking_global":0,"blocking_country":0,"blocking_isp":0,"blocking_local":0,"analysis":{"blocking_type":"http-failure"}},"test_name":"web_connectivity","measurement_url":"https://api.ooni.io/api/v1/raw_measurement?measurement_uid=20260824134822.391731_IR_webconnectivity_5df6f2c0d389839e","measurement_start_time":"2026-08-24T13:48:18.000000Z","measurement_uid":"20260824134822.391731_IR_webconnectivity_5df6f2c0d389839e","verification_status":"verified"}]};

beforeEach(() => {
  vi.clearAllMocks();
});

describe('isCensorshipEvent', () => {
  it('flags a confirmed, anomaly-marked measurement as an event', () => {
    const blocked = OONI_FIXTURE.results?.find((r) => r.anomaly === true) as OoniMeasurement;
    expect(blocked).toBeDefined();
    expect(isCensorshipEvent(blocked)).toBe(true);
  });

  it('flags a measurement with a general blocking score above zero', () => {
    const blocked = OONI_FIXTURE.results?.find((r) => (r.scores?.blocking_general ?? 0) > 0) as OoniMeasurement;
    expect(blocked).toBeDefined();
    expect(isCensorshipEvent(blocked)).toBe(true);
  });

  it('does not flag a clean measurement', () => {
    const clean = OONI_FIXTURE.results?.find(
      (r) => !r.anomaly && !r.confirmed && (r.scores?.blocking_general ?? 0) === 0
    );
    expect(clean).toBeDefined();
    expect(isCensorshipEvent(clean as OoniMeasurement)).toBe(false);
  });
});

describe('mapMeasurementToItem', () => {
  it('maps a blocking measurement to a country-centered point', () => {
    const blocked = OONI_FIXTURE.results?.find((r) => r.anomaly === true) as OoniMeasurement;
    const item = mapMeasurementToItem(blocked);
    expect(item).not.toBeNull();
    expect(item).toMatchObject({
      id: blocked.measurement_uid,
      probeCc: 'IR',
      probeAsn: blocked.probe_asn,
      lat: 32.43, // Iran centroid
      lon: 53.69,
      anomaly: true,
      confirmed: blocked.confirmed,
    });
    expect(item?.measuredAt).toBe(blocked.measurement_start_time);
    expect(typeof item?.blockingGeneral).toBe('number');
  });

  it('returns null for a clean measurement', () => {
    const clean = OONI_FIXTURE.results?.find(
      (r) => !r.anomaly && !r.confirmed && (r.scores?.blocking_general ?? 0) === 0
    ) as OoniMeasurement;
    expect(mapMeasurementToItem(clean)).toBeNull();
  });

  it('returns null when the probe country has no known centroid', () => {
    const unknown: OoniMeasurement = {
      anomaly: true,
      confirmed: false,
      failure: false,
      input: 'https://example.com/',
      probe_asn: 'AS1',
      probe_cc: 'ZZ',
    };
    expect(mapMeasurementToItem(unknown)).toBeNull();
  });
});

describe('parseOoniResponse', () => {
  it('extracts the live IR slice into 5 censorship events', () => {
    const items = parseOoniResponse(OONI_FIXTURE);
    expect(items).toHaveLength(5);
    expect(items.every((i) => i.probeCc === 'IR')).toBe(true);
    expect(items.every((i) => i.lat === 32.43 && i.lon === 53.69)).toBe(true);
  });

  it('returns an empty list for a missing or null results array', () => {
    expect(parseOoniResponse({})).toEqual([]);
    expect(parseOoniResponse({ metadata: {}, results: null })).toEqual([]);
  });
});

describe('seedInternetCensorshipOoni integration', () => {
  it('fetches per country, persists events, and snapshots end-to-end', async () => {
    // Only the IR probe_cc returns the blocking fixture; every other country
    // comes back with zero results.
    vi.mocked(fetchWithTimeout).mockImplementation(async (url: string) => {
      const payload = url.includes('probe_cc=IR') ? OONI_FIXTURE : { metadata: {}, results: [] };
      return { json: async () => payload } as never;
    });

    await seedInternetCensorshipOoni();

    expect(withRetry).toHaveBeenCalledTimes(12); // PROBE_COUNTRIES length
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'internet-censorship-ooni',
      expect.objectContaining({
        source: 'internet-censorship-ooni',
        totalCount: 5,
        items: expect.any(Array),
      }),
      3600
    );
    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as {
      items: CensorshipEventItem[];
      totalCount: number;
    };
    expect(snapshot.items).toHaveLength(snapshot.totalCount);
    expect(new Set(snapshot.items.map((i) => i.probeCc))).toEqual(new Set(['IR']));
  });

  it('persists each event with id, JSON payload, source_ts, and fetched_at', async () => {
    vi.mocked(fetchWithTimeout).mockImplementation(async (url: string) => {
      const payload = url.includes('probe_cc=IR') ? OONI_FIXTURE : { metadata: {}, results: [] };
      return { json: async () => payload } as never;
    });

    await seedInternetCensorshipOoni();

    expect(insertRunMock).toHaveBeenCalledTimes(5);
    const args = insertRunMock.mock.calls[0][0] as {
      id: string;
      payload: string;
      source_ts: number;
      fetched_at: number;
    };
    expect(args.id).toBe(OONI_FIXTURE.results?.find((r) => r.anomaly === true)?.measurement_uid);
    expect(args.source_ts).toBe(
      new Date(OONI_FIXTURE.results?.find((r) => r.anomaly === true)?.measurement_start_time as string).getTime()
    );
    expect(typeof args.fetched_at).toBe('number');
    const payload = JSON.parse(args.payload) as CensorshipEventItem;
    expect(payload).toMatchObject({ probeCc: 'IR', lat: 32.43, lon: 53.69 });
  });

  it('logs and skips the snapshot when the fetch fails', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('network down'));

    await expect(seedInternetCensorshipOoni()).resolves.toBeUndefined();

    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});

describe('module-level SQLite wiring', () => {
  it('ensures the internet_censorship_ooni table with a CREATE TABLE IF NOT EXISTS guard', () => {
    expect(createTableSql).toContain('CREATE TABLE IF NOT EXISTS internet_censorship_ooni');
    expect(createTableSql).toContain('id TEXT PRIMARY KEY');
  });

  it('prepares an INSERT OR IGNORE against the internet_censorship_ooni table', () => {
    expect(insertSql).toContain('INSERT OR IGNORE INTO internet_censorship_ooni');
  });
});

describe('default export contract', () => {
  it('registers as "internet-censorship-ooni" on an hourly cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('internet-censorship-ooni');
    expect(seeder.cron).toBe('0 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});