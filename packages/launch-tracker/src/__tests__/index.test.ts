// Unit tests for the launch-tracker seeder's pure logic (LL2 2.3.0 mapping
// and upcoming/coordinate filtering) and end-to-end snapshot wiring.
//
// The @worldwideview/seeder-sdk is fully mocked so better-sqlite3 native
// bindings never load in the test environment (same pattern as
// packages/earthquakes/src/__tests__/index.test.ts).
import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn((fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(async () => ({ json: async () => ({}) })),
  db: {
    exec: vi.fn(),
    prepare: vi.fn(() => ({
      run: vi.fn(() => ({ changes: 0 })),
    })),
  },
}));

import {
  mapLaunchToItem,
  seedLaunchTracker,
  type LL2Launch,
  type LaunchTrackerItem,
} from '../index';
import {
  setLiveSnapshot,
  fetchWithTimeout,
  withRetry,
  db,
} from '@worldwideview/seeder-sdk';

// db.exec is invoked once at module load (CREATE TABLE IF NOT EXISTS
// launch_tracker) and db.prepare once (top-level insert statement). Capture the
// returned run mock so tests can assert per-launch insert calls.
const prepareMock = vi.mocked(db.prepare);
const insertRunMock = prepareMock.mock.results[0].value.run as ReturnType<typeof vi.fn>;

beforeEach(() => {
  vi.clearAllMocks();
});

/** A launch 10 days in the future with finite pad coordinates. */
function makeLaunch(overrides: {
  id?: string;
  net?: string | null;
  padLat?: number | string | null;
  padLon?: number | string | null;
  status?: LL2Launch['status'];
  name?: string;
} = {}): LL2Launch {
  const future = new Date(Date.now() + 10 * 86_400_000).toISOString();
  return {
    id: overrides.id ?? 'abc-123',
    name: overrides.name ?? 'Falcon 9 | Demo Mission',
    net: overrides.net ?? future,
    url: `https://ll.thespacedevs.com/2.3.0/launches/${overrides.id ?? 'abc-123'}/`,
    status: overrides.status ?? { id: 1, name: 'Go for Launch' },
    pad: {
      name: 'SLC-40',
      latitude: 'padLat' in overrides ? overrides.padLat : 28.5619,
      longitude: 'padLon' in overrides ? overrides.padLon : -80.5771,
      location: { name: 'Cape Canaveral SFS, FL, USA' },
    },
    mission: { name: 'Demo Mission' },
    rocket: { configuration: { name: 'Falcon 9', family: 'Falcon' } },
    launch_service_provider: { name: 'SpaceX' },
    webcast_live: false,
  };
}

describe('mapLaunchToItem', () => {
  it('maps a valid upcoming launch with finite pad coords', () => {
    const item = mapLaunchToItem(makeLaunch());

    expect(item).toMatchObject({
      id: 'abc-123',
      name: 'Falcon 9 | Demo Mission',
      net: expect.any(String),
      status: 'Go for Launch',
      padName: 'SLC-40',
      latitude: 28.5619,
      longitude: -80.5771,
      location: 'Cape Canaveral SFS, FL, USA',
      mission: 'Demo Mission',
      rocket: 'Falcon 9',
      provider: 'SpaceX',
      webcast_live: false,
    });
  });

  it('skips launches with a null pad latitude', () => {
    expect(mapLaunchToItem(makeLaunch({ padLat: null }))).toBeNull();
  });

  it('skips launches with a null pad longitude', () => {
    expect(mapLaunchToItem(makeLaunch({ padLon: null }))).toBeNull();
  });

  it('coerces string pad coordinates', () => {
    const item = mapLaunchToItem(makeLaunch({ padLat: '28.5619', padLon: '-80.5771' }));
    expect(item?.latitude).toBe(28.5619);
    expect(item?.longitude).toBe(-80.5771);
  });

  it('skips launches in the past (NET before now)', () => {
    const past = new Date(Date.now() - 86_400_000).toISOString();
    expect(mapLaunchToItem(makeLaunch({ net: past }))).toBeNull();
  });

  it('falls back to launch_service_provider (2.3.0) when provider is absent', () => {
    const launch = makeLaunch();
    delete (launch as { provider?: unknown }).provider;
    const item = mapLaunchToItem(launch);
    expect(item?.provider).toBe('SpaceX');
  });
});

describe('seedLaunchTracker integration', () => {
  it('fetches, maps, persists, and snapshots end-to-end with mocked IO', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => ({ results: [makeLaunch(), makeLaunch({ id: 'def-456', name: 'Ariane 6 | Test Flight' })] }),
    } as never);

    await seedLaunchTracker();

    expect(withRetry).toHaveBeenCalledTimes(1);
    expect(setLiveSnapshot).toHaveBeenCalledWith(
      'launch-tracker',
      expect.objectContaining({
        source: 'launch-tracker',
        totalCount: 2,
      }),
      3600
    );
    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as {
      items: LaunchTrackerItem[];
      totalCount: number;
    };
    expect(snapshot.items).toHaveLength(snapshot.totalCount);
    expect(snapshot.items[0]).toMatchObject({ id: 'abc-123', rocket: 'Falcon 9' });
    expect(snapshot.items[1]).toMatchObject({ id: 'def-456', name: 'Ariane 6 | Test Flight' });
  });

  it('persists each launch with id, JSON payload, source_ts, and fetched_at', async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => ({ results: [makeLaunch()] }),
    } as never);

    await seedLaunchTracker();

    expect(insertRunMock).toHaveBeenCalledTimes(1);
    const args = insertRunMock.mock.calls[0][0] as {
      id: string;
      payload: string;
      source_ts: number;
      fetched_at: number;
    };
    expect(args.id).toBe('abc-123');
    expect(typeof args.source_ts).toBe('number');
    expect(typeof args.fetched_at).toBe('number');
    const payload = JSON.parse(args.payload) as LaunchTrackerItem;
    expect(payload).toMatchObject({
      id: 'abc-123',
      latitude: 28.5619,
      longitude: -80.5771,
      rocket: 'Falcon 9',
    });
  });

  it('drops non-upcoming launches from both the snapshot and inserts', async () => {
    const past = new Date(Date.now() - 86_400_000).toISOString();
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => ({ results: [makeLaunch({ id: 'recent', net: past }), makeLaunch({ id: 'soon' })] }),
    } as never);

    await seedLaunchTracker();

    const snapshot = vi.mocked(setLiveSnapshot).mock.calls[0][1] as { items: LaunchTrackerItem[] };
    expect(snapshot.items.map((i) => i.id)).toEqual(['soon']);
    expect(insertRunMock).toHaveBeenCalledTimes(1);
  });

  it('warns and skips the snapshot on an invalid response', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.mocked(fetchWithTimeout).mockResolvedValue({
      json: async () => ({}),
    } as never);

    await seedLaunchTracker();

    expect(warnSpy).toHaveBeenCalledWith('[Launch Tracker] Invalid response from Launch Library 2');
    expect(setLiveSnapshot).not.toHaveBeenCalled();
    expect(insertRunMock).not.toHaveBeenCalled();
    warnSpy.mockRestore();
  });

  it('does not throw when the upstream fetch fails (body wrapped in try/catch)', async () => {
    vi.mocked(fetchWithTimeout).mockRejectedValue(new Error('network down'));
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    await expect(seedLaunchTracker()).resolves.toBeUndefined();
    expect(errorSpy).toHaveBeenCalled();
    expect(setLiveSnapshot).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});

describe('default export contract', () => {
  it('registers as "launch-tracker" on an hourly cron', async () => {
    const mod = await import('../index');
    const seeder = mod.default;

    expect(seeder.name).toBe('launch-tracker');
    expect(seeder.cron).toBe('0 * * * *');
    expect(typeof seeder.fn).toBe('function');
  });
});