// Contract + containment-fix tests for the conflictEvents seeder.
// Mocks prevent native bindings (better-sqlite3) from loading in the test environment.
import { describe, it, expect, beforeEach, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  run: vi.fn(),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(),
}));

// Mock the full @worldwideview/seeder-sdk to avoid loading better-sqlite3 native bindings.
vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: mocks.setLiveSnapshot,
  fetchWithTimeout: mocks.fetchWithTimeout,
  db: { prepare: vi.fn(() => ({ run: mocks.run })) },
}));

const MENTION = {
  type: 'Feature',
  geometry: { type: 'Point', coordinates: [30.5, 50.4] },
  properties: {
    urlpubtimedate: '20240607123000',
    name: 'Battle near Kyiv',
    domain: 'example.com',
    url: 'https://example.com/article1',
    urltone: -3.2,
  },
};

function mockGkgResponse(features: unknown[]) {
  mocks.fetchWithTimeout.mockResolvedValue({
    ok: true,
    status: 200,
    json: async () => ({ features }),
  });
}

describe('seeder default export contract', () => {
  it('has name === "conflict-events"', async () => {
    const mod = await import('../index');
    expect(mod.default.name).toBe('conflict-events');
  });

  it('has cron === "*/30 * * * *"', async () => {
    const mod = await import('../index');
    expect(mod.default.cron).toBe('*/30 * * * *');
  });

  it('has fn typed as function', async () => {
    const mod = await import('../index');
    expect(typeof mod.default.fn).toBe('function');
  });
});

describe('data-quality containment fix', () => {
  beforeEach(() => {
    mocks.run.mockClear();
    mocks.setLiveSnapshot.mockClear();
    mocks.fetchWithTimeout.mockClear();
  });

  it('derives stable ids from mention attributes, not fetchedAt or items.length', async () => {
    mockGkgResponse([MENTION]);
    const mod = await import('../index');
    await mod.fetchConflictEvents();
    await mod.fetchConflictEvents();

    const firstPayload = JSON.parse(mocks.run.mock.calls[0][0].payload);
    const secondPayload = JSON.parse(mocks.run.mock.calls[1][0].payload);

    expect(firstPayload.id).toBe('gdelt-battle-near-kyiv-50.4000-30.5000-20240607123000');
    // Identical across polls -> INSERT OR REPLACE now dedupes instead of accumulating rows.
    expect(secondPayload.id).toBe(firstPayload.id);
    // No dependence on fetch time or array index.
    expect(firstPayload.id).not.toContain(String(Date.now()).slice(0, 8));
  });

  it('never fabricates casualties (fatalities === 0 for every classification)', async () => {
    // "Violence against civilians" was previously assigned Math.random() 1-10.
    mockGkgResponse([
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [44.5, 33.3] },
        properties: {
          urlpubtimedate: '20240607120000',
          name: 'Attack on civilian convoy',
          domain: 'news.org',
          url: 'https://news.org/article2',
          urltone: -8.1,
        },
      },
    ]);
    const mod = await import('../index');
    await mod.fetchConflictEvents();

    const item = JSON.parse(mocks.run.mock.calls[0][0].payload);
    expect(item.type).toBe('Violence against civilians');
    expect(item.fatalities).toBe(0);
  });

  it('preserves the source url in the DB item and the snapshot properties', async () => {
    mockGkgResponse([MENTION]);
    const mod = await import('../index');
    await mod.fetchConflictEvents();

    const item = JSON.parse(mocks.run.mock.calls[0][0].payload);
    expect(item.url).toBe('https://example.com/article1');

    const [, geoEntities] = mocks.setLiveSnapshot.mock.calls[0];
    expect(geoEntities[0].properties.url).toBe('https://example.com/article1');
  });

  it('adds a provenance marker to snapshot properties', async () => {
    mockGkgResponse([MENTION]);
    const mod = await import('../index');
    await mod.fetchConflictEvents();

    const [, geoEntities] = mocks.setLiveSnapshot.mock.calls[0];
    expect(geoEntities[0].properties.verification).toBe('unverified-mention');
  });

  it('skips mentions without a name or with invalid coordinates', async () => {
    mockGkgResponse([
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [10, 20] },
        properties: { urlpubtimedate: '20240607120000', name: '', url: 'https://x.example' },
      },
      {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [999, 999] },
        properties: { urlpubtimedate: '20240607120000', name: 'Invalid coords', url: 'https://x.example' },
      },
      MENTION,
    ]);
    const mod = await import('../index');
    await mod.fetchConflictEvents();

    expect(mocks.run).toHaveBeenCalledTimes(1);
    const item = JSON.parse(mocks.run.mock.calls[0][0].payload);
    expect(item.id).toContain('gdelt-battle-near-kyiv');
  });
});
