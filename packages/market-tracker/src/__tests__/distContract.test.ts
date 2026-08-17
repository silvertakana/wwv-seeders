// Locks the BUILT artifact's contract (dist/index.mjs), not just the source.
// Run after: pnpm --filter @wwv-seeders/market-tracker build
//
// The dist bundle inlines @worldwideview/seeder-sdk (tsup noExternal) and imports
// yahoo-finance2 as an external. We mock the native/transitive externals here so
// dist/index.mjs can be dynamically imported without triggering better-sqlite3
// native binding resolution.
import { describe, it, expect, beforeAll, vi } from 'vitest';
import { existsSync } from 'fs';
import { resolve } from 'path';

// The SDK is inlined into the dist, so this mock is inert for the dist path but
// harmless to keep (it is still the mock used by the other suites for src imports).
vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(),
  db: {},
}));

vi.mock('yahoo-finance2', () => ({
  default: {
    quote: vi.fn(async () => []),
  },
}));

// Native/transitive externals pulled in by the inlined SDK bundle. better-sqlite3
// is constructed at dist import time, so the mock must be a constructor.
vi.mock('better-sqlite3', () => ({
  default: class {
    pragma = vi.fn();
    exec = vi.fn();
    prepare = vi.fn(() => ({ run: vi.fn(), get: vi.fn(), all: vi.fn() }));
    close = vi.fn();
  },
}));

vi.mock('ioredis', () => ({ Redis: class { on = vi.fn(); } }));
vi.mock('dotenv', () => ({ default: { config: vi.fn() } }));
vi.mock('geoip-lite', () => ({ default: { lookup: vi.fn() } }));

const DIST_PATH = resolve(__dirname, '../../dist/index.mjs');

describe('dist bundle contract', () => {
  beforeAll(() => {
    if (!existsSync(DIST_PATH)) {
      throw new Error(
        `dist/index.mjs not found at ${DIST_PATH}. Run pnpm build first.`
      );
    }
  });

  it('default export has name === "market-tracker"', async () => {
    const mod = await import(DIST_PATH);
    expect(mod.default.name).toBe('market-tracker');
  });

  it('default export has interval === 30000', async () => {
    const mod = await import(DIST_PATH);
    expect(mod.default.interval).toBe(30000);
  });

  it('default export has fetch typeof "function"', async () => {
    const mod = await import(DIST_PATH);
    expect(typeof mod.default.fetch).toBe('function');
  });
});
