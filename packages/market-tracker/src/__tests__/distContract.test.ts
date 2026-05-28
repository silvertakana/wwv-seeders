// Locks the BUILT artifact's contract (dist/index.mjs), not just the source.
// Run after: pnpm --filter @wwv-seeders/market-tracker build
//
// The dist bundle imports @worldwideview/seeder-sdk and yahoo-finance2 as externals.
// We mock those here to prevent better-sqlite3 native bindings from loading in tests.
import { describe, it, expect, beforeAll, vi } from 'vitest';
import { existsSync } from 'fs';
import { resolve } from 'path';

// Mock the SDK and yahoo-finance2 so dist/index.mjs can be dynamically imported
// without triggering better-sqlite3 native binding resolution
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
