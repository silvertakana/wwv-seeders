// Contract tests for the seeder default export shape.
// Mocks prevent native bindings (better-sqlite3) from loading in the test environment.
import { describe, it, expect, vi } from 'vitest';

// Mock the full @worldwideview/seeder-sdk to avoid loading better-sqlite3 native bindings
vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
  getLiveSnapshot: vi.fn(async () => null),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(),
  db: {},
}));

// yahoo-finance2 v4 exports a class; src/index.ts constructs it at module load.
vi.mock('yahoo-finance2', () => ({
  default: class {
    quote = vi.fn(async () => []);
  },
}));

describe('seeder default export contract', () => {
  it('has name === "market-tracker"', async () => {
    const mod = await import('../index');
    const seeder = mod.default;
    expect(seeder.name).toBe('market-tracker');
  });

  it('has interval === 30000', async () => {
    const mod = await import('../index');
    const seeder = mod.default;
    expect(seeder.interval).toBe(30000);
  });

  it('has fetch typed as function', async () => {
    const mod = await import('../index');
    const seeder = mod.default;
    expect(typeof seeder.fetch).toBe('function');
  });
});
