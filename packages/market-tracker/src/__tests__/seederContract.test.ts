// Contract tests for the seeder default export shape.
// Mocks prevent native bindings (better-sqlite3) from loading in the test environment.
import { describe, it, expect, vi } from 'vitest';

// Mock the full @worldwideview/seeder-sdk to avoid loading better-sqlite3 native bindings
vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
  setLiveSnapshot: vi.fn(),
  fetchWithTimeout: vi.fn(),
  db: {},
}));

// Mock yahoo-finance2 to avoid external HTTP calls during unit tests
vi.mock('yahoo-finance2', () => ({
  default: {
    quote: vi.fn(async () => []),
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
