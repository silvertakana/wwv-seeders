// Placeholder contract tests - unskipped in plan 10-02 once src/index.ts exists
import { describe, it, expect } from 'vitest';

describe('seeder default export contract', () => {
  it.skip('has name === "market-tracker"', async () => {
    const mod = await import('../index');
    const seeder = mod.default;
    expect(seeder.name).toBe('market-tracker');
  });

  it.skip('has interval === 30000', async () => {
    const mod = await import('../index');
    const seeder = mod.default;
    expect(seeder.interval).toBe(30000);
  });

  it.skip('has fetch typed as function', async () => {
    const mod = await import('../index');
    const seeder = mod.default;
    expect(typeof seeder.fetch).toBe('function');
  });
});
