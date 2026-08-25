// Locks the BUILT artifact's contract (dist/index.mjs), not just the source.
// Run after: pnpm --filter @wwv-seeders/deforestation-gfw build
//
// The dist bundle inlines @worldwideview/seeder-sdk (tsup noExternal), so its
// native/transitive externals are mocked here to allow importing dist/index.mjs
// without triggering better-sqlite3 native binding resolution.
import { describe, it, expect, beforeAll, vi } from 'vitest';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

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

const DIST_URL = new URL('../../dist/index.mjs', import.meta.url);
const DIST_PATH = fileURLToPath(DIST_URL);

describe('dist bundle contract', () => {
  beforeAll(() => {
    if (!existsSync(DIST_PATH)) {
      throw new Error(
        `dist/index.mjs not found at ${DIST_PATH}. Run pnpm build first.`
      );
    }
  });

  it('default export has name === "deforestation-gfw"', async () => {
    const mod = await import(DIST_URL.href);
    expect(mod.default.name).toBe('deforestation-gfw');
  });

  it('default export has interval === 1500000', async () => {
    const mod = await import(DIST_URL.href);
    expect(mod.default.interval).toBe(1_500_000);
  });

  it('default export has fetch typeof "function"', async () => {
    const mod = await import(DIST_URL.href);
    expect(typeof mod.default.fetch).toBe('function');
  });
});