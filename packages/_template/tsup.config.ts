import { defineConfig } from 'tsup';

// Standard seeder build. The host engine provides common deps (zod, ws,
// node-cron, undici, ...) at runtime, so they stay external; only
// @wwv-seeders/* workspace code is bundled in.
export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm'],
  clean: true,
  shims: true,
  noExternal: [/@wwv-seeders\/.*/, '@worldwideview/seeder-sdk'],
  external: [/^(?!@wwv-seeders)[a-z@].*/],
});
