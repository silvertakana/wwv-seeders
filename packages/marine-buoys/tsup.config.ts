import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm'],
  clean: true,
  shims: true,
  noExternal: [/@wwv-seeders\/.*/, '@worldwideview/seeder-sdk'],
  external: [/^(?!@wwv-seeders)[a-z@].*/],
});