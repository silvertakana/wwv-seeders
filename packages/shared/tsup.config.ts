import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['index.ts'],
  format: ['esm', 'cjs'],
  // tsup bundles rollup-plugin-dts 6.1.1 inside dist/rollup.js, and that copy
  // reads `ts.sys`, which TypeScript 7 no longer exposes. tsc emits the
  // declarations instead (see tsconfig.build.json). Keep dts off until tsup
  // ships a TypeScript 7 compatible DTS pipeline.
  dts: false,
  clean: true,
  shims: true,
  noExternal: [/@wwv-seeders\/.*/],
  external: [/^(?!@wwv-seeders)[a-z@].*/],
});
