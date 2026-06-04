// =============================================================================
// SEEDER TEMPLATE — copy this folder to packages/<your-seeder> to start.
//
// A seeder is a small package the data engine auto-discovers and runs. The
// engine scans for a folder with `dist/index.mjs` and imports its DEFAULT
// EXPORT. The seeder's id is the FOLDER NAME, but you must also export a
// matching `name` (see ADR-0002: name === the frontend plugin's id, kebab-case).
//
// HOW TO PUBLISH DATA — pick ONE shape:
//
//   1. cron + fn   (the common idiom, shown below)
//        Import { setLiveSnapshot } from '@worldwideview/seeder-sdk' and call it
//        yourself inside fn. The scheduler does NOT publish your return value.
//
//   2. interval + fetch   (simplest; see the commented alternative at the bottom)
//        Just RETURN the array from fetch(); the scheduler wraps it into a
//        snapshot, stores it, and broadcasts it for you. No publish call.
//
//   3. init(ctx)   (persistent listener, e.g. an upstream WebSocket)
//
// THE ONE TRAP TO AVOID:
//   The argument every handler receives (`ctx`) is ONLY `{ redis }`. There is
//   NO `ctx.setLiveSnapshot` — calling it throws and your data silently never
//   publishes. Every helper (setLiveSnapshot, db, fetchWithTimeout, ...) comes
//   from '@worldwideview/seeder-sdk', never from ctx and never from a bare global.
//
// WHEN YOU COPY THIS:
//   - rename the package in package.json (e.g. "@wwv-seeders/volcanoes")
//   - add  "scripts": { "build": "tsup" }  (this template omits it so it is
//     never built or released by accident)
//   - set `name` to your plugin id, replace the fetch logic
//   - DELETE the `ENABLED` dormancy guard below (it exists only so this template
//     is skipped by the engine if it ever ships unmodified)
// =============================================================================

import { setLiveSnapshot, fetchWithTimeout } from '@worldwideview/seeder-sdk';

// DORMANCY GUARD — delete this when you copy the template. It keeps the
// unmodified template inert: a module whose default export has no `name` is
// skipped by the loader, so even if this ever gets built and deployed it does
// nothing.
const ENABLED = process.env.SEEDER_TEMPLATE_ENABLE === '1';

async function seedExample() {
  // Replace with your real upstream call.
  const raw = await fetchWithTimeout('https://example.org/api/things');
  const things = (await raw.json()) as Array<{ id: string; lat: number; lon: number }>;

  // Shape each item as a GeoEntity. A flat GeoEntity[] needs no
  // mapWebsocketPayload on the frontend (see data-engine-architecture rule §7).
  const items = things.map((t) => ({
    id: t.id,
    latitude: t.lat,
    longitude: t.lon,
    timestamp: new Date().toISOString(),
  }));

  // Publish: this is the call ctx does NOT provide — it comes from the SDK.
  await setLiveSnapshot(
    'template-example', // MUST equal your frontend plugin id (and folder name)
    {
      source: 'template-example',
      fetchedAt: new Date().toISOString(),
      items,
      totalCount: items.length,
    },
    3600, // TTL seconds (~3x your cadence)
  );
}

export default ENABLED
  ? {
      name: 'template-example',
      cron: '*/5 * * * *', // every 5 minutes
      fn: seedExample,
    }
  : {};

// -----------------------------------------------------------------------------
// ALTERNATIVE — interval + fetch (no SDK publish call needed):
//
//   export default {
//     name: 'template-example',
//     interval: 300_000, // 5 minutes, in ms
//     fetch: async () => {
//       const items = await loadThings();
//       return items; // scheduler wraps + stores + broadcasts this for you
//     },
//   };
// -----------------------------------------------------------------------------
