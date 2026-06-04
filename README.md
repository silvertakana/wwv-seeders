# WorldWideView Community Seeders

Open-source **seeders** for the WorldWideView data engine. A seeder fetches data
from some upstream source and publishes it so a frontend plugin can stream it
onto the globe in real time.

This is a `pnpm` workspace: each seeder is a package under `packages/`.

## What a seeder is

The engine **auto-discovers** seeders — you never register one. At startup it
scans for any `packages/<seeder>/dist/index.mjs` and imports its **default
export**. The seeder's **id is the folder name**, and you must export a matching
`name` (kebab-case) that equals the frontend plugin's `id` — see
[ADR-0002](#plugin-id-rule).

## Quick start

```bash
# 1. Copy the template
cp -r packages/_template packages/my-seeder

# 2. In packages/my-seeder/package.json: rename to "@wwv-seeders/my-seeder"
#    and add the build script:  "scripts": { "build": "tsup" }

# 3. Edit src/index.ts: set `name`, replace the fetch logic, delete the
#    ENABLED dormancy guard.

# 4. Build + test from the repo root
pnpm install
pnpm -r build
```

`packages/_template/` is a complete, commented reference. Read it first.

## The module contract

Your default export is `{ name }` plus **one** of these shapes:

| Shape | You provide | How data is published |
|---|---|---|
| `interval` (ms) + `fetch(ctx)` | `fetch` **returns** the array | the scheduler wraps + stores + broadcasts it for you |
| `cron` (cron string) + `fn(ctx)` | `fn` publishes itself | `import { setLiveSnapshot } from '@worldwideview/seeder-sdk'` and call it |
| `init(ctx)` | a persistent listener | publish via the SDK as push data arrives |

### The one trap: `ctx` is only `{ redis }`

The argument your handler receives is **only `{ redis }`**. There is **no
`ctx.setLiveSnapshot`** — calling it throws and your data silently never
publishes (this caused a real outage in CI). Every helper — `setLiveSnapshot`,
`db`, `fetchWithTimeout`, `withRetry` — comes from **`@worldwideview/seeder-sdk`**,
never from `ctx` and never from a bare global.

```ts
// ❌ WRONG — ctx has no such method; dies silently
fn: async (ctx) => { await ctx.setLiveSnapshot('x', items, 3600); }

// ✅ RIGHT — import it from the SDK
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
fn: async () => { await setLiveSnapshot('x', { source: 'x', fetchedAt: new Date().toISOString(), items, totalCount: items.length }, 3600); }

// ✅ ALSO RIGHT — interval + fetch: just return the array
fetch: async () => items;
```

### Host-provided dependencies

The engine provides common deps at runtime (`zod`, `ws`, `node-cron`, `undici`,
`satellite.js`, `geoip-lite`, ...). **Do not bundle them** — keep them `external`
in `tsup` (the template's `tsup.config.ts` already does this). Bundling balloons
the seeder and can break dynamic resolution.

## <a id="plugin-id-rule"></a>Plugin ID rule (ADR-0002)

`name` MUST exactly equal the frontend plugin's `id` (kebab-case). The engine
uses it for the `/manifest` list, WebSocket `pluginId`, and the Redis key
`data:<name>:live`. Folder name should match too.

## Payload shape

A flat `GeoEntity[]` needs no frontend handler. An object payload
(`{ items: [...] }` or `{ satellites: [...] }`) requires the frontend plugin to
implement `mapWebsocketPayload`, or the engine **silently drops it**. See the
`data-engine-architecture` rule §7 in the main app repo for the full contract.

## Release & deploy

Push to `main` runs `.github/workflows/release.yml`: it builds every package
that has a `build` script, zips each one's `dist/` (skipping `shared/` and any
package without a `dist/`), publishes a GitHub Release, and restarts the engine
on Coolify. `_template` has no build script, so it is never built or released.

## Authoritative reference

The canonical seeder contract lives in `wwv-data-engine/README.md` →
"Authoring a Seeder". The engine types `ctx` as `SeederContext { redis }`, so the
`ctx.setLiveSnapshot` mistake is a compile error for type-checked seeders.
