import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

// TeleGeography submarine cable dataset, served by the Submarine Cable Map
// project (CC BY 4.0) as a GeoJSON FeatureCollection of cable routes.
export const SOURCE_URL = 'https://www.submarinecablemap.com/api/v3/cable/cable-geo.json';

// Cable networks change slowly (new systems land over months), so a 6-hour
// cadence keeps snapshots fresh without hammering the upstream endpoint.
export const INTERVAL_MS = 6 * 60 * 60 * 1000;

const FETCH_TIMEOUT_MS = 30_000;
const RETRY_LIMIT = 2;

export interface CableFeatureCollection {
  type: 'FeatureCollection';
  name?: string;
  features: unknown[];
  [key: string]: unknown;
}

export async function fetchUnderseaCables(): Promise<CableFeatureCollection | null> {
  const response = await withRetry(
    () =>
      fetchWithTimeout(
        SOURCE_URL,
        {
          headers: {
            'User-Agent': 'WorldWideView/1.0',
            Accept: 'application/json',
          },
        },
        FETCH_TIMEOUT_MS
      ),
    RETRY_LIMIT
  );

  // fetchWithTimeout already throws on non-2xx; this is a defensive double-check.
  if (!response.ok) {
    throw new Error(`UnderseaCables: HTTP ${response.status} from ${SOURCE_URL}`);
  }

  let parsed: unknown;
  try {
    parsed = await response.json();
  } catch (err) {
    throw new Error(
      `UnderseaCables: source returned malformed JSON: ${err instanceof Error ? err.message : String(err)}`
    );
  }

  const collection = parsed as CableFeatureCollection;
  if (!collection || typeof collection !== 'object' || collection.type !== 'FeatureCollection') {
    throw new Error('UnderseaCables: source payload is not a GeoJSON FeatureCollection');
  }
  if (!Array.isArray(collection.features)) {
    throw new Error('UnderseaCables: source payload is missing the features array');
  }

  // Nothing to publish: return null so the engine scheduler skips the snapshot.
  if (collection.features.length === 0) {
    console.warn('[UnderseaCables] Empty FeatureCollection from source; skipping publish.');
    return null;
  }

  return collection;
}

export default {
  name: 'undersea-cables',
  interval: INTERVAL_MS,
  fetch: fetchUnderseaCables,
};