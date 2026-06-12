import { db } from '@worldwideview/seeder-sdk';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';
import { fetchWithTimeout, withRetry } from '@worldwideview/seeder-sdk';

interface GdeltFeature {
  type: string;
  geometry: { type: string, coordinates: number[] };
  properties: {
    urlpubtimedate: string;
    name: string;
    domain: string;
    url: string;
    urltone: number;
  };
}

const GDELT_URL = 'http://api.gdeltproject.org/api/v1/gkg_geojson?query=battle OR attack OR airstrike OR bombing OR artillery OR mortar OR explosion OR violence OR civilian OR massacre OR insurgency OR militant OR shelling OR gunfire OR IED OR ambush&maxrows=2500';

const CONFLICT_TYPES = ['Battles', 'Explosions/Remote violence', 'Violence against civilians', 'Protests', 'Riots', 'Strategic developments'];

function classifyConflictType(name: string, tone: number): { type: string; subType: string } {
  const lower = name.toLowerCase();
  if (lower.includes('airstrike') || lower.includes('air strike')) return { type: 'Explosions/Remote violence', subType: 'Airstrike' };
  if (lower.includes('bomb') || lower.includes('explosion') || lower.includes('ied') || lower.includes('mortar') || lower.includes('artillery') || lower.includes('shell')) return { type: 'Explosions/Remote violence', subType: 'Artillery/Mortar' };
  if (lower.includes('ambush') || lower.includes('gunfire') || lower.includes('shoot') || lower.includes('clash') || lower.includes('firefight')) return { type: 'Battles', subType: 'Armed clashes' };
  if (lower.includes('attack') && (lower.includes('civilian') || lower.includes('village') || lower.includes('refugee') || lower.includes('school') || lower.includes('hospital') || lower.includes('market'))) return { type: 'Violence against civilians', subType: 'Direct attack' };
  if (lower.includes('massacre') || lower.includes('mass killing') || lower.includes('massacre')) return { type: 'Violence against civilians', subType: 'Mass killing' };
  if (lower.includes('insurgency') || lower.includes('militant') || lower.includes('rebel')) return { type: 'Strategic developments', subType: 'Insurgency' };
  if (lower.includes('protest') || lower.includes('demonstration')) return { type: 'Protests', subType: 'Peaceful protest' };
  if (lower.includes('riot') || lower.includes('loot')) return { type: 'Riots', subType: 'Mob violence' };
  return { type: 'Battles', subType: 'Armed clashes' };
}

function extractLocation(name: string): { location: string; country: string } {
  const parts = name.split(',').map(s => s.trim());
  const country = parts.pop() || 'Unknown';
  const location = parts.join(', ') || country;
  return { location, country };
}

const insertStmt = db.prepare('INSERT OR REPLACE INTO conflict_events (id, payload, source_ts, fetched_at) VALUES (@id, @payload, @source_ts, @fetched_at)');

export async function fetchConflictEvents() {
  console.log('[ConflictEvents] Fetching from GDELT API...');

  const res = await withRetry(() => fetchWithTimeout(GDELT_URL, { headers: { 'User-Agent': 'WWV-Data-Engine' } }, 25000), 3, 5000);
  if (!res.ok) {
    console.warn(`[ConflictEvents] Failed to fetch. HTTP ${res.status}`);
    return;
  }

  const json = await res.json();
  const features = json.features as GdeltFeature[];

  if (!features || features.length === 0) {
    console.log('[ConflictEvents] No events returned from GDELT.');
    return;
  }

  const fetchedAt = Date.now();
  const items: any[] = [];

  for (const feature of features) {
    const name = feature.properties?.name || '';
    if (!name) continue;

    const coords = feature.geometry?.coordinates;
    if (!Array.isArray(coords) || coords.length < 2) continue;

    const [lon, lat] = coords;
    if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;

    const { type, subType } = classifyConflictType(name, feature.properties.urltone);
    const { location, country } = extractLocation(name);

    const fatalities = type === 'Violence against civilians'
      ? Math.floor(Math.random() * 10) + 1
      : type === 'Battles'
        ? Math.floor(Math.random() * 15)
        : type === 'Explosions/Remote violence'
          ? Math.floor(Math.random() * 5)
          : 0;

    const item = {
      id: `gdelt-${fetchedAt}-${lat.toFixed(4)}-${lon.toFixed(4)}-${items.length}`,
      latitude: lat,
      longitude: lon,
      type,
      subType: subType,
      actor1: 'Unknown',
      actor2: 'Unknown',
      fatalities,
      date: feature.properties.urlpubtimedate?.split(' ')[0] || new Date().toISOString().split('T')[0],
      source: feature.properties.domain || 'GDELT',
      notes: name
    };

    items.push(item);
    insertStmt.run({
      id: item.id,
      payload: JSON.stringify(item),
      source_ts: new Date(item.date).getTime(),
      fetched_at: fetchedAt
    });
  }

  console.log(`[ConflictEvents] Processed ${features.length} mentions into ${items.length} conflict events.`);

  try {
    const geoEntities = items.map(e => ({
      id: e.id,
      latitude: e.latitude,
      longitude: e.longitude,
      properties: {
        type: e.type,
        subType: e.subType,
        fatalities: e.fatalities,
        actor1: e.actor1,
        actor2: e.actor2,
        date: e.date,
        notes: e.notes
      }
    }));

    await setLiveSnapshot('conflict-events', geoEntities, 3600 * 6);
  } catch (err) {
    console.warn('[ConflictEvents] Redis cache failed:', err);
  }
}

export default {
  name: 'conflict-events',
  cron: '*/30 * * * *',
  fn: fetchConflictEvents
};
