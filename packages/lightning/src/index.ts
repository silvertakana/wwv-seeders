import WebSocket from 'ws';
import { setLiveSnapshot } from '@worldwideview/seeder-sdk';

// Blitzortung live lightning feed — WebSocket protocol v24 (unofficial,
// treated as stable-today, unstable-long-term). The server sends JSON text
// frames; the primary data frame is a strokes batch:
//   {"strokes":[{src,id,time,lat,lon,srv,del,dev,sta}, ...]}
// where time = epoch ms, del = server delay ms, dev = accuracy diameter m.
//
// TERMS RISK (documented in the plugin README + marketplace description):
// blitzortung.org and limaps.org terms bar use for storm-warning systems,
// overvoltage plausibility checks, or precautionary risk analysis — including
// via third-party websites; non-commercial use only; Home Assistant data-usage
// policy requires apps to serve their OWN clients from their OWN servers
// (WWV seeder -> engine -> own clients complies). Our use is an informational
// globe overlay, NOT a storm-warning system.
const HOSTS = [
  'wss://live.lightningmaps.org:443/',
  'wss://live2.lightningmaps.org:443/',
];
const V24_REQUEST = {
  v: 24, i: {}, s: true, x: 0, w: 0, tx: 0, tw: 0, a: 4, z: 5, b: true,
  h: '', l: 0, t: 0, from_lightningmaps_org: true, p: [90, 180, -90, -180], r: 'feed',
};

const PLUGIN_ID = 'lightning';
const FLUSH_INTERVAL_MS = 10000; // WS broadcast cadence
const SNAPSHOT_TTL_SECONDS = 900; // > 3x the SDK's 5-minute Redis write throttle
const STROKE_WINDOW_MS = 120_000; // keep only the last 2 minutes of strokes
const MAX_STROKES = 20_000; // hard cap on the in-memory ring

export interface LightningStroke {
  src: number;
  id: number;
  time: number; // epoch ms
  lat: number;
  lon: number;
  dev?: number; // accuracy diameter (m)
  del?: number; // server delay (ms)
}

export interface LightningItem {
  id: string; // `${src}/${id}`
  latitude: number;
  longitude: number;
  timestamp: string; // ISO 8601 (epoch ms -> ISO)
  amplitude: number | null; // accuracy diameter (m)
  serverDelayMs: number | null;
  src: number | null;
  rawId: number | null;
}

// Reconnect backoff state — resets on a successful connection
const RECONNECT_BASE_MS = 5000;
const RECONNECT_MAX_MS = 5 * 60 * 1000; // 5 minutes cap
const HOST_FAILOVER_ATTEMPTS = 3; // rotate host after this many consecutive failures
let reconnectAttempts = 0;
let reconnectScheduled = false;
let currentHostIndex = 0;
let activeWs: WebSocket | null = null;

// Recent-stroke ring, keyed by `${src}/${id}` (dedupe), insertion ordered.
const strokeRing = new Map<string, LightningItem>();

export function parseStrokesFrame(frame: unknown): LightningStroke[] {
  if (typeof frame !== 'object' || frame === null) return [];
  const strokes = (frame as { strokes?: unknown }).strokes;
  if (!Array.isArray(strokes)) return [];

  const out: LightningStroke[] = [];
  for (const raw of strokes) {
    if (typeof raw !== 'object' || raw === null) continue;
    const s = raw as Record<string, unknown>;
    const src = Number(s.src);
    const id = Number(s.id);
    const time = Number(s.time);
    const lat = Number(s.lat);
    const lon = Number(s.lon);
    if (![src, id, time, lat, lon].every(Number.isFinite)) continue;
    if (lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;

    const stroke: LightningStroke = { src, id, time, lat, lon };
    if (Number.isFinite(Number(s.dev))) stroke.dev = Number(s.dev);
    if (Number.isFinite(Number(s.del))) stroke.del = Number(s.del);
    out.push(stroke);
  }
  return out;
}

export function strokeToItem(stroke: LightningStroke): LightningItem {
  return {
    id: `${stroke.src}/${stroke.id}`,
    latitude: stroke.lat,
    longitude: stroke.lon,
    timestamp: new Date(stroke.time).toISOString(),
    amplitude: stroke.dev ?? null,
    serverDelayMs: stroke.del ?? null,
    src: stroke.src,
    rawId: stroke.id,
  };
}

/**
 * Add parsed strokes to the ring, deduping by `${src}/${id}` (a duplicate
 * stroke replaces the earlier entry). Returns the number of new strokes added.
 */
export function ingestStrokes(strokes: LightningStroke[], nowMs = Date.now()): number {
  let added = 0;
  for (const stroke of strokes) {
    if (nowMs - stroke.time > STROKE_WINDOW_MS) continue; // too old to matter
    const item = strokeToItem(stroke);
    if (!strokeRing.has(item.id)) added++;
    strokeRing.set(item.id, item);
  }

  // Bound the ring: drop strokes older than the window, then enforce the cap.
  let overflow = 0;
  for (const [key, item] of strokeRing) {
    if (nowMs - Date.parse(item.timestamp) > STROKE_WINDOW_MS) {
      strokeRing.delete(key);
      overflow++;
    }
  }
  while (strokeRing.size > MAX_STROKES) {
    const oldest = strokeRing.keys().next().value as string | undefined;
    if (oldest === undefined) break;
    strokeRing.delete(oldest);
    overflow++;
  }
  return added - overflow; // net new strokes currently held
}

async function flushSnapshot() {
  if (strokeRing.size === 0) return;
  const now = Date.now();
  // One more sweep before publishing so the snapshot never carries stale rows.
  for (const [key, item] of strokeRing) {
    if (now - Date.parse(item.timestamp) > STROKE_WINDOW_MS) strokeRing.delete(key);
  }
  const items = [...strokeRing.values()];
  try {
    await setLiveSnapshot(
      PLUGIN_ID,
      {
        source: PLUGIN_ID,
        fetchedAt: new Date(now).toISOString(),
        items,
        totalCount: items.length,
      },
      SNAPSHOT_TTL_SECONDS
    );
  } catch (err) {
    console.error('[Lightning] snapshot failed:', err instanceof Error ? err.message : err);
  }
}

function scheduleReconnect() {
  if (reconnectScheduled) return; // coalesce concurrent error+close events
  reconnectScheduled = true;
  reconnectAttempts++;
  if (reconnectAttempts % HOST_FAILOVER_ATTEMPTS === 0) {
    currentHostIndex = (currentHostIndex + 1) % HOSTS.length;
    console.warn(`[Lightning] Rotating host to ${HOSTS[currentHostIndex]}`);
  }
  const delay = Math.min(
    RECONNECT_BASE_MS * Math.pow(2, reconnectAttempts),
    RECONNECT_MAX_MS
  );
  console.log(`[Lightning] Reconnecting in ${Math.round(delay / 1000)}s (attempt ${reconnectAttempts})`);
  setTimeout(() => {
    reconnectScheduled = false;
    startLightningWebsocket();
  }, delay);
}

export function startLightningWebsocket() {
  // The engine scheduler may retry init after a crash — never stack sockets.
  if (activeWs && (activeWs.readyState === WebSocket.OPEN || activeWs.readyState === WebSocket.CONNECTING)) {
    return;
  }

  const host = HOSTS[currentHostIndex];
  console.log(`[Lightning] Connecting to ${host}`);
  let ws: WebSocket;
  try {
    ws = new WebSocket(host);
  } catch (err) {
    console.error('[Lightning] socket creation failed:', err instanceof Error ? err.message : err);
    scheduleReconnect();
    return;
  }
  activeWs = ws;

  let watchdogTimer: NodeJS.Timeout | null = null;
  const resetWatchdog = () => {
    if (watchdogTimer) clearTimeout(watchdogTimer);
    watchdogTimer = setTimeout(() => {
      console.warn('[Lightning] Watchdog timeout: no frames in 30s. Forcing reconnect...');
      try { ws.terminate(); } catch { /* already closed */ }
    }, 30000);
  };

  ws.on('open', () => {
    console.log('[Lightning] WebSocket connected. Sending v24 feed request...');
    reconnectAttempts = 0; // reset backoff on a successful connection
    ws.send(JSON.stringify(V24_REQUEST));
    resetWatchdog();
  });

  ws.on('message', (data) => {
    resetWatchdog();
    try {
      const frame = JSON.parse(data.toString());
      const strokes = parseStrokesFrame(frame);
      if (strokes.length > 0) {
        ingestStrokes(strokes);
      }
      // Unknown frame types (e.g. server keepalives) are intentionally ignored.
    } catch (e) {
      console.error('[Lightning] Parse error on frame:', data.toString().slice(0, 200));
    }
  });

  ws.on('error', (err) => {
    console.error('[Lightning] WebSocket error:', err.message);
  });

  ws.on('close', () => {
    if (watchdogTimer) clearTimeout(watchdogTimer);
    if (activeWs === ws) activeWs = null;
    console.log('[Lightning] WebSocket closed.');
    scheduleReconnect();
  });
}

let flushIntervalStarted = false;

// Register initialization logic. No cron needed, runs infinitely.
export default {
  name: PLUGIN_ID,
  init: () => {
    try {
      startLightningWebsocket();
      if (!flushIntervalStarted) {
        setInterval(flushSnapshot, FLUSH_INTERVAL_MS);
        flushIntervalStarted = true;
      }
    } catch (err) {
      console.error('[Lightning] init failed:', err instanceof Error ? err.message : err);
    }
  },
};