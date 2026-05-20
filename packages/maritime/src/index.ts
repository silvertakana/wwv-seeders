import WebSocket from 'ws';
import { db } from '@worldwideview/seeder-sdk';
import { redis, setLiveSnapshot } from '@worldwideview/seeder-sdk';
import * as Sentry from '@sentry/node';

const AISSTREAM_URL = 'wss://stream.aisstream.io/v0/stream';
const API_KEY = process.env.AISSTREAM_API_KEY;

// Buffer to batch SQLite inserts
let messageBuffer: any[] = [];
const activeFleetCache = new Map<string, any>();
const FLUSH_INTERVAL_MS = 15000;
let isFlushIntervalRunning = false;

// Reconnect backoff state — resets on a successful connection
const RECONNECT_BASE_MS = 5000;
const RECONNECT_MAX_MS = 5 * 60 * 1000; // 5 minutes cap
let reconnectAttempts = 0;
let reconnectScheduled = false;

function scheduleReconnect() {
  if (reconnectScheduled) return; // Coalesce concurrent error+close events
  reconnectScheduled = true;
  const delay = Math.min(
    RECONNECT_BASE_MS * Math.pow(2, reconnectAttempts),
    RECONNECT_MAX_MS
  );
  reconnectAttempts++;
  console.log(`[Maritime] Reconnecting in ${Math.round(delay / 1000)}s (attempt ${reconnectAttempts})`);
  setTimeout(() => {
    reconnectScheduled = false;
    startMaritimeWebsocket();
  }, delay);
}

// SQLite insert statement
const insertHistory = db.prepare(`
  INSERT OR IGNORE INTO maritime_history (mmsi, ts, lat, lon, hdg, spd, fetched_at)
  VALUES (@mmsi, @ts, @lat, @lon, @hdg, @spd, @fetched_at)
`);

async function flushBuffer() {
  if (messageBuffer.length === 0) return;
  
  const batch = [...messageBuffer];
  messageBuffer = [];

  const fetchedAt = Date.now();
  let insertedCount = 0;

  // 1. Bulk insert to SQLite for permanent history
  const insertMany = db.transaction((msgs) => {
    for (const msg of msgs) {
      if (!msg.MetaData?.MMSI || !msg.Message?.PositionReport) continue;
      
      const mmsi = msg.MetaData.MMSI.toString();
      const report = msg.Message.PositionReport;
      const ts = Math.floor(new Date(msg.MetaData.time_utc).getTime() / 1000); // Epoch seconds
      
      const result = insertHistory.run({
        mmsi,
        ts,
        lat: report.Latitude,
        lon: report.Longitude,
        hdg: report.TrueHeading,
        spd: report.Sog,
        fetched_at: fetchedAt
      });
      if (result.changes > 0) insertedCount++;
    }
  });

  try {
    insertMany(batch);
    if (insertedCount > 0) {
      // console.log(`[Maritime] Flushed ${insertedCount} new positions to SQLite history`);
    }

    // 2. Update memory cache and serialize to snapshot
    const nowSecs = Math.floor(Date.now() / 1000);
    // Cleanup stale ships (>6 hours)
    for (const [mmsi, ship] of activeFleetCache.entries()) {
      if (nowSecs - ship.last_updated > 6 * 3600) {
        activeFleetCache.delete(mmsi);
      }
    }

    for (const msg of batch) {
      if (!msg.MetaData?.MMSI || !msg.Message?.PositionReport) continue;
      const mmsi = msg.MetaData.MMSI.toString();
      const report = msg.Message.PositionReport;
      const ts = Math.floor(new Date(msg.MetaData.time_utc).getTime() / 1000);
      
      const shipState = {
        id: `mmsi-${mmsi}`,
        mmsi,
        name: msg.MetaData.ShipName ? msg.MetaData.ShipName.trim() : `Unknown (${mmsi})`,
        lat: report.Latitude,
        lon: report.Longitude,
        hdg: report.TrueHeading,
        spd: report.Sog,
        last_updated: ts
      };
      
      activeFleetCache.set(mmsi, shipState);
    }
    
    await setLiveSnapshot('maritime', Object.fromEntries(activeFleetCache), 6 * 3600);

  } catch (err) {
    console.error('[Maritime] Buffer flush failed:', err);
    Sentry.captureException(err, { extra: { context: 'flushBuffer', type: 'maritime' } });
  }
}

export function startMaritimeWebsocket() {
  if (!API_KEY) {
    console.warn('[Maritime] Skipping AIS websocket: AISSTREAM_API_KEY not set.');
    return;
  }

  console.log('[Maritime] Connecting to AisStream.io...');
  // AisStream's TLS cert has expired in the past — bypass cert verification so a
  // broken upstream cert doesn't kill our seeder. We don't auth using their cert.
  const ws = new WebSocket(AISSTREAM_URL, { rejectUnauthorized: false });

  let watchdogTimer: NodeJS.Timeout | null = null;

  const resetWatchdog = () => {
    if (watchdogTimer) clearTimeout(watchdogTimer);
    watchdogTimer = setTimeout(() => {
      console.warn('[Maritime] Watchdog timeout: No messages received in 30s. Forcing reconnect...');
      ws.terminate(); // Force close the socket to trigger reconnect
    }, 30000);
  };

  ws.on('open', () => {
    console.log('[Maritime] WebSocket connected. Subscribing to global feed...');
    reconnectAttempts = 0; // Reset backoff on a successful connection
    const subscriptionMessage = {
      APIKey: API_KEY,
      BoundingBoxes: [[[-90, -180], [90, 180]]],
      FiltersShipMMSI: [],
      FilterMessageTypes: ["PositionReport"]
    };
    ws.send(JSON.stringify(subscriptionMessage));
    resetWatchdog();
  });

  ws.on('message', (data) => {
    resetWatchdog();
    try {
      const msg = JSON.parse(data.toString());
      if (msg.MessageType === "PositionReport" || msg.Message?.PositionReport) {
        messageBuffer.push(msg);
      } else if (msg.Error || msg.error) {
        console.error('[Maritime] AISStream Error message received:', msg);
      } else {
        if (msg.MessageType !== 'SubscriptionMessage') {
          // console.log('[Maritime] Non-PositionReport msg:', msg.MessageType);
        }
      }
    } catch (e) {
      console.error('[Maritime] AISStream Parse Error. Raw data:', data.toString());
    }
  });

  ws.on('error', (err) => {
    console.error('[Maritime] WebSocket error:', err.message);
    Sentry.captureException(err, { extra: { context: 'websocket_error', type: 'maritime' } });
  });

  ws.on('close', () => {
    if (watchdogTimer) clearTimeout(watchdogTimer);
    console.log('[Maritime] WebSocket closed.');
    scheduleReconnect();
  });

  // Start background flush loop safely
  if (!isFlushIntervalRunning) {
    setInterval(flushBuffer, FLUSH_INTERVAL_MS);
    isFlushIntervalRunning = true;
  }
}

// Register initialization logic. No cron needed, runs infinitely.
export default {
  name: "maritime",
  init: startMaritimeWebsocket
};
