// Unit tests for the lightning seeder (Blitzortung WebSocket protocol v24).
//
// Tests only the pure logic (frame parsing, item mapping, dedupe ring) with a
// realistic stroke batch; the WebSocket itself is not exercised (same
// philosophy as the other seeders: SDK is fully mocked, no native bindings).
import { describe, it, expect, vi } from 'vitest';

vi.mock('@worldwideview/seeder-sdk', () => ({
  setLiveSnapshot: vi.fn(),
}));

import { parseStrokesFrame, strokeToItem, ingestStrokes, type LightningStroke } from '../index';

describe('parseStrokesFrame', () => {
  it('parses a realistic strokes batch', () => {
    const frame = {
      strokes: [
        { src: 1, id: 13523, time: 1787674000000, lat: 51.51, lon: -0.12, srv: 0, del: 45, dev: 3500, sta: 0 },
        { src: 2, id: 812345, time: 1787674001000, lat: 40.4, lon: -3.7, srv: 0, del: 120, dev: 1200, sta: 0 },
      ],
    };
    const strokes = parseStrokesFrame(frame);
    expect(strokes).toHaveLength(2);
    expect(strokes[0]).toMatchObject({ src: 1, id: 13523, time: 1787674000000, lat: 51.51, lon: -0.12, del: 45, dev: 3500 });
  });

  it('drops non-finite and out-of-range entries', () => {
    const frame = {
      strokes: [
        { src: 1, id: 1, time: 1787674000000, lat: 91, lon: 0 },      // lat out of range
        { src: 2, id: 2, time: 1787674000000, lat: 0, lon: -181 },    // lon out of range
        { src: 3, id: 3, time: 'x', lat: 0, lon: 0 },                 // non-numeric time
        { src: 4, id: 4, time: 1787674000000, lat: 10, lon: 20 },     // valid
      ],
    };
    const strokes = parseStrokesFrame(frame);
    expect(strokes).toHaveLength(1);
    expect(strokes[0].src).toBe(4);
  });

  it('returns [] for non-stroke frames (keepalives, pings, etc.)', () => {
    expect(parseStrokesFrame({ s: 1 })).toEqual([]);
    expect(parseStrokesFrame(null)).toEqual([]);
    expect(parseStrokesFrame('nope')).toEqual([]);
    expect(parseStrokesFrame({ strokes: 'nope' })).toEqual([]);
  });

  it('omits absent dev/del instead of emitting NaN', () => {
    const [stroke] = parseStrokesFrame({ strokes: [{ src: 7, id: 9, time: 1787674000000, lat: 1, lon: 2 }] });
    expect(stroke!.dev).toBeUndefined();
    expect(stroke!.del).toBeUndefined();
  });
});

describe('strokeToItem', () => {
  it('maps a stroke to a flat GeoEntity-style item', () => {
    const item = strokeToItem({ src: 1, id: 13523, time: 1787674000000, lat: 51.51, lon: -0.12, dev: 3500, del: 45 });
    expect(item).toEqual({
      id: '1/13523',
      latitude: 51.51,
      longitude: -0.12,
      timestamp: new Date(1787674000000).toISOString(),
      amplitude: 3500,
      serverDelayMs: 45,
      src: 1,
      rawId: 13523,
    });
  });

  it('uses null for missing amplitude/delay', () => {
    const item = strokeToItem({ src: 7, id: 9, time: 1787674000000, lat: 1, lon: 2 });
    expect(item.amplitude).toBeNull();
    expect(item.serverDelayMs).toBeNull();
  });
});

describe('ingestStrokes ring', () => {
  it('dedupes by `${src}/${id}`', () => {
    const t = Date.now();
    const a: LightningStroke = { src: 1, id: 100, time: t, lat: 1, lon: 1 };
    const b: LightningStroke = { src: 1, id: 101, time: t, lat: 2, lon: 2 };
    expect(ingestStrokes([a], t)).toBe(1);
    // Duplicate id replaces the earlier entry — 0 NEW strokes, but ring still
    // holds 1 (verified by the next add being a net +1).
    expect(ingestStrokes([{ ...a, lat: 3 }], t)).toBe(0);
    expect(ingestStrokes([b], t)).toBe(1);
    expect(ingestStrokes([], t)).toBe(0);
  });

  it('rejects strokes older than the 2-minute window', () => {
    const now = Date.now();
    const old: LightningStroke = { src: 1, id: 200, time: now - 200_000, lat: 1, lon: 1 };
    expect(ingestStrokes([old], now)).toBe(0);
  });

  it('enforces the ring cap', () => {
    const now = Date.now();
    const strokes: LightningStroke[] = [];
    for (let i = 0; i < 20_500; i++) {
      strokes.push({ src: 1, id: i, time: now - i, lat: 10, lon: 10 });
    }
    const added = ingestStrokes(strokes, now);
    expect(added).toBeLessThanOrEqual(20_000);
  });
});