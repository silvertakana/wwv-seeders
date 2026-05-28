import { describe, it, expect } from 'vitest';
import { isMarketOpen } from '../isMarketOpen';

describe('isMarketOpen', () => {
  // Weekend tests
  it('returns false on Saturday at noon ET', () => {
    // 2026-05-30 is a Saturday. 16:00 UTC = 12:00 EDT (UTC-4)
    const saturday = new Date('2026-05-30T16:00:00Z');
    expect(isMarketOpen(saturday)).toBe(false);
  });

  it('returns false on Sunday at noon ET', () => {
    // 2026-05-31 is a Sunday. 16:00 UTC = 12:00 EDT (UTC-4)
    const sunday = new Date('2026-05-31T16:00:00Z');
    expect(isMarketOpen(sunday)).toBe(false);
  });

  // Weekday boundary tests
  it('returns false at 09:29 ET on a weekday', () => {
    // 2026-05-25 is a Monday. 13:29 UTC = 09:29 EDT (UTC-4)
    const beforeOpen = new Date('2026-05-25T13:29:00Z');
    expect(isMarketOpen(beforeOpen)).toBe(false);
  });

  it('returns true at 09:30 ET on a weekday', () => {
    // 2026-05-25 is a Monday. 13:30 UTC = 09:30 EDT (UTC-4)
    const atOpen = new Date('2026-05-25T13:30:00Z');
    expect(isMarketOpen(atOpen)).toBe(true);
  });

  it('returns true at 12:00 ET on a weekday', () => {
    // 2026-05-25 is a Monday. 16:00 UTC = 12:00 EDT (UTC-4)
    const midday = new Date('2026-05-25T16:00:00Z');
    expect(isMarketOpen(midday)).toBe(true);
  });

  it('returns true at 15:59 ET on a weekday', () => {
    // 2026-05-25 is a Monday. 19:59 UTC = 15:59 EDT (UTC-4)
    const nearClose = new Date('2026-05-25T19:59:00Z');
    expect(isMarketOpen(nearClose)).toBe(true);
  });

  it('returns false at 16:00 ET on a weekday', () => {
    // 2026-05-25 is a Monday. 20:00 UTC = 16:00 EDT (UTC-4)
    const atClose = new Date('2026-05-25T20:00:00Z');
    expect(isMarketOpen(atClose)).toBe(false);
  });

  // DST tests - EDT (UTC-4, summer)
  it('returns false at 09:00 EDT (13:00 UTC) on a summer weekday', () => {
    // 2026-07-15 is a Wednesday in EDT (UTC-4). 13:00 UTC = 09:00 EDT
    const beforeOpenEDT = new Date('2026-07-15T13:00:00Z');
    expect(isMarketOpen(beforeOpenEDT)).toBe(false);
  });

  it('returns true at 09:30 EDT (13:30 UTC) on a summer weekday', () => {
    // 2026-07-15 is a Wednesday in EDT (UTC-4). 13:30 UTC = 09:30 EDT
    const atOpenEDT = new Date('2026-07-15T13:30:00Z');
    expect(isMarketOpen(atOpenEDT)).toBe(true);
  });

  // DST tests - EST (UTC-5, winter)
  it('returns false at 14:29 UTC (09:29 EST) on a winter weekday', () => {
    // 2026-12-15 is a Tuesday in EST (UTC-5). 14:29 UTC = 09:29 EST
    const beforeOpenEST = new Date('2026-12-15T14:29:00Z');
    expect(isMarketOpen(beforeOpenEST)).toBe(false);
  });

  it('returns true at 14:30 UTC (09:30 EST) on a winter weekday', () => {
    // 2026-12-15 is a Tuesday in EST (UTC-5). 14:30 UTC = 09:30 EST
    const atOpenEST = new Date('2026-12-15T14:30:00Z');
    expect(isMarketOpen(atOpenEST)).toBe(true);
  });
});
