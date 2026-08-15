// Unit tests for the cyberAttacks seeder's pure logic:
// classifyThreat() classification precedence and the OTX pulses→indicators
// flattening inside seedCyberAttacks().
//
// The full @worldwideview/seeder-sdk is module-mocked (same pattern as
// packages/market-tracker/src/__tests__/seederContract.test.ts) so the
// better-sqlite3 native bindings and geoip-lite DB never load in the test
// environment. classifyThreat() is tested directly; the flattening logic is
// exercised end-to-end through seedCyberAttacks() with a mocked fetch response.
import { describe, it, expect, vi, beforeEach } from 'vitest';

const h = vi.hoisted(() => {
  const run = vi.fn();
  return {
    withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
    fetchWithTimeout: vi.fn(),
    setLiveSnapshot: vi.fn(),
    geolocateIp: vi.fn(),
    run,
    // prepare is invoked at module load (db.prepare at top of index.ts), so
    // the { run } shape must be wired here, not in beforeEach.
    prepare: vi.fn(() => ({ run })),
  };
});

vi.mock('@worldwideview/seeder-sdk', () => ({
  withRetry: h.withRetry,
  fetchWithTimeout: h.fetchWithTimeout,
  setLiveSnapshot: h.setLiveSnapshot,
  geolocateIp: h.geolocateIp,
  db: { prepare: h.prepare },
}));

import { classifyThreat, seedCyberAttacks } from '../index';

// Minimal structural pulse fixture — mirrors the OtxPulse interface used by
// the seeder without requiring the (module-private) interface to be exported.
interface Pulse {
  id: string;
  name: string;
  description?: string;
  created: string;
  modified: string;
  adversary?: string;
  targeted_countries?: string[];
  malware_families?: string[];
  tags?: string[];
  indicators?: { id: number; indicator: string; type: string }[];
}

function makePulse(overrides: Partial<Pulse> = {}): Pulse {
  return {
    id: 'p1',
    name: 'Sample pulse',
    description: 'A generic security pulse description.',
    created: '2024-01-01T00:00:00.000Z',
    modified: '2024-01-02T00:00:00.000Z',
    targeted_countries: ['US'],
    malware_families: ['family-a'],
    tags: ['threat'],
    indicators: [],
    ...overrides,
  };
}

function makeIndicator(
  id: number,
  indicator: string,
  type: string = 'IPv4'
): { id: number; indicator: string; type: string } {
  return { id, indicator, type };
}

const GEO = { lat: 40.71, lon: -74.0, country: 'US', city: 'New York' };

function mockFetchResults(results: Pulse[]) {
  h.fetchWithTimeout.mockResolvedValue({
    json: async () => ({ results }),
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  delete process.env.OTX_API_KEY;
  h.geolocateIp.mockReturnValue(GEO);
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.spyOn(console, 'log').mockImplementation(() => {});
});

describe('classifyThreat', () => {
  it('returns APT when tags contain apt', () => {
    expect(classifyThreat(makePulse({ tags: ['apt'] }))).toBe('APT');
  });

  it('returns APT when name contains "advanced persistent"', () => {
    expect(classifyThreat(makePulse({ name: 'Advanced Persistent Threat report' }))).toBe('APT');
  });

  it('returns APT when description contains apt', () => {
    expect(classifyThreat(makePulse({ description: 'this is an APT campaign' }))).toBe('APT');
  });

  it('gives APT precedence over generic malware when both appear', () => {
    const pulse = makePulse({ tags: ['malware', 'apt'] });
    expect(classifyThreat(pulse)).toBe('APT');
  });

  it('returns Ransomware', () => {
    expect(classifyThreat(makePulse({ tags: ['ransomware'] }))).toBe('Ransomware');
  });

  it('returns Botnet', () => {
    expect(classifyThreat(makePulse({ name: 'New botnet infrastructure' }))).toBe('Botnet');
  });

  it('returns Phishing', () => {
    expect(classifyThreat(makePulse({ tags: ['phishing'] }))).toBe('Phishing');
  });

  it('returns DDoS and is case-insensitive', () => {
    expect(classifyThreat(makePulse({ tags: ['DDOS'] }))).toBe('DDoS');
  });

  it('returns Malware for the malware keyword', () => {
    expect(classifyThreat(makePulse({ description: 'malware distribution' }))).toBe('Malware');
  });

  it('returns Malware for the trojan keyword', () => {
    expect(classifyThreat(makePulse({ tags: ['trojan'] }))).toBe('Malware');
  });

  it('returns C2 Server for the c2 keyword', () => {
    expect(classifyThreat(makePulse({ tags: ['c2'] }))).toBe('C2 Server');
  });

  it('returns C2 Server for "command and control"', () => {
    expect(classifyThreat(makePulse({ name: 'command and control panel' }))).toBe('C2 Server');
  });

  it('classifies from the combined tags+name+description text', () => {
    // Keyword only present in description
    expect(classifyThreat(makePulse({ tags: [], description: 'botnet takedown notes' }))).toBe(
      'Botnet'
    );
  });

  it('returns Other for unknown content', () => {
    expect(classifyThreat(makePulse({ tags: ['benign'], name: 'Misc' }))).toBe('Other');
  });

  it('returns Other for an empty pulse', () => {
    expect(classifyThreat(makePulse({ tags: undefined, description: undefined }))).toBe('Other');
  });

  it('returns Other when tags and description are empty strings', () => {
    expect(classifyThreat(makePulse({ tags: [], description: '' }))).toBe('Other');
  });
});

describe('seedCyberAttacks flattening', () => {
  it('skips the poll when OTX_API_KEY is not set', async () => {
    await seedCyberAttacks();
    expect(h.fetchWithTimeout).not.toHaveBeenCalled();
    expect(h.setLiveSnapshot).not.toHaveBeenCalled();
  });

  it('warns and returns on an invalid OTX response', async () => {
    process.env.OTX_API_KEY = 'test-key';
    h.fetchWithTimeout.mockResolvedValue({ json: async () => ({ notResults: true }) });
    await seedCyberAttacks();
    expect(console.warn).toHaveBeenCalledWith('[CyberAttacks] Invalid OTX response');
    expect(h.setLiveSnapshot).not.toHaveBeenCalled();
  });

  it('filters to IPv4 indicators only', async () => {
    process.env.OTX_API_KEY = 'test-key';
    mockFetchResults([
      makePulse({
        indicators: [
          makeIndicator(1, '1.2.3.4', 'IPv4'),
          makeIndicator(2, '2001:db8::1', 'IPv6'),
          makeIndicator(3, 'evil.example.com', 'domain'),
          makeIndicator(4, 'http://evil.example.com/path', 'URL'),
          makeIndicator(5, 'deadbeefdeadbeef', 'FileHash-SHA256'),
        ],
      }),
    ]);

    await seedCyberAttacks();

    expect(h.run).toHaveBeenCalledTimes(1);
    const payload = JSON.parse(h.run.mock.calls[0][0].payload);
    expect(payload.ip).toBe('1.2.3.4');
    expect(h.setLiveSnapshot).toHaveBeenCalledTimes(1);
    const snapshot = h.setLiveSnapshot.mock.calls[0];
    expect(snapshot[0]).toBe('cyber-attacks');
    expect(snapshot[1].items).toHaveLength(1);
    expect(snapshot[1].totalCount).toBe(1);
  });

  it('dedupes the same IP across indicators and pulses', async () => {
    process.env.OTX_API_KEY = 'test-key';
    mockFetchResults([
      makePulse({
        id: 'p1',
        indicators: [makeIndicator(1, '1.2.3.4'), makeIndicator(2, '1.2.3.4'), makeIndicator(3, '5.6.7.8')],
      }),
      makePulse({
        id: 'p2',
        indicators: [makeIndicator(1, '1.2.3.4'), makeIndicator(2, '9.9.9.9')],
      }),
    ]);

    await seedCyberAttacks();

    expect(h.run).toHaveBeenCalledTimes(3); // 1.2.3.4, 5.6.7.8, 9.9.9.9
    const snapshot = h.setLiveSnapshot.mock.calls[0][1];
    const ips = snapshot.items.map((i: { ip: string }) => i.ip).sort();
    expect(ips).toEqual(['1.2.3.4', '5.6.7.8', '9.9.9.9']);
    expect(snapshot.totalCount).toBe(3);
  });

  it('skips IPs that cannot be geolocated', async () => {
    process.env.OTX_API_KEY = 'test-key';
    h.geolocateIp
      .mockReturnValueOnce(null) // 1.2.3.4 skipped
      .mockReturnValueOnce(GEO); // 5.6.7.8 kept
    mockFetchResults([
      makePulse({
        indicators: [makeIndicator(1, '1.2.3.4'), makeIndicator(2, '5.6.7.8')],
      }),
    ]);

    await seedCyberAttacks();

    expect(h.run).toHaveBeenCalledTimes(1);
    expect(JSON.parse(h.run.mock.calls[0][0].payload).ip).toBe('5.6.7.8');
    expect(h.setLiveSnapshot.mock.calls[0][1].totalCount).toBe(1);
  });

  it('truncates description to 300 chars', async () => {
    process.env.OTX_API_KEY = 'test-key';
    mockFetchResults([
      makePulse({
        description: 'x'.repeat(500),
        indicators: [makeIndicator(1, '1.2.3.4')],
      }),
    ]);

    await seedCyberAttacks();

    const payload = JSON.parse(h.run.mock.calls[0][0].payload);
    expect(payload.pulseDescription).toHaveLength(300);
    expect(payload.pulseDescription).toBe('x'.repeat(300));
  });

  it('caps tags at 5 entries', async () => {
    process.env.OTX_API_KEY = 'test-key';
    mockFetchResults([
      makePulse({
        tags: ['t1', 't2', 't3', 't4', 't5', 't6', 't7'],
        indicators: [makeIndicator(1, '1.2.3.4')],
      }),
    ]);

    await seedCyberAttacks();

    const payload = JSON.parse(h.run.mock.calls[0][0].payload);
    expect(payload.tags).toEqual(['t1', 't2', 't3', 't4', 't5']);
  });

  it('builds a complete item payload with geo fields, threat type and source data', async () => {
    process.env.OTX_API_KEY = 'test-key';
    const pulse = makePulse({
      id: 'abc123',
      name: 'Ransomware campaign targets logistics',
      adversary: 'EvilCorp',
      malware_families: ['lockbit', 'conti'],
      targeted_countries: ['US', 'DE'],
      tags: ['ransomware', 'extortion'],
      indicators: [makeIndicator(42, '10.0.0.1')],
    });
    mockFetchResults([pulse]);

    await seedCyberAttacks();

    expect(h.run).toHaveBeenCalledTimes(1);
    const [insertArgs] = h.run.mock.calls[0];
    expect(insertArgs.id).toBe('otx-abc123-42');
    expect(insertArgs.source_ts).toBe(new Date('2024-01-02T00:00:00.000Z').getTime());
    expect(insertArgs.fetched_at).toEqual(expect.any(Number));

    const payload = JSON.parse(insertArgs.payload);
    expect(payload).toEqual({
      id: 'otx-abc123-42',
      ip: '10.0.0.1',
      lat: 40.71,
      lon: -74.0,
      country: 'US',
      city: 'New York',
      threatType: 'Ransomware',
      adversary: 'EvilCorp',
      pulseName: 'Ransomware campaign targets logistics',
      pulseDescription: 'A generic security pulse description.',
      malwareFamilies: ['lockbit', 'conti'],
      tags: ['ransomware', 'extortion'],
      targetedCountries: ['US', 'DE'],
      pulseId: 'abc123',
      pulseCreated: '2024-01-01T00:00:00.000Z',
      pulseModified: '2024-01-02T00:00:00.000Z',
    });
  });

  it('defaults adversary to Unknown when absent', async () => {
    process.env.OTX_API_KEY = 'test-key';
    mockFetchResults([
      makePulse({ adversary: undefined, indicators: [makeIndicator(1, '1.2.3.4')] }),
    ]);

    await seedCyberAttacks();

    const payload = JSON.parse(h.run.mock.calls[0][0].payload);
    expect(payload.adversary).toBe('Unknown');
  });

  it('handles missing pulse description and tags gracefully', async () => {
    process.env.OTX_API_KEY = 'test-key';
    mockFetchResults([
      makePulse({
        description: undefined,
        tags: undefined,
        indicators: [makeIndicator(1, '1.2.3.4')],
      }),
    ]);

    await seedCyberAttacks();

    const payload = JSON.parse(h.run.mock.calls[0][0].payload);
    expect(payload.pulseDescription).toBe('');
    expect(payload.tags).toEqual([]);
  });

  it('publishes an empty snapshot when the API returns zero pulses', async () => {
    process.env.OTX_API_KEY = 'test-key';
    mockFetchResults([]);

    await seedCyberAttacks();

    expect(h.run).not.toHaveBeenCalled();
    expect(h.setLiveSnapshot).toHaveBeenCalledTimes(1);
    const snapshot = h.setLiveSnapshot.mock.calls[0];
    expect(snapshot[0]).toBe('cyber-attacks');
    expect(snapshot[1].items).toEqual([]);
    expect(snapshot[1].totalCount).toBe(0);
    expect(snapshot[2]).toBe(7200);
  });

  it('runs the poll through withRetry and passes the OTX auth header', async () => {
    process.env.OTX_API_KEY = 'secret-key';
    mockFetchResults([
      makePulse({ indicators: [makeIndicator(1, '1.2.3.4')] }),
    ]);

    await seedCyberAttacks();

    expect(h.withRetry).toHaveBeenCalledTimes(1);
    expect(h.fetchWithTimeout).toHaveBeenCalledTimes(1);
    const [url, options] = h.fetchWithTimeout.mock.calls[0];
    expect(String(url)).toContain('https://otx.alienvault.com/api/v1/pulses/subscribed');
    expect(options.headers['X-OTX-API-KEY']).toBe('secret-key');
    expect(options.headers['User-Agent']).toBe('WWV-Data-Engine');
  });
});
