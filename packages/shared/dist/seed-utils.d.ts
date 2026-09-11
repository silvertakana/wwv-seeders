export declare function withRetry<T>(fn: () => Promise<T>, maxRetries?: number, delayMs?: number): Promise<T>;
export declare function fetchWithTimeout(url: string, options?: any, timeoutMs?: number): Promise<Response>;
/**
 * Calculates distance in Kilometers between two coordinates.
 * Useful for filtering events by proximity (e.g., nuclear test sites).
 */
export declare function haversineKm(lat1: number, lon1: number, lat2: number, lon2: number): number;
export declare const CHROME_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36";
export declare function sleep(ms: number): Promise<unknown>;
