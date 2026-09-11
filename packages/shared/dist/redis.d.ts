import { Redis } from 'ioredis';
export declare const redis: Redis;
/**
 * Convenience method to write a JSON payload to Redis with an expiration.
 * Writes are throttled to save Redis requests, but websockets are always broadcasted.
 */
export declare function setLiveSnapshot(source: string, payload: any, ttlSeconds: number): Promise<void>;
/**
 * Convenience method to read a JSON payload from Redis.
 */
export declare function getLiveSnapshot(source: string): Promise<any>;
