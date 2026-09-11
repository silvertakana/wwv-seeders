import { type Database as BetterSqlite3Database } from 'better-sqlite3';
export declare const db: BetterSqlite3Database;
/**
 * Initialize all required tables for the different seeders.
 * This runs synchronously on boot.
 */
export declare function initDB(): void;
export declare function pruneHistoryTables(): void;
