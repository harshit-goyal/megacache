export interface ClientOptions {
  host?: string; port?: number; password?: string; username?: string;
  timeoutMs?: number; tls?: boolean; ca?: string | Buffer; serverName?: string;
  l1MaxEntries?: number; l1MaxBytes?: number; invalidationPollMs?: number;
  maxResponseBytes?: number; maxBulkLength?: number; maxArrayLength?: number;
  maxResponseDepth?: number;
  traceparentProvider?: () => string | null | Promise<string | null>;
}
export interface CachePolicy {
  ttlSeconds?: number; staleSeconds?: number; tags?: string[]; staleIfError?: boolean;
}
export declare const VERSION: "0.9.0";
export declare class MegaCacheError extends Error { code: string; }
export declare class ConnectionError extends MegaCacheError {}
export declare class ProtocolError extends MegaCacheError {}
export declare class CommandError extends MegaCacheError {}
export declare class LocalCache {
  constructor(maxEntries?: number, maxBytes?: number);
  get(key: string): {value: Buffer; state: string} | null;
  put(key: string, value: Buffer | string, ttlSeconds: number, staleSeconds: number): boolean;
  clear(): void;
}
export declare class MegaCacheClient {
  constructor(options?: ClientOptions);
  ping(): Promise<string>;
  get(key: string): Promise<Buffer | null>;
  set(key: string, value: Buffer | string, expireSeconds?: number): Promise<string>;
  mget(keys: string[]): Promise<(Buffer | null)[]>;
  mset(values: [string, Buffer | string][]): Promise<string>;
  delete(...keys: string[]): Promise<number>;
  exists(...keys: string[]): Promise<number>;
  expire(key: string, seconds: number): Promise<number>;
  ttl(key: string): Promise<number>;
  put(key: string, value: Buffer | string, options?: CachePolicy & {leaseToken?: string}): Promise<string>;
  lease(key: string): Promise<Record<string, unknown>>;
  fetch(key: string, origin: string, path: string, options?: {refresh?: boolean; traceparent?: string}): Promise<Record<string, unknown>>;
  invalidate(...tags: string[]): Promise<number>;
  status(): Promise<Record<string, unknown>>;
  invalidations(): Promise<Record<string, unknown>>;
  setTraceparent(value: string): Promise<void>;
  withTraceparent<T>(value: string | null | undefined, callback: () => T): T;
  getOrLoad(key: string, loader: () => Promise<Buffer | string>, policy?: CachePolicy): Promise<{value: Buffer; state: string}>;
  close(): void;
}
