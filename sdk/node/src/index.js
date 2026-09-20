"use strict";

const net = require("node:net");
const tls = require("node:tls");
const { AsyncLocalStorage } = require("node:async_hooks");
const { performance } = require("node:perf_hooks");
const VERSION = "0.8.0";
const requestTraceparent = new AsyncLocalStorage();

function elapsedSince(started) {
  return Math.max(0, performance.now() - started);
}

function deadlineAfter(started, budgetMs) {
  const now = performance.now();
  return now + Math.max(0, budgetMs - Math.max(0, now - started));
}

function remainingWindows(freshMs, staleMs, elapsedMs) {
  const fresh = Math.max(0, freshMs - elapsedMs);
  const hard = Math.max(0, freshMs + staleMs - elapsedMs);
  return [fresh, Math.max(0, hard - fresh)];
}

class MegaCacheError extends Error {
  constructor(message, code = "CLIENT") {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
  }
}
class ConnectionError extends MegaCacheError {}
class ProtocolError extends MegaCacheError {}
class CommandError extends MegaCacheError {
  constructor(message) {
    super(message, message.split(" ", 1)[0] || "ERR");
  }
}

function encode(parts) {
  const buffers = parts.map((part) => Buffer.isBuffer(part) ? part : Buffer.from(String(part)));
  return Buffer.concat([
    Buffer.from(`*${buffers.length}\r\n`),
    ...buffers.flatMap((part) => [Buffer.from(`$${part.length}\r\n`), part, Buffer.from("\r\n")])
  ]);
}

function parse(buffer, offset = 0, limits = {}, depth = 0, start = offset) {
  const selected = {
    maxResponseBytes: 16 * 1024 * 1024,
    maxBulkLength: 16 * 1024 * 1024,
    maxArrayLength: 100000,
    maxResponseDepth: 128,
    ...limits
  };
  if (depth > selected.maxResponseDepth) throw new ProtocolError("RESP nesting exceeds limit");
  if (offset >= buffer.length) return null;
  const ensureSize = (end) => {
    if (end - start > selected.maxResponseBytes) {
      throw new ProtocolError("response exceeds byte limit");
    }
  };
  const marker = String.fromCharCode(buffer[offset]);
  const end = buffer.indexOf("\r\n", offset + 1);
  if (end < 0) {
    ensureSize(buffer.length);
    return null;
  }
  ensureSize(end + 2);
  const line = buffer.subarray(offset + 1, end).toString();
  let cursor = end + 2;
  if (marker === "+" || marker === "-") return { value: line, error: marker === "-", next: cursor };
  if (marker === ":") {
    if (!/^-?\d+$/.test(line)) throw new ProtocolError("invalid integer");
    const value = Number(line);
    if (!Number.isSafeInteger(value)) throw new ProtocolError("integer exceeds safe range");
    return { value, next: cursor };
  }
  if (marker === "$") {
    if (!/^-?\d+$/.test(line)) throw new ProtocolError("invalid bulk length");
    const length = Number(line);
    if (length === -1) return { value: null, next: cursor };
    if (!Number.isSafeInteger(length) || length < 0) throw new ProtocolError("invalid bulk length");
    if (length > selected.maxBulkLength) throw new ProtocolError("bulk string exceeds limit");
    ensureSize(cursor + length + 2);
    if (buffer.length < cursor + length + 2) return null;
    if (buffer.subarray(cursor + length, cursor + length + 2).toString() !== "\r\n") {
      throw new ProtocolError("invalid bulk terminator");
    }
    return { value: Buffer.from(buffer.subarray(cursor, cursor + length)), next: cursor + length + 2 };
  }
  if (marker === "*") {
    if (!/^-?\d+$/.test(line)) throw new ProtocolError("invalid array length");
    const count = Number(line);
    if (count === -1) return { value: null, next: cursor };
    if (!Number.isSafeInteger(count) || count < 0 || count > selected.maxArrayLength) {
      throw new ProtocolError("invalid array length");
    }
    const values = [];
    for (let index = 0; index < count; index += 1) {
      const item = parse(buffer, cursor, selected, depth + 1, start);
      if (!item) return null;
      if (item.error) throw new CommandError(item.value);
      values.push(item.value);
      cursor = item.next;
    }
    return { value: values, next: cursor };
  }
  throw new ProtocolError(`unknown RESP marker ${marker}`);
}

class RespConnection {
  constructor(options = {}) {
    this.options = {
      host: "127.0.0.1",
      port: 6380,
      timeoutMs: 5000,
      maxResponseBytes: 16 * 1024 * 1024,
      maxBulkLength: 16 * 1024 * 1024,
      maxArrayLength: 100000,
      maxResponseDepth: 128,
      ...options
    };
    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this.waiters = [];
    this.connecting = null;
    this.pendingSocket = null;
  }

  async connect() {
    if (this.connecting) return this.connecting;
    if (this.socket && !this.socket.destroyed) return;
    this.buffer = Buffer.alloc(0);
    const attempt = new Promise((resolve, reject) => {
      const onConnect = async () => {
        socket.removeListener("error", rejectConnect);
        try {
          if (this.options.password) {
            const auth = this.options.username
              ? ["AUTH", this.options.username, this.options.password]
              : ["AUTH", this.options.password];
            const response = await this.writeConnected(socket, auth);
            if (typeof response !== "string" || response !== "OK") {
              throw new ProtocolError("AUTH response must be OK");
            }
          }
          if (socket.destroyed) throw new ConnectionError("connection closed", "CONNECTION");
          this.socket = socket;
          this.pendingSocket = null;
          resolve();
        } catch (error) {
          socket.destroy();
          reject(error);
        }
      };
      const rejectConnect = (error) => {
        this.pendingSocket = null;
        reject(error instanceof MegaCacheError
          ? error : new ConnectionError(error.message, "CONNECTION"));
      };
      const socket = this.options.tls
        ? tls.connect({
            host: this.options.host,
            port: this.options.port,
            servername: this.options.serverName || this.options.host,
            ca: this.options.ca,
            rejectUnauthorized: true,
            minVersion: "TLSv1.2"
          }, onConnect)
        : net.createConnection({ host: this.options.host, port: this.options.port }, onConnect);
      this.pendingSocket = socket;
      socket.once("error", rejectConnect);
      socket.setTimeout(this.options.timeoutMs, () => socket.destroy(new Error("connection timed out")));
      socket.on("data", (chunk) => {
        this.buffer = Buffer.concat([this.buffer, chunk]);
        const allowed = this.options.maxResponseBytes * Math.max(1, this.waiters.length);
        if (this.buffer.length > allowed || this.waiters.length === 0) {
          const error = new ProtocolError("unsolicited or oversized response");
          this.fail(error);
          this.buffer = Buffer.alloc(0);
          socket.destroy();
          return;
        }
        this.drain();
      });
      socket.on("error", (error) => this.fail(new ConnectionError(error.message, "CONNECTION")));
      socket.on("close", () => {
        this.fail(new ConnectionError("connection closed", "CONNECTION"));
        if (this.pendingSocket === socket) {
          rejectConnect(new Error("connection closed"));
        }
        if (this.socket === socket) {
          this.socket = null;
          this.buffer = Buffer.alloc(0);
        }
      });
    });
    this.connecting = attempt;
    try {
      return await attempt;
    } finally {
      if (this.connecting === attempt) this.connecting = null;
    }
  }

  writeConnected(socket, parts) {
    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject });
      socket.write(encode(parts), (error) => {
        if (error) {
          const wrapped = new ConnectionError(error.message, "CONNECTION");
          this.fail(wrapped);
          this.buffer = Buffer.alloc(0);
          socket.destroy();
        }
      });
    });
  }

  async command(parts) {
    await this.connect();
    const socket = this.socket;
    if (!socket || socket.destroyed) {
      throw new ConnectionError("connection closed", "CONNECTION");
    }
    return this.writeConnected(socket, parts);
  }

  drain() {
    while (this.waiters.length) {
      let decoded;
      try {
        decoded = parse(this.buffer, 0, this.options);
      } catch (error) {
        this.fail(error);
        this.buffer = Buffer.alloc(0);
        if (this.socket) this.socket.destroy();
        return;
      }
      if (!decoded) return;
      this.buffer = this.buffer.subarray(decoded.next);
      const waiter = this.waiters.shift();
      if (decoded.error) waiter.reject(new CommandError(decoded.value));
      else waiter.resolve(decoded.value);
    }
    if (this.buffer.length) {
      this.poison(new ProtocolError("unsolicited response"));
    }
  }

  fail(error) {
    while (this.waiters.length) this.waiters.shift().reject(error);
  }

  poison(error) {
    this.fail(error);
    this.buffer = Buffer.alloc(0);
    this.pendingSocket?.destroy();
    this.socket?.destroy();
    this.pendingSocket = null;
    this.socket = null;
  }

  close() {
    this.poison(new ConnectionError("connection closed", "CONNECTION"));
  }
}

class LocalCache {
  constructor(maxEntries = 1000, maxBytes = 16 * 1024 * 1024) {
    if (maxEntries <= 0 || maxBytes <= 0) throw new RangeError("L1 limits must be positive");
    this.maxEntries = maxEntries;
    this.maxBytes = maxBytes;
    this.entries = new Map();
    this.usedBytes = 0;
  }
  get(key) {
    const entry = this.entries.get(key);
    if (!entry) return null;
    const now = performance.now();
    if (now >= entry.staleUntil) {
      this.remove(key);
      return null;
    }
    this.entries.delete(key);
    this.entries.set(key, entry);
    return {
      value: Buffer.from(entry.value),
      state: now < entry.freshUntil ? "fresh" : "stale",
      staleDeadline: entry.staleUntil
    };
  }
  put(key, value, ttlSeconds, staleSeconds) {
    if (ttlSeconds < 0 || staleSeconds < 0 || ttlSeconds + staleSeconds <= 0) {
      throw new RangeError("invalid L1 freshness policy");
    }
    const bytes = Buffer.isBuffer(value) ? Buffer.from(value) : Buffer.from(String(value));
    const size = Buffer.byteLength(key) + bytes.length;
    if (size > this.maxBytes) return false;
    this.remove(key);
    const now = performance.now();
    this.entries.set(key, {
      value: bytes,
      freshUntil: now + ttlSeconds * 1000,
      staleUntil: now + (ttlSeconds + staleSeconds) * 1000,
      size
    });
    this.usedBytes += size;
    while (this.entries.size > this.maxEntries || this.usedBytes > this.maxBytes) {
      this.remove(this.entries.keys().next().value);
    }
    return true;
  }
  remove(key) {
    const entry = this.entries.get(key);
    if (entry) this.usedBytes -= entry.size;
    this.entries.delete(key);
  }
  clear() {
    this.entries.clear();
    this.usedBytes = 0;
  }
}

class MegaCacheClient {
  constructor(options = {}) {
    this.connection = new RespConnection(options);
    this.local = new LocalCache(options.l1MaxEntries, options.l1MaxBytes);
    this.pollMs = options.invalidationPollMs ?? 1000;
    this.cursor = null;
    this.lastPoll = 0;
    this.flights = new Map();
    this.generation = 0;
    this.mutationQueue = Promise.resolve();
    this.traceparentProvider = options.traceparentProvider;
    this.activeTraceparent = null;
  }
  command(...parts) { return this.connection.command(parts); }
  async ping() {
    const value = this.expectText(await this.command("PING"), "PING");
    if (value !== "PONG") throw this.protocolError("PING response must be PONG");
    return value;
  }
  async get(key) {
    return this.expectBulk(await this.command("GET", key), "GET", true);
  }
  async set(key, value, expireSeconds) {
    const parts = ["SET", key, value];
    if (expireSeconds != null) parts.push("EX", expireSeconds);
    return this.runMutation(async () =>
      this.expectOk(await this.command(...parts), "SET"));
  }
  async mget(keys) {
    const value = await this.command("MGET", ...keys);
    if (!Array.isArray(value) || value.length !== keys.length) {
      throw this.protocolError("MGET response must contain one value per key");
    }
    return value.map((item) => this.expectBulk(item, "MGET", true));
  }
  async mset(values) {
    return this.runMutation(async () =>
      this.expectOk(await this.command("MSET", ...values.flat()), "MSET"));
  }
  async delete(...keys) {
    return this.runMutation(
      async () => this.expectInteger(
        await this.command("DEL", ...keys), "DEL", 0, keys.length
      ),
      (result) => Boolean(result)
    );
  }
  async exists(...keys) {
    return this.expectInteger(
      await this.command("EXISTS", ...keys), "EXISTS", 0, keys.length
    );
  }
  async expire(key, seconds) {
    return this.runMutation(
      async () => this.expectInteger(
        await this.command("EXPIRE", key, seconds), "EXPIRE", 0, 1
      ),
      (result) => Boolean(result)
    );
  }
  async ttl(key) {
    return this.expectInteger(await this.command("TTL", key), "TTL", -2);
  }
  async put(key, value, options = {}) {
    const parts = ["MC.SET", key, value];
    if (options.ttlSeconds != null) parts.push("TTL", options.ttlSeconds);
    if (options.staleSeconds != null) parts.push("STALE", options.staleSeconds);
    if (options.tags?.length) parts.push("TAGS", options.tags.length, ...options.tags);
    if (options.leaseToken) parts.push("LEASE", options.leaseToken);
    return this.runMutation(async () =>
      this.expectOk(await this.command(...parts), "MC.SET"));
  }
  async lease(key) {
    const value = await this.command("MC.LEASE", key, "WINDOWS");
    if (!Array.isArray(value) || value.length === 0) {
      throw this.protocolError("invalid MC.LEASE response");
    }
    const state = this.expectText(value[0], "MC.LEASE state");
    if (state === "fresh") {
      this.expectLeaseArity(value, state, [2, 4]);
      return {
        state, value: this.expectBulk(value[1], "MC.LEASE fresh"),
        expiresInMs: value.length > 2
          ? this.expectInteger(value[2], "MC.LEASE expires", -1) : undefined,
        staleForMs: value.length > 3
          ? this.expectInteger(value[3], "MC.LEASE stale", -1) : undefined
      };
    }
    if (state === "stale") {
      this.expectLeaseArity(value, state, [2, 3]);
      return {
        state, value: this.expectBulk(value[1], "MC.LEASE stale"),
        staleForMs: value.length > 2
          ? this.expectInteger(value[2], "MC.LEASE stale", -1) : undefined
      };
    }
    if (state === "stale_lease") {
      this.expectLeaseArity(value, state, [3, 4]);
      return {
        state, value: this.expectBulk(value[1], "MC.LEASE stale_lease"),
        leaseToken: this.expectText(value[2], "MC.LEASE lease token"),
        staleForMs: value.length > 3
          ? this.expectInteger(value[3], "MC.LEASE stale", -1) : undefined
      };
    }
    if (state === "lease") {
      this.expectLeaseArity(value, state, [2]);
      return {
        state, leaseToken: this.expectText(value[1], "MC.LEASE lease token")
      };
    }
    if (state === "loading") {
      this.expectLeaseArity(value, state, [2]);
      return {
        state,
        retryAfterMs: this.expectInteger(value[1], "MC.LEASE retry", 0)
      };
    }
    throw this.protocolError("unknown MC.LEASE state");
  }
  async fetch(key, origin, path, options = {}) {
    const parts = ["MC.FETCH", key, origin, path];
    if (options.refresh) parts.push("REFRESH");
    const traceparent = options.traceparent ?? await this.currentTraceparent();
    if (traceparent) parts.push("TRACEPARENT", traceparent);
    const document = this.expectJsonObject(await this.command(...parts), "MC.FETCH");
    if (typeof document.state !== "string" || typeof document.origin !== "string") {
      throw this.protocolError("MC.FETCH JSON requires string state and origin");
    }
    this.expectOptionalJsonInteger(document, "status_code", 100);
    this.expectOptionalJsonInteger(document, "attempts", 0);
    if (document.error != null && typeof document.error !== "string") {
      throw this.protocolError("MC.FETCH JSON error must be a string or null");
    }
    return document;
  }
  async invalidate(...tags) {
    return this.runMutation(
      async () => this.expectInteger(
        await this.command("MC.INVALIDATE", ...tags), "MC.INVALIDATE", 0
      ),
      (result) => Boolean(result)
    );
  }
  async status() {
    const document = this.expectJsonObject(
      await this.command("MC.STATUS"), "MC.STATUS"
    );
    if (typeof document.degraded !== "boolean") {
      throw this.protocolError("MC.STATUS JSON degraded must be a boolean");
    }
    return document;
  }
  async invalidations() {
    const document = this.expectJsonObject(
      await this.command("MC.INVALIDATIONS", this.cursor ?? ""),
      "MC.INVALIDATIONS"
    );
    if (
      (typeof document.cursor !== "string" &&
       !(Number.isSafeInteger(document.cursor) && document.cursor >= 0))
    ) {
      throw this.protocolError(
        "MC.INVALIDATIONS JSON cursor must be a string or non-negative integer"
      );
    }
    const hasEpoch = Object.prototype.hasOwnProperty.call(document, "epoch");
    const hasGeneration = Object.prototype.hasOwnProperty.call(document, "generation");
    if (hasEpoch !== hasGeneration) {
      throw this.protocolError(
        "MC.INVALIDATIONS JSON epoch and generation must appear together"
      );
    }
    if (hasEpoch && typeof document.epoch !== "string") {
      throw this.protocolError("MC.INVALIDATIONS JSON epoch must be a string");
    }
    if (hasGeneration) {
      this.expectOptionalJsonInteger(document, "generation", 0, true);
    }
    if (
      Object.prototype.hasOwnProperty.call(document, "changed") &&
      typeof document.changed !== "boolean"
    ) {
      throw this.protocolError("MC.INVALIDATIONS JSON changed must be a boolean");
    }
    return document;
  }
  async setTraceparent(value) {
    this.expectOk(await this.command("MC.TRACEPARENT", value), "MC.TRACEPARENT");
    this.activeTraceparent = value.toLowerCase();
  }
  async currentTraceparent() {
    const scoped = requestTraceparent.getStore();
    if (scoped) return scoped;
    if (this.traceparentProvider) {
      const value = await this.traceparentProvider();
      if (value) return value;
    }
    return this.activeTraceparent;
  }
  withTraceparent(value, callback) {
    return requestTraceparent.run(value?.toLowerCase() || null, callback);
  }
  protocolError(message) {
    const error = new ProtocolError(message);
    this.connection.poison(error);
    return error;
  }
  expectText(value, command) {
    if (typeof value === "string") return value;
    if (Buffer.isBuffer(value)) {
      const text = value.toString("utf8");
      if (!Buffer.from(text, "utf8").equals(value)) {
        throw this.protocolError(`${command} response is not valid UTF-8`);
      }
      return text;
    }
    throw this.protocolError(`${command} response must be a string`);
  }
  expectOk(value, command) {
    if (typeof value !== "string" || value !== "OK") {
      throw this.protocolError(`${command} response must be OK`);
    }
    return value;
  }
  expectBulk(value, command, nullable = false) {
    if (value === null && nullable) return null;
    if (!Buffer.isBuffer(value)) {
      throw this.protocolError(
        `${command} response must be a bulk string${nullable ? " or null" : ""}`
      );
    }
    return value;
  }
  expectInteger(value, command, minimum, maximum) {
    if (!Number.isSafeInteger(value)) {
      throw this.protocolError(`${command} response must be an integer`);
    }
    if (minimum != null && value < minimum) {
      throw this.protocolError(`${command} response is below its valid range`);
    }
    if (maximum != null && value > maximum) {
      throw this.protocolError(`${command} response is above its valid range`);
    }
    return value;
  }
  expectLeaseArity(value, state, allowed) {
    if (!allowed.includes(value.length)) {
      throw this.protocolError(`MC.LEASE ${state} response has invalid arity`);
    }
    return {};
  }
  expectJsonObject(value, command) {
    const bytes = this.expectBulk(value, command);
    let document;
    try {
      const text = bytes.toString("utf8");
      if (!Buffer.from(text, "utf8").equals(bytes)) {
        throw new Error("invalid UTF-8");
      }
      document = JSON.parse(text);
    } catch (error) {
      throw this.protocolError(`${command} returned invalid JSON`);
    }
    if (document === null || Array.isArray(document) || typeof document !== "object") {
      throw this.protocolError(`${command} must return a JSON object`);
    }
    return document;
  }
  expectOptionalJsonInteger(document, field, minimum, required = false) {
    if (!Object.prototype.hasOwnProperty.call(document, field)) {
      if (required) throw this.protocolError(`missing JSON field ${field}`);
      return;
    }
    if (document[field] === null && !required) return;
    this.expectInteger(document[field], `JSON field ${field}`, minimum);
  }
  mutationSucceeded() {
    this.generation += 1;
    this.local.clear();
    return this.generation;
  }
  runMutation(operation, changed = () => true) {
    const run = async () => {
      const result = await operation();
      if (changed(result)) this.mutationSucceeded();
      return result;
    };
    const pending = this.mutationQueue.then(run, run);
    this.mutationQueue = pending.then(() => undefined, () => undefined);
    return pending;
  }
  admitIfGeneration(generation, key, value, ttlSeconds, staleSeconds) {
    if (generation !== this.generation) return false;
    return this.local.put(key, value, ttlSeconds, staleSeconds);
  }
  async pollInvalidations() {
    if (Date.now() - this.lastPoll < this.pollMs) return;
    const result = await this.invalidations();
    const changed = this.cursor !== null && (
      result.epoch == null || result.generation == null
        ? result.cursor !== this.cursor
        : result.epoch !== this.cursorEpoch || result.generation !== this.cursorGeneration
    );
    if (changed) {
      this.mutationSucceeded();
    }
    this.cursor = result.cursor;
    this.cursorEpoch = result.epoch;
    this.cursorGeneration = result.generation;
    this.lastPoll = Date.now();
  }
  async consumeFlight(flight) {
    const result = await flight;
    if (
      result.state === "stale_if_error" &&
      (
        result.staleDeadline == null ||
        performance.now() >= result.staleDeadline
      )
    ) {
      throw result.staleError;
    }
    return { value: result.value, state: result.state };
  }
  async getOrLoad(key, loader, policy = {}) {
    const selected = { ttlSeconds: 60, staleSeconds: 300, tags: [], staleIfError: true, ...policy };
    await this.pollInvalidations();
    let cached = this.local.get(key);
    if (cached?.state === "fresh") return cached;
    if (this.flights.has(key)) {
      return this.consumeFlight(this.flights.get(key));
    }
    const flight = (async () => {
      try {
        for (;;) {
          const expectedGeneration = this.generation;
          const leaseStarted = performance.now();
          const lease = await this.lease(key);
          const leaseElapsed = elapsedSince(leaseStarted);
          if (lease.state === "fresh" && lease.value) {
            const freshBudget = Math.min(
              selected.ttlSeconds * 1000,
              lease.expiresInMs == null || lease.expiresInMs < 0
                ? selected.ttlSeconds * 1000 : lease.expiresInMs
            );
            const staleBudget = Math.min(
              selected.staleSeconds * 1000,
              lease.staleForMs == null || lease.staleForMs < 0
                ? selected.staleSeconds * 1000 : lease.staleForMs
            );
            const [freshMs, staleMs] = remainingWindows(
              freshBudget, staleBudget, leaseElapsed
            );
            if (freshMs + staleMs > 0) {
              this.admitIfGeneration(
                expectedGeneration, key, lease.value, freshMs / 1000, staleMs / 1000
              );
            }
            return { value: lease.value, state: "fresh" };
          }
          if (lease.state === "stale" && lease.value) {
            const staleBudget = Math.min(
              selected.staleSeconds * 1000,
              lease.staleForMs == null || lease.staleForMs < 0
                ? selected.staleSeconds * 1000 : lease.staleForMs
            );
            const remaining = Math.max(0, staleBudget - leaseElapsed);
            if (remaining > 0) {
              this.admitIfGeneration(expectedGeneration, key, lease.value, 0, remaining / 1000);
            }
            return {
              value: lease.value,
              state: "stale",
              staleDeadline: deadlineAfter(leaseStarted, staleBudget)
            };
          }
          if (lease.state === "loading") {
            await new Promise((resolve) => setTimeout(resolve, Math.max(1, lease.retryAfterMs)));
            continue;
          }
          if (lease.state === "stale_lease" && lease.value) {
            const staleBudget = Math.min(
              selected.staleSeconds * 1000,
              lease.staleForMs == null || lease.staleForMs < 0
                ? selected.staleSeconds * 1000 : lease.staleForMs
            );
            cached = {
              value: lease.value,
              state: "stale",
              staleDeadline: deadlineAfter(leaseStarted, staleBudget)
            };
          }
          const loaded = await loader();
          const bytes = Buffer.isBuffer(loaded) ? loaded : Buffer.from(String(loaded));
          const putStarted = performance.now();
          await this.put(key, bytes, { ...selected, leaseToken: lease.leaseToken });
          const ownGeneration = this.generation;
          const [freshMs, staleMs] = remainingWindows(
            selected.ttlSeconds * 1000,
            selected.staleSeconds * 1000,
            elapsedSince(putStarted)
          );
          if (freshMs + staleMs > 0) {
            this.admitIfGeneration(
              ownGeneration, key, bytes, freshMs / 1000, staleMs / 1000
            );
          }
          return { value: bytes, state: "loaded" };
        }
      } catch (error) {
        if (
          cached && selected.staleIfError &&
          cached.staleDeadline != null &&
          performance.now() < cached.staleDeadline
        ) {
          return {
            value: cached.value,
            state: "stale_if_error",
            staleDeadline: cached.staleDeadline,
            staleError: error
          };
        }
        throw error;
      } finally {
        this.flights.delete(key);
      }
    })();
    this.flights.set(key, flight);
    return this.consumeFlight(flight);
  }
  close() { this.connection.close(); }
}

module.exports = {
  VERSION, MegaCacheClient, LocalCache, MegaCacheError, ConnectionError, ProtocolError, CommandError
};
