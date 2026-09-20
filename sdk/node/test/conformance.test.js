"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const net = require("node:net");
const test = require("node:test");
const {
  MegaCacheClient, ProtocolError
} = require("../src");

function fixtures() {
  return Object.fromEntries(
    fs.readFileSync(process.env.MEGACACHE_FIXTURES, "utf8")
      .split("\n")
      .filter((line) => line && !line.startsWith("#"))
      .map((line) => line.split("\t"))
  );
}

function decodeCommands(socket, onCommand) {
  let buffered = Buffer.alloc(0);
  socket.on("data", (chunk) => {
    buffered = Buffer.concat([buffered, chunk]);
    for (;;) {
      const lineEnd = buffered.indexOf("\r\n");
      if (lineEnd < 0 || buffered[0] !== 42) return;
      const count = Number(buffered.subarray(1, lineEnd).toString());
      let offset = lineEnd + 2;
      const parts = [];
      for (let index = 0; index < count; index += 1) {
        const lengthEnd = buffered.indexOf("\r\n", offset);
        if (lengthEnd < 0 || buffered[offset] !== 36) return;
        const length = Number(buffered.subarray(offset + 1, lengthEnd).toString());
        const valueStart = lengthEnd + 2;
        const valueEnd = valueStart + length;
        if (buffered.length < valueEnd + 2) return;
        parts.push(buffered.subarray(valueStart, valueEnd).toString());
        offset = valueEnd + 2;
      }
      buffered = buffered.subarray(offset);
      onCommand(parts);
    }
  });
}

test("concurrent first commands wait for connection and AUTH", async () => {
  const seen = [];
  let connections = 0;
  const server = net.createServer((socket) => {
    connections += 1;
    decodeCommands(socket, (parts) => {
      seen.push(parts);
      if (parts[0] === "AUTH") {
        setTimeout(() => socket.write("+OK\r\n"), 40);
      } else {
        socket.write("+PONG\r\n");
      }
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const client = new MegaCacheClient({
    host: "127.0.0.1",
    port: server.address().port,
    password: "secret",
    timeoutMs: 1000
  });
  try {
    const first = client.ping();
    const second = client.ping();
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.deepEqual(seen.map((parts) => parts[0]), ["AUTH"]);
    assert.deepEqual(await Promise.all([first, second]), ["PONG", "PONG"]);
    assert.equal(connections, 1);
    assert.deepEqual(
      seen.map((parts) => parts[0]), ["AUTH", "PING", "PING"]
    );
  } finally {
    client.close();
    await new Promise((resolve) => server.close(resolve));
  }
});

test("all command decoders reject malformed responses", async () => {
  const cases = [
    ["ping type", 1, (client) => client.ping()],
    ["get type", "value", (client) => client.get("key")],
    ["set shape", Buffer.from("OK"), (client) => client.set("key", "value")],
    ["mget arity", [Buffer.from("value")],
      (client) => client.mget(["first", "second"])],
    ["mget element", [1], (client) => client.mget(["key"])],
    ["mset shape", Buffer.from("OK"),
      (client) => client.mset([["key", "value"]])],
    ["delete range", -1, (client) => client.delete("key")],
    ["exists range", 2, (client) => client.exists("key")],
    ["expire range", 2, (client) => client.expire("key", 30)],
    ["ttl range", -3, (client) => client.ttl("key")],
    ["put shape", 1, (client) => client.put("key", "value")],
    ["lease arity", [Buffer.from("fresh")], (client) => client.lease("key")],
    ["lease element", [
      Buffer.from("fresh"), Buffer.from("value"), Buffer.from("bad"), 1
    ], (client) => client.lease("key")],
    ["lease retry", [Buffer.from("loading"), -1],
      (client) => client.lease("key")],
    ["fetch JSON", Buffer.from("{"),
      (client) => client.fetch("key", "origin", "/path")],
    ["fetch semantics", Buffer.from('{"state":1,"origin":"origin"}'),
      (client) => client.fetch("key", "origin", "/path")],
    ["invalidate range", -1, (client) => client.invalidate("tag")],
    ["status shape", Buffer.from('{"degraded":"false"}'),
      (client) => client.status()],
    ["invalidations semantics", Buffer.from(
      '{"cursor":"x","epoch":"e","generation":"bad"}'
    ), (client) => client.invalidations()],
    ["traceparent shape", Buffer.from("OK"),
      (client) => client.setTraceparent("trace")]
  ];
  for (const [name, response, operation] of cases) {
    const client = new MegaCacheClient();
    client.command = async () => response;
    await assert.rejects(operation(client), ProtocolError, name);
  }
});

test("RESP2 and MegaCache conformance", {
  skip: !process.env.MEGACACHE_FIXTURES
}, async () => {
  const v = fixtures();
  const client = new MegaCacheClient({
    host: process.env.MEGACACHE_CONFORMANCE_HOST,
    port: Number(process.env.MEGACACHE_CONFORMANCE_PORT),
    invalidationPollMs: 0
  });
  try {
    assert.equal(await client.ping(), "PONG");
    assert.equal(await client.set(v.key, v.value), "OK");
    assert.equal((await client.get(v.key)).toString(), v.value);
    assert.equal(await client.mset([[v.second_key, v.second_value]]), "OK");
    assert.deepEqual((await client.mget([v.key, v.second_key])).map(String), [v.value, v.second_value]);
    assert.equal(await client.exists(v.key, v.second_key), 2);
    assert.equal(await client.expire(v.key, 30), 1);
    assert.ok(await client.ttl(v.key) >= 0);
    assert.equal(await client.delete(v.second_key), 1);
    await client.put(v.key, v.value, { ttlSeconds: 30, staleSeconds: 30, tags: [v.tag] });
    const lease = await client.lease(v.key);
    assert.equal(lease.state, "fresh");
    assert.ok(lease.expiresInMs > 0);
    assert.ok(lease.staleForMs > 0);
    assert.equal(await client.invalidate(v.tag), 1);
    assert.equal(await client.get(v.key), null);
    await client.setTraceparent(v.traceparent);
    const fetched = await client.fetch(v.fetch_key, v.origin, v.path);
    assert.equal(fetched.value, v.path);
    assert.equal((await client.status()).degraded, false);
  } finally {
    client.close();
  }
});

test("L1 coalesces concurrent loaders", {
  skip: !process.env.MEGACACHE_FIXTURES
}, async () => {
  const v = fixtures();
  const client = new MegaCacheClient({
    host: process.env.MEGACACHE_CONFORMANCE_HOST,
    port: Number(process.env.MEGACACHE_CONFORMANCE_PORT),
    invalidationPollMs: 0
  });

  let calls = 0;
  try {
    await client.delete(v.l1_key);
    const loader = async () => {
      calls += 1;
      await new Promise((resolve) => setTimeout(resolve, 20));
      return "loaded";
    };
    const results = await Promise.all([
      client.getOrLoad(v.l1_key, loader, { ttlSeconds: 30, staleSeconds: 30 }),
      client.getOrLoad(v.l1_key, loader, { ttlSeconds: 30, staleSeconds: 30 })
    ]);
    assert.equal(calls, 1);
    assert.deepEqual(results.map((item) => item.value.toString()), ["loaded", "loaded"]);
    const writer = new MegaCacheClient({
      host: process.env.MEGACACHE_CONFORMANCE_HOST,
      port: Number(process.env.MEGACACHE_CONFORMANCE_PORT)
    });
    await writer.set(v.l1_key, "external");
    writer.close();
    const refreshed = await client.getOrLoad(v.l1_key, async () => {
      throw new Error("fresh L2 value should avoid the loader");
    }, { ttlSeconds: 30, staleSeconds: 30 });
    assert.equal(refreshed.value.toString(), "external");
  } finally {
    client.close();
  }
});

test("zero lease freshness is not admitted and absent windows use policy", async () => {
  const client = new MegaCacheClient({ invalidationPollMs: 60000 });
  client.lastPoll = Date.now();
  client.lease = async () => ({
    state: "fresh", value: Buffer.from("zero"), expiresInMs: 0, staleForMs: 0
  });
  assert.equal((await client.getOrLoad("zero", async () => "unused")).value.toString(), "zero");
  assert.equal(client.local.get("zero"), null);

  client.lease = async () => ({ state: "fresh", value: Buffer.from("legacy") });
  await client.getOrLoad("legacy", async () => "unused", { ttlSeconds: 30, staleSeconds: 30 });
  assert.equal(client.local.get("legacy").state, "fresh");
});

test("in-flight reads cannot admit across a successful mutation", async () => {
  const client = new MegaCacheClient({ invalidationPollMs: 60000 });
  client.lastPoll = Date.now();
  let release;
  client.lease = () => new Promise((resolve) => { release = resolve; });
  const loading = client.getOrLoad("key", async () => "unused", {
    ttlSeconds: 30, staleSeconds: 30
  });
  await new Promise((resolve) => setImmediate(resolve));
  client.mutationSucceeded();
  release({ state: "fresh", value: Buffer.from("old"), expiresInMs: 30000, staleForMs: 30000 });
  assert.equal((await loading).value.toString(), "old");
  assert.equal(client.local.get("key"), null);
});

test("lease elapsed time is removed from the L1 hard deadline", async () => {
  const client = new MegaCacheClient({ invalidationPollMs: 60000 });
  client.lastPoll = Date.now();
  client.lease = async () => {
    await new Promise((resolve) => setTimeout(resolve, 60));
    return {
      state: "stale", value: Buffer.from("value"), staleForMs: 100
    };
  };
  await client.getOrLoad("key", async () => "unused", {
    ttlSeconds: 1, staleSeconds: 1
  });
  await new Promise((resolve) => setTimeout(resolve, 55));
  assert.equal(client.local.get("key"), null);
});

test("stale-if-error does not cross the lease hard deadline", async () => {
  const client = new MegaCacheClient({ invalidationPollMs: 60000 });
  client.lastPoll = Date.now();
  client.lease = async () => ({
    state: "stale_lease",
    value: Buffer.from("stale"),
    leaseToken: "token",
    staleForMs: 20
  });
  await assert.rejects(
    client.getOrLoad("key", async () => {
      await new Promise((resolve) => setTimeout(resolve, 40));
      throw new Error("load failed");
    }, { ttlSeconds: 1, staleSeconds: 1 }),
    /load failed/
  );
});

test("failed mutations retain L1 entries and generation", async () => {
  const client = new MegaCacheClient();
  client.local.put("key", "value", 30, 30);
  const generation = client.generation;
  client.command = async () => { throw new Error("rejected"); };
  await assert.rejects(client.set("other", "value"), /rejected/);
  assert.equal(client.generation, generation);
  assert.equal(client.local.get("key").value.toString(), "value");
});

test("request traceparents are isolated across asynchronous scopes", async () => {
  const client = new MegaCacheClient();
  const seen = [];
  client.command = async (...parts) => {
    seen.push(parts[parts.indexOf("TRACEPARENT") + 1]);
    return Buffer.from('{"state":"fresh","origin":"catalog"}');
  };
  await Promise.all([
    client.withTraceparent("trace-a", async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      await client.fetch("a", "catalog", "/a");
    }),
    client.withTraceparent("trace-b", async () => client.fetch("b", "catalog", "/b"))
  ]);
  assert.deepEqual(new Set(seen), new Set(["trace-a", "trace-b"]));
});

test("protocol limit failures poison and reset the connection", async () => {
  const responses = [
    "$16777217\r\n",
    "*100001\r\n",
    "*1\r\n".repeat(130) + "$1\r\nx\r\n"
  ];
  for (const response of responses) {
    let connections = 0;
    const server = net.createServer((socket) => {
      connections += 1;
      socket.once("data", () => {
        if (connections === 1) socket.write(response);
        else socket.write("+PONG\r\n");
      });
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const client = new MegaCacheClient({
      host: "127.0.0.1", port: server.address().port, timeoutMs: 1000
    });
    try {
      await assert.rejects(client.ping(), ProtocolError);
      assert.equal(client.connection.buffer.length, 0);
      assert.equal(await client.ping(), "PONG");
    } finally {
      client.close();
      await new Promise((resolve) => server.close(resolve));
    }
  }
});
