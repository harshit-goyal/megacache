package io.megacache;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class ConformanceTest {
    @FunctionalInterface
    private interface ClientCall {
        void run(MegaCacheClient client) throws Exception;
    }

    private static Map<String, String> fixtures() throws Exception {
        Map<String, String> values = new LinkedHashMap<>();
        for (String line : Files.readAllLines(Path.of(System.getenv("MEGACACHE_FIXTURES")))) {
            if (line.isEmpty() || line.startsWith("#")) continue;
            String[] parts = line.split("\t", 2); values.put(parts[0], parts[1]);
        }
        return values;
    }
    private static MegaCacheClient client() {
        MegaCacheClient.Options options = new MegaCacheClient.Options();
        options.host = System.getenv("MEGACACHE_CONFORMANCE_HOST");
        options.port = Integer.parseInt(System.getenv("MEGACACHE_CONFORMANCE_PORT"));
        options.invalidationPoll = Duration.ZERO;
        return new MegaCacheClient(options);
    }
    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    private static void protocolLimitRegression(String response) throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            Thread responder = new Thread(() -> {
                try {
                    try (Socket socket = server.accept()) {
                        socket.getInputStream().read();
                        OutputStream output = socket.getOutputStream();
                        output.write(response.getBytes(StandardCharsets.US_ASCII));
                        output.flush();
                    }
                    try (Socket socket = server.accept()) {
                        socket.getInputStream().read();
                        OutputStream output = socket.getOutputStream();
                        output.write("+PONG\r\n".getBytes(StandardCharsets.US_ASCII));
                        output.flush();
                    }
                } catch (Exception error) {
                    throw new RuntimeException(error);
                }
            });
            responder.start();
            MegaCacheClient.Options options = new MegaCacheClient.Options();
            options.host = "127.0.0.1";
            options.port = server.getLocalPort();
            options.timeout = Duration.ofSeconds(1);
            try (MegaCacheClient client = new MegaCacheClient(options)) {
                boolean rejected = false;
                try { client.ping(); }
                catch (MegaCacheClient.ProtocolException expected) { rejected = true; }
                check(rejected, "oversized bulk response was accepted");
                check("PONG".equals(client.ping()), "poisoned connection was reused");
            }
            responder.join();
        }
    }
    private static String bulk(String value) {
        return "$" + value.getBytes(StandardCharsets.UTF_8).length
            + "\r\n" + value + "\r\n";
    }
    private static void malformedResponseRegression(
        String name, String response, ClientCall call
    ) throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            Thread responder = respondOnce(server, response, 0);
            try (MegaCacheClient client = deadlineClient(server.getLocalPort())) {
                boolean rejected = false;
                try { call.run(client); }
                catch (MegaCacheClient.ProtocolException expected) {
                    rejected = true;
                }
                check(rejected, name + " did not raise ProtocolException");
            }
            responder.join();
        }
    }
    private static void authExceptionRegressions() throws Exception {
        for (boolean protocol : new boolean[]{false, true}) {
            try (ServerSocket server = new ServerSocket(0)) {
                Thread responder = respondOnce(
                    server,
                    protocol ? ":1\r\n" : "-WRONGPASS invalid credentials\r\n",
                    0);
                MegaCacheClient.Options options = new MegaCacheClient.Options();
                options.host = "127.0.0.1";
                options.port = server.getLocalPort();
                options.password = "secret";
                options.timeout = Duration.ofSeconds(1);
                try (MegaCacheClient client = new MegaCacheClient(options)) {
                    boolean rejected = false;
                    try { client.ping(); }
                    catch (MegaCacheClient.ProtocolException expected) {
                        rejected = protocol;
                    }
                    catch (MegaCacheClient.CommandException expected) {
                        rejected = !protocol;
                    }
                    check(rejected, "AUTH exception type was not preserved");
                }
                responder.join();
            }
        }
    }
    private static void commandDecoderRegressions() throws Exception {
        malformedResponseRegression(
            "PING type", ":1\r\n", MegaCacheClient::ping);
        malformedResponseRegression(
            "GET type", "+value\r\n", client -> client.get("key"));
        malformedResponseRegression(
            "SET shape", bulk("OK"),
            client -> client.set("key", "value".getBytes(StandardCharsets.UTF_8), null));
        malformedResponseRegression(
            "MGET arity", "*1\r\n$5\r\nvalue\r\n",
            client -> client.mget(java.util.List.of("first", "second")));
        malformedResponseRegression(
            "MGET element", "*1\r\n:1\r\n",
            client -> client.mget(java.util.List.of("key")));
        malformedResponseRegression(
            "MSET shape", bulk("OK"),
            client -> client.mset(Map.of(
                "key", "value".getBytes(StandardCharsets.UTF_8))));
        malformedResponseRegression(
            "DEL range", ":-1\r\n", client -> client.delete("key"));
        malformedResponseRegression(
            "EXISTS range", ":2\r\n", client -> client.exists("key"));
        malformedResponseRegression(
            "EXPIRE range", ":2\r\n",
            client -> client.expire("key", Duration.ofSeconds(30)));
        malformedResponseRegression(
            "TTL range", ":-3\r\n", client -> client.ttl("key"));
        malformedResponseRegression(
            "MC.SET shape", ":1\r\n",
            client -> client.put(
                "key", "value".getBytes(StandardCharsets.UTF_8),
                new MegaCacheClient.CachePolicy(), null));
        malformedResponseRegression(
            "MC.LEASE arity", "*1\r\n$5\r\nfresh\r\n",
            client -> client.lease("key"));
        malformedResponseRegression(
            "MC.LEASE element",
            "*4\r\n$5\r\nfresh\r\n$5\r\nvalue\r\n$3\r\nbad\r\n:1\r\n",
            client -> client.lease("key"));
        malformedResponseRegression(
            "MC.LEASE retry", "*2\r\n$7\r\nloading\r\n:-1\r\n",
            client -> client.lease("key"));
        malformedResponseRegression(
            "MC.FETCH JSON", bulk("{"),
            client -> client.fetch("key", "origin", "/path", false, null));
        malformedResponseRegression(
            "MC.FETCH semantics",
            bulk("{\"state\":1,\"origin\":\"origin\"}"),
            client -> client.fetch("key", "origin", "/path", false, null));
        malformedResponseRegression(
            "MC.INVALIDATE range", ":-1\r\n",
            client -> client.invalidate("tag"));
        malformedResponseRegression(
            "MC.STATUS semantics", bulk("{\"degraded\":\"false\"}"),
            MegaCacheClient::status);
        malformedResponseRegression(
            "MC.INVALIDATIONS semantics",
            bulk("{\"cursor\":\"x\",\"epoch\":\"e\",\"generation\":\"bad\"}"),
            client -> {
                Field field = MegaCacheClient.class.getDeclaredField(
                    "lastPollNanos");
                field.setAccessible(true);
                field.setLong(
                    client,
                    System.nanoTime() - Duration.ofDays(3650).toNanos());
                client.getOrLoad(
                    "key", () -> "value".getBytes(StandardCharsets.UTF_8),
                    new MegaCacheClient.CachePolicy());
            });
        malformedResponseRegression(
            "MC.TRACEPARENT shape", bulk("OK"),
            client -> client.setTraceparent("trace"));
    }
    private static Thread respondOnce(
        ServerSocket server, String response, long delayMillis
    ) {
        Thread responder = new Thread(() -> {
            try (Socket socket = server.accept()) {
                socket.getInputStream().read();
                Thread.sleep(delayMillis);
                OutputStream output = socket.getOutputStream();
                output.write(response.getBytes(StandardCharsets.US_ASCII));
                output.flush();
            } catch (Exception error) {
                throw new RuntimeException(error);
            }
        });
        responder.start();
        return responder;
    }
    private static MegaCacheClient deadlineClient(int port) {
        MegaCacheClient.Options options = new MegaCacheClient.Options();
        options.host = "127.0.0.1";
        options.port = port;
        options.timeout = Duration.ofSeconds(1);
        options.invalidationPoll = Duration.ofDays(3650);
        return new MegaCacheClient(options);
    }
    private static MegaCacheClient.CachedValue localValue(
        MegaCacheClient client, String key
    ) throws Exception {
        Field localField = MegaCacheClient.class.getDeclaredField("local");
        localField.setAccessible(true);
        Object local = localField.get(client);
        Method get = local.getClass().getDeclaredMethod("get", String.class);
        get.setAccessible(true);
        return (MegaCacheClient.CachedValue) get.invoke(local, key);
    }
    private static void staleDeadlineRegressions() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            Thread responder = respondOnce(
                server,
                "*3\r\n$5\r\nstale\r\n$5\r\nvalue\r\n:100\r\n",
                60);
            try (MegaCacheClient client = deadlineClient(server.getLocalPort())) {
                MegaCacheClient.CachePolicy policy = new MegaCacheClient.CachePolicy();
                policy.ttl = Duration.ofSeconds(1);
                policy.stale = Duration.ofSeconds(1);
                MegaCacheClient.CachedValue value = client.getOrLoad(
                    "key", () -> "unused".getBytes(StandardCharsets.UTF_8), policy);
                check("stale".equals(value.state), "stale lease response");
                Thread.sleep(55);
                check(localValue(client, "key") == null, "L1 hard deadline exceeded");
            }
            responder.join();
        }
        try (ServerSocket server = new ServerSocket(0)) {
            Thread responder = respondOnce(
                server,
                "*4\r\n$11\r\nstale_lease\r\n$5\r\nstale\r\n"
                    + "$5\r\ntoken\r\n:20\r\n",
                0);
            try (MegaCacheClient client = deadlineClient(server.getLocalPort())) {
                MegaCacheClient.CachePolicy policy = new MegaCacheClient.CachePolicy();
                policy.ttl = Duration.ofSeconds(1);
                policy.stale = Duration.ofSeconds(1);
                boolean failed = false;
                try {
                    client.getOrLoad("key", () -> {
                        try { Thread.sleep(40); }
                        catch (InterruptedException error) {
                            Thread.currentThread().interrupt();
                        }
                        throw new IllegalStateException("load failed");
                    }, policy);
                } catch (IllegalStateException expected) {
                    failed = true;
                }
                check(failed, "expired stale-if-error value was returned");
            }
            responder.join();
        }
    }
    private static void coalescedErrorRegression() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            Thread responder = respondOnce(
                server,
                "*2\r\n$5\r\nlease\r\n$5\r\ntoken\r\n",
                0);
            try (MegaCacheClient client = deadlineClient(server.getLocalPort())) {
                MegaCacheClient.CachePolicy policy =
                    new MegaCacheClient.CachePolicy();
                CountDownLatch entered = new CountDownLatch(1);
                CountDownLatch release = new CountDownLatch(1);
                AssertionError failure = new AssertionError("loader failed");
                AtomicReference<Throwable> leaderFailure =
                    new AtomicReference<>();
                AtomicReference<Throwable> followerFailure =
                    new AtomicReference<>();
                Thread leader = new Thread(() -> {
                    try {
                        client.getOrLoad("key", () -> {
                            entered.countDown();
                            try { release.await(); }
                            catch (InterruptedException error) {
                                Thread.currentThread().interrupt();
                            }
                            throw failure;
                        }, policy);
                    } catch (Throwable error) {
                        leaderFailure.set(error);
                    }
                });
                leader.start();
                check(entered.await(1, java.util.concurrent.TimeUnit.SECONDS),
                    "leader loader did not start");
                Thread follower = new Thread(() -> {
                    try {
                        client.getOrLoad(
                            "key",
                            () -> {
                                throw new AssertionError(
                                    "follower unexpectedly became leader");
                            },
                            policy);
                    } catch (Throwable error) {
                        followerFailure.set(error);
                    }
                });
                follower.start();
                Thread.sleep(20);
                release.countDown();
                leader.join(1000);
                follower.join(1000);
                check(!leader.isAlive(), "leader hung after loader Error");
                check(!follower.isAlive(), "follower hung after loader Error");
                check(leaderFailure.get() == failure,
                    "leader Error semantics changed");
                check(followerFailure.get() == failure,
                    "follower did not receive the leader Error");
            }
            responder.join();
        }
    }
    public static void main(String[] args) throws Exception {
        Map<String, String> v = fixtures();
        try (MegaCacheClient client = client()) {
            check("PONG".equals(client.ping()), "ping");
            client.set(v.get("key"), v.get("value").getBytes(StandardCharsets.UTF_8), null);
            check(v.get("value").equals(new String(client.get(v.get("key")), StandardCharsets.UTF_8)), "get");
            client.mset(Map.of(v.get("second_key"), v.get("second_value").getBytes(StandardCharsets.UTF_8)));
            check(client.mget(java.util.List.of(v.get("key"), v.get("second_key"))).size() == 2, "mget");
            check(client.exists(v.get("key"), v.get("second_key")) == 2, "exists");
            check(client.expire(v.get("key"), Duration.ofSeconds(30)), "expire");
            check(client.ttl(v.get("key")) >= 0, "ttl");
            check(client.delete(v.get("second_key")) == 1, "delete");
            MegaCacheClient.CachePolicy policy = new MegaCacheClient.CachePolicy();
            policy.ttl = Duration.ofSeconds(30); policy.stale = Duration.ofSeconds(30);
            policy.tags = java.util.List.of(v.get("tag"));
            client.put(v.get("key"), v.get("value").getBytes(StandardCharsets.UTF_8), policy, null);
            MegaCacheClient.LeaseResult lease = client.lease(v.get("key"));
            check("fresh".equals(lease.state), "lease");
            check(!lease.expiresIn.isZero() && !lease.staleFor.isZero(), "lease windows");
            check(client.invalidate(v.get("tag")) == 1, "invalidate");
            client.setTraceparent(v.get("traceparent"));
            check(client.fetch(v.get("fetch_key"), v.get("origin"), v.get("path"), false, null).contains(v.get("path")), "fetch");
            check(client.status().contains("\"degraded\":false"), "status");
        }
        try (MegaCacheClient client = client()) {
            client.delete(v.get("l1_key"));
            MegaCacheClient.CachePolicy policy = new MegaCacheClient.CachePolicy();
            policy.ttl = Duration.ofSeconds(30); policy.stale = Duration.ofSeconds(30);
            AtomicInteger calls = new AtomicInteger();
            CountDownLatch start = new CountDownLatch(1);
            Runnable load = () -> {
                try {
                    start.await();
                    MegaCacheClient.CachedValue value = client.getOrLoad(v.get("l1_key"), () -> {
                        calls.incrementAndGet();
                        try { Thread.sleep(20); } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
                        return "loaded".getBytes(StandardCharsets.UTF_8);
                    }, policy);
                    check("loaded".equals(new String(value.value, StandardCharsets.UTF_8)), "L1 value");
                } catch (Exception error) { throw new RuntimeException(error); }
            };
            Thread first = new Thread(load); Thread second = new Thread(load);
            first.start(); second.start(); start.countDown(); first.join(); second.join();
            check(calls.get() == 1, "coalescing");
            try (MegaCacheClient writer = client()) {
                writer.set(v.get("l1_key"), "external".getBytes(StandardCharsets.UTF_8), null);
            }
            MegaCacheClient.CachedValue refreshed = client.getOrLoad(
                v.get("l1_key"),
                () -> { throw new AssertionError("fresh L2 value should avoid loader"); },
                policy
            );
            check("external".equals(new String(refreshed.value, StandardCharsets.UTF_8)), "invalidation");
        }
        protocolLimitRegression("$16777217\r\n");
        protocolLimitRegression("*100001\r\n");
        protocolLimitRegression(
            String.join("", java.util.Collections.nCopies(130, "*1\r\n")) +
            "$1\r\nx\r\n"
        );
        authExceptionRegressions();
        commandDecoderRegressions();
        staleDeadlineRegressions();
        coalescedErrorRegression();
        System.out.println("Java SDK conformance passed");
    }
}
