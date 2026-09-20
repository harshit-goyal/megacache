package io.megacache;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

public final class MegaCacheClient implements Closeable {
    public static final String VERSION = "0.8.0";
    private static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
    private static final int MAX_BULK_BYTES = 16 * 1024 * 1024;
    private static final int MAX_ARRAY_ITEMS = 100_000;
    private static final int MAX_RESPONSE_DEPTH = 128;
    private static final int MAX_RESPONSE_LINE = 65_536;
    public static class MegaCacheException extends IOException {
        private static final long serialVersionUID = 1L;
        public final String code;
        MegaCacheException(String message, String code) { super(message); this.code = code; }
        MegaCacheException(String message, String code, Throwable cause) { super(message, cause); this.code = code; }
    }
    public static final class ConnectionException extends MegaCacheException {
        private static final long serialVersionUID = 1L;
        ConnectionException(String message, Throwable cause) { super(message, "CONNECTION", cause); }
    }
    public static final class ProtocolException extends MegaCacheException {
        private static final long serialVersionUID = 1L;
        ProtocolException(String message) { super(message, "PROTOCOL"); }
    }
    public static final class CommandException extends MegaCacheException {
        private static final long serialVersionUID = 1L;
        CommandException(String message) { super(message, message.isEmpty() ? "ERR" : message.split(" ", 2)[0]); }
    }

    public static final class Options {
        public String host = "127.0.0.1";
        public int port = 6380;
        public String username;
        public String password;
        public Duration timeout = Duration.ofSeconds(5);
        public boolean tls;
        public String serverName;
        public SSLContext sslContext;
        public int l1MaxEntries = 1000;
        public int l1MaxBytes = 16 * 1024 * 1024;
        public Duration invalidationPoll = Duration.ofSeconds(1);
        public Supplier<String> traceparentProvider;
    }
    public static final class CachePolicy {
        public Duration ttl = Duration.ofSeconds(60);
        public Duration stale = Duration.ofSeconds(300);
        public List<String> tags = Collections.emptyList();
        public boolean staleIfError = true;
    }
    public static final class CachedValue {
        public final byte[] value;
        public final String state;
        private final long staleDeadlineNanos;
        private final Throwable staleError;
        CachedValue(byte[] value, String state) {
            this(value, state, 0, null);
        }
        CachedValue(byte[] value, String state, long staleDeadlineNanos) {
            this(value, state, staleDeadlineNanos, null);
        }
        CachedValue(
            byte[] value, String state, long staleDeadlineNanos,
            Throwable staleError
        ) {
            this.value = value.clone(); this.state = state;
            this.staleDeadlineNanos = staleDeadlineNanos;
            this.staleError = staleError;
        }
    }
    public static final class LeaseResult {
        public final String state;
        public final byte[] value;
        public final String leaseToken;
        public final Duration retryAfter;
        public final Duration expiresIn;
        public final Duration staleFor;
        LeaseResult(String state, byte[] value, String leaseToken, Duration retryAfter) {
            this(state, value, leaseToken, retryAfter, Duration.ZERO, Duration.ZERO);
        }
        LeaseResult(
            String state, byte[] value, String leaseToken, Duration retryAfter,
            Duration expiresIn, Duration staleFor
        ) {
            this.state = state; this.value = value == null ? null : value.clone();
            this.leaseToken = leaseToken; this.retryAfter = retryAfter;
            this.expiresIn = expiresIn; this.staleFor = staleFor;
        }
    }

    private final Options options;
    private final LocalCache local;
    private final Map<String, CompletableFuture<CachedValue>> flights = new LinkedHashMap<>();
    private final Object mutationLock = new Object();
    private Socket socket;
    private InputStream input;
    private OutputStream output;
    private String cursorToken;
    private String cursorEpoch;
    private long cursorGeneration;
    private boolean cursorInitialized;
    private long lastPollNanos;
    private String activeTraceparent;
    private long mutationGeneration;
    private final ThreadLocal<String> requestTraceparent = new ThreadLocal<>();

    public MegaCacheClient() { this(new Options()); }
    public MegaCacheClient(Options options) {
        this.options = options;
        this.local = new LocalCache(options.l1MaxEntries, options.l1MaxBytes);
    }

    private synchronized void connect() throws IOException {
        if (socket != null && socket.isConnected() && !socket.isClosed()) return;
        Socket rawSocket = null;
        SSLSocket tlsSocket = null;
        try {
            if (options.tls) {
                SSLContext context = options.sslContext == null ? SSLContext.getDefault() : options.sslContext;
                rawSocket = new Socket();
                rawSocket.connect(new InetSocketAddress(options.host, options.port), (int) options.timeout.toMillis());
                rawSocket.setSoTimeout((int) options.timeout.toMillis());
                String peerName = options.serverName == null ? options.host : options.serverName;
                tlsSocket = (SSLSocket) context.getSocketFactory().createSocket(
                    rawSocket, peerName, options.port, true);
                tlsSocket.setSoTimeout((int) options.timeout.toMillis());
                SSLParameters parameters = tlsSocket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                parameters.setProtocols(new String[]{"TLSv1.3", "TLSv1.2"});
                tlsSocket.setSSLParameters(parameters);
                tlsSocket.startHandshake();
                socket = tlsSocket;
                rawSocket = null;
                tlsSocket = null;
            } else {
                rawSocket = new Socket();
                rawSocket.connect(new InetSocketAddress(options.host, options.port), (int) options.timeout.toMillis());
                socket = rawSocket;
                rawSocket = null;
            }
            socket.setSoTimeout((int) options.timeout.toMillis());
            input = new BufferedInputStream(socket.getInputStream());
            output = new BufferedOutputStream(socket.getOutputStream());
            if (options.password != null) {
                Object response = options.username == null
                    ? commandInternal("AUTH", options.password)
                    : commandInternal("AUTH", options.username, options.password);
                expectOk(response, "AUTH");
            }
        } catch (CommandException | ProtocolException error) {
            closeSocket(tlsSocket);
            closeSocket(rawSocket);
            closeQuietly();
            throw error;
        } catch (IOException error) {
            closeSocket(tlsSocket);
            closeSocket(rawSocket);
            closeQuietly();
            throw new ConnectionException(error.getMessage(), error);
        } catch (Exception error) {
            closeSocket(tlsSocket);
            closeSocket(rawSocket);
            closeQuietly();
            throw new ConnectionException(error.getMessage(), error);
        }
    }

    public synchronized Object command(Object... parts) throws IOException {
        connect();
        return commandInternal(parts);
    }

    private Object commandInternal(Object... parts) throws IOException {
        try {
            writeCommand(parts);
            output.flush();
            return readResponse(new ResponseBudget(), 0);
        } catch (CommandException error) {
            throw error;
        } catch (ProtocolException error) {
            closeQuietly();
            throw error;
        } catch (IOException error) {
            closeQuietly();
            throw new ConnectionException(error.getMessage(), error);
        }
    }

    private void writeCommand(Object... parts) throws IOException {
        output.write(("*" + parts.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        for (Object part : parts) {
            byte[] value = part instanceof byte[] ? (byte[]) part :
                String.valueOf(part).getBytes(StandardCharsets.UTF_8);
            output.write(("$" + value.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            output.write(value);
            output.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
    }

    private static final class ResponseBudget {
        int bytes;
        void consume(int count) throws ProtocolException {
            bytes += count;
            if (bytes > MAX_RESPONSE_BYTES) throw new ProtocolException("response exceeds byte limit");
        }
    }

    private Object readResponse(ResponseBudget budget, int depth) throws IOException {
        if (depth > MAX_RESPONSE_DEPTH) throw new ProtocolException("RESP nesting exceeds limit");
        int marker = input.read();
        if (marker < 0) throw new EOFException("server closed connection");
        budget.consume(1);
        String line = readLine(budget);
        switch (marker) {
            case '+': return line;
            case '-': throw new CommandException(line);
            case ':':
                try { return Long.parseLong(line); }
                catch (NumberFormatException error) { throw new ProtocolException("invalid integer"); }
            case '$': {
                int length = parseLength(line);
                if (length == -1) return null;
                if (length > MAX_BULK_BYTES) throw new ProtocolException("bulk string exceeds limit");
                budget.consume(length + 2);
                byte[] value = readExactly(length);
                if (!Arrays.equals(readExactly(2), "\r\n".getBytes(StandardCharsets.US_ASCII))) {
                    throw new ProtocolException("invalid bulk terminator");
                }
                return value;
            }
            case '*': {
                int count = parseLength(line);
                if (count == -1) return null;
                if (count > MAX_ARRAY_ITEMS) throw new ProtocolException("array exceeds item limit");
                List<Object> values = new ArrayList<>();
                for (int index = 0; index < count; index++) {
                    values.add(readResponse(budget, depth + 1));
                }
                return values;
            }
            default: throw new ProtocolException("unknown RESP marker");
        }
    }

    private int parseLength(String value) throws ProtocolException {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < -1) throw new NumberFormatException();
            return parsed;
        } catch (NumberFormatException error) { throw new ProtocolException("invalid RESP length"); }
    }
    private String readLine(ResponseBudget budget) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int previous = -1;
        while (bytes.size() <= MAX_RESPONSE_LINE) {
            int current = input.read();
            if (current < 0) throw new EOFException("incomplete RESP line");
            budget.consume(1);
            if (previous == '\r' && current == '\n') {
                byte[] value = bytes.toByteArray();
                return new String(value, 0, value.length - 1, StandardCharsets.UTF_8);
            }
            bytes.write(current);
            previous = current;
        }
        throw new ProtocolException("oversized RESP line");
    }
    private byte[] readExactly(int length) throws IOException {
        byte[] value = new byte[length];
        int offset = 0;
        while (offset < length) {
            int count = input.read(value, offset, length - offset);
            if (count < 0) throw new EOFException("incomplete RESP value");
            offset += count;
        }
        return value;
    }

    public String ping() throws IOException {
        Object value = command("PING");
        if (!(value instanceof String) || !"PONG".equals(value)) {
            throw protocolFailure("PING response must be PONG");
        }
        return (String) value;
    }
    public byte[] get(String key) throws IOException {
        return expectBulk(command("GET", key), "GET", true);
    }
    public void set(String key, byte[] value, Duration expiry) throws IOException {
        synchronized (mutationLock) {
            Object response = expiry == null
                ? command("SET", key, value)
                : command("SET", key, value, "EX", seconds(expiry));
            expectOk(response, "SET");
            mutationSucceeded();
        }
    }
    public List<byte[]> mget(List<String> keys) throws IOException {
        List<Object> parts = new ArrayList<>(); parts.add("MGET"); parts.addAll(keys);
        Object response = command(parts.toArray());
        if (!(response instanceof List<?>) || ((List<?>) response).size() != keys.size()) {
            throw protocolFailure("MGET response must contain one value per key");
        }
        List<byte[]> result = new ArrayList<>();
        for (Object item : (List<?>) response) {
            result.add(expectBulk(item, "MGET", true));
        }
        return result;
    }
    public void mset(Map<String, byte[]> values) throws IOException {
        List<Object> parts = new ArrayList<>(); parts.add("MSET");
        values.forEach((key, value) -> { parts.add(key); parts.add(value); });
        synchronized (mutationLock) {
            expectOk(command(parts.toArray()), "MSET");
            mutationSucceeded();
        }
    }
    public long delete(String... keys) throws IOException {
        List<Object> parts = new ArrayList<>(); parts.add("DEL"); parts.addAll(Arrays.asList(keys));
        synchronized (mutationLock) {
            long changed = expectInteger(
                command(parts.toArray()), "DEL", 0, (long) keys.length);
            if (changed > 0) mutationSucceeded();
            return changed;
        }
    }
    public long exists(String... keys) throws IOException {
        List<Object> parts = new ArrayList<>(); parts.add("EXISTS"); parts.addAll(Arrays.asList(keys));
        return expectInteger(
            command(parts.toArray()), "EXISTS", 0, (long) keys.length);
    }
    public boolean expire(String key, Duration ttl) throws IOException {
        synchronized (mutationLock) {
            boolean changed = expectInteger(
                command("EXPIRE", key, seconds(ttl)), "EXPIRE", 0, 1L) != 0;
            if (changed) mutationSucceeded();
            return changed;
        }
    }
    public long ttl(String key) throws IOException {
        return expectInteger(command("TTL", key), "TTL", -2, null);
    }
    public void put(String key, byte[] value, CachePolicy policy, String leaseToken) throws IOException {
        putWithGeneration(key, value, policy, leaseToken);
    }
    private long putWithGeneration(
        String key, byte[] value, CachePolicy policy, String leaseToken
    ) throws IOException {
        List<Object> parts = new ArrayList<>(Arrays.asList(
            "MC.SET", key, value, "TTL", seconds(policy.ttl), "STALE", nonNegativeSeconds(policy.stale)));
        if (!policy.tags.isEmpty()) { parts.add("TAGS"); parts.add(policy.tags.size()); parts.addAll(policy.tags); }
        if (leaseToken != null) { parts.add("LEASE"); parts.add(leaseToken); }
        synchronized (mutationLock) {
            expectOk(command(parts.toArray()), "MC.SET");
            return mutationSucceeded();
        }
    }
    public LeaseResult lease(String key) throws IOException {
        Object response = command("MC.LEASE", key, "WINDOWS");
        if (!(response instanceof List<?>) || ((List<?>) response).isEmpty()) {
            throw protocolFailure("invalid MC.LEASE response");
        }
        List<?> values = (List<?>) response;
        String state = expectText(values.get(0), "MC.LEASE state");
        if ("fresh".equals(state)) {
            expectArity(values, "MC.LEASE fresh", 2, 4);
            return new LeaseResult(
                state, expectBulk(values.get(1), "MC.LEASE fresh", false),
                null, Duration.ZERO, millis(values, 2), millis(values, 3));
        }
        if ("stale".equals(state)) {
            expectArity(values, "MC.LEASE stale", 2, 3);
            return new LeaseResult(
                state, expectBulk(values.get(1), "MC.LEASE stale", false),
                null, Duration.ZERO, Duration.ZERO, millis(values, 2));
        }
        if ("stale_lease".equals(state)) {
            expectArity(values, "MC.LEASE stale_lease", 3, 4);
            return new LeaseResult(
                state,
                expectBulk(values.get(1), "MC.LEASE stale_lease", false),
                expectText(values.get(2), "MC.LEASE lease token"),
                Duration.ZERO, Duration.ZERO, millis(values, 3));
        }
        if ("lease".equals(state)) {
            expectArity(values, "MC.LEASE lease", 2);
            return new LeaseResult(
                state, null,
                expectText(values.get(1), "MC.LEASE lease token"),
                Duration.ZERO);
        }
        if ("loading".equals(state)) {
            expectArity(values, "MC.LEASE loading", 2);
            return new LeaseResult(
                state, null, null,
                Duration.ofMillis(expectInteger(
                    values.get(1), "MC.LEASE retry", 0, null)));
        }
        throw protocolFailure("unknown MC.LEASE state");
    }
    public String fetch(String key, String origin, String path, boolean refresh, String traceparent) throws IOException {
        List<Object> parts = new ArrayList<>(Arrays.asList("MC.FETCH", key, origin, path));
        if (refresh) parts.add("REFRESH");
        if (traceparent == null) traceparent = requestTraceparent.get();
        if (traceparent == null && options.traceparentProvider != null) traceparent = options.traceparentProvider.get();
        if (traceparent == null) traceparent = activeTraceparent;
        if (traceparent != null) { parts.add("TRACEPARENT"); parts.add(traceparent); }
        Object response = command(parts.toArray());
        String document = expectJsonObject(response, "MC.FETCH");
        Map<String, Object> parsed = parseJsonObject(document, "MC.FETCH");
        if (!(parsed.get("state") instanceof String)
            || !(parsed.get("origin") instanceof String)) {
            throw protocolFailure(
                "MC.FETCH JSON requires string state and origin");
        }
        validateOptionalJsonInteger(parsed, "status_code", 100, false);
        validateOptionalJsonInteger(parsed, "attempts", 0, false);
        Object error = parsed.get("error");
        if (error != null && !(error instanceof String)) {
            throw protocolFailure(
                "MC.FETCH JSON error must be a string or null");
        }
        return document;
    }
    public long invalidate(String... tags) throws IOException {
        List<Object> parts = new ArrayList<>(); parts.add("MC.INVALIDATE"); parts.addAll(Arrays.asList(tags));
        synchronized (mutationLock) {
            long changed = expectInteger(
                command(parts.toArray()), "MC.INVALIDATE", 0, null);
            if (changed > 0) mutationSucceeded();
            return changed;
        }
    }
    public String status() throws IOException {
        String document = expectJsonObject(command("MC.STATUS"), "MC.STATUS");
        Map<String, Object> parsed = parseJsonObject(document, "MC.STATUS");
        if (!(parsed.get("degraded") instanceof Boolean)) {
            throw protocolFailure(
                "MC.STATUS JSON degraded must be a boolean");
        }
        return document;
    }
    public void setTraceparent(String value) throws IOException {
        expectOk(command("MC.TRACEPARENT", value), "MC.TRACEPARENT");
        activeTraceparent = value.toLowerCase();
    }

    public final class TraceparentScope implements AutoCloseable {
        private final String previous;
        private boolean closed;
        private TraceparentScope(String value) {
            previous = requestTraceparent.get();
            if (value == null) requestTraceparent.remove();
            else requestTraceparent.set(value.toLowerCase());
        }
        @Override public void close() {
            if (closed) return;
            closed = true;
            if (previous == null) requestTraceparent.remove();
            else requestTraceparent.set(previous);
        }
    }
    public TraceparentScope requestTraceparent(String value) {
        return new TraceparentScope(value);
    }

    public CachedValue getOrLoad(String key, Supplier<byte[]> loader, CachePolicy policy) throws IOException {
        pollInvalidations();
        CachedValue cached = local.get(key);
        if (cached != null && "fresh".equals(cached.state)) return cached;
        CompletableFuture<CachedValue> future;
        boolean leader;
        synchronized (flights) {
            future = flights.get(key);
            leader = future == null;
            if (leader) { future = new CompletableFuture<>(); flights.put(key, future); }
        }
        if (!leader) {
            try { return completedFlight(future.join()); }
            catch (CompletionException error) {
                Throwable cause = error.getCause();
                if (cause instanceof IOException) throw (IOException) cause;
                if (cause instanceof RuntimeException) throw (RuntimeException) cause;
                if (cause instanceof Error) throw (Error) cause;
                throw error;
            }
        }
        try {
            while (true) {
                long expectedGeneration = currentGeneration();
                long leaseStarted = System.nanoTime();
                LeaseResult lease = lease(key);
                long leaseElapsed = elapsedSince(leaseStarted);
                if ("fresh".equals(lease.state)) {
                    Duration freshBudget = (
                        lease.expiresIn == null || lease.expiresIn.isNegative()
                    )
                        ? policy.ttl : min(policy.ttl, lease.expiresIn);
                    Duration staleBudget = lease.staleFor == null || lease.staleFor.isNegative()
                        ? policy.stale : min(policy.stale, lease.staleFor);
                    Duration[] windows = remainingWindows(
                        freshBudget, staleBudget, leaseElapsed);
                    Duration freshFor = windows[0];
                    Duration staleFor = windows[1];
                    if (!freshFor.plus(staleFor).isZero()) {
                        admitIfGeneration(expectedGeneration, key, lease.value, freshFor, staleFor);
                    }
                    CachedValue value = new CachedValue(lease.value, "fresh"); future.complete(value); return value;
                }
                if ("stale".equals(lease.state)) {
                    Duration staleBudget = lease.staleFor == null || lease.staleFor.isNegative()
                        ? policy.stale : min(policy.stale, lease.staleFor);
                    Duration remaining = remaining(staleBudget, leaseElapsed);
                    if (!remaining.isZero()) {
                        admitIfGeneration(
                            expectedGeneration, key, lease.value, Duration.ZERO, remaining);
                    }
                    CachedValue value = new CachedValue(
                        lease.value, "stale", deadlineAfter(leaseStarted, staleBudget));
                    future.complete(value); return value;
                }
                if ("loading".equals(lease.state)) {
                    try { Thread.sleep(Math.max(1, lease.retryAfter.toMillis())); }
                    catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IOException(error); }
                    continue;
                }
                if ("stale_lease".equals(lease.state) && lease.value != null) {
                    Duration staleBudget = lease.staleFor == null || lease.staleFor.isNegative()
                        ? policy.stale : min(policy.stale, lease.staleFor);
                    cached = new CachedValue(
                        lease.value, "stale", deadlineAfter(leaseStarted, staleBudget));
                }
                byte[] loaded = loader.get();
                long putStarted = System.nanoTime();
                long ownGeneration = putWithGeneration(key, loaded, policy, lease.leaseToken);
                Duration[] windows = remainingWindows(
                    policy.ttl, policy.stale, elapsedSince(putStarted));
                if (!windows[0].plus(windows[1]).isZero()) {
                    admitIfGeneration(ownGeneration, key, loaded, windows[0], windows[1]);
                }
                CachedValue value = new CachedValue(loaded, "loaded"); future.complete(value); return value;
            }
        } catch (Throwable error) {
            if (!(error instanceof Error)
                    && cached != null && policy.staleIfError
                    && staleValid(cached)) {
                CachedValue value = new CachedValue(
                    cached.value, "stale_if_error",
                    cached.staleDeadlineNanos, error);
                future.complete(value);
                return completedFlight(value);
            }
            future.completeExceptionally(error);
            if (error instanceof IOException) throw (IOException) error;
            if (error instanceof RuntimeException) {
                throw (RuntimeException) error;
            }
            if (error instanceof Error) throw (Error) error;
            throw new IOException("unexpected load failure", error);
        } finally {
            synchronized (flights) { flights.remove(key); }
        }
    }

    private synchronized void pollInvalidations() throws IOException {
        long now = System.nanoTime();
        if (now - lastPollNanos < options.invalidationPoll.toNanos()) return;
        String document = expectJsonObject(
            command("MC.INVALIDATIONS", cursorToken == null ? "" : cursorToken),
            "MC.INVALIDATIONS");
        Map<String, Object> parsed = parseJsonObject(
            document, "MC.INVALIDATIONS");
        Object cursor = parsed.get("cursor");
        String foundToken;
        if (cursor instanceof String) {
            foundToken = (String) cursor;
        } else if (cursor instanceof Long && (Long) cursor >= 0) {
            foundToken = String.valueOf(cursor);
        } else {
            throw protocolFailure(
                "MC.INVALIDATIONS JSON cursor must be a string or non-negative integer");
        }
        boolean hasEpoch = parsed.containsKey("epoch");
        boolean hasGeneration = parsed.containsKey("generation");
        if (hasEpoch != hasGeneration) {
            throw protocolFailure(
                "MC.INVALIDATIONS JSON epoch and generation must appear together");
        }
        String foundEpoch = null;
        Long foundGeneration = null;
        if (hasEpoch) {
            if (!(parsed.get("epoch") instanceof String)) {
                throw protocolFailure(
                    "MC.INVALIDATIONS JSON epoch must be a string");
            }
            foundEpoch = (String) parsed.get("epoch");
            foundGeneration = validateOptionalJsonInteger(
                parsed, "generation", 0, true);
        }
        if (parsed.containsKey("changed")
            && !(parsed.get("changed") instanceof Boolean)) {
            throw protocolFailure(
                "MC.INVALIDATIONS JSON changed must be a boolean");
        }
        boolean changed = cursorInitialized && !foundToken.equals(cursorToken);
        if (foundEpoch != null && foundGeneration != null) {
            changed = cursorInitialized &&
                (!foundEpoch.equals(cursorEpoch) || foundGeneration != cursorGeneration);
        }
        if (changed) {
            mutationSucceeded();
        }
        cursorToken = foundToken; cursorEpoch = foundEpoch;
        cursorGeneration = foundGeneration == null ? 0 : foundGeneration;
        cursorInitialized = true; lastPollNanos = now;
    }
    private ProtocolException protocolFailure(String message) {
        closeQuietly();
        return new ProtocolException(message);
    }
    private void expectOk(Object value, String command) throws ProtocolException {
        if (!(value instanceof String) || !"OK".equals(value)) {
            throw protocolFailure(command + " response must be OK");
        }
    }
    private String expectText(Object value, String command)
        throws ProtocolException {
        if (value instanceof String) return (String) value;
        if (value instanceof byte[]) {
            try {
                return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap((byte[]) value)).toString();
            } catch (CharacterCodingException error) {
                throw protocolFailure(
                    command + " response is not valid UTF-8");
            }
        }
        throw protocolFailure(command + " response must be a string");
    }
    private byte[] expectBulk(
        Object value, String command, boolean nullable
    ) throws ProtocolException {
        if (value == null && nullable) return null;
        if (!(value instanceof byte[])) {
            throw protocolFailure(
                command + " response must be a bulk string"
                    + (nullable ? " or null" : ""));
        }
        return (byte[]) value;
    }
    private long expectInteger(
        Object value, String command, long minimum, Long maximum
    ) throws ProtocolException {
        if (!(value instanceof Long)) {
            throw protocolFailure(command + " response must be an integer");
        }
        long result = (Long) value;
        if (result < minimum) {
            throw protocolFailure(
                command + " response is below its valid range");
        }
        if (maximum != null && result > maximum) {
            throw protocolFailure(
                command + " response is above its valid range");
        }
        return result;
    }
    private void expectArity(
        List<?> values, String command, int... allowed
    ) throws ProtocolException {
        for (int count : allowed) {
            if (values.size() == count) return;
        }
        throw protocolFailure(command + " response has invalid arity");
    }
    private String expectJsonObject(Object value, String command)
        throws ProtocolException {
        byte[] bytes = expectBulk(value, command, false);
        String document;
        try {
            document = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException error) {
            throw protocolFailure(command + " JSON is not valid UTF-8");
        }
        parseJsonObject(document, command);
        return document;
    }
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonObject(
        String document, String command
    ) throws ProtocolException {
        try {
            Object parsed = new JsonParser(document).parse();
            if (!(parsed instanceof Map<?, ?>)) {
                throw new IllegalArgumentException("expected object");
            }
            return (Map<String, Object>) parsed;
        } catch (IllegalArgumentException error) {
            throw protocolFailure(command + " returned invalid JSON object");
        }
    }
    private Long validateOptionalJsonInteger(
        Map<String, Object> document, String field, long minimum,
        boolean required
    ) throws ProtocolException {
        if (!document.containsKey(field)) {
            if (required) {
                throw protocolFailure("missing JSON field " + field);
            }
            return null;
        }
        Object value = document.get(field);
        if (value == null && !required) return null;
        if (!(value instanceof Long) || (Long) value < minimum) {
            throw protocolFailure(
                "JSON field " + field + " must be an integer");
        }
        return (Long) value;
    }
    private static final class JsonParser {
        private final String value;
        private int offset;
        JsonParser(String value) { this.value = value; }
        Object parse() {
            Object result = parseValue(0);
            whitespace();
            if (offset != value.length()) invalid();
            return result;
        }
        private Object parseValue(int depth) {
            if (depth > MAX_RESPONSE_DEPTH) invalid();
            whitespace();
            if (offset >= value.length()) invalid();
            char marker = value.charAt(offset);
            if (marker == '{') return object(depth + 1);
            if (marker == '[') return array(depth + 1);
            if (marker == '"') return string();
            if (marker == 't') { literal("true"); return Boolean.TRUE; }
            if (marker == 'f') { literal("false"); return Boolean.FALSE; }
            if (marker == 'n') { literal("null"); return null; }
            if (marker == '-' || Character.isDigit(marker)) return number();
            return invalid();
        }
        private Map<String, Object> object(int depth) {
            offset++;
            Map<String, Object> result = new LinkedHashMap<>();
            whitespace();
            if (consume('}')) return result;
            while (true) {
                whitespace();
                if (offset >= value.length() || value.charAt(offset) != '"') {
                    return invalid();
                }
                String key = string();
                whitespace();
                if (!consume(':')) return invalid();
                result.put(key, parseValue(depth));
                whitespace();
                if (consume('}')) return result;
                if (!consume(',')) return invalid();
            }
        }
        private List<Object> array(int depth) {
            offset++;
            List<Object> result = new ArrayList<>();
            whitespace();
            if (consume(']')) return result;
            while (true) {
                result.add(parseValue(depth));
                whitespace();
                if (consume(']')) return result;
                if (!consume(',')) return invalid();
            }
        }
        private String string() {
            if (!consume('"')) return invalid();
            StringBuilder result = new StringBuilder();
            while (offset < value.length()) {
                char current = value.charAt(offset++);
                if (current == '"') return result.toString();
                if (current < 0x20) return invalid();
                if (current != '\\') {
                    result.append(current);
                    continue;
                }
                if (offset >= value.length()) return invalid();
                char escaped = value.charAt(offset++);
                switch (escaped) {
                    case '"': case '\\': case '/': result.append(escaped); break;
                    case 'b': result.append('\b'); break;
                    case 'f': result.append('\f'); break;
                    case 'n': result.append('\n'); break;
                    case 'r': result.append('\r'); break;
                    case 't': result.append('\t'); break;
                    case 'u':
                        if (offset + 4 > value.length()) return invalid();
                        try {
                            result.append((char) Integer.parseInt(
                                value.substring(offset, offset + 4), 16));
                        } catch (NumberFormatException error) {
                            return invalid();
                        }
                        offset += 4;
                        break;
                    default: return invalid();
                }
            }
            return invalid();
        }
        private Number number() {
            int start = offset;
            if (consume('-') && offset >= value.length()) return invalid();
            if (consume('0')) {
                if (offset < value.length()
                    && Character.isDigit(value.charAt(offset))) return invalid();
            } else {
                if (offset >= value.length()
                    || !Character.isDigit(value.charAt(offset))) return invalid();
                while (offset < value.length()
                    && Character.isDigit(value.charAt(offset))) offset++;
            }
            if (consume('.')) {
                if (offset >= value.length()
                    || !Character.isDigit(value.charAt(offset))) return invalid();
                while (offset < value.length()
                    && Character.isDigit(value.charAt(offset))) offset++;
            }
            if (offset < value.length()
                && (value.charAt(offset) == 'e' || value.charAt(offset) == 'E')) {
                offset++;
                if (offset < value.length()
                    && (value.charAt(offset) == '+' || value.charAt(offset) == '-')) {
                    offset++;
                }
                if (offset >= value.length()
                    || !Character.isDigit(value.charAt(offset))) return invalid();
                while (offset < value.length()
                    && Character.isDigit(value.charAt(offset))) offset++;
            }
            try {
                BigDecimal number = new BigDecimal(value.substring(start, offset));
                try { return number.longValueExact(); }
                catch (ArithmeticException ignored) {
                    double result = number.doubleValue();
                    if (!Double.isFinite(result)) return invalid();
                    return result;
                }
            } catch (NumberFormatException error) {
                return invalid();
            }
        }
        private void literal(String expected) {
            if (!value.startsWith(expected, offset)) invalid();
            offset += expected.length();
        }
        private boolean consume(char expected) {
            if (offset < value.length() && value.charAt(offset) == expected) {
                offset++;
                return true;
            }
            return false;
        }
        private void whitespace() {
            while (offset < value.length()) {
                char current = value.charAt(offset);
                if (current != ' ' && current != '\n'
                    && current != '\r' && current != '\t') return;
                offset++;
            }
        }
        private static <T> T invalid() {
            throw new IllegalArgumentException("invalid JSON");
        }
    }
    private static int seconds(Duration duration) {
        long value = duration.getSeconds();
        if (value <= 0 || value > Integer.MAX_VALUE) throw new IllegalArgumentException("duration must be 1..2147483647 seconds");
        return (int) value;
    }
    private Duration millis(List<?> values, int index) throws ProtocolException {
        return values.size() > index
            ? Duration.ofMillis(expectInteger(
                values.get(index), "MC.LEASE window", -1, null))
            : null;
    }
    private static Duration min(Duration first, Duration second) {
        return first.compareTo(second) < 0 ? first : second;
    }
    private static long elapsedSince(long started) {
        return Math.max(0, System.nanoTime() - started);
    }
    private static Duration remaining(Duration budget, long elapsedNanos) {
        long budgetNanos = budget.toNanos();
        return Duration.ofNanos(Math.max(0, budgetNanos - elapsedNanos));
    }
    private static Duration[] remainingWindows(
        Duration fresh, Duration stale, long elapsedNanos
    ) {
        long freshNanos = fresh.toNanos();
        long hardNanos = Math.addExact(freshNanos, stale.toNanos());
        long remainingFresh = Math.max(0, freshNanos - elapsedNanos);
        long remainingHard = Math.max(0, hardNanos - elapsedNanos);
        return new Duration[]{
            Duration.ofNanos(remainingFresh),
            Duration.ofNanos(remainingHard - remainingFresh)
        };
    }
    private static long deadlineAfter(long started, Duration budget) {
        long now = System.nanoTime();
        long elapsed = Math.max(0, now - started);
        return now + Math.max(0, budget.toNanos() - elapsed);
    }
    private static boolean staleValid(CachedValue value) {
        return value.staleDeadlineNanos != 0
            && System.nanoTime() - value.staleDeadlineNanos < 0;
    }
    private static CachedValue completedFlight(CachedValue value) throws IOException {
        if (!"stale_if_error".equals(value.state) || staleValid(value)) {
            return value;
        }
        if (value.staleError instanceof IOException) {
            throw (IOException) value.staleError;
        }
        if (value.staleError instanceof RuntimeException) {
            throw (RuntimeException) value.staleError;
        }
        throw new IOException("stale cache entry expired during load failure");
    }
    private static int nonNegativeSeconds(Duration duration) {
        long value = duration.getSeconds();
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("duration must be 0..2147483647 seconds");
        }
        return (int) value;
    }
    private void closeQuietly() {
        try { if (socket != null) socket.close(); } catch (IOException ignored) {}
        socket = null; input = null; output = null;
    }
    private static void closeSocket(Socket value) {
        try { if (value != null) value.close(); } catch (IOException ignored) {}
    }
    private long mutationSucceeded() {
        synchronized (flights) {
            mutationGeneration++;
            local.clear();
            return mutationGeneration;
        }
    }
    private long currentGeneration() {
        synchronized (flights) { return mutationGeneration; }
    }
    private boolean admitIfGeneration(
        long generation, String key, byte[] value, Duration ttl, Duration stale
    ) {
        synchronized (flights) {
            if (generation != mutationGeneration) return false;
            return local.put(key, value, ttl, stale);
        }
    }
    @Override public synchronized void close() { closeQuietly(); }

    private static final class LocalCache {
        private static final class Entry {
            final byte[] value; final long freshUntil, staleUntil; final int size;
            Entry(byte[] value, long freshUntil, long staleUntil, int size) {
                this.value = value; this.freshUntil = freshUntil; this.staleUntil = staleUntil; this.size = size;
            }
        }
        private final int maxEntries, maxBytes;
        private int usedBytes;
        private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>(16, .75f, true);
        LocalCache(int maxEntries, int maxBytes) {
            if (maxEntries <= 0 || maxBytes <= 0) throw new IllegalArgumentException("L1 limits must be positive");
            this.maxEntries = maxEntries; this.maxBytes = maxBytes;
        }
        synchronized CachedValue get(String key) {
            Entry entry = entries.get(key); if (entry == null) return null;
            long now = System.nanoTime(); if (now >= entry.staleUntil) { remove(key); return null; }
            return new CachedValue(
                entry.value,
                now < entry.freshUntil ? "fresh" : "stale",
                entry.staleUntil);
        }
        synchronized boolean put(String key, byte[] value, Duration ttl, Duration stale) {
            if (ttl.isNegative() || stale.isNegative() || ttl.plus(stale).isZero()) {
                throw new IllegalArgumentException("invalid L1 freshness policy");
            }
            byte[] copy = value.clone(); int size = key.getBytes(StandardCharsets.UTF_8).length + copy.length;
            if (size > maxBytes) return false; remove(key); long now = System.nanoTime();
            entries.put(key, new Entry(copy, now + ttl.toNanos(), now + ttl.plus(stale).toNanos(), size)); usedBytes += size;
            while (entries.size() > maxEntries || usedBytes > maxBytes) remove(entries.keySet().iterator().next());
            return true;
        }
        synchronized void clear() { entries.clear(); usedBytes = 0; }
        private void remove(String key) { Entry old = entries.remove(key); if (old != null) usedBytes -= old.size; }
    }
}
