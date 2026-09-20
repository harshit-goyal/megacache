package megacache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fixtureValues(t *testing.T) map[string]string {
	file, err := os.Open(os.Getenv("MEGACACHE_FIXTURES"))
	if err != nil { t.Fatal(err) }
	defer file.Close()
	values := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") { continue }
		parts := strings.SplitN(line, "\t", 2); values[parts[0]] = parts[1]
	}
	if err := scanner.Err(); err != nil { t.Fatal(err) }
	return values
}

func conformanceClient(t *testing.T) *Client {
	port, err := strconv.Atoi(os.Getenv("MEGACACHE_CONFORMANCE_PORT"))
	if err != nil { t.Fatal(err) }
	client, err := New(Options{
		Address: os.Getenv("MEGACACHE_CONFORMANCE_HOST") + ":" + strconv.Itoa(port),
		InvalidationPoll: time.Nanosecond,
	})
	if err != nil { t.Fatal(err) }
	return client
}

func TestProtocolConformance(t *testing.T) {
	if os.Getenv("MEGACACHE_FIXTURES") == "" { t.Skip("conformance environment unavailable") }
	v, client := fixtureValues(t), conformanceClient(t); defer client.Close()
	if value, err := client.Ping(); err != nil || value != "PONG" { t.Fatalf("ping: %q %v", value, err) }
	if err := client.Set(v["key"], []byte(v["value"]), 0); err != nil { t.Fatal(err) }
	if value, err := client.Get(v["key"]); err != nil || string(value) != v["value"] { t.Fatalf("get: %q %v", value, err) }
	if err := client.MSet(map[string][]byte{v["second_key"]: []byte(v["second_value"])}); err != nil { t.Fatal(err) }
	if values, err := client.MGet(v["key"], v["second_key"]); err != nil || string(values[1]) != v["second_value"] { t.Fatalf("mget: %v %v", values, err) }
	if count, err := client.Exists(v["key"], v["second_key"]); err != nil || count != 2 { t.Fatalf("exists: %d %v", count, err) }
	if changed, err := client.Expire(v["key"], 30); err != nil || !changed { t.Fatalf("expire: %v %v", changed, err) }
	if ttl, err := client.TTL(v["key"]); err != nil || ttl < 0 { t.Fatalf("ttl: %d %v", ttl, err) }
	if count, err := client.Delete(v["second_key"]); err != nil || count != 1 { t.Fatalf("delete: %d %v", count, err) }
	policy := CachePolicy{TTL: 30*time.Second, Stale: 30*time.Second, Tags: []string{v["tag"]}, StaleIfError: true}
	if err := client.Put(v["key"], []byte(v["value"]), policy, ""); err != nil { t.Fatal(err) }
	if lease, err := client.Lease(v["key"]); err != nil || lease.State != "fresh" ||
		lease.ExpiresIn == nil || *lease.ExpiresIn <= 0 ||
		lease.StaleFor == nil || *lease.StaleFor <= 0 {
		t.Fatalf("lease: %+v %v", lease, err)
	}
	if count, err := client.Invalidate(v["tag"]); err != nil || count != 1 { t.Fatalf("invalidate: %d %v", count, err) }
	if err := client.SetTraceparent(v["traceparent"]); err != nil { t.Fatal(err) }
	if fetched, err := client.Fetch(v["fetch_key"], v["origin"], v["path"], false, ""); err != nil || fetched.Value != v["path"] { t.Fatalf("fetch: %+v %v", fetched, err) }
	if status, err := client.Status(); err != nil || status["degraded"] != false { t.Fatalf("status: %v %v", status, err) }
}

func TestExplicitZeroStaleConformance(t *testing.T) {
	if os.Getenv("MEGACACHE_FIXTURES") == "" { t.Skip("conformance environment unavailable") }
	v, client := fixtureValues(t), conformanceClient(t); defer client.Close()
	policy := CachePolicy{TTL: 30 * time.Second}.WithStale(0)
	key := v["key"] + ":zero-stale"
	if err := client.Put(key, []byte(v["value"]), policy, ""); err != nil { t.Fatal(err) }
	lease, err := client.Lease(key)
	if err != nil { t.Fatal(err) }
	if lease.StaleFor == nil || *lease.StaleFor != 0 {
		t.Fatalf("explicit zero stale window was not preserved: %+v", lease)
	}
}

func TestL1Coalescing(t *testing.T) {
	if os.Getenv("MEGACACHE_FIXTURES") == "" { t.Skip("conformance environment unavailable") }
	v, client := fixtureValues(t), conformanceClient(t); defer client.Close()
	if _, err := client.Delete(v["l1_key"]); err != nil { t.Fatal(err) }
	var calls int32
	policy := CachePolicy{TTL: 30*time.Second, Stale: 30*time.Second, StaleIfError: true}
	loader := func() ([]byte, error) { atomic.AddInt32(&calls, 1); time.Sleep(20*time.Millisecond); return []byte("loaded"), nil }
	var wait sync.WaitGroup; wait.Add(2); results := make(chan CachedValue, 2)
	for i := 0; i < 2; i++ { go func() { defer wait.Done(); value, err := client.GetOrLoad(v["l1_key"], loader, policy); if err != nil { t.Error(err); return }; results <- value }() }
	wait.Wait(); close(results)
	if calls != 1 { t.Fatalf("loader called %d times", calls) }
	for value := range results { if string(value.Value) != "loaded" { t.Fatalf("unexpected value %q", value.Value) } }
	writer := conformanceClient(t)
	if err := writer.Set(v["l1_key"], []byte("external"), 0); err != nil { t.Fatal(err) }
	writer.Close()
	refreshed, err := client.GetOrLoad(v["l1_key"], func() ([]byte, error) {
		return nil, os.ErrInvalid
	}, policy)
	if err != nil || string(refreshed.Value) != "external" { t.Fatalf("invalidation: %q %v", refreshed.Value, err) }
}

func TestZeroFreshnessAndGenerationGuard(t *testing.T) {
	client, err := New(Options{InvalidationPoll: time.Second})
	if err != nil { t.Fatal(err) }
	if !client.admitIfGeneration(0, "zero", []byte("value"), 0, time.Second) {
		t.Fatal("stale-only admission should be allowed")
	}
	if value, found := client.l1.Get("zero"); !found || value.State != "stale" {
		t.Fatalf("zero freshness was not immediately stale: %+v %v", value, found)
	}
	generation := client.currentGeneration()
	client.mutationSucceeded()
	if client.admitIfGeneration(generation, "old", []byte("old"), time.Second, time.Second) {
		t.Fatal("stale generation was admitted")
	}
}

func leaseResponseClient(t *testing.T, response string, delay time.Duration) *Client {
	t.Helper()
	client, err := New(Options{InvalidationPoll: time.Hour})
	if err != nil { t.Fatal(err) }
	clientSide, serverSide := net.Pipe()
	client.conn = clientSide
	client.reader = bufio.NewReader(clientSide)
	client.lastPoll = time.Now()
	go func() {
		defer serverSide.Close()
		reader := bufio.NewReader(serverSide)
		line, readErr := reader.ReadString('\n')
		if readErr != nil { return }
		count, parseErr := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
		if parseErr != nil { return }
		for i := 0; i < count; i++ {
			lengthLine, err := reader.ReadString('\n')
			if err != nil { return }
			length, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(lengthLine, "$")))
			if err != nil { return }
			if _, err = io.ReadFull(reader, make([]byte, length+2)); err != nil { return }
		}
		time.Sleep(delay)
		_, _ = serverSide.Write([]byte(response))
	}()
	t.Cleanup(client.Close)
	return client
}

func TestLoaderPanicIsReportedToCoalescedFollowers(t *testing.T) {
	client := leaseResponseClient(
		t,
		"*2\r\n$5\r\nlease\r\n$5\r\ntoken\r\n",
		0,
	)
	entered := make(chan struct{})
	release := make(chan struct{})
	panicValue := errors.New("loader panic")
	leaderPanic := make(chan interface{}, 1)
	go func() {
		defer func() { leaderPanic <- recover() }()
		_, _ = client.GetOrLoad(
			"key",
			func() ([]byte, error) {
				close(entered)
				<-release
				panic(panicValue)
			},
			CachePolicy{TTL: time.Second}.WithStale(0),
		)
	}()
	<-entered
	followerStarted := make(chan struct{})
	followerErr := make(chan error, 1)
	go func() {
		close(followerStarted)
		_, err := client.GetOrLoad(
			"key",
			func() ([]byte, error) {
				return nil, errors.New("follower unexpectedly became leader")
			},
			CachePolicy{TTL: time.Second}.WithStale(0),
		)
		followerErr <- err
	}()
	<-followerStarted
	time.Sleep(20 * time.Millisecond)
	close(release)
	select {
	case recovered := <-leaderPanic:
		if recovered != panicValue { t.Fatalf("leader panic changed: %v", recovered) }
	case <-time.After(time.Second):
		t.Fatal("leader panic did not return")
	}
	select {
	case err := <-followerErr:
		var panicErr *LoaderPanicError
		if !errors.As(err, &panicErr) {
			t.Fatalf("follower received %T instead of LoaderPanicError: %v", err, err)
		}
		if panicErr.Panic != panicValue {
			t.Fatalf("follower panic value changed: %v", panicErr.Panic)
		}
	case <-time.After(time.Second):
		t.Fatal("coalesced follower hung after loader panic")
	}
}

func TestLeaseElapsedTimeReducesL1HardDeadline(t *testing.T) {
	client := leaseResponseClient(
		t,
		"*3\r\n$5\r\nstale\r\n$5\r\nvalue\r\n:100\r\n",
		60*time.Millisecond,
	)
	value, err := client.GetOrLoad(
		"key",
		func() ([]byte, error) { return nil, errors.New("unused") },
		CachePolicy{TTL: time.Second, Stale: time.Second, StaleIfError: true},
	)
	if err != nil || value.State != "stale" { t.Fatalf("stale lease: %+v %v", value, err) }
	time.Sleep(55 * time.Millisecond)
	if _, found := client.l1.Get("key"); found {
		t.Fatal("L1 entry outlived the lease hard deadline")
	}
}

func TestStaleIfErrorDoesNotCrossHardDeadline(t *testing.T) {
	client := leaseResponseClient(
		t,
		"*4\r\n$11\r\nstale_lease\r\n$5\r\nstale\r\n$5\r\ntoken\r\n:20\r\n",
		0,
	)
	failure := errors.New("load failed")
	_, err := client.GetOrLoad(
		"key",
		func() ([]byte, error) {
			time.Sleep(40 * time.Millisecond)
			return nil, failure
		},
		CachePolicy{TTL: time.Second, Stale: time.Second, StaleIfError: true},
	)
	if !errors.Is(err, failure) { t.Fatalf("expected loader error, got %v", err) }
}

func TestBoundedRESPParser(t *testing.T) {
	cases := []string{
		"$16777217\r\n",
		"*100001\r\n",
		strings.Repeat("*1\r\n", maxResponseDepth+2) + "$1\r\nx\r\n",
	}
	for _, response := range cases {
		_, err := readRESP(bufio.NewReader(strings.NewReader(response)))
		var protocol *ProtocolError
		if !errors.As(err, &protocol) {
			t.Fatalf("expected ProtocolError for %q, got %v", response[:minInt(len(response), 20)], err)
		}
	}
}

func TestTraceparentContextIsRequestScoped(t *testing.T) {
	first := WithTraceparent(context.Background(), "trace-a")
	second := WithTraceparent(context.Background(), "trace-b")
	if TraceparentFromContext(first) != "trace-a" || TraceparentFromContext(second) != "trace-b" {
		t.Fatal("traceparent contexts leaked")
	}
	if TraceparentFromContext(context.Background()) != "" {
		t.Fatal("empty context inherited a traceparent")
	}
}

func TestTraceparentStateIsRaceSafe(t *testing.T) {
	client, err := New(Options{InvalidationPoll: time.Hour})
	if err != nil { t.Fatal(err) }
	clientSide, serverSide := net.Pipe()
	client.conn = clientSide
	client.reader = bufio.NewReader(clientSide)
	t.Cleanup(client.Close)
	go func() {
		defer serverSide.Close()
		reader := bufio.NewReader(serverSide)
		for {
			parts, readErr := readCommand(reader)
			if readErr != nil { return }
			response := "+OK\r\n"
			if parts[0] == "MC.FETCH" {
				document := `{"state":"hit","origin":"origin","value":"value","attempts":1,"error":null}`
				response = fmt.Sprintf("$%d\r\n%s\r\n", len(document), document)
			}
			if _, writeErr := serverSide.Write([]byte(response)); writeErr != nil { return }
		}
	}()
	failures := make(chan error, 100)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		for index := 0; index < 50; index++ {
			if err := client.SetTraceparent(fmt.Sprintf("trace-%d", index)); err != nil {
				failures <- err
			}
		}
	}()
	go func() {
		defer wait.Done()
		for index := 0; index < 50; index++ {
			if _, err := client.Fetch("key", "origin", "/path", false, ""); err != nil {
				failures <- err
			}
		}
	}()
	wait.Wait()
	close(failures)
	for err := range failures { t.Error(err) }
}

func readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil { return nil, err }
	count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil { return nil, err }
	parts := make([]string, count)
	for index := 0; index < count; index++ {
		lengthLine, readErr := reader.ReadString('\n')
		if readErr != nil { return nil, readErr }
		length, parseErr := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(lengthLine, "$")))
		if parseErr != nil { return nil, parseErr }
		value := make([]byte, length)
		if _, readErr = io.ReadFull(reader, value); readErr != nil { return nil, readErr }
		if _, readErr = io.ReadFull(reader, make([]byte, 2)); readErr != nil { return nil, readErr }
		parts[index] = string(value)
	}
	return parts, nil
}

func TestCachePolicyExplicitZeroAndNegativeValidation(t *testing.T) {
	defaulted, err := normalizeCachePolicy(CachePolicy{TTL: time.Second})
	if err != nil || defaulted.Stale != 300*time.Second {
		t.Fatalf("omitted stale window did not default: %+v %v", defaulted, err)
	}
	explicit, err := normalizeCachePolicy(
		CachePolicy{TTL: time.Second}.WithStale(0),
	)
	if err != nil || explicit.Stale != 0 || !explicit.StaleSet {
		t.Fatalf("explicit zero stale window was not preserved: %+v %v", explicit, err)
	}
	client, err := New(Options{InvalidationPoll: time.Hour})
	if err != nil { t.Fatal(err) }
	clientSide, serverSide := net.Pipe()
	client.conn = clientSide
	client.reader = bufio.NewReader(clientSide)
	received := make(chan []string, 1)
	go func() {
		defer serverSide.Close()
		parts, readErr := readCommand(bufio.NewReader(serverSide))
		if readErr == nil {
			received <- parts
			_, _ = serverSide.Write([]byte("+OK\r\n"))
		}
	}()
	if err = client.Put(
		"key", []byte("value"),
		CachePolicy{TTL: time.Second}.WithStale(0), "",
	); err != nil {
		t.Fatal(err)
	}
	parts := <-received
	if got := strings.Join(parts, " "); got != "MC.SET key value TTL 1 STALE 0" {
		t.Fatalf("unexpected explicit-zero command: %s", got)
	}
	if err = client.Put(
		"key", []byte("value"),
		CachePolicy{TTL: time.Second, Stale: -time.Second}, "",
	); err == nil {
		t.Fatal("negative stale duration was accepted")
	}
	if _, err = client.GetOrLoad(
		"key", func() ([]byte, error) { return nil, nil },
		CachePolicy{TTL: -time.Second},
	); err == nil {
		t.Fatal("negative TTL was accepted")
	}
}

func TestAllCommandDecodersReturnProtocolErrors(t *testing.T) {
	jsonBulk := func(value string) string {
		return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	}
	tests := []struct {
		name string
		response string
		call func(*Client) error
	}{
		{"ping type", ":1\r\n", func(c *Client) error { _, err := c.Ping(); return err }},
		{"get type", "+value\r\n", func(c *Client) error { _, err := c.Get("key"); return err }},
		{"set shape", "$2\r\nOK\r\n", func(c *Client) error { return c.Set("key", []byte("value"), 0) }},
		{"mget arity", "*1\r\n$5\r\nvalue\r\n", func(c *Client) error { _, err := c.MGet("a", "b"); return err }},
		{"mget element", "*1\r\n:1\r\n", func(c *Client) error { _, err := c.MGet("a"); return err }},
		{"mset shape", "$2\r\nOK\r\n", func(c *Client) error { return c.MSet(map[string][]byte{"key": []byte("value")}) }},
		{"delete range", ":-1\r\n", func(c *Client) error { _, err := c.Delete("key"); return err }},
		{"exists range", ":2\r\n", func(c *Client) error { _, err := c.Exists("key"); return err }},
		{"expire range", ":2\r\n", func(c *Client) error { _, err := c.Expire("key", 30); return err }},
		{"ttl range", ":-3\r\n", func(c *Client) error { _, err := c.TTL("key"); return err }},
		{"put shape", ":1\r\n", func(c *Client) error { return c.Put("key", []byte("value"), CachePolicy{}, "") }},
		{"lease arity", "*1\r\n$5\r\nfresh\r\n", func(c *Client) error { _, err := c.Lease("key"); return err }},
		{"lease element", "*4\r\n$5\r\nfresh\r\n$5\r\nvalue\r\n$3\r\nbad\r\n:1\r\n", func(c *Client) error { _, err := c.Lease("key"); return err }},
		{"lease retry", "*2\r\n$7\r\nloading\r\n:-1\r\n", func(c *Client) error { _, err := c.Lease("key"); return err }},
		{"fetch JSON", jsonBulk("{"), func(c *Client) error { _, err := c.Fetch("key", "origin", "/path", false, ""); return err }},
		{"fetch semantics", jsonBulk(`{"state":1,"origin":"origin"}`), func(c *Client) error { _, err := c.Fetch("key", "origin", "/path", false, ""); return err }},
		{"invalidate range", ":-1\r\n", func(c *Client) error { _, err := c.Invalidate("tag"); return err }},
		{"status shape", jsonBulk(`{"degraded":"false"}`), func(c *Client) error { _, err := c.Status(); return err }},
		{"invalidations semantics", jsonBulk(`{"cursor":"x","epoch":"e","generation":"bad"}`), func(c *Client) error { c.lastPoll = time.Time{}; return c.pollInvalidations() }},
		{"traceparent shape", "$2\r\nOK\r\n", func(c *Client) error { return c.SetTraceparent("trace") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := leaseResponseClient(t, test.response, 0)
			err := test.call(client)
			var protocol *ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("expected ProtocolError, got %T: %v", err, err)
			}
			if client.conn != nil {
				t.Fatal("protocol-incompatible connection was not reset")
			}
		})
	}
}

func minInt(a, b int) int { if a < b { return a }; return b }
