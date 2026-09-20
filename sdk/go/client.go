package megacache

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const Version = "1.0.0"
const (
	maxResponseBytes = 16 * 1024 * 1024
	maxBulkBytes = 16 * 1024 * 1024
	maxArrayItems = 100000
	maxResponseDepth = 128
	maxResponseLine = 65536
)

type ConnectionError struct{ Err error }
func (e *ConnectionError) Error() string { return "megacache connection: " + e.Err.Error() }
func (e *ConnectionError) Unwrap() error { return e.Err }

type ProtocolError struct{ Message string }
func (e *ProtocolError) Error() string { return "megacache protocol: " + e.Message }

type CommandError struct{ Code, Message string }
func (e *CommandError) Error() string { return e.Message }

type LoaderPanicError struct{ Panic interface{} }
func (e *LoaderPanicError) Error() string {
	return fmt.Sprintf("megacache loader panicked: %v", e.Panic)
}

type Options struct {
	Address, Username, Password, ServerName, CAFile string
	Timeout time.Duration
	TLS bool
	L1MaxEntries, L1MaxBytes int
	InvalidationPoll time.Duration
	TraceparentProvider func() string
}

type Client struct {
	options Options
	mu sync.Mutex
	mutationMu sync.Mutex
	stateMu sync.Mutex
	conn net.Conn
	reader *bufio.Reader
	l1 *LocalCache
	cursor string
	cursorEpoch string
	cursorGeneration int64
	cursorInitialized bool
	lastPoll time.Time
	flights map[string]*flight
	traceparent string
	generation uint64
}

type flight struct {
	done chan struct{}
	value CachedValue
	err error
}

type LeaseResult struct {
	State string
	Value []byte
	LeaseToken string
	RetryAfter time.Duration
	ExpiresIn *time.Duration
	StaleFor *time.Duration
}

type FetchResult struct {
	State string `json:"state"`
	Origin string `json:"origin"`
	Value interface{} `json:"value"`
	StatusCode *int `json:"status_code"`
	Attempts int `json:"attempts"`
	Error string `json:"error"`
}

type CachePolicy struct {
	TTL, Stale time.Duration
	Tags []string
	StaleIfError bool
	// StaleSet distinguishes an explicitly configured zero stale window from
	// the zero value, which retains the default stale window for compatibility.
	StaleSet bool
}

func (p CachePolicy) WithStale(stale time.Duration) CachePolicy {
	p.Stale = stale
	p.StaleSet = true
	return p
}

type CachedValue struct {
	Value []byte
	State string
	staleDeadline time.Time
}

func New(options Options) (*Client, error) {
	if options.Address == "" { options.Address = "127.0.0.1:6380" }
	if options.Timeout == 0 { options.Timeout = 5 * time.Second }
	if options.L1MaxEntries == 0 { options.L1MaxEntries = 1000 }
	if options.L1MaxBytes == 0 { options.L1MaxBytes = 16 * 1024 * 1024 }
	if options.InvalidationPoll == 0 { options.InvalidationPoll = time.Second }
	l1, err := NewLocalCache(options.L1MaxEntries, options.L1MaxBytes)
	if err != nil { return nil, err }
	return &Client{options: options, l1: l1, flights: make(map[string]*flight)}, nil
}

func (c *Client) connectLocked() error {
	if c.conn != nil { return nil }
	dialer := net.Dialer{Timeout: c.options.Timeout}
	var conn net.Conn
	var err error
	if c.options.TLS {
		host, _, splitErr := net.SplitHostPort(c.options.Address)
		if splitErr != nil { return splitErr }
		serverName := c.options.ServerName
		if serverName == "" { serverName = host }
		config := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
		if c.options.CAFile != "" {
			pem, readErr := os.ReadFile(c.options.CAFile)
			if readErr != nil { return readErr }
			roots, rootsErr := x509.SystemCertPool()
			if rootsErr != nil { return rootsErr }
			if !roots.AppendCertsFromPEM(pem) { return errors.New("CA file contains no certificates") }
			config.RootCAs = roots
		}
		conn, err = tls.DialWithDialer(&dialer, "tcp", c.options.Address, config)
	} else {
		conn, err = dialer.Dial("tcp", c.options.Address)
	}
	if err != nil { return &ConnectionError{err} }
	c.conn, c.reader = conn, bufio.NewReader(conn)
	if c.options.Password != "" {
		args := []interface{}{"AUTH"}
		if c.options.Username != "" { args = append(args, c.options.Username) }
		args = append(args, c.options.Password)
		var response interface{}
		if response, err = c.commandLocked(args...); err == nil {
			if text, ok := response.(string); !ok || text != "OK" {
				err = &ProtocolError{"AUTH response must be OK"}
			}
		}
		if err != nil {
			conn.Close(); c.conn = nil; c.reader = nil
			return err
		}
	}
	return nil
}

func (c *Client) Command(parts ...interface{}) (interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.connectLocked(); err != nil { return nil, err }
	return c.commandLocked(parts...)
}

func (c *Client) commandLocked(parts ...interface{}) (interface{}, error) {
	if len(parts) == 0 { return nil, errors.New("command is required") }
	var builder strings.Builder
	fmt.Fprintf(&builder, "*%d\r\n", len(parts))
	for _, part := range parts {
		var value []byte
		switch item := part.(type) {
		case []byte: value = item
		case string: value = []byte(item)
		case int: value = []byte(strconv.Itoa(item))
		case int64: value = []byte(strconv.FormatInt(item, 10))
		default: return nil, fmt.Errorf("unsupported command part %T", part)
		}
		fmt.Fprintf(&builder, "$%d\r\n%s\r\n", len(value), value)
	}
	if err := c.conn.SetDeadline(time.Now().Add(c.options.Timeout)); err != nil { return nil, err }
	if _, err := io.WriteString(c.conn, builder.String()); err != nil {
		c.closeLocked()
		return nil, &ConnectionError{err}
	}
	value, err := readRESP(c.reader)
	if err != nil {
		if _, ok := err.(*CommandError); !ok { c.closeLocked() }
	}
	return value, err
}

type respReader struct {
	reader *bufio.Reader
	bytes int
}

func readRESP(reader *bufio.Reader) (interface{}, error) {
	parser := &respReader{reader: reader}
	return parser.read(0)
}

func (p *respReader) consume(count int) error {
	p.bytes += count
	if p.bytes > maxResponseBytes { return &ProtocolError{"response exceeds byte limit"} }
	return nil
}

func (p *respReader) read(depth int) (interface{}, error) {
	if depth > maxResponseDepth { return nil, &ProtocolError{"RESP nesting exceeds limit"} }
	marker, err := p.reader.ReadByte()
	if err != nil { return nil, &ConnectionError{err} }
	if err = p.consume(1); err != nil { return nil, err }
	line, err := p.readLine()
	if err != nil { return nil, err }
	switch marker {
	case '+': return line, nil
	case '-':
		code := strings.SplitN(line, " ", 2)[0]
		return nil, &CommandError{Code: code, Message: line}
	case ':':
		value, parseErr := strconv.Atoi(line)
		if parseErr != nil { return nil, &ProtocolError{"invalid integer"} }
		return value, nil
	case '$':
		length, parseErr := strconv.Atoi(line)
		if parseErr != nil || length < -1 { return nil, &ProtocolError{"invalid bulk length"} }
		if length == -1 { return nil, nil }
		if length > maxBulkBytes { return nil, &ProtocolError{"bulk string exceeds limit"} }
		if err = p.consume(length + 2); err != nil { return nil, err }
		value := make([]byte, length)
		if _, err = io.ReadFull(p.reader, value); err != nil { return nil, &ConnectionError{err} }
		var terminator [2]byte
		if _, err = io.ReadFull(p.reader, terminator[:]); err != nil || string(terminator[:]) != "\r\n" {
			return nil, &ProtocolError{"invalid bulk terminator"}
		}
		return value, nil
	case '*':
		count, parseErr := strconv.Atoi(line)
		if parseErr != nil || count < -1 || count > maxArrayItems { return nil, &ProtocolError{"invalid array length"} }
		if count == -1 { return nil, nil }
		values := make([]interface{}, 0)
		for index := 0; index < count; index++ {
			var value interface{}
			value, err = p.read(depth + 1)
			if err != nil { return nil, err }
			values = append(values, value)
		}
		return values, nil
	default: return nil, &ProtocolError{"unknown RESP marker"}
	}
}

func (p *respReader) readLine() (string, error) {
	line := make([]byte, 0, 64)
	for len(line) <= maxResponseLine {
		value, err := p.reader.ReadByte()
		if err != nil { return "", &ConnectionError{err} }
		if err = p.consume(1); err != nil { return "", err }
		line = append(line, value)
		if len(line) >= 2 && line[len(line)-2] == '\r' && line[len(line)-1] == '\n' {
			return string(line[:len(line)-2]), nil
		}
	}
	return "", &ProtocolError{"oversized RESP line"}
}

func (c *Client) Close() error {
	c.mu.Lock(); defer c.mu.Unlock()
	if c.conn == nil { return nil }
	err := c.conn.Close(); c.conn = nil; c.reader = nil
	return err
}
func (c *Client) closeLocked() { if c.conn != nil { _ = c.conn.Close() }; c.conn = nil; c.reader = nil }

func (c *Client) Ping() (string, error) {
	value, err := c.Command("PING")
	if err != nil { return "", err }
	result, ok := value.(string)
	if !ok || result != "PONG" { return "", c.protocolFailure("PING response must be PONG") }
	return result, nil
}
func (c *Client) Get(key string) ([]byte, error) {
	value, err := c.Command("GET", key)
	if err != nil || value == nil { return nil, err }
	result, ok := value.([]byte)
	if !ok { return nil, c.protocolFailure("GET response must be a bulk string or null") }
	return result, nil
}
func (c *Client) Set(key string, value []byte, expiry time.Duration) error {
	c.mutationMu.Lock(); defer c.mutationMu.Unlock()
	parts := []interface{}{"SET", key, value}
	if expiry > 0 { parts = append(parts, "EX", int(expiry/time.Second)) }
	response, err := c.Command(parts...)
	if err != nil { return err }
	if err = c.expectOK(response, "SET"); err != nil { return err }
	c.mutationSucceeded()
	return nil
}
func (c *Client) MGet(keys ...string) ([][]byte, error) {
	parts := []interface{}{"MGET"}; for _, key := range keys { parts = append(parts, key) }
	value, err := c.Command(parts...); if err != nil { return nil, err }
	raw, ok := value.([]interface{})
	if !ok || len(raw) != len(keys) {
		return nil, c.protocolFailure("MGET response must contain one value per key")
	}
	result := make([][]byte, len(raw))
	for index, item := range raw {
		if item == nil { continue }
		bytes, itemOK := item.([]byte)
		if !itemOK { return nil, c.protocolFailure("MGET values must be bulk strings or null") }
		result[index] = bytes
	}
	return result, nil
}
func (c *Client) MSet(values map[string][]byte) error {
	c.mutationMu.Lock(); defer c.mutationMu.Unlock()
	parts := []interface{}{"MSET"}; for key, value := range values { parts = append(parts, key, value) }
	response, err := c.Command(parts...)
	if err != nil { return err }
	if err = c.expectOK(response, "MSET"); err != nil { return err }
	c.mutationSucceeded()
	return nil
}
func (c *Client) Delete(keys ...string) (int, error) {
	return c.integerMutation("DEL", len(keys), stringInterfaces(keys)...)
}
func (c *Client) Exists(keys ...string) (int, error) {
	value, err := c.Command(append([]interface{}{"EXISTS"}, stringInterfaces(keys)...)...)
	if err != nil { return 0, err }
	return c.expectInteger(value, "EXISTS", 0, len(keys))
}
func (c *Client) Expire(key string, seconds int) (bool, error) {
	c.mutationMu.Lock(); defer c.mutationMu.Unlock()
	value, err := c.Command("EXPIRE", key, seconds); if err != nil { return false, err }
	result, err := c.expectInteger(value, "EXPIRE", 0, 1)
	if err != nil { return false, err }
	if result != 0 { c.mutationSucceeded(); return true, nil }
	return false, nil
}
func (c *Client) TTL(key string) (int, error) {
	value, err := c.Command("TTL", key)
	if err != nil { return 0, err }
	return c.expectInteger(value, "TTL", -2, -3)
}
func (c *Client) Put(key string, value []byte, policy CachePolicy, leaseToken string) error {
	_, err := c.putWithGeneration(key, value, policy, leaseToken)
	return err
}
func (c *Client) putWithGeneration(key string, value []byte, policy CachePolicy, leaseToken string) (uint64, error) {
	policy, err := normalizeCachePolicy(policy)
	if err != nil { return 0, err }
	c.mutationMu.Lock(); defer c.mutationMu.Unlock()
	parts := []interface{}{"MC.SET", key, value, "TTL", int(policy.TTL/time.Second), "STALE", int(policy.Stale/time.Second)}
	if len(policy.Tags) > 0 { parts = append(parts, "TAGS", len(policy.Tags)); parts = append(parts, stringInterfaces(policy.Tags)...)}
	if leaseToken != "" { parts = append(parts, "LEASE", leaseToken) }
	response, err := c.Command(parts...); if err != nil { return 0, err }
	if err = c.expectOK(response, "MC.SET"); err != nil { return 0, err }
	return c.mutationSucceeded(), nil
}
func (c *Client) Lease(key string) (LeaseResult, error) {
	value, err := c.Command("MC.LEASE", key, "WINDOWS"); if err != nil { return LeaseResult{}, err }
	items, ok := value.([]interface{})
	if !ok || len(items) == 0 { return LeaseResult{}, c.protocolFailure("invalid MC.LEASE response") }
	state, err := c.expectText(items[0], "MC.LEASE state")
	if err != nil { return LeaseResult{}, err }
	result := LeaseResult{State: state}
	switch result.State {
	case "fresh":
		if err = c.expectArity(items, "MC.LEASE fresh", 2, 4); err != nil { return LeaseResult{}, err }
		if result.Value, err = c.expectBulk(items[1], "MC.LEASE fresh", false); err != nil { return LeaseResult{}, err }
		if len(items) > 2 {
			window, windowErr := c.expectInteger(items[2], "MC.LEASE expires", -1, -2)
			if windowErr != nil { return LeaseResult{}, windowErr }
			duration := time.Duration(window) * time.Millisecond; result.ExpiresIn = &duration
		}
		if len(items) > 3 {
			window, windowErr := c.expectInteger(items[3], "MC.LEASE stale", -1, -2)
			if windowErr != nil { return LeaseResult{}, windowErr }
			duration := time.Duration(window) * time.Millisecond; result.StaleFor = &duration
		}
	case "stale":
		if err = c.expectArity(items, "MC.LEASE stale", 2, 3); err != nil { return LeaseResult{}, err }
		if result.Value, err = c.expectBulk(items[1], "MC.LEASE stale", false); err != nil { return LeaseResult{}, err }
		if len(items) > 2 {
			window, windowErr := c.expectInteger(items[2], "MC.LEASE stale", -1, -2)
			if windowErr != nil { return LeaseResult{}, windowErr }
			duration := time.Duration(window) * time.Millisecond; result.StaleFor = &duration
		}
	case "stale_lease":
		if err = c.expectArity(items, "MC.LEASE stale_lease", 3, 4); err != nil { return LeaseResult{}, err }
		if result.Value, err = c.expectBulk(items[1], "MC.LEASE stale_lease", false); err != nil { return LeaseResult{}, err }
		if result.LeaseToken, err = c.expectText(items[2], "MC.LEASE lease token"); err != nil { return LeaseResult{}, err }
		if len(items) > 3 {
			window, windowErr := c.expectInteger(items[3], "MC.LEASE stale", -1, -2)
			if windowErr != nil { return LeaseResult{}, windowErr }
			duration := time.Duration(window) * time.Millisecond; result.StaleFor = &duration
		}
	case "lease":
		if err = c.expectArity(items, "MC.LEASE lease", 2); err != nil { return LeaseResult{}, err }
		result.LeaseToken, err = c.expectText(items[1], "MC.LEASE lease token")
		if err != nil { return LeaseResult{}, err }
	case "loading":
		if err = c.expectArity(items, "MC.LEASE loading", 2); err != nil { return LeaseResult{}, err }
		retry, retryErr := c.expectInteger(items[1], "MC.LEASE retry", 0, -1)
		if retryErr != nil { return LeaseResult{}, retryErr }
		result.RetryAfter = time.Duration(retry) * time.Millisecond
	default: return LeaseResult{}, c.protocolFailure("unknown MC.LEASE state")
	}
	return result, nil
}
func (c *Client) Fetch(key, origin, path string, refresh bool, traceparent string) (FetchResult, error) {
	parts := []interface{}{"MC.FETCH", key, origin, path}
	if refresh { parts = append(parts, "REFRESH") }
	if traceparent == "" && c.options.TraceparentProvider != nil { traceparent = c.options.TraceparentProvider() }
	if traceparent == "" {
		c.stateMu.Lock()
		traceparent = c.traceparent
		c.stateMu.Unlock()
	}
	if traceparent != "" { parts = append(parts, "TRACEPARENT", traceparent) }
	value, err := c.Command(parts...); if err != nil { return FetchResult{}, err }
	document, err := c.decodeJSONObject(value, "MC.FETCH")
	if err != nil { return FetchResult{}, err }
	if _, ok := document["state"].(string); !ok { return FetchResult{}, c.protocolFailure("MC.FETCH JSON state must be a string") }
	if _, ok := document["origin"].(string); !ok { return FetchResult{}, c.protocolFailure("MC.FETCH JSON origin must be a string") }
	if err = c.validateOptionalJSONInteger(document, "status_code", 100, false); err != nil { return FetchResult{}, err }
	if err = c.validateOptionalJSONInteger(document, "attempts", 0, false); err != nil { return FetchResult{}, err }
	if raw, exists := document["error"]; exists && raw != nil {
		if _, ok := raw.(string); !ok { return FetchResult{}, c.protocolFailure("MC.FETCH JSON error must be a string or null") }
	}
	encoded, marshalErr := json.Marshal(document)
	if marshalErr != nil { return FetchResult{}, c.protocolFailure("MC.FETCH JSON could not be decoded") }
	var result FetchResult
	if unmarshalErr := json.Unmarshal(encoded, &result); unmarshalErr != nil {
		return FetchResult{}, c.protocolFailure("MC.FETCH JSON has invalid field types")
	}
	return result, nil
}
func (c *Client) Invalidate(tags ...string) (int, error) {
	return c.integerMutation("MC.INVALIDATE", -1, stringInterfaces(tags)...)
}
func (c *Client) Status() (map[string]interface{}, error) {
	document, err := c.jsonCommand("MC.STATUS")
	if err != nil { return nil, err }
	if _, ok := document["degraded"].(bool); !ok {
		return nil, c.protocolFailure("MC.STATUS JSON degraded must be a boolean")
	}
	return document, nil
}
func (c *Client) SetTraceparent(value string) error {
	response, err := c.Command("MC.TRACEPARENT", value)
	if err != nil { return err }
	if err = c.expectOK(response, "MC.TRACEPARENT"); err != nil { return err }
	c.stateMu.Lock()
	c.traceparent = strings.ToLower(value)
	c.stateMu.Unlock()
	return nil
}

func (c *Client) GetOrLoad(key string, loader func() ([]byte, error), policy CachePolicy) (CachedValue, error) {
	var err error
	policy, err = normalizeCachePolicy(policy)
	if err != nil { return CachedValue{}, err }
	if err := c.pollInvalidations(); err != nil { return CachedValue{}, err }
	stale, found := c.l1.Get(key)
	if found && stale.State == "fresh" { return stale, nil }
	c.stateMu.Lock()
	if existing := c.flights[key]; existing != nil {
		c.stateMu.Unlock()
		<-existing.done
		return completedFlight(existing)
	}
	current := &flight{done: make(chan struct{})}; c.flights[key] = current; c.stateMu.Unlock()
	defer func() { c.stateMu.Lock(); delete(c.flights, key); close(current.done); c.stateMu.Unlock() }()
	for {
		expectedGeneration := c.currentGeneration()
		leaseStarted := time.Now()
		lease, err := c.Lease(key)
		leaseElapsed := elapsedSince(leaseStarted)
		if err != nil {
			if found && policy.StaleIfError && staleValid(stale) {
				current.value = CachedValue{
					Value: stale.Value, State: "stale_if_error",
					staleDeadline: stale.staleDeadline,
				}
				current.err = err
				return completedFlight(current)
			}
			current.err = err; return CachedValue{}, err
		}
		if lease.State == "fresh" {
			freshBudget := policy.TTL
			if lease.ExpiresIn != nil && *lease.ExpiresIn >= 0 { freshBudget = minDuration(policy.TTL, *lease.ExpiresIn) }
			staleBudget := policy.Stale
			if lease.StaleFor != nil && *lease.StaleFor >= 0 { staleBudget = minDuration(policy.Stale, *lease.StaleFor) }
			freshFor, staleFor := remainingWindows(freshBudget, staleBudget, leaseElapsed)
			if freshFor + staleFor > 0 { c.admitIfGeneration(expectedGeneration, key, lease.Value, freshFor, staleFor) }
			current.value = CachedValue{Value: lease.Value, State: "fresh"}; return current.value, nil
		}
		if lease.State == "stale" {
			staleBudget := policy.Stale
			if lease.StaleFor != nil && *lease.StaleFor >= 0 { staleBudget = minDuration(policy.Stale, *lease.StaleFor) }
			remaining := staleBudget - leaseElapsed
			if remaining < 0 { remaining = 0 }
			if remaining > 0 { c.admitIfGeneration(expectedGeneration, key, lease.Value, 0, remaining) }
			current.value = CachedValue{
				Value: lease.Value, State: "stale",
				staleDeadline: deadlineAfter(leaseStarted, staleBudget),
			}
			return current.value, nil
		}
		if lease.State == "loading" { time.Sleep(maxDuration(time.Millisecond, lease.RetryAfter)); continue }
		if lease.State == "stale_lease" && lease.Value != nil {
			staleBudget := policy.Stale
			if lease.StaleFor != nil && *lease.StaleFor >= 0 { staleBudget = minDuration(policy.Stale, *lease.StaleFor) }
			stale, found = CachedValue{
				Value: lease.Value, State: "stale",
				staleDeadline: deadlineAfter(leaseStarted, staleBudget),
			}, true
		}
		value, loadErr, panicked, panicValue := callLoader(loader)
		if panicked {
			current.err = &LoaderPanicError{Panic: panicValue}
			panic(panicValue)
		}
		var ownGeneration uint64
		putStarted := time.Now()
		if loadErr == nil { ownGeneration, loadErr = c.putWithGeneration(key, value, policy, lease.LeaseToken) }
		if loadErr != nil {
			if found && policy.StaleIfError && staleValid(stale) {
				current.value = CachedValue{
					Value: stale.Value, State: "stale_if_error",
					staleDeadline: stale.staleDeadline,
				}
				current.err = loadErr
				return completedFlight(current)
			}
			current.err = loadErr; return CachedValue{}, loadErr
		}
		freshFor, staleFor := remainingWindows(policy.TTL, policy.Stale, elapsedSince(putStarted))
		if freshFor + staleFor > 0 { c.admitIfGeneration(ownGeneration, key, value, freshFor, staleFor) }
		current.value = CachedValue{Value: value, State: "loaded"}; return current.value, nil
	}
}

func (c *Client) pollInvalidations() error {
	c.stateMu.Lock()
	if time.Since(c.lastPoll) < c.options.InvalidationPoll { c.stateMu.Unlock(); return nil }
	previous := c.cursor
	previousEpoch := c.cursorEpoch
	previousGeneration := c.cursorGeneration
	initialized := c.cursorInitialized
	c.stateMu.Unlock()
	document, err := c.jsonCommand("MC.INVALIDATIONS", previous); if err != nil { return err }
	rawCursor, hasCursor := document["cursor"]
	if !hasCursor { return c.protocolFailure("MC.INVALIDATIONS JSON is missing cursor") }
	var cursor string
	switch value := rawCursor.(type) {
	case string:
		cursor = value
	case json.Number:
		integer, parseErr := strconv.ParseInt(string(value), 10, 64)
		if parseErr != nil || integer < 0 {
			return c.protocolFailure("MC.INVALIDATIONS JSON cursor must be a string or non-negative integer")
		}
		cursor = strconv.FormatInt(integer, 10)
	default:
		return c.protocolFailure("MC.INVALIDATIONS JSON cursor must be a string or non-negative integer")
	}
	rawEpoch, hasEpoch := document["epoch"]
	rawGeneration, hasGeneration := document["generation"]
	if hasEpoch != hasGeneration {
		return c.protocolFailure("MC.INVALIDATIONS JSON epoch and generation must appear together")
	}
	epoch := ""
	generation := int64(0)
	if hasEpoch {
		var ok bool
		epoch, ok = rawEpoch.(string)
		if !ok { return c.protocolFailure("MC.INVALIDATIONS JSON epoch must be a string") }
		number, ok := rawGeneration.(json.Number)
		if !ok {
			return c.protocolFailure("MC.INVALIDATIONS JSON generation must be a non-negative integer")
		}
		generation, err = strconv.ParseInt(string(number), 10, 64)
		if err != nil || generation < 0 {
			return c.protocolFailure("MC.INVALIDATIONS JSON generation must be a non-negative integer")
		}
	}
	if changedValue, exists := document["changed"]; exists {
		if _, ok := changedValue.(bool); !ok {
			return c.protocolFailure("MC.INVALIDATIONS JSON changed must be a boolean")
		}
	}
	changed := initialized && cursor != previous
	if hasEpoch && hasGeneration {
		changed = initialized && (epoch != previousEpoch || generation != previousGeneration)
	}
	if changed { c.mutationSucceeded() }
	c.stateMu.Lock()
	c.cursor, c.cursorEpoch, c.cursorGeneration, c.cursorInitialized, c.lastPoll = cursor, epoch, generation, true, time.Now()
	c.stateMu.Unlock()
	return nil
}
func (c *Client) jsonCommand(parts ...interface{}) (map[string]interface{}, error) {
	value, err := c.Command(parts...); if err != nil { return nil, err }
	return c.decodeJSONObject(value, fmt.Sprint(parts[0]))
}
func (c *Client) integerMutation(command string, maximum int, parts ...interface{}) (int, error) {
	c.mutationMu.Lock(); defer c.mutationMu.Unlock()
	value, err := c.Command(append([]interface{}{command}, parts...)...); if err != nil { return 0, err }
	result, err := c.expectInteger(value, command, 0, maximum)
	if err != nil { return 0, err }
	if result != 0 { c.mutationSucceeded() }
	return result, nil
}
func (c *Client) mutationSucceeded() uint64 {
	c.stateMu.Lock(); defer c.stateMu.Unlock()
	c.generation++
	c.l1.Clear()
	return c.generation
}
func (c *Client) currentGeneration() uint64 {
	c.stateMu.Lock(); defer c.stateMu.Unlock(); return c.generation
}
func (c *Client) admitIfGeneration(generation uint64, key string, value []byte, ttl, stale time.Duration) bool {
	c.stateMu.Lock(); defer c.stateMu.Unlock()
	if generation != c.generation { return false }
	return c.l1.Put(key, value, ttl, stale)
}
func stringInterfaces(values []string) []interface{} { result := make([]interface{}, len(values)); for i, v := range values { result[i] = v }; return result }
func callLoader(loader func() ([]byte, error)) (
	value []byte, err error, panicked bool, panicValue interface{},
) {
	panicked = true
	defer func() { panicValue = recover() }()
	value, err = loader()
	panicked = false
	return
}
func normalizeCachePolicy(policy CachePolicy) (CachePolicy, error) {
	if policy.TTL < 0 { return CachePolicy{}, errors.New("cache TTL cannot be negative") }
	if policy.Stale < 0 { return CachePolicy{}, errors.New("cache stale duration cannot be negative") }
	if policy.TTL == 0 { policy.TTL = 60 * time.Second }
	if policy.Stale == 0 && !policy.StaleSet { policy.Stale = 300 * time.Second }
	if policy.TTL < time.Second { return CachePolicy{}, errors.New("cache TTL must be at least one second") }
	return policy, nil
}
func (c *Client) protocolFailure(message string) error {
	_ = c.Close()
	return &ProtocolError{Message: message}
}
func (c *Client) expectOK(value interface{}, command string) error {
	text, ok := value.(string)
	if !ok || text != "OK" { return c.protocolFailure(command + " response must be OK") }
	return nil
}
func (c *Client) expectText(value interface{}, command string) (string, error) {
	switch item := value.(type) {
	case string:
		return item, nil
	case []byte:
		if !utf8.Valid(item) { return "", c.protocolFailure(command + " response is not valid UTF-8") }
		return string(item), nil
	default:
		return "", c.protocolFailure(command + " response must be a string")
	}
}
func (c *Client) expectBulk(value interface{}, command string, nullable bool) ([]byte, error) {
	if value == nil && nullable { return nil, nil }
	bytes, ok := value.([]byte)
	if !ok { return nil, c.protocolFailure(command + " response must be a bulk string") }
	return bytes, nil
}
func (c *Client) expectInteger(value interface{}, command string, minimum, maximum int) (int, error) {
	integer, ok := value.(int)
	if !ok { return 0, c.protocolFailure(command + " response must be an integer") }
	if integer < minimum { return 0, c.protocolFailure(command + " response is below its valid range") }
	if maximum >= minimum && integer > maximum {
		return 0, c.protocolFailure(command + " response is above its valid range")
	}
	return integer, nil
}
func (c *Client) expectArity(values []interface{}, command string, allowed ...int) error {
	for _, count := range allowed { if len(values) == count { return nil } }
	return c.protocolFailure(command + " response has invalid arity")
}
func (c *Client) decodeJSONObject(value interface{}, command string) (map[string]interface{}, error) {
	encoded, ok := value.([]byte)
	if !ok { return nil, c.protocolFailure(command + " response must be a bulk string") }
	if !utf8.Valid(encoded) { return nil, c.protocolFailure(command + " JSON is not valid UTF-8") }
	var result map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, c.protocolFailure(command + " returned invalid JSON object")
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, c.protocolFailure(command + " returned invalid JSON object")
	}
	return result, nil
}
func (c *Client) validateOptionalJSONInteger(
	document map[string]interface{}, field string, minimum int, required bool,
) error {
	raw, exists := document[field]
	if !exists {
		if required { return c.protocolFailure("missing JSON field " + field) }
		return nil
	}
	if raw == nil && !required { return nil }
	number, ok := raw.(json.Number)
	if !ok {
		return c.protocolFailure("JSON field " + field + " must be an integer")
	}
	integer, err := strconv.ParseInt(string(number), 10, 64)
	if err != nil || integer < int64(minimum) {
		return c.protocolFailure("JSON field " + field + " must be an integer")
	}
	return nil
}
func maxDuration(a, b time.Duration) time.Duration { if a > b { return a }; return b }
func minDuration(a, b time.Duration) time.Duration { if a < b { return a }; return b }
func elapsedSince(started time.Time) time.Duration {
	elapsed := time.Since(started)
	if elapsed < 0 { return 0 }
	return elapsed
}
func deadlineAfter(started time.Time, budget time.Duration) time.Time {
	now := time.Now()
	elapsed := now.Sub(started)
	if elapsed < 0 { elapsed = 0 }
	remaining := budget - elapsed
	if remaining < 0 { remaining = 0 }
	return now.Add(remaining)
}
func remainingWindows(fresh, stale, elapsed time.Duration) (time.Duration, time.Duration) {
	remainingFresh := fresh - elapsed
	if remainingFresh < 0 { remainingFresh = 0 }
	remainingHard := fresh + stale - elapsed
	if remainingHard < 0 { remainingHard = 0 }
	return remainingFresh, remainingHard - remainingFresh
}
func staleValid(value CachedValue) bool {
	return !value.staleDeadline.IsZero() && time.Now().Before(value.staleDeadline)
}
func completedFlight(value *flight) (CachedValue, error) {
	if value.value.State == "stale_if_error" && staleValid(value.value) {
		return value.value, nil
	}
	if value.err != nil { return CachedValue{}, value.err }
	if value.value.State == "stale_if_error" {
		return CachedValue{}, errors.New("stale cache entry expired during load failure")
	}
	return value.value, nil
}

type localEntry struct {
	value []byte
	freshUntil, staleUntil time.Time
	size int
}
type LocalCache struct {
	mu sync.Mutex
	maxEntries, maxBytes, usedBytes int
	entries map[string]localEntry
	order []string
}
func NewLocalCache(maxEntries, maxBytes int) (*LocalCache, error) {
	if maxEntries <= 0 || maxBytes <= 0 { return nil, errors.New("L1 limits must be positive") }
	return &LocalCache{maxEntries: maxEntries, maxBytes: maxBytes, entries: make(map[string]localEntry)}, nil
}
func (l *LocalCache) Get(key string) (CachedValue, bool) {
	l.mu.Lock(); defer l.mu.Unlock(); entry, ok := l.entries[key]
	if !ok { return CachedValue{}, false }
	now := time.Now(); if !now.Before(entry.staleUntil) { l.removeLocked(key); return CachedValue{}, false }
	l.touchLocked(key); state := "stale"; if now.Before(entry.freshUntil) { state = "fresh" }
	return CachedValue{
		Value: append([]byte(nil), entry.value...),
		State: state,
		staleDeadline: entry.staleUntil,
	}, true
}
func (l *LocalCache) Put(key string, value []byte, ttl, stale time.Duration) bool {
	if ttl < 0 || stale < 0 || ttl+stale <= 0 { return false }; copied := append([]byte(nil), value...); size := len(key)+len(copied)
	l.mu.Lock(); defer l.mu.Unlock(); if size > l.maxBytes { return false }; l.removeLocked(key)
	now := time.Now(); l.entries[key] = localEntry{copied, now.Add(ttl), now.Add(ttl+stale), size}; l.order = append(l.order, key); l.usedBytes += size
	for len(l.entries) > l.maxEntries || l.usedBytes > l.maxBytes { l.removeLocked(l.order[0]) }; return true
}
func (l *LocalCache) Clear() { l.mu.Lock(); defer l.mu.Unlock(); l.entries = make(map[string]localEntry); l.order = nil; l.usedBytes = 0 }
func (l *LocalCache) touchLocked(key string) { for i, item := range l.order { if item == key { l.order = append(append(l.order[:i], l.order[i+1:]...), key); return } } }
func (l *LocalCache) removeLocked(key string) { if entry, ok := l.entries[key]; ok { delete(l.entries, key); l.usedBytes -= entry.size }; for i, item := range l.order { if item == key { l.order = append(l.order[:i], l.order[i+1:]...); return } } }
