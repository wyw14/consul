// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/structs"
)

// fakeClock is a manually advanced clock used to exercise TTL semantics
// deterministically.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fakeNodesFetcher is a controllable dnsServiceNodesFetchFunc. It counts
// invocations, can block fetches on a gate and returns a mutable result.
type fakeNodesFetcher struct {
	calls int32

	mu    sync.Mutex
	nodes structs.CheckServiceNodes
	err   error

	// gate, when non-nil and open, blocks a fetch until it is closed or the
	// context is cancelled.
	gate chan struct{}
}

func (f *fakeNodesFetcher) fetch(ctx context.Context) (structs.CheckServiceNodes, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneServiceNodes(f.nodes), f.err
}

func (f *fakeNodesFetcher) set(nodes structs.CheckServiceNodes, err error) {
	f.mu.Lock()
	f.nodes = nodes
	f.err = err
	f.mu.Unlock()
}

func (f *fakeNodesFetcher) setGate(gate chan struct{}) {
	f.mu.Lock()
	f.gate = gate
	f.mu.Unlock()
}

func (f *fakeNodesFetcher) callCount() int32 { return atomic.LoadInt32(&f.calls) }

func testServiceNodes(names ...string) structs.CheckServiceNodes {
	var out structs.CheckServiceNodes
	for i, n := range names {
		addr := "198.18.0." + strconv.Itoa(i+1)
		out = append(out, structs.CheckServiceNode{
			Node: &structs.Node{
				Node:       n,
				Address:    addr,
				Datacenter: "dc1",
			},
			Service: &structs.NodeService{
				ID:      "db-1",
				Service: "db",
				Address: addr,
				Port:    8080,
			},
		})
	}
	return out
}

func testCacheKey(service, tag, dc string, passing bool) dnsServiceCacheKey {
	return dnsServiceCacheKey{
		service:     service,
		tag:         tag,
		datacenter:  dc,
		onlyPassing: passing,
	}
}

func newTestServiceResponseCache(t *testing.T, clock *fakeClock, cfg dnsResponseCacheConfig) *dnsServiceResponseCache {
	t.Helper()
	return newDNSServiceResponseCache(cfg, clock.now, hclog.NewNullLogger())
}

func TestDNSResponseCache_KeyStableAndDistinct(t *testing.T) {
	base := dnsServiceCacheKey{
		datacenter:    "dc1",
		tag:           "v1",
		onlyPassing:   true,
		service:       "db",
		samenessGroup: "sg",
		peerName:      "peer",
		connect:       true,
		token:         "secret",
	}

	// The same inputs always produce the same stable representation/hash.
	require.Equal(t, base.String(), base.String())
	require.Equal(t, base.hash(), base.hash())
	require.Len(t, base.hash(), 64) // sha256 hex

	same := base
	require.Equal(t, base.String(), same.String())

	distinct := []dnsServiceCacheKey{
		{datacenter: "dc2", tag: "v1", onlyPassing: true, service: "db"},
		{datacenter: "dc1", tag: "v2", onlyPassing: true, service: "db"},
		{datacenter: "dc1", tag: "v1", onlyPassing: false, service: "db"},
		{datacenter: "dc1", tag: "v1", onlyPassing: true, service: "web"},
		{datacenter: "dc1", tag: "v1", onlyPassing: true, service: "db", peerName: "p2"},
		{datacenter: "dc1", tag: "v1", onlyPassing: true, service: "db", connect: true},
		{datacenter: "dc1", tag: "v1", onlyPassing: true, service: "db", ingress: true},
		{datacenter: "dc1", tag: "v1", onlyPassing: true, service: "db", token: "other"},
	}
	seen := map[string]bool{base.String(): true}
	for _, k := range distinct {
		require.False(t, seen[k.String()], "duplicate key for %#v", k)
		seen[k.String()] = true
	}

	// Length prefixing makes the encoding collision free: a naive
	// concatenation would render both pairs as the same string.
	left := dnsServiceCacheKey{datacenter: "a", tag: "bc"}
	right := dnsServiceCacheKey{datacenter: "ab", tag: "c"}
	require.NotEqual(t, left.String(), right.String())

	// Tokens never appear in the key representation used at runtime.
	require.NotContains(t, base.hash(), "secret")
}

func TestDNSResponseCache_DisabledBypassesCache(t *testing.T) {
	clock := newFakeClock()
	cache := newTestServiceResponseCache(t, clock, dnsResponseCacheConfig{Enabled: false})
	fetcher := &fakeNodesFetcher{nodes: testServiceNodes("a")}

	key := testCacheKey("db", "", "dc1", false)
	for i := 0; i < 3; i++ {
		nodes, found, err := cache.get(context.Background(), key, fetcher.fetch)
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, nodes, 1)
	}
	require.Equal(t, int32(3), fetcher.callCount())
}

func TestDNSResponseCache_ColdSingleFlight(t *testing.T) {
	clock := newFakeClock()
	cache := newTestServiceResponseCache(t, clock, dnsResponseCacheConfig{
		Enabled:     true,
		TTL:         10 * time.Minute,
		NegativeTTL: time.Second,
	})
	fetcher := &fakeNodesFetcher{nodes: testServiceNodes("a", "b")}

	// Block the single backend fetch until all concurrent callers are queued.
	gate := make(chan struct{})
	fetcher.setGate(gate)

	const n = 50
	key := testCacheKey("db", "", "dc1", false)
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]structs.CheckServiceNodes, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			nodes, _, err := cache.get(context.Background(), key, fetcher.fetch)
			results[i] = nodes
			errs[i] = err
		}(i)
	}

	close(start)
	// Wait until the coalesced fetch has started, then give all goroutines a
	// chance to join the singleflight group before releasing it.
	require.Eventually(t, func() bool { return fetcher.callCount() == 1 }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	close(gate)
	wg.Wait()

	require.Equal(t, int32(1), fetcher.callCount(), "exactly one backend fetch must happen")
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		require.Len(t, results[i], 2)
	}
}

func TestDNSResponseCache_FreshHitDoesNotRefetch(t *testing.T) {
	clock := newFakeClock()
	cache := newTestServiceResponseCache(t, clock, dnsResponseCacheConfig{
		Enabled:     true,
		TTL:         10 * time.Second,
		NegativeTTL: time.Second,
	})
	fetcher := &fakeNodesFetcher{nodes: testServiceNodes("a")}
	key := testCacheKey("db", "", "dc1", false)

	nodes, found, err := cache.get(context.Background(), key, fetcher.fetch)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, nodes, 1)
	require.Equal(t, int32(1), fetcher.callCount())

	clock.advance(5 * time.Second)
	for i := 0; i < 3; i++ {
		nodes, found, err = cache.get(context.Background(), key, fetcher.fetch)
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, nodes, 1)
	}
	require.Equal(t, int32(1), fetcher.callCount())
}

func TestDNSResponseCache_BackgroundRefreshAfterTTL(t *testing.T) {
	clock := newFakeClock()
	cache := newTestServiceResponseCache(t, clock, dnsResponseCacheConfig{
		Enabled:     true,
		TTL:         10 * time.Second,
		NegativeTTL: time.Second,
	})
	fetcher := &fakeNodesFetcher{nodes: testServiceNodes("a")}
	key := testCacheKey("db", "", "dc1", false)

	// Prime the cache.
	nodes, found, err := cache.get(context.Background(), key, fetcher.fetch)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "a", nodes[0].Node.Node)

	// After the TTL the next request is served from the stale value while a
	// single background refresh runs.
	fetcher.set(testServiceNodes("b"), nil)
	clock.advance(11 * time.Second)
	nodes, found, err = cache.get(context.Background(), key, fetcher.fetch)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "a", nodes[0].Node.Node, "stale value must be served immediately")

	// The refresh runs in the background; exactly one fetch must start.
	require.Eventually(t, func() bool { return fetcher.callCount() == 2 }, time.Second, time.Millisecond)

	// Once the refresh completes subsequent requests see the new value and
	// no further fetch happens while the entry is fresh.
	require.Eventually(t, func() bool {
		n, _, err := cache.get(context.Background(), key, fetcher.fetch)
		return err == nil && len(n) == 1 && n[0].Node.Node == "b"
	}, time.Second, time.Millisecond)

	require.Equal(t, int32(2), fetcher.callCount())
}

func TestDNSResponseCache_StaleRefreshIsCoalesced(t *testing.T) {
	clock := newFakeClock()
	cache := newTestServiceResponseCache(t, clock, dnsResponseCacheConfig{
		Enabled:     true,
		TTL:         10 * time.Second,
		NegativeTTL: time.Second,
	})

	gate := make(chan struct{})
	fetcher := &fakeNodesFetcher{nodes: testServiceNodes("a")}
	fetcher.setGate(gate)
	key := testCacheKey("db", "", "dc1", false)

	// Prime with a blocking fetch.
	go func() {
		_, _, _ = cache.get(context.Background(), key, fetcher.fetch)
	}()
	require.Eventually(t, func() bool { return fetcher.callCount() == 1 }, time.Second, time.Millisecond)
	close(gate)
	require.Eventually(t, func() bool {
		n, f, err := cache.get(context.Background(), key, fetcher.fetch)
		return err == nil && f && len(n) == 1
	}, time.Second, time.Millisecond)

	// Expire the entry and fan out concurrent stale requests; the background
	// refresh must happen exactly once.
	clock.advance(11 * time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = cache.get(context.Background(), key, fetcher.fetch)
		}()
	}
	wg.Wait()
	require.Equal(t, int32(2), fetcher.callCount())

	// Wait for the refresh to finish, then all requests are fresh hits.
	require.Eventually(t, func() bool {
		n, f, err := cache.get(context.Background(), key, fetcher.fetch)
		return err == nil && f && len(n) == 1 && fetcher.callCount() == 2
	}, time.Second, time.Millisecond)
}

func TestDNSResponseCache_DeregistrationReplacesStale(t *testing.T) {
	clock := newFakeClock()
	cache := newTestServiceResponseCache(t, clock, dnsResponseCacheConfig{
		Enabled:     true,
		TTL:         10 * time.Second,
		NegativeTTL: 2 * time.Second,
	})
	fetcher := &fakeNodesFetcher{nodes: testServiceNodes("a", "b")}
	key := testCacheKey("db", "", "dc1", false)

	nodes, found, err := cache.get(context.Background(), key, fetcher.fetch)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, nodes, 2)

	// The service is deregistered: the catalog now returns an empty result.
	fetcher.set(structs.CheckServiceNodes{}, nil)
	clock.advance(11 * time.Second)

	// The first stale request triggers a background refresh; the caller may
	// still observe the old value once, but as soon as the refresh lands the
	// old addresses are gone for good.
	_, _, _ = cache.get(context.Background(), key, fetcher.fetch)
	require.Eventually(t, func() bool {
		_, found, err := cache.get(context.Background(), key, fetcher.fetch)
		return err == nil && !found
	}, time.Second, time.Millisecond)

	// No stale addresses may be returned after deregistration, even under
	// concurrent reads. Negative results are cached so these must not hit the
	// backend again.
	startCalls := fetcher.callCount()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodes, found, err := cache.get(context.Background(), key, fetcher.fetch)
			require.NoError(t, err)
			require.False(t, found)
			require.Empty(t, nodes)
		}()
	}
	wg.Wait()
	require.Equal(t, startCalls, fetcher.callCount(), "negative result must be served from cache")

	// Advancing within the negative TTL keeps serving the empty result.
	clock.advance(time.Second)
	nodes, found, err = cache.get(context.Background(), key, fetcher.fetch)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, nodes)
	require.Equal(t, startCalls, fetcher.callCount())
}

func TestDNSResponseCache_RefreshErrorKeepsStaleThenRecovers(t *testing.T) {
	clock := newFakeClock()
	cache := newTestServiceResponseCache(t, clock, dnsResponseCacheConfig{
		Enabled:     true,
		TTL:         10 * time.Second,
		NegativeTTL: time.Second,
	})
	fetcher := &fakeNodesFetcher{nodes: testServiceNodes("a")}
	key := testCacheKey("db", "", "dc1", false)

	_, _, err := cache.get(context.Background(), key, fetcher.fetch)
	require.NoError(t, err)

	// A failing refresh must not evict the last known good value and must not
	// surface an error to the stale caller.
	fetcher.set(nil, errors.New("catalog unavailable"))
	clock.advance(11 * time.Second)
	nodes, found, err := cache.get(context.Background(), key, fetcher.fetch)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "a", nodes[0].Node.Node)
	require.Eventually(t, func() bool { return fetcher.callCount() == 2 }, time.Second, time.Millisecond)

	// Once the catalog is healthy again a refresh picks up the new value.
	fetcher.set(testServiceNodes("b"), nil)
	require.Eventually(t, func() bool {
		n, _, err := cache.get(context.Background(), key, fetcher.fetch)
		return err == nil && len(n) == 1 && n[0].Node.Node == "b"
	}, time.Second, time.Millisecond)
}

func TestDNSResponseCache_MaxStaleBlocksForFreshResult(t *testing.T) {
	clock := newFakeClock()
	cache := newTestServiceResponseCache(t, clock, dnsResponseCacheConfig{
		Enabled:     true,
		TTL:         10 * time.Second,
		NegativeTTL: time.Second,
		MaxStale:    5 * time.Second,
	})
	fetcher := &fakeNodesFetcher{nodes: testServiceNodes("a")}
	key := testCacheKey("db", "", "dc1", false)

	_, _, err := cache.get(context.Background(), key, fetcher.fetch)
	require.NoError(t, err)

	// Past the max-stale bound the caller must block on a fresh fetch rather
	// than receiving an ancient value.
	clock.advance(16 * time.Second)
	fetcher.set(testServiceNodes("b"), nil)
	gate := make(chan struct{})
	fetcher.setGate(gate)

	done := make(chan structs.CheckServiceNodes, 1)
	go func() {
		nodes, _, err := cache.get(context.Background(), key, fetcher.fetch)
		require.NoError(t, err)
		done <- nodes
	}()

	require.Eventually(t, func() bool { return fetcher.callCount() == 2 }, time.Second, time.Millisecond)
	select {
	case <-done:
		t.Fatal("stale value returned past max-stale bound")
	case <-time.After(50 * time.Millisecond):
	}

	close(gate)
	select {
	case nodes := <-done:
		require.Len(t, nodes, 1)
		require.Equal(t, "b", nodes[0].Node.Node)
	case <-time.After(time.Second):
		t.Fatal("fresh fetch never returned")
	}
}

func TestDNSResponseCache_DimensionsAreIsolated(t *testing.T) {
	clock := newFakeClock()
	cache := newTestServiceResponseCache(t, clock, dnsResponseCacheConfig{
		Enabled:     true,
		TTL:         time.Minute,
		NegativeTTL: time.Second,
	})

	type variant struct {
		key   dnsServiceCacheKey
		nodes structs.CheckServiceNodes
	}
	variants := []variant{
		{testCacheKey("db", "v1", "dc1", false), testServiceNodes("dc1-v1")},
		{testCacheKey("db", "v2", "dc1", false), testServiceNodes("dc1-v2")},
		{testCacheKey("db", "v1", "dc2", false), testServiceNodes("dc2-v1")},
		{testCacheKey("db", "v1", "dc1", true), testServiceNodes("dc1-v1-passing")},
	}

	// Prime each variant with its own backend result.
	for _, v := range variants {
		f := &fakeNodesFetcher{nodes: v.nodes}
		nodes, found, err := cache.get(context.Background(), v.key, f.fetch)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, v.nodes[0].Node.Node, nodes[0].Node.Node)
	}

	// Each variant keeps serving its own cached value without invoking any
	// backend again.
	for _, v := range variants {
		f := &fakeNodesFetcher{nodes: testServiceNodes("SHOULD-NOT-BE-FETCHED")}
		nodes, found, err := cache.get(context.Background(), v.key, f.fetch)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, v.nodes[0].Node.Node, nodes[0].Node.Node)
		require.Equal(t, int32(0), f.callCount())
	}
}

func TestDNSResponseCache_ShuffledAnswersDoNotCorruptCache(t *testing.T) {
	clock := newFakeClock()
	cache := newTestServiceResponseCache(t, clock, dnsResponseCacheConfig{
		Enabled:     true,
		TTL:         time.Minute,
		NegativeTTL: time.Second,
	})
	fetcher := &fakeNodesFetcher{nodes: testServiceNodes("a", "b", "c")}
	key := testCacheKey("db", "", "dc1", false)

	first, _, err := cache.get(context.Background(), key, fetcher.fetch)
	require.NoError(t, err)
	require.Len(t, first, 3)

	// Callers shuffle the slice they receive; the cached copy must stay
	// intact (same membership regardless of order).
	first[0], first[1], first[2] = first[2], first[0], first[1]
	first = first[:1]

	second, _, err := cache.get(context.Background(), key, fetcher.fetch)
	require.NoError(t, err)
	require.Len(t, second, 3, "cached slice must not be mutated by callers")

	names := make([]string, 0, len(second))
	for _, n := range second {
		names = append(names, n.Node.Node)
	}
	require.ElementsMatch(t, []string{"a", "b", "c"}, names)
}
