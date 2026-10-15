// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-metrics"
	"golang.org/x/sync/singleflight"

	"github.com/hashicorp/consul/agent/structs"
)

const (
	// defaultDNSResponseCacheTTL is used for positive responses when
	// response_cache_ttl is not configured.
	defaultDNSResponseCacheTTL = 30 * time.Second

	// defaultDNSNegativeCacheTTL bounds how long an empty (NODATA/NXDOMAIN)
	// response is cached when the SOA minimum TTL does not provide one.
	defaultDNSNegativeCacheTTL = 5 * time.Second
)

// dnsResponseCacheConfig is the hot-reloadable configuration for the DNS
// service response cache.
type dnsResponseCacheConfig struct {
	// Enabled controls whether service lookups are served from the cache.
	Enabled bool

	// TTL is the time for which a positive cached response is served
	// without triggering a background refresh.
	TTL time.Duration

	// NegativeTTL bounds how long an empty response is cached. It is derived
	// from the SOA minimum TTL when available.
	NegativeTTL time.Duration

	// MaxStale optionally bounds how long an expired entry may be served
	// while a background refresh is in flight. A zero value means stale
	// responses are served until the refresh completes (stale-while-revalidate).
	MaxStale time.Duration
}

// effective fills in the default TTL values.
func (c dnsResponseCacheConfig) effective() dnsResponseCacheConfig {
	if c.TTL <= 0 {
		c.TTL = defaultDNSResponseCacheTTL
	}
	if c.NegativeTTL <= 0 {
		c.NegativeTTL = defaultDNSNegativeCacheTTL
	}
	return c
}

// dnsServiceCacheKey identifies the inputs to a service lookup that can
// change the response. The datacenter, tag and passing filter are the key
// dimensions operators reason about, but the key also covers the service
// kind (regular/connect/ingress/api-gateway), peering and the ACL token
// identity so cached answers can never be shared across identities.
type dnsServiceCacheKey struct {
	datacenter    string
	tag           string
	onlyPassing   bool
	service       string
	peerName      string
	samenessGroup string
	connect       bool
	ingress       bool
	apiGateway    bool
	token         string
	tenant        string
}

// String renders the key as a stable, unambiguous representation. Every
// field is length-prefixed so that values containing the separators can
// never collide.
func (k dnsServiceCacheKey) String() string {
	var b strings.Builder
	writeKeyField := func(value string) {
		b.WriteString(strconv.Itoa(len(value)))
		b.WriteByte(':')
		b.WriteString(value)
	}
	writeKeyField(k.datacenter)
	writeKeyField(k.tag)
	writeKeyField(k.service)
	writeKeyField(k.peerName)
	writeKeyField(k.samenessGroup)
	writeKeyField(k.token)
	writeKeyField(k.tenant)
	bools := [...]bool{k.onlyPassing, k.connect, k.ingress, k.apiGateway}
	for _, v := range bools {
		if v {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	}
	return b.String()
}

// hash returns a stable fixed-length identifier used as the map key. It
// keeps ACL tokens out of the map keys and log output.
func (k dnsServiceCacheKey) hash() string {
	sum := sha256.Sum256([]byte(k.String()))
	return hex.EncodeToString(sum[:])
}

// dnsServiceNodesFetchFunc fetches the current service nodes from the
// catalog (or agent cache).
type dnsServiceNodesFetchFunc func(context.Context) (structs.CheckServiceNodes, error)

// dnsServiceCacheEntry is a single cached lookup result.
type dnsServiceCacheEntry struct {
	mu sync.Mutex

	// ready is true once the first successful fetch has populated the entry.
	ready bool

	// nodes is a defensive copy owned by the entry; callers always receive
	// their own copy so shuffling answers cannot corrupt the cache.
	nodes structs.CheckServiceNodes
	found bool

	// expireAt is the time after which the entry must be refreshed. Callers
	// may keep being served the stale value while a refresh is in flight.
	expireAt time.Time

	// staleUntil caps how long an expired entry may be served. The zero
	// value means there is no cap beyond the refresh itself.
	staleUntil time.Time

	// refreshing is true while a background refresh is running so that at
	// most one refresh is scheduled.
	refreshing bool
}

// dnsServiceResponseCache caches DNS service lookup results with TTL based
// freshness and stale-while-revalidate semantics.
//
// The first request for a key blocks until the result is available and
// concurrent requests are coalesced (single-flight). Fresh entries are
// served directly. Expired entries are served stale while a single
// background refresh is in flight. Empty results (a deregistered service)
// replace the previous positive entry immediately once observed, so stale
// addresses are never returned after a refresh completes.
type dnsServiceResponseCache struct {
	config atomic.Value // dnsResponseCacheConfig, always effective

	// now is indirected for tests.
	now func() time.Time

	logger hclog.Logger

	sf singleflight.Group

	mu      sync.Mutex
	entries map[string]*dnsServiceCacheEntry
}

func newDNSServiceResponseCache(cfg dnsResponseCacheConfig, now func() time.Time, logger hclog.Logger) *dnsServiceResponseCache {
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = hclog.NewNullLogger()
	}
	c := &dnsServiceResponseCache{
		now:     now,
		logger:  logger,
		entries: make(map[string]*dnsServiceCacheEntry),
	}
	c.config.Store(cfg.effective())
	return c
}

// configure hot-reloads the cache settings. Existing entries keep their
// current expiry and pick up the new settings on the next refresh.
func (c *dnsServiceResponseCache) configure(cfg dnsResponseCacheConfig) {
	c.config.Store(cfg.effective())
}

func (c *dnsServiceResponseCache) getConfig() dnsResponseCacheConfig {
	return c.config.Load().(dnsResponseCacheConfig)
}

// get returns the cached nodes for the key. The found result is false for
// empty responses (NODATA/NXDOMAIN). When the cache is disabled the fetch
// function is invoked directly.
func (c *dnsServiceResponseCache) get(
	ctx context.Context,
	key dnsServiceCacheKey,
	fetch dnsServiceNodesFetchFunc,
) (structs.CheckServiceNodes, bool, error) {
	cfg := c.getConfig()
	if !cfg.Enabled {
		nodes, err := fetch(ctx)
		return nodes, len(nodes) > 0, err
	}

	keyHash := key.hash()

	c.mu.Lock()
	entry := c.entries[keyHash]
	if entry == nil {
		entry = &dnsServiceCacheEntry{}
		c.entries[keyHash] = entry
	}
	c.mu.Unlock()

	entry.mu.Lock()
	now := c.now()
	switch {
	case !entry.ready:
		entry.mu.Unlock()
		return c.fetchCold(ctx, keyHash, entry, cfg, fetch)
	case now.Before(entry.expireAt):
		// Fresh: serve straight from the cache.
		nodes := cloneServiceNodes(entry.nodes)
		found := entry.found
		entry.mu.Unlock()
		metrics.IncrCounter([]string{"dns", "cache", "hit"}, 1)
		return nodes, found, nil
	case cfg.MaxStale > 0 && now.After(entry.staleUntil):
		// The entry is older than the configured stale bound. Block until a
		// fresh result is available, coalescing with any in-flight refresh.
		entry.mu.Unlock()
		metrics.IncrCounter([]string{"dns", "cache", "miss"}, 1)
		return c.fetchCold(ctx, keyHash, entry, cfg, fetch)
	default:
		// Expired but still servable: return the stale value and refresh in
		// the background so the next request sees fresh data.
		nodes := cloneServiceNodes(entry.nodes)
		found := entry.found
		c.scheduleRefresh(keyHash, entry, cfg, fetch)
		entry.mu.Unlock()
		metrics.IncrCounter([]string{"dns", "cache", "stale"}, 1)
		return nodes, found, nil
	}
}

// fetchCold performs a coalesced blocking fetch. All concurrent callers for
// the same key wait on the singleflight result.
func (c *dnsServiceResponseCache) fetchCold(
	ctx context.Context,
	keyHash string,
	entry *dnsServiceCacheEntry,
	cfg dnsResponseCacheConfig,
	fetch dnsServiceNodesFetchFunc,
) (structs.CheckServiceNodes, bool, error) {
	res := c.sf.DoChan(keyHash, func() (interface{}, error) {
		nodes, err := fetch(ctx)
		if err == nil {
			entry.mu.Lock()
			c.applyResultLocked(entry, nodes, cfg)
			entry.mu.Unlock()
		}
		return nodes, err
	})

	select {
	case r := <-res:
		if r.Err != nil {
			// Do not pin failures: drop the entry only while it is still
			// unready so a later request retries. A ready entry keeps its
			// last known good (stale) value.
			entry.mu.Lock()
			unready := !entry.ready
			entry.mu.Unlock()
			if unready {
				c.mu.Lock()
				if cur := c.entries[keyHash]; cur == entry {
					delete(c.entries, keyHash)
				}
				c.mu.Unlock()
			}
			metrics.IncrCounter([]string{"dns", "cache", "fetch_error"}, 1)
			return nil, false, r.Err
		}
		nodes, _ := r.Val.(structs.CheckServiceNodes)
		if len(nodes) == 0 {
			metrics.IncrCounter([]string{"dns", "cache", "negative"}, 1)
		}
		return cloneServiceNodes(nodes), len(nodes) > 0, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

// scheduleRefresh starts a single background refresh for the entry. It must
// be called with entry.mu held; it marks the entry refreshing so concurrent
// stale requests do not start duplicate fetches.
func (c *dnsServiceResponseCache) scheduleRefresh(
	keyHash string,
	entry *dnsServiceCacheEntry,
	cfg dnsResponseCacheConfig,
	fetch dnsServiceNodesFetchFunc,
) {
	if entry.refreshing {
		return
	}
	entry.refreshing = true
	metrics.IncrCounter([]string{"dns", "cache", "refresh"}, 1)

	// The singleflight worker performs the update itself; the returned
	// channel is buffered and always receives a result, so it does not need
	// a reader.
	c.sf.DoChan(keyHash, func() (interface{}, error) {
		nodes, err := fetch(context.Background())

		entry.mu.Lock()
		defer entry.mu.Unlock()
		entry.refreshing = false

		if err != nil {
			// Keep serving the last known good value but expire it
			// immediately so the next request retries the refresh. The error
			// is surfaced to any caller coalesced onto this refresh (e.g. a
			// request past the max-stale bound) while plain stale requests
			// ignore the channel result and keep their stale answer.
			c.logger.Warn("dns response cache background refresh failed, serving stale value", "error", err)
			entry.expireAt = c.now()
			entry.staleUntil = time.Time{}
			metrics.IncrCounter([]string{"dns", "cache", "fetch_error"}, 1)
			return nil, err
		}

		// A successful refresh always replaces the value, including with an
		// empty result: once the catalog reports the service is gone, the
		// previously cached addresses must not be served anymore.
		c.applyResultLocked(entry, nodes, cfg)
		if len(nodes) == 0 {
			metrics.IncrCounter([]string{"dns", "cache", "negative"}, 1)
		}
		return entry.nodes, nil
	})
}

// applyResultLocked stores a freshly fetched result and computes its
// expiry. It must be called with entry.mu held.
func (c *dnsServiceResponseCache) applyResultLocked(
	entry *dnsServiceCacheEntry,
	nodes structs.CheckServiceNodes,
	cfg dnsResponseCacheConfig,
) {
	ttl := cfg.TTL
	if len(nodes) == 0 {
		ttl = cfg.NegativeTTL
	}

	now := c.now()
	entry.ready = true
	entry.nodes = cloneServiceNodes(nodes)
	entry.found = len(nodes) > 0
	entry.expireAt = now.Add(ttl)
	if cfg.MaxStale > 0 {
		entry.staleUntil = entry.expireAt.Add(cfg.MaxStale)
	} else {
		entry.staleUntil = time.Time{}
	}
}

// cloneServiceNodes returns a copy of the slice so callers (which shuffle
// and iterate the answers) never mutate the cached slice. The elements
// themselves are read-only after decoding.
func cloneServiceNodes(nodes structs.CheckServiceNodes) structs.CheckServiceNodes {
	if nodes == nil {
		return nil
	}
	out := make(structs.CheckServiceNodes, len(nodes))
	copy(out, nodes)
	return out
}
