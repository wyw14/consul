// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"context"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/sdk/testutil/retry"
	"github.com/hashicorp/consul/testrpc"
)

// TestDNS_ResponseCache_ConfigReload verifies the response cache settings are
// parsed and hot-reloaded into both the DNS server config and the live cache.
func TestDNS_ResponseCache_ConfigReload(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	a := NewTestAgent(t, `
		dns_config {
			enable_response_cache = true
			response_cache_ttl = "11s"
			response_cache_max_stale = "22s"
			soa = {
				min_ttl = 7
			}
		}
	`)
	defer a.Shutdown()
	testrpc.WaitForLeader(t, a.RPC, "dc1")

	assertConfig := func(enabled bool, ttl, maxStale, negativeTTL time.Duration) {
		t.Helper()
		for _, s := range a.dnsServers {
			server, ok := s.(*DNSServer)
			require.True(t, ok)

			cfg := server.config.Load().(*dnsServerConfig)
			require.Equal(t, enabled, cfg.ResponseCacheEnabled)
			require.Equal(t, ttl, cfg.ResponseCacheTTL)
			require.Equal(t, maxStale, cfg.ResponseCacheMaxStale)
			require.Equal(t, negativeTTL, cfg.ResponseCacheNegativeTTL)

			live := server.responseCache.getConfig()
			require.Equal(t, enabled, live.Enabled)
			require.Equal(t, ttl, live.TTL)
			require.Equal(t, maxStale, live.MaxStale)
			require.Equal(t, negativeTTL, live.NegativeTTL)
		}
	}

	assertConfig(true, 11*time.Second, 22*time.Second, 7*time.Second)

	newCfg := *a.Config
	newCfg.DNSResponseCacheEnabled = false
	newCfg.DNSResponseCacheTTL = 33 * time.Second
	newCfg.DNSResponseCacheMaxStale = 44 * time.Second
	require.NoError(t, a.reloadConfigInternal(&newCfg))

	assertConfig(false, 33*time.Second, 44*time.Second, 7*time.Second)
}

// TestDNS_ResponseCache_DeregisterDropsStaleAddresses verifies that after a
// service deregistration the DNS server stops returning the cached address
// and answers with NXDOMAIN plus an SOA record in the authority section.
func TestDNS_ResponseCache_DeregisterDropsStaleAddresses(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	a := NewTestAgent(t, `
		dns_config {
			enable_response_cache = true
			response_cache_ttl = "200ms"
		}
	`)
	defer a.Shutdown()
	testrpc.WaitForLeader(t, a.RPC, "dc1")

	var out struct{}
	require.NoError(t, a.RPC(context.Background(), "Catalog.Register", &structs.RegisterRequest{
		Datacenter: "dc1",
		Node:       "foo",
		Address:    "198.18.0.55",
		Service: &structs.NodeService{
			ID:      "db",
			Service: "db",
			Address: "198.18.0.55",
			Port:    5432,
		},
	}, &out))

	query := func(t require.TestingT) *dns.Msg {
		m := new(dns.Msg)
		m.SetQuestion("db.service.consul.", dns.TypeA)
		in, _, err := new(dns.Client).Exchange(m, a.DNSAddr())
		require.NoError(t, err)
		return in
	}

	// First the cached response carries the registered address.
	retry.Run(t, func(r *retry.R) {
		in := query(r)
		require.Equal(r, dns.RcodeSuccess, in.Rcode)
		require.Len(r, in.Answer, 1)
		require.Equal(r, "198.18.0.55", in.Answer[0].(*dns.A).A.String())
	})

	// Deregister; the TTL may serve one stale answer but after the background
	// refresh every response must be NXDOMAIN with an SOA authority record.
	require.NoError(t, a.RPC(context.Background(), "Catalog.Deregister", &structs.DeregisterRequest{
		Datacenter: "dc1",
		Node:       "foo",
		ServiceID:  "db",
	}, &out))

	retry.Run(t, func(r *retry.R) {
		in := query(r)
		require.Equal(r, dns.RcodeNameError, in.Rcode)
		require.Empty(r, in.Answer)
		require.Len(r, in.Ns, 1)
		require.IsType(r, &dns.SOA{}, in.Ns[0])
	})

	// Negative caching and the empty catalog both guarantee the expired
	// address never comes back.
	for i := 0; i < 5; i++ {
		in := query(t)
		require.Equal(t, dns.RcodeNameError, in.Rcode)
		require.Empty(t, in.Answer)
		require.IsType(t, &dns.SOA{}, in.Ns[0])
	}
}

// TestDNS_ResponseCache_EmptyAnswersHaveSOA verifies the two empty response
// shapes carry explicit SOA semantics with the cache enabled: NXDOMAIN for
// unknown names and NOERROR/NODATA for an existing service with no record of
// the requested type.
func TestDNS_ResponseCache_EmptyAnswersHaveSOA(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	a := NewTestAgent(t, `
		dns_config {
			enable_response_cache = true
			response_cache_ttl = "30s"
			soa = {
				min_ttl = 42
			}
		}
	`)
	defer a.Shutdown()
	testrpc.WaitForLeader(t, a.RPC, "dc1")

	var out struct{}
	require.NoError(t, a.RPC(context.Background(), "Catalog.Register", &structs.RegisterRequest{
		Datacenter: "dc1",
		Node:       "foo",
		Address:    "198.18.0.66",
		Service: &structs.NodeService{
			ID:      "db",
			Service: "db",
			Address: "198.18.0.66",
			Port:    5432,
		},
	}, &out))

	ask := func(name string, qtype uint16) *dns.Msg {
		m := new(dns.Msg)
		m.SetQuestion(name, qtype)
		in, _, err := new(dns.Client).Exchange(m, a.DNSAddr())
		require.NoError(t, err)
		return in
	}

	// A never-registered service is NXDOMAIN with SOA, and stays so across
	// cached repeats.
	for i := 0; i < 2; i++ {
		in := ask("ghost.service.consul.", dns.TypeA)
		require.Equal(t, dns.RcodeNameError, in.Rcode)
		require.Empty(t, in.Answer)
		require.Len(t, in.Ns, 1)
		soa, ok := in.Ns[0].(*dns.SOA)
		require.True(t, ok)
		require.Equal(t, uint32(42), soa.Minttl, "negative TTL must come from the SOA minimum field")
	}

	// Wait for the IPv4 service to be available, then an AAAA question is a
	// NODATA (NOERROR, no answers) with an SOA authority record.
	retry.Run(t, func(r *retry.R) {
		in := ask("db.service.consul.", dns.TypeA)
		require.Equal(r, dns.RcodeSuccess, in.Rcode)
		require.Len(r, in.Answer, 1)
	})

	for i := 0; i < 2; i++ {
		in := ask("db.service.consul.", dns.TypeAAAA)
		require.Equal(t, dns.RcodeSuccess, in.Rcode)
		require.Empty(t, in.Answer)
		require.Len(t, in.Ns, 1)
		require.IsType(t, &dns.SOA{}, in.Ns[0])
	}
}
