// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/testrpc"
)

// makeDualRecursor starts a recursor mock listening on UDP and TCP on the
// same address with independent handlers, returning a shutdown function.
func makeDualRecursor(t *testing.T, udpHandler, tcpHandler dns.HandlerFunc) (string, func()) {
	t.Helper()

	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)

	addr := udpConn.LocalAddr().String()
	tcpLn, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	udpServer := &dns.Server{PacketConn: udpConn, Handler: udpHandler}
	tcpServer := &dns.Server{Listener: tcpLn, Handler: tcpHandler}
	go func() { _ = udpServer.ActivateAndServe() }()
	go func() { _ = tcpServer.ActivateAndServe() }()

	return addr, func() {
		_ = udpServer.Shutdown()
		_ = tcpServer.Shutdown()
	}
}

// staticRecursorHandler replies to every query with the given answers,
// optionally flagging the response as truncated.
func staticRecursorHandler(hits *int32, truncated bool, answers []dns.RR) dns.HandlerFunc {
	return func(resp dns.ResponseWriter, req *dns.Msg) {
		atomic.AddInt32(hits, 1)
		m := new(dns.Msg)
		m.SetReply(req)
		m.Truncated = truncated
		m.Answer = append(m.Answer, answers...)
		_ = resp.WriteMsg(m)
	}
}

func recursorExchangeTestReq() *dns.Msg {
	req := new(dns.Msg)
	req.SetQuestion("apple.com.", dns.TypeA)
	return req
}

// A truncated UDP response must transparently be retried over TCP and the
// full TCP answer must be returned to the caller.
func TestDNS_dnsRecursorExchange_TruncatedFallsBackToTCP(t *testing.T) {
	udpAnswers := []dns.RR{dnsA("apple.com", "1.2.3.4")}
	tcpAnswers := make([]dns.RR, 0, 20)
	for i := 0; i < 20; i++ {
		tcpAnswers = append(tcpAnswers, dnsA("apple.com", fmt.Sprintf("10.0.0.%d", i)))
	}

	var udpHits, tcpHits int32
	addr, shutdown := makeDualRecursor(t,
		staticRecursorHandler(&udpHits, true, udpAnswers),
		staticRecursorHandler(&tcpHits, false, tcpAnswers),
	)
	defer shutdown()

	r, _, err := dnsRecursorExchange("udp", 2*time.Second, addr, recursorExchangeTestReq())
	require.NoError(t, err)
	require.NotNil(t, r)
	require.False(t, r.Truncated, "TCP fallback must clear the truncation bit")
	require.Len(t, r.Answer, 20)
	require.Equal(t, int32(1), atomic.LoadInt32(&udpHits))
	require.Equal(t, int32(1), atomic.LoadInt32(&tcpHits))
}

// When TCP is not available the truncated UDP response is returned so the
// downstream client can still perform its own TCP retry.
func TestDNS_dnsRecursorExchange_TruncatedTCPUnavailable(t *testing.T) {
	recursor := makeRecursor(t, dns.Msg{
		MsgHdr: dns.MsgHdr{Truncated: true},
		Answer: []dns.RR{dnsA("apple.com", "1.2.3.4")},
	})
	defer recursor.Shutdown()

	r, _, err := dnsRecursorExchange("udp", time.Second, recursor.Addr, recursorExchangeTestReq())
	require.NoError(t, err)
	require.NotNil(t, r)
	require.True(t, r.Truncated)
	require.Len(t, r.Answer, 1)
}

// TCP clients must keep talking TCP without a UDP attempt.
func TestDNS_dnsRecursorExchange_TCPClientNoUDPLookup(t *testing.T) {
	tcpAnswers := []dns.RR{dnsA("apple.com", "10.1.1.1")}

	var udpHits, tcpHits int32
	addr, shutdown := makeDualRecursor(t,
		staticRecursorHandler(&udpHits, false, tcpAnswers),
		staticRecursorHandler(&tcpHits, false, tcpAnswers),
	)
	defer shutdown()

	r, _, err := dnsRecursorExchange("tcp", 2*time.Second, addr, recursorExchangeTestReq())
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Len(t, r.Answer, 1)
	require.Equal(t, int32(0), atomic.LoadInt32(&udpHits))
	require.Equal(t, int32(1), atomic.LoadInt32(&tcpHits))
}

// A non-truncated UDP response must not trigger a TCP lookup.
func TestDNS_dnsRecursorExchange_NonTruncatedSkipsTCP(t *testing.T) {
	udpAnswers := []dns.RR{dnsA("apple.com", "1.2.3.4")}

	var udpHits, tcpHits int32
	addr, shutdown := makeDualRecursor(t,
		staticRecursorHandler(&udpHits, false, udpAnswers),
		staticRecursorHandler(&tcpHits, false, udpAnswers),
	)
	defer shutdown()

	r, _, err := dnsRecursorExchange("udp", 2*time.Second, addr, recursorExchangeTestReq())
	require.NoError(t, err)
	require.NotNil(t, r)
	require.False(t, r.Truncated)
	require.Len(t, r.Answer, 1)
	require.Equal(t, int32(1), atomic.LoadInt32(&udpHits))
	require.Equal(t, int32(0), atomic.LoadInt32(&tcpHits))
}

// Network errors must still be surfaced to the caller.
func TestDNS_dnsRecursorExchange_NetworkError(t *testing.T) {
	// Bind and immediately close a socket to obtain an address that refuses
	// connections.
	ln, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.LocalAddr().String()
	require.NoError(t, ln.Close())

	r, _, err := dnsRecursorExchange("udp", 200*time.Millisecond, addr, recursorExchangeTestReq())
	require.Error(t, err)
	require.Nil(t, r)
}

// End to end behavior: a UDP client querying Consul with a recursor that
// truncates UDP responses must receive the full answer set because Consul
// internally switches to TCP.
func TestDNS_Recurse_TruncationFallsBackToTCP(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	udpAnswers := []dns.RR{dnsA("apple.com", "1.2.3.4")}
	tcpAnswers := make([]dns.RR, 0, 10)
	for i := 0; i < 10; i++ {
		tcpAnswers = append(tcpAnswers, dnsA("apple.com", fmt.Sprintf("10.0.0.%d", i)))
	}

	var udpHits, tcpHits int32
	addr, shutdown := makeDualRecursor(t,
		staticRecursorHandler(&udpHits, true, udpAnswers),
		staticRecursorHandler(&tcpHits, false, tcpAnswers),
	)
	defer shutdown()

	a := NewTestAgent(t, `
		recursors = ["`+addr+`"]
	`)
	defer a.Shutdown()
	testrpc.WaitForLeader(t, a.RPC, "dc1")

	in, _, err := new(dns.Client).Exchange(recursorExchangeTestReq(), a.DNSAddr())
	require.NoError(t, err)
	require.False(t, in.Truncated)
	require.Len(t, in.Answer, 10)
	require.Equal(t, int32(1), atomic.LoadInt32(&udpHits))
	require.Equal(t, int32(1), atomic.LoadInt32(&tcpHits))
}
