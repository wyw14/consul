// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package consul

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	msgpackrpc "github.com/hashicorp/consul-net-rpc/net-rpc-msgpackrpc"
	"github.com/hashicorp/consul-net-rpc/net/rpc"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/sdk/testutil/retry"
	"github.com/hashicorp/consul/testrpc"
)

func idempotentRegisterArgs(key string, mutate func(*structs.RegisterRequest)) *structs.RegisterRequest {
	args := &structs.RegisterRequest{
		Datacenter:     "dc1",
		Node:           "idempotent-node",
		Address:        "127.0.0.1",
		IdempotencyKey: key,
		Service: &structs.NodeService{
			ID:      "db",
			Service: "db",
			Tags:    []string{"primary"},
			Address: "10.0.0.1",
			Port:    5432,
			Meta:    map[string]string{"version": "1"},
		},
	}
	if mutate != nil {
		mutate(args)
	}
	return args
}

func registerViaRPC(t *testing.T, codec rpc.ClientCodec, args *structs.RegisterRequest) *structs.RegisterReply {
	t.Helper()
	var out structs.RegisterReply
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Catalog.Register", args, &out))
	return &out
}

func TestCatalog_Register_Idempotent_Replay(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	t.Parallel()

	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	first := registerViaRPC(t, codec, idempotentRegisterArgs("key-replay", nil))
	require.False(t, first.Replayed)
	require.Equal(t, uint64(1), first.NodeVersion)
	require.Equal(t, uint64(1), first.ServiceVersion)
	require.Equal(t, structs.ChangeCreated, first.Summary.Node)
	require.Equal(t, structs.ChangeCreated, first.Summary.Service)

	// The duplicate request returns the exact same result.
	second := registerViaRPC(t, codec, idempotentRegisterArgs("key-replay", nil))
	require.True(t, second.Replayed)
	require.Equal(t, first.NodeVersion, second.NodeVersion)
	require.Equal(t, first.ServiceVersion, second.ServiceVersion)
	require.Equal(t, first.ModifyIndex, second.ModifyIndex)
	require.Equal(t, first.Summary, second.Summary)

	// A subsequent, different change with a fresh key bumps the service
	// version instead of being mistaken for the previous request.
	update := idempotentRegisterArgs("key-update", func(args *structs.RegisterRequest) {
		args.Service.Port = 6543
	})
	updated := registerViaRPC(t, codec, update)
	require.False(t, updated.Replayed)
	require.Equal(t, uint64(2), updated.ServiceVersion)
	require.Equal(t, structs.ChangeUpdated, updated.Summary.Service)

	// Replaying the original key still returns the original (v1) result.
	replay := registerViaRPC(t, codec, idempotentRegisterArgs("key-replay", nil))
	require.True(t, replay.Replayed)
	require.Equal(t, uint64(1), replay.ServiceVersion)
}

func TestCatalog_Register_Idempotent_Conflict(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	t.Parallel()

	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	require.NotNil(t, registerViaRPC(t, codec, idempotentRegisterArgs("key-conflict", nil)))

	conflict := idempotentRegisterArgs("key-conflict", func(args *structs.RegisterRequest) {
		args.Service.Port = 9999
	})
	var out structs.RegisterReply
	err := msgpackrpc.CallWithCodec(codec, "Catalog.Register", conflict, &out)
	require.Error(t, err)
	require.Contains(t, err.Error(), structs.ErrIdempotencyConflict.Error())

	// The conflicting payload was not applied.
	getArgs := structs.NodeSpecificRequest{Datacenter: "dc1", Node: "idempotent-node"}
	var services structs.IndexedNodeServices
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Catalog.NodeServices", &getArgs, &services))
	require.Equal(t, 5432, services.NodeServices.Services["db"].Port)
}

// TestCatalog_Register_Idempotent_Retry models a client that cannot tell
// whether its first attempt committed (e.g. the reply was lost): retrying the
// very same request is safe and yields the same result.
func TestCatalog_Register_Idempotent_RetryAfterLostReply(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	t.Parallel()

	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	var first structs.RegisterReply
	require.NoError(t, msgpackrpc.CallWithCodec(
		codec, "Catalog.Register", idempotentRegisterArgs("retry-key", nil), &first))

	for i := 0; i < 3; i++ {
		retryReply := registerViaRPC(t, codec, idempotentRegisterArgs("retry-key", nil))
		require.True(t, retryReply.Replayed)
		require.Equal(t, first.IdempotencyKey, retryReply.IdempotencyKey)
		require.Equal(t, first.NodeVersion, retryReply.NodeVersion)
		require.Equal(t, first.ServiceVersion, retryReply.ServiceVersion)
		require.Equal(t, first.Summary, retryReply.Summary)
		require.Equal(t, first.RaftIndex, retryReply.RaftIndex)
	}
}

func TestCatalog_Register_Idempotent_ConcurrentRegistrations(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	t.Parallel()

	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	const n = 20
	var creates, replays, failures atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine uses its own codec and its own copy of the
			// request, as independent clients would.
			codec := rpcClient(t, s1)
			defer codec.Close()
			var out structs.RegisterReply
			err := msgpackrpc.CallWithCodec(codec, "Catalog.Register",
				idempotentRegisterArgs("concurrent-key", nil), &out)
			switch {
			case err != nil:
				failures.Add(1)
			case out.Replayed:
				replays.Add(1)
			default:
				creates.Add(1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), creates.Load(), "exactly one request applies")
	require.Equal(t, int64(n-1), replays.Load(), "all duplicates replay")
	require.Equal(t, int64(0), failures.Load())
}

func TestCatalog_Register_MergeService(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	t.Parallel()

	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	// Register a fully populated service (without an idempotency key).
	full := idempotentRegisterArgs("", nil)
	full.IdempotencyKey = ""
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Catalog.Register", full, &structs.RegisterReply{}))

	// Patch only the port.
	patch := &structs.RegisterRequest{
		Datacenter:   "dc1",
		Node:         "idempotent-node",
		Address:      "127.0.0.1",
		MergeService: true,
		Service:      &structs.NodeService{ID: "db", Service: "db", Port: 6543},
	}
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Catalog.Register", patch, &structs.RegisterReply{}))

	getArgs := structs.NodeSpecificRequest{Datacenter: "dc1", Node: "idempotent-node"}
	var services structs.IndexedNodeServices
	require.NoError(t, msgpackrpc.CallWithCodec(codec, "Catalog.NodeServices", &getArgs, &services))
	svc := services.NodeServices.Services["db"]
	require.Equal(t, 6543, svc.Port, "patched field updated")
	require.Equal(t, "10.0.0.1", svc.Address, "omitted address preserved")
	require.Equal(t, []string{"primary"}, svc.Tags, "omitted tags preserved")
	require.Equal(t, "1", svc.Meta["version"], "omitted meta preserved")
	require.Equal(t, uint64(2), svc.Version, "partial update bumps the version")
}

// TestCatalog_Register_Idempotent_LeaderSwitchRecovery verifies that a
// keyed registration retried after a leader change returns the committed
// result from the new leader instead of applying twice.
func TestCatalog_Register_Idempotent_LeaderSwitchRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	t.Parallel()

	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc1"
		c.Bootstrap = true
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc1"
		c.Bootstrap = false
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc1"
		c.Bootstrap = false
	})
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	joinLAN(t, s2, s1)
	joinLAN(t, s3, s1)
	testrpc.WaitForLeader(t, s1.RPC, "dc1")
	testrpc.WaitForLeader(t, s2.RPC, "dc1")
	testrpc.WaitForLeader(t, s3.RPC, "dc1")
	require.True(t, s1.IsLeader())

	// Commit the keyed registration through the bootstrap leader.
	codec1 := rpcClient(t, s1)
	defer codec1.Close()
	first := registerViaRPC(t, codec1, idempotentRegisterArgs("failover-key", nil))
	require.False(t, first.Replayed)
	require.Equal(t, uint64(1), first.ServiceVersion)

	// Take the leader down, as during a node switch.
	s1.Shutdown()

	var newLeader *Server
	retry.Run(t, func(r *retry.R) {
		for _, candidate := range []*Server{s2, s3} {
			if candidate.IsLeader() {
				newLeader = candidate
				return
			}
		}
		r.Fatalf("no new leader elected")
	})

	// Retrying the same request against the new leader recovers the
	// unfinished logical operation: the committed record is replicated and
	// the new leader returns the identical result instead of double-applying.
	codec2 := rpcClient(t, newLeader)
	defer codec2.Close()

	var replayed structs.RegisterReply
	require.NoError(t, msgpackrpc.CallWithCodec(codec2, "Catalog.Register",
		idempotentRegisterArgs("failover-key", nil), &replayed))
	require.True(t, replayed.Replayed)
	require.Equal(t, first.ServiceVersion, replayed.ServiceVersion)
	require.Equal(t, first.NodeVersion, replayed.NodeVersion)
	require.Equal(t, first.ModifyIndex, replayed.ModifyIndex)
	require.Equal(t, first.Summary, replayed.Summary)

	// The service still exists once with its original version.
	getArgs := structs.NodeSpecificRequest{Datacenter: "dc1", Node: "idempotent-node"}
	var services structs.IndexedNodeServices
	require.NoError(t, msgpackrpc.CallWithCodec(codec2, "Catalog.NodeServices", &getArgs, &services))
	require.Len(t, services.NodeServices.Services, 1)
	require.Equal(t, uint64(1), services.NodeServices.Services["db"].Version)
}
