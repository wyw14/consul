// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package state

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/structs"
)

func testKeyedRegisterRequest(key string, mutate func(*structs.RegisterRequest)) *structs.RegisterRequest {
	req := &structs.RegisterRequest{
		Datacenter:     "dc1",
		Node:           "node1",
		Address:        "127.0.0.1",
		IdempotencyKey: key,
		EnterpriseMeta: *structs.NodeEnterpriseMetaInDefaultPartition(),
		Service: &structs.NodeService{
			Service: "db",
			ID:      "db",
			Tags:    []string{"primary"},
			Address: "10.0.0.1",
			Port:    5432,
			Meta:    map[string]string{"version": "1"},
		},
	}
	if mutate != nil {
		mutate(req)
	}
	return req
}

func TestStateStore_EnsureRegistration_Idempotent_Replay(t *testing.T) {
	s := testStateStore(t)

	// First application commits the registration and its record.
	reply, err := s.EnsureRegistrationWithResult(1, testKeyedRegisterRequest("key-1", nil))
	require.NoError(t, err)
	require.NotNil(t, reply)
	require.False(t, reply.Replayed)
	require.Equal(t, "key-1", reply.IdempotencyKey)
	require.Equal(t, uint64(1), reply.NodeVersion)
	require.Equal(t, uint64(1), reply.ServiceVersion)
	require.Equal(t, structs.ChangeCreated, reply.Summary.Node)
	require.Equal(t, structs.ChangeCreated, reply.Summary.Service)
	require.Equal(t, uint64(1), reply.ModifyIndex)

	// The record is persisted and retrievable.
	rec, err := s.GetRegistrationRecord(structs.NodeEnterpriseMetaInDefaultPartition(), "", "key-1")
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Equal(t, uint64(1), rec.NodeVersion)

	// An identical request applied at a later raft index must replay the
	// stored result instead of touching state again.
	replay, err := s.EnsureRegistrationWithResult(5, testKeyedRegisterRequest("key-1", nil))
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	require.Equal(t, reply.NodeVersion, replay.NodeVersion)
	require.Equal(t, reply.ServiceVersion, replay.ServiceVersion)
	require.Equal(t, reply.Summary, replay.Summary)
	// The replay reports the original commit indexes, not index 5.
	require.Equal(t, uint64(1), replay.ModifyIndex)

	// The service was not rewritten: its modify index stays at 1.
	_, services, err := s.NodeServices(nil, "node1", structs.NodeEnterpriseMetaInDefaultPartition(), "")
	require.NoError(t, err)
	require.Equal(t, uint64(1), services.Services["db"].ModifyIndex)
}

func TestStateStore_EnsureRegistration_Idempotent_Conflict(t *testing.T) {
	s := testStateStore(t)

	_, err := s.EnsureRegistrationWithResult(1, testKeyedRegisterRequest("key-1", nil))
	require.NoError(t, err)

	// Reusing the key with a different payload conflicts and changes nothing.
	conflict := testKeyedRegisterRequest("key-1", func(req *structs.RegisterRequest) {
		req.Service.Port = 9999
	})
	reply, err := s.EnsureRegistrationWithResult(2, conflict)
	require.Error(t, err)
	require.True(t, errors.Is(err, structs.ErrIdempotencyConflict), "got %v", err)
	require.Nil(t, reply)

	// The conflicting port was not applied.
	_, services, err := s.NodeServices(nil, "node1", structs.NodeEnterpriseMetaInDefaultPartition(), "")
	require.NoError(t, err)
	require.Equal(t, 5432, services.Services["db"].Port)
}

// Registration writes are serialized through raft in production; this test
// models that serialization while many clients race their retries. Exactly one
// request creates the registration, every other identical one replays.
func TestStateStore_EnsureRegistration_Idempotent_ConcurrentDuplicates(t *testing.T) {
	s := testStateStore(t)

	const n = 24
	var index atomic.Uint64
	index.Store(1)

	var applyMu sync.Mutex
	var wg sync.WaitGroup
	var creates, replays, failures atomic.Int64

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// The mutex stands in for raft's strictly ordered FSM applies.
			applyMu.Lock()
			defer applyMu.Unlock()
			reply, err := s.EnsureRegistrationWithResult(index.Add(1), testKeyedRegisterRequest("race-key", nil))
			switch {
			case err != nil:
				failures.Add(1)
			case reply.Replayed:
				replays.Add(1)
			default:
				creates.Add(1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), creates.Load(), "exactly one registration creates")
	require.Equal(t, int64(n-1), replays.Load(), "all other attempts replay")
	require.Equal(t, int64(0), failures.Load())
}

// When concurrent contenders for one key split into two different payloads,
// only one payload can win: the other payload conflicts for every attempt.
func TestStateStore_EnsureRegistration_Idempotent_ConcurrentConflictingPayloads(t *testing.T) {
	s := testStateStore(t)

	var index atomic.Uint64
	index.Store(1)
	var applyMu sync.Mutex
	var wg sync.WaitGroup
	var success, replay, conflict atomic.Int64

	apply := func(port int) {
		defer wg.Done()
		applyMu.Lock()
		defer applyMu.Unlock()
		req := testKeyedRegisterRequest("contested", func(r *structs.RegisterRequest) {
			r.Service.Port = port
		})
		reply, err := s.EnsureRegistrationWithResult(index.Add(1), req)
		switch {
		case errors.Is(err, structs.ErrIdempotencyConflict):
			conflict.Add(1)
		case err != nil:
			t.Errorf("unexpected err: %v", err)
		case reply.Replayed:
			replay.Add(1)
		default:
			success.Add(1)
		}
	}

	for i := 0; i < 8; i++ {
		wg.Add(2)
		go apply(5432)
		go apply(9999)
	}
	wg.Wait()

	require.Equal(t, int64(1), success.Load(), "exactly one payload wins the key")
	require.Equal(t, int64(15), replay.Load()+conflict.Load(), "the losing payloads never apply")

	// The single winning port is the one stored.
	_, services, err := s.NodeServices(nil, "node1", structs.NodeEnterpriseMetaInDefaultPartition(), "")
	require.NoError(t, err)
	port := services.Services["db"].Port
	require.True(t, port == 5432 || port == 9999)
	winningReplays := replay.Load()
	if port == 5432 {
		require.Equal(t, int64(7), winningReplays, "same-payload losers replay")
	}
}

func TestStateStore_EnsureRegistration_MergeService_PartialUpdate(t *testing.T) {
	s := testStateStore(t)

	require.NoError(t, s.EnsureRegistration(1, testKeyedRegisterRequest("", nil)))

	// A merge that changes only the port must preserve every other field.
	merge := testKeyedRegisterRequest("", func(req *structs.RegisterRequest) {
		req.Service = &structs.NodeService{
			ID:      "db",
			Service: "db",
			Port:    6543,
		}
		req.MergeService = true
	})
	require.NoError(t, s.EnsureRegistration(2, merge))

	_, services, err := s.NodeServices(nil, "node1", structs.NodeEnterpriseMetaInDefaultPartition(), "")
	require.NoError(t, err)
	svc := services.Services["db"]
	require.Equal(t, 6543, svc.Port, "provided field overrides")
	require.Equal(t, "10.0.0.1", svc.Address, "omitted address preserved")
	require.Equal(t, []string{"primary"}, svc.Tags, "omitted tags preserved")
	require.Equal(t, "1", svc.Meta["version"], "omitted meta preserved")
	require.Equal(t, uint64(2), svc.Version, "the partial change bumps the version")

	// Merge adds map keys without removing existing ones.
	mergeMeta := testKeyedRegisterRequest("", func(req *structs.RegisterRequest) {
		req.Service = &structs.NodeService{
			ID:      "db",
			Service: "db",
			Meta:    map[string]string{"region": "us-east"},
		}
		req.MergeService = true
	})
	require.NoError(t, s.EnsureRegistration(3, mergeMeta))
	_, services, err = s.NodeServices(nil, "node1", structs.NodeEnterpriseMetaInDefaultPartition(), "")
	require.NoError(t, err)
	svc = services.Services["db"]
	require.Equal(t, "1", svc.Meta["version"])
	require.Equal(t, "us-east", svc.Meta["region"])
	require.Equal(t, uint64(3), svc.Version)

	// Without MergeService a sparse payload is a full replace and clears
	// omitted fields.
	replace := testKeyedRegisterRequest("", func(req *structs.RegisterRequest) {
		req.Service = &structs.NodeService{ID: "db", Service: "db", Port: 7000}
	})
	require.NoError(t, s.EnsureRegistration(4, replace))
	_, services, err = s.NodeServices(nil, "node1", structs.NodeEnterpriseMetaInDefaultPartition(), "")
	require.NoError(t, err)
	svc = services.Services["db"]
	require.Equal(t, 7000, svc.Port)
	require.Empty(t, svc.Tags)
	require.Empty(t, svc.Meta)
}

func TestStateStore_EnsureRegistration_VersionsAreMonotonicAndSortable(t *testing.T) {
	s := testStateStore(t)

	// Create: node and service both at version 1.
	require.NoError(t, s.EnsureRegistration(1, testKeyedRegisterRequest("", nil)))
	_, node, err := s.GetNode("node1", nil, "")
	require.NoError(t, err)
	require.Equal(t, uint64(1), node.Version)

	// An identical registration is a no-op: no version bump, no index churn.
	require.NoError(t, s.EnsureRegistration(2, testKeyedRegisterRequest("", nil)))
	_, node, err = s.GetNode("node1", nil, "")
	require.NoError(t, err)
	require.Equal(t, uint64(1), node.Version)
	require.Equal(t, uint64(1), node.ModifyIndex)

	// Changing the node bumps only the node version.
	require.NoError(t, s.EnsureRegistration(3, testKeyedRegisterRequest("", func(req *structs.RegisterRequest) {
		req.Address = "127.0.0.2"
	})))
	_, node, err = s.GetNode("node1", nil, "")
	require.NoError(t, err)
	require.Equal(t, uint64(2), node.Version)

	_, services, err := s.NodeServices(nil, "node1", structs.NodeEnterpriseMetaInDefaultPartition(), "")
	require.NoError(t, err)
	require.Equal(t, uint64(1), services.Services["db"].Version, "service version untouched by node change")

	// Changing the service bumps only the service version.
	require.NoError(t, s.EnsureRegistration(4, testKeyedRegisterRequest("", func(req *structs.RegisterRequest) {
		req.Service.Port = 5555
	})))
	_, services, err = s.NodeServices(nil, "node1", structs.NodeEnterpriseMetaInDefaultPartition(), "")
	require.NoError(t, err)
	require.Equal(t, uint64(2), services.Services["db"].Version)

	// Versions are strictly ordered and therefore sortable: later versions
	// always compare greater than earlier ones.
	assert.Less(t, uint64(1), node.Version)
}

func TestStateStore_RegistrationRecord_PeerScoping(t *testing.T) {
	s := testStateStore(t)

	local := testKeyedRegisterRequest("shared", nil)
	_, err := s.EnsureRegistrationWithResult(1, local)
	require.NoError(t, err)

	remote := testKeyedRegisterRequest("shared", func(req *structs.RegisterRequest) {
		req.PeerName = "peer-a"
		req.Service.PeerName = "peer-a"
	})
	_, err = s.EnsureRegistrationWithResult(2, remote)
	require.NoError(t, err)

	// Same key in a different peer scope is independent.
	localRec, err := s.GetRegistrationRecord(structs.NodeEnterpriseMetaInDefaultPartition(), "", "shared")
	require.NoError(t, err)
	require.NotNil(t, localRec)
	require.Equal(t, "", localRec.PeerName)

	peerRec, err := s.GetRegistrationRecord(structs.NodeEnterpriseMetaInDefaultPartition(), "peer-a", "shared")
	require.NoError(t, err)
	require.NotNil(t, peerRec)
	require.Equal(t, "peer-a", peerRec.PeerName)
}

func TestStateStore_RegistrationRecord_Restore(t *testing.T) {
	s := testStateStore(t)

	_, err := s.EnsureRegistrationWithResult(7, testKeyedRegisterRequest("restore-key", nil))
	require.NoError(t, err)
	original, err := s.GetRegistrationRecord(structs.NodeEnterpriseMetaInDefaultPartition(), "", "restore-key")
	require.NoError(t, err)

	s2 := testStateStore(t)
	restore := s2.Restore()
	require.NoError(t, restore.RegistrationRecord(original))
	require.NoError(t, restore.Commit())

	got, err := s2.GetRegistrationRecord(structs.NodeEnterpriseMetaInDefaultPartition(), "", "restore-key")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, original.RequestHash, got.RequestHash)
	require.Equal(t, original.NodeVersion, got.NodeVersion)
	require.Equal(t, original.ServiceVersion, got.ServiceVersion)
	require.Equal(t, original.Summary, got.Summary)
}
