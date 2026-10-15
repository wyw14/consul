// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package fsm

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/sdk/testutil"
)

func newFSMWithStorage(t *testing.T) *FSM {
	return NewFromDeps(Deps{
		Logger: testutil.Logger(t),
		NewStateStore: func() *state.Store {
			return state.NewStateStore(nil)
		},
		StorageBackend: newStorageBackend(t, nil),
	})
}

func fsmKeyedRegisterBuf(t *testing.T, key string, mutate func(*structs.RegisterRequest)) []byte {
	t.Helper()
	req := &structs.RegisterRequest{
		Datacenter:     "dc1",
		Node:           "node1",
		Address:        "127.0.0.1",
		IdempotencyKey: key,
		EnterpriseMeta: *structs.NodeEnterpriseMetaInDefaultPartition(),
		Service: &structs.NodeService{
			ID:      "db",
			Service: "db",
			Tags:    []string{"primary"},
			Port:    5432,
		},
	}
	if mutate != nil {
		mutate(req)
	}
	buf, err := structs.Encode(structs.RegisterRequestType, req)
	require.NoError(t, err)
	return buf
}

func TestFSM_Register_Idempotent_ReplayAndConflict(t *testing.T) {
	t.Parallel()
	logger := testutil.Logger(t)
	fsm, err := New(nil, logger)
	require.NoError(t, err)

	buf := fsmKeyedRegisterBuf(t, "fsm-key", nil)
	resp := fsm.Apply(makeLog(buf))
	reply, ok := resp.(*structs.RegisterReply)
	require.True(t, ok, "expected *RegisterReply, got %T", resp)
	require.False(t, reply.Replayed)
	require.Equal(t, uint64(1), reply.NodeVersion)
	require.Equal(t, uint64(1), reply.ServiceVersion)
	require.Equal(t, structs.ChangeCreated, reply.Summary.Node)

	// Replaying the same request returns the stored result and does not
	// modify state.
	replayResp := fsm.Apply(makeLog(fsmKeyedRegisterBuf(t, "fsm-key", nil)))
	replay, ok := replayResp.(*structs.RegisterReply)
	require.True(t, ok)
	require.True(t, replay.Replayed)
	require.Equal(t, reply.NodeVersion, replay.NodeVersion)
	require.Equal(t, reply.ServiceVersion, replay.ServiceVersion)
	require.Equal(t, reply.ModifyIndex, replay.ModifyIndex)

	_, node, err := fsm.state.GetNode("node1", nil, "")
	require.NoError(t, err)
	require.Equal(t, uint64(1), node.ModifyIndex)
	require.Equal(t, uint64(1), node.Version)

	// The same key with a different payload conflicts.
	conflictBuf := fsmKeyedRegisterBuf(t, "fsm-key", func(req *structs.RegisterRequest) {
		req.Service.Port = 9999
	})
	conflictResp := fsm.Apply(makeLog(conflictBuf))
	applyErr, ok := conflictResp.(error)
	require.True(t, ok)
	require.True(t, errors.Is(applyErr, structs.ErrIdempotencyConflict))

	// The conflict changed nothing.
	_, services, err := fsm.state.NodeServices(nil, "node1", structs.NodeEnterpriseMetaInDefaultPartition(), "")
	require.NoError(t, err)
	require.Equal(t, 5432, services.Services["db"].Port)
}

// TestFSM_Register_Idempotent_SnapshotRestore proves that idempotency records
// and entity versions survive snapshots, so retries after a leader/node switch
// observe the same result instead of applying a second time.
func TestFSM_Register_Idempotent_SnapshotRestore(t *testing.T) {
	t.Parallel()
	fsm := newFSMWithStorage(t)

	resp := fsm.Apply(makeLog(fsmKeyedRegisterBuf(t, "durable-key", nil)))
	reply, ok := resp.(*structs.RegisterReply)
	require.True(t, ok)
	require.False(t, reply.Replayed)

	// Snapshot and restore into a brand new FSM, as a freshly elected leader
	// rebuilds from the latest snapshot.
	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	defer snap.Release()

	persisted := bytes.NewBuffer(nil)
	sink := &MockSink{persisted, false}
	require.NoError(t, snap.Persist(sink))

	fsm2 := newFSMWithStorage(t)
	require.NoError(t, fsm2.Restore(sink))

	// Entity versions are preserved across the restore.
	_, node, err := fsm2.state.GetNode("node1", nil, "")
	require.NoError(t, err)
	require.NotNil(t, node)
	require.Equal(t, uint64(1), node.Version)

	_, services, err := fsm2.state.NodeServices(nil, "node1", structs.NodeEnterpriseMetaInDefaultPartition(), "")
	require.NoError(t, err)
	require.Equal(t, uint64(1), services.Services["db"].Version)

	// The idempotency record survived: replaying on the new leader returns
	// the original result without a second application.
	replayResp := fsm2.Apply(makeLog(fsmKeyedRegisterBuf(t, "durable-key", nil)))
	replay, ok := replayResp.(*structs.RegisterReply)
	require.True(t, ok, "expected replay reply, got %T", replayResp)
	require.True(t, replay.Replayed)
	require.Equal(t, reply.NodeVersion, replay.NodeVersion)
	require.Equal(t, reply.ServiceVersion, replay.ServiceVersion)
	require.Equal(t, reply.Summary, replay.Summary)
	require.Equal(t, reply.ModifyIndex, replay.ModifyIndex)
}
