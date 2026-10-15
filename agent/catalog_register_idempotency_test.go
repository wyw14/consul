// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/structs"
)

func TestCatalogRegister_IdempotentReplyAndConflict(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	t.Parallel()

	a := NewTestAgent(t, "")
	defer a.Shutdown()

	args := &structs.RegisterRequest{
		Node:           "foo",
		Address:        "127.0.0.1",
		IdempotencyKey: "http-key",
		Service:        &structs.NodeService{Service: "db", ID: "db", Port: 8000},
	}

	// First call returns the registration result.
	req, _ := http.NewRequest("PUT", "/v1/catalog/register", jsonReader(args))
	obj, err := a.srv.CatalogRegister(nil, req)
	require.NoError(t, err)
	reply, ok := obj.(*structs.RegisterReply)
	require.True(t, ok, "expected *RegisterReply body, got %T", obj)
	require.False(t, reply.Replayed)
	require.Equal(t, "http-key", reply.IdempotencyKey)
	require.Equal(t, uint64(1), reply.ServiceVersion)
	require.Equal(t, structs.ChangeCreated, reply.Summary.Service)

	// Duplicate call replays the same result.
	req, _ = http.NewRequest("PUT", "/v1/catalog/register", jsonReader(args))
	obj, err = a.srv.CatalogRegister(nil, req)
	require.NoError(t, err)
	replay, ok := obj.(*structs.RegisterReply)
	require.True(t, ok)
	require.True(t, replay.Replayed)
	require.Equal(t, reply.ServiceVersion, replay.ServiceVersion)
	require.Equal(t, reply.ModifyIndex, replay.ModifyIndex)

	// Same key with a different payload is a 409 conflict.
	conflict := &structs.RegisterRequest{
		Node:           "foo",
		Address:        "127.0.0.1",
		IdempotencyKey: "http-key",
		Service:        &structs.NodeService{Service: "db", ID: "db", Port: 9000},
	}
	req, _ = http.NewRequest("PUT", "/v1/catalog/register", jsonReader(conflict))
	obj, err = a.srv.CatalogRegister(nil, req)
	require.Error(t, err)
	require.Nil(t, obj)
	httpErr, ok := err.(HTTPError)
	require.True(t, ok, "expected HTTPError, got %T", err)
	require.Equal(t, http.StatusConflict, httpErr.StatusCode)
}

func TestCatalogRegister_KeylessStillReturnsTrue(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	t.Parallel()

	a := NewTestAgent(t, "")
	defer a.Shutdown()

	args := &structs.RegisterRequest{
		Node:    "bar",
		Address: "127.0.0.1",
		Service: &structs.NodeService{Service: "web", Port: 8080},
	}
	req, _ := http.NewRequest("PUT", "/v1/catalog/register", jsonReader(args))
	obj, err := a.srv.CatalogRegister(nil, req)
	require.NoError(t, err)
	applied, ok := obj.(bool)
	require.True(t, ok)
	require.True(t, applied)
}
