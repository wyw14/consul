// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package structs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/types"
)

func fullNodeService() *NodeService {
	w := Weights{Passing: 10, Warning: 1}
	return &NodeService{
		Kind:              ServiceKindTypical,
		ID:                "db-1",
		Service:           "db",
		Tags:              []string{"primary", "v2"},
		Address:           "10.0.0.1",
		Port:              5432,
		Meta:              map[string]string{"version": "2", "tier": "backend"},
		TaggedAddresses:   map[string]ServiceAddress{"lan": {Address: "10.0.0.1", Port: 5432}},
		Weights:           &w,
		EnableTagOverride: true,
	}
}

func TestMergeNodeService_PartialUpdatePreservesOmittedFields(t *testing.T) {
	existing := fullNodeService()

	patch := &NodeService{
		ID:      "db-1",
		Service: "db",
		Address: "10.0.0.2", // only change the address
	}

	merged := MergeNodeService(existing, patch)

	// Explicitly provided fields win.
	require.Equal(t, "10.0.0.2", merged.Address)

	// Omitted scalar fields are preserved.
	require.Equal(t, 5432, merged.Port)
	require.Equal(t, ServiceKindTypical, merged.Kind)
	require.True(t, merged.EnableTagOverride)
	require.Equal(t, []string{"primary", "v2"}, merged.Tags)
	require.Equal(t, &Weights{Passing: 10, Warning: 1}, merged.Weights)

	// Maps are preserved wholesale when omitted.
	require.Equal(t, "2", merged.Meta["version"])
	require.Equal(t, "backend", merged.Meta["tier"])
	require.Equal(t, ServiceAddress{Address: "10.0.0.1", Port: 5432}, merged.TaggedAddresses["lan"])

	// The inputs must not be mutated.
	require.Equal(t, "10.0.0.1", existing.Address)
}

func TestMergeNodeService_MapsMergeKeyByKey(t *testing.T) {
	existing := fullNodeService()

	patch := &NodeService{
		ID:      "db-1",
		Service: "db",
		Meta: map[string]string{
			"version": "3",       // override
			"region":  "us-east", // add
		},
		TaggedAddresses: map[string]ServiceAddress{
			"wan": {Address: "192.0.2.1", Port: 15432}, // add
		},
	}

	merged := MergeNodeService(existing, patch)

	require.Equal(t, "3", merged.Meta["version"], "patched key overrides")
	require.Equal(t, "us-east", merged.Meta["region"], "new key is added")
	require.Equal(t, "backend", merged.Meta["tier"], "untouched key is preserved")

	require.Equal(t, ServiceAddress{Address: "192.0.2.1", Port: 15432}, merged.TaggedAddresses["wan"])
	require.Equal(t, ServiceAddress{Address: "10.0.0.1", Port: 5432}, merged.TaggedAddresses["lan"])
}

func TestMergeNodeService_TagsAndWeightsOverrideWhenProvided(t *testing.T) {
	existing := fullNodeService()

	// An explicitly empty, non-nil slice clears the tags.
	patch := &NodeService{
		ID:      "db-1",
		Service: "db",
		Tags:    []string{},
		Weights: &Weights{Passing: 5, Warning: 0},
	}

	merged := MergeNodeService(existing, patch)
	require.Empty(t, merged.Tags)
	require.Equal(t, &Weights{Passing: 5, Warning: 0}, merged.Weights)
}

func TestMergeNodeService_DoesNotAliasStateMemory(t *testing.T) {
	existing := fullNodeService()
	merged := MergeNodeService(existing, &NodeService{ID: "db-1", Service: "db", Meta: map[string]string{"a": "b"}})

	// Mutating the merged copy must not affect the original service.
	merged.Meta["version"] = "mutated"
	merged.Tags[0] = "mutated"
	require.Equal(t, "2", existing.Meta["version"])
	require.Equal(t, "primary", existing.Tags[0])
	require.NotNil(t, merged.Weights)
	require.NotSame(t, existing.Weights, merged.Weights)
}

func TestRegisterRequest_NormalizedContentHash(t *testing.T) {
	base := func() *RegisterRequest {
		return &RegisterRequest{
			Datacenter: "dc1",
			Node:       "node1",
			Address:    "127.0.0.1",
			NodeMeta:   map[string]string{"a": "b"},
			Service: &NodeService{
				Service: "db",
				Tags:    []string{"primary"},
				Port:    8000,
			},
			Checks: HealthChecks{
				{Node: "node1", CheckID: types.CheckID("db-check")},
			},
		}
	}

	t.Run("identical payloads hash equally regardless of map order", func(t *testing.T) {
		a := base()
		b := base()
		b.NodeMeta = map[string]string{"a": "b"}

		// Different map iteration order is handled by canonical JSON
		// encoding; repeated hashing must stay stable too.
		require.Equal(t, a.NormalizedContentHash(), b.NormalizedContentHash())
		require.Equal(t, a.NormalizedContentHash(), a.NormalizedContentHash())
	})

	t.Run("non-content fields are ignored", func(t *testing.T) {
		a := base()
		b := base()
		b.IdempotencyKey = "key-123"
		b.Token = "root-secret"
		b.RaftIndex = RaftIndex{CreateIndex: 10, ModifyIndex: 20}
		b.NodeVersion = 42
		b.Service.Version = 7
		require.Equal(t, a.NormalizedContentHash(), b.NormalizedContentHash())
	})

	t.Run("payload differences are detected", func(t *testing.T) {
		a := base()
		b := base()
		b.Service.Port = 9000
		assert.NotEqual(t, a.NormalizedContentHash(), b.NormalizedContentHash())

		c := base()
		c.Address = "127.0.0.2"
		assert.NotEqual(t, a.NormalizedContentHash(), c.NormalizedContentHash())
	})

	t.Run("legacy Check and Checks are equivalent", func(t *testing.T) {
		a := base()
		b := base()
		b.Checks = nil
		b.Check = &HealthCheck{Node: "node1", CheckID: types.CheckID("db-check")}
		require.Equal(t, a.NormalizedContentHash(), b.NormalizedContentHash())
	})
}

func TestNextEntityVersion(t *testing.T) {
	require.Equal(t, uint64(1), NextEntityVersion(0))
	require.Equal(t, uint64(2), NextEntityVersion(1))
	require.Equal(t, uint64(43), NextEntityVersion(42))
}
