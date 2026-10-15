// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package structs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/hashicorp/consul-net-rpc/go-msgpack/codec"
)

// ErrIdempotencyConflict is returned when an IdempotencyKey is reused with a
// request payload that differs from the original request that claimed the key.
// Callers must use a fresh key for a distinct logical registration.
var ErrIdempotencyConflict = errors.New("idempotency key was already used with a different request")

// ChangeAction describes what happened to a single catalog entity as the
// result of applying a registration.
type ChangeAction string

const (
	// ChangeUnspecified means the registration did not touch the entity.
	ChangeUnspecified ChangeAction = ""

	// ChangeCreated means the entity did not exist before and was inserted.
	ChangeCreated ChangeAction = "created"

	// ChangeUpdated means the entity existed and its contents changed.
	ChangeUpdated ChangeAction = "updated"

	// ChangeUnchanged means the entity existed and the request did not modify
	// any of its contents.
	ChangeUnchanged ChangeAction = "unchanged"
)

// CheckChange reports the outcome for a single check carried in a register
// request.
type CheckChange struct {
	CheckID string       `json:",omitempty"`
	Action  ChangeAction `json:",omitempty"`
}

// ChangeSummary describes the outcome of a registration by classifying the
// changes made to the node, the service and each check. It is part of the
// stored idempotency record so that retried requests observe the exact same
// summary.
type ChangeSummary struct {
	Node    ChangeAction  `json:",omitempty"`
	Service ChangeAction  `json:",omitempty"`
	Checks  []CheckChange `json:",omitempty"`
}

// RegisterReply is the result of a Catalog.Register RPC. It is populated for
// requests that carry an IdempotencyKey; requests without a key preserve the
// historical empty reply.
type RegisterReply struct {
	// IdempotencyKey echoes the request's key.
	IdempotencyKey string `json:",omitempty"`

	// Replayed is true when the request matched a previously applied
	// registration with the same key and payload, so no state was modified.
	Replayed bool

	// NodeVersion and ServiceVersion are the resulting per-entity, sortable
	// versions. They stay stable across replays of the same request.
	NodeVersion    uint64
	ServiceVersion uint64

	// Summary classifies the changes made by the registration.
	Summary ChangeSummary

	RaftIndex `bexpr:"-"`
}

// nextEntityVersion returns the version to record for a modified entity.
// Versions start at 1; entities loaded from older data (version 0) are seeded
// on their first modification.
func nextEntityVersion(existing uint64) uint64 {
	if existing == 0 {
		return 1
	}
	return existing + 1
}

// NextEntityVersion exposes the versioning rule to the state store without
// duplicating it.
func NextEntityVersion(existing uint64) uint64 { return nextEntityVersion(existing) }

// MergeNodeService overlays a partial service update on top of an existing
// service and returns the merged value. Fields left at their zero value in
// patch retain the existing value, while map-valued fields are merged key by
// key. Neither input is mutated; the returned value is an independent copy.
func MergeNodeService(existing, patch *NodeService) *NodeService {
	if existing == nil {
		return patch
	}
	if patch == nil {
		return cloneNodeService(existing)
	}

	merged := cloneNodeService(existing)

	// Identifying fields always come from the request. They have already been
	// validated and identify the same service instance.
	merged.Kind = patch.Kind
	merged.ID = patch.ID
	merged.Service = patch.Service
	merged.EnterpriseMeta = patch.EnterpriseMeta
	merged.PeerName = patch.PeerName

	if patch.Tags != nil {
		merged.Tags = cloneStrings(patch.Tags)
	}
	if patch.Address != "" {
		merged.Address = patch.Address
	}
	if patch.Port != 0 {
		merged.Port = patch.Port
	}
	if len(patch.Ports) > 0 {
		ports := make(ServicePorts, len(patch.Ports))
		copy(ports, patch.Ports)
		merged.Ports = ports
	}
	if patch.SocketPath != "" {
		merged.SocketPath = patch.SocketPath
	}
	if patch.TaggedAddresses != nil {
		if merged.TaggedAddresses == nil {
			merged.TaggedAddresses = make(map[string]ServiceAddress, len(patch.TaggedAddresses))
		}
		for k, v := range patch.TaggedAddresses {
			merged.TaggedAddresses[k] = v
		}
	}
	if patch.Meta != nil {
		if merged.Meta == nil {
			merged.Meta = make(map[string]string, len(patch.Meta))
		}
		for k, v := range patch.Meta {
			merged.Meta[k] = v
		}
	}
	if patch.Weights != nil {
		w := *patch.Weights
		merged.Weights = &w
	}
	// A boolean cannot distinguish "set to false" from "omitted" in a partial
	// update, so a merge can only enable tag override, never disable it.
	if patch.EnableTagOverride {
		merged.EnableTagOverride = true
	}
	if patch.Locality != nil {
		merged.Locality = patch.Locality
	}
	if patch.AI != nil {
		merged.AI = patch.AI
	}
	var zeroProxy ConnectProxyConfig
	if !reflect.DeepEqual(patch.Proxy, zeroProxy) {
		merged.Proxy = patch.Proxy
	}
	var zeroConnect ServiceConnect
	if patch.Connect != zeroConnect {
		merged.Connect = patch.Connect
	}

	return merged
}

func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

// cloneNodeService returns a deep, independent copy of the service so that
// partial merges never alias state-store memory (in particular the Weights
// pointer and map/slice fields).
func cloneNodeService(in *NodeService) *NodeService {
	var buf bytes.Buffer
	if err := codec.NewEncoder(&buf, MsgpackHandle).Encode(in); err != nil {
		// Encoding an in-memory struct with the shared handle cannot fail.
		panic(fmt.Sprintf("failed encoding node service for merge: %v", err))
	}
	var out NodeService
	if err := codec.NewDecoder(&buf, MsgpackHandle).Decode(&out); err != nil {
		panic(fmt.Sprintf("failed decoding node service for merge: %v", err))
	}
	return &out
}

// NormalizedContentHash returns a stable hash of the request's meaningful
// payload. It is used to detect idempotency-key conflicts and is identical for
// byte-different but semantically equal retries (ignoring the key itself, the
// ACL token and raft bookkeeping). Map keys are serialized through encoding
// json, which emits them in sorted order, so the hash is independent of map
// iteration order.
func (r *RegisterRequest) NormalizedContentHash() string {
	clone := r.cloneForHash()

	// Flatten the legacy single check exactly like the pre-apply path does.
	if clone.Check != nil {
		clone.Checks = append(clone.Checks, clone.Check)
		clone.Check = nil
	}

	// Mirror validateRegisterRequestPeerNamesTxn in the state store so that
	// the hash computed before raft submission matches the one computed
	// inside the FSM transaction.
	normalizePeerName := func(peer string) string {
		if peer == "" {
			return clone.PeerName
		}
		return peer
	}
	if clone.Service != nil {
		clone.Service.PeerName = normalizePeerName(clone.Service.PeerName)
		clone.Service.RaftIndex = RaftIndex{}
		clone.Service.Version = 0
	}
	for _, check := range clone.Checks {
		check.PeerName = normalizePeerName(check.PeerName)
		check.RaftIndex = RaftIndex{}
	}

	// Non-content bookkeeping must not influence the hash.
	clone.IdempotencyKey = ""
	clone.Token = ""
	clone.RaftIndex = RaftIndex{}
	clone.NodeVersion = 0

	b, err := json.Marshal(clone)
	if err != nil {
		// RegisterRequest only contains JSON-encodable fields.
		panic(fmt.Sprintf("failed hashing register request: %v", err))
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// cloneForHash returns a value copy deep enough for hashing: a json round
// trip preserves every content field (maps are reallocated) while dropping no
// fields used by the hash.
func (r *RegisterRequest) cloneForHash() *RegisterRequest {
	b, err := json.Marshal(r)
	if err != nil {
		panic(fmt.Sprintf("failed encoding register request for hash: %v", err))
	}
	var clone RegisterRequest
	if err := json.Unmarshal(b, &clone); err != nil {
		panic(fmt.Sprintf("failed decoding register request for hash: %v", err))
	}
	return &clone
}
