// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package state

import (
	"fmt"
	"strings"

	memdb "github.com/hashicorp/go-memdb"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/structs"
)

const tableRegistrationRecords = "registration-records"

func registrationRecordsTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: tableRegistrationRecords,
		Indexes: map[string]*memdb.IndexSchema{
			indexID: {
				Name:         indexID,
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field:     "LookupKey",
					Lowercase: true,
				},
			},
		},
	}
}

// RegistrationRecord stores the outcome of an idempotent Catalog.Register
// request. It is written atomically with the registration inside the same
// raft log entry and state transaction, and is replicated and snapshotted, so
// retries observe the same result after leader changes.
type RegistrationRecord struct {
	// LookupKey is the partition/namespace/peer scoped primary key.
	LookupKey string

	Partition string
	Namespace string
	PeerName  string

	// Key is the client supplied idempotency key.
	Key string

	// RequestHash identifies the exact request payload that claimed the key.
	RequestHash string

	// NodeVersion and ServiceVersion are the entity versions resulting from
	// the original application.
	NodeVersion    uint64
	ServiceVersion uint64

	// Summary is the change summary observed by the original application.
	Summary structs.ChangeSummary

	structs.RaftIndex
}

// registrationRecordLookupKey builds the scope-qualified lookup key for an
// idempotency record. Keys never collide across partitions, namespaces or
// peers.
func registrationRecordLookupKey(partition, namespace, peerName, key string) string {
	var b strings.Builder
	b.WriteString(strings.ToLower(partition))
	b.WriteByte(0)
	b.WriteString(namespace)
	b.WriteByte(0)
	b.WriteString(strings.ToLower(peerName))
	b.WriteByte(0)
	b.WriteString(key)
	return b.String()
}

// ToReply converts a stored record into the reply returned for both the
// original request and every subsequent replay.
func (r *RegistrationRecord) ToReply(key string) *structs.RegisterReply {
	return &structs.RegisterReply{
		IdempotencyKey: key,
		Replayed:       true,
		NodeVersion:    r.NodeVersion,
		ServiceVersion: r.ServiceVersion,
		Summary:        r.Summary,
		RaftIndex:      r.RaftIndex,
	}
}

func newRegistrationRecord(idx uint64, req *structs.RegisterRequest, reply *structs.RegisterReply) *RegistrationRecord {
	entMeta := req.EnterpriseMeta
	return &RegistrationRecord{
		LookupKey: registrationRecordLookupKey(
			entMeta.PartitionOrDefault(),
			entMeta.NamespaceOrDefault(),
			req.PeerName,
			req.IdempotencyKey,
		),
		Partition:      entMeta.PartitionOrDefault(),
		Namespace:      entMeta.NamespaceOrDefault(),
		PeerName:       req.PeerName,
		Key:            req.IdempotencyKey,
		RequestHash:    req.NormalizedContentHash(),
		NodeVersion:    reply.NodeVersion,
		ServiceVersion: reply.ServiceVersion,
		Summary:        reply.Summary,
		RaftIndex: structs.RaftIndex{
			CreateIndex: idx,
			ModifyIndex: idx,
		},
	}
}

func txnRegistrationRecord(tx ReadTxn, entMeta *acl.EnterpriseMeta, peerName, key string) (*RegistrationRecord, error) {
	lookup := registrationRecordLookupKey(
		entMeta.PartitionOrDefault(),
		entMeta.NamespaceOrDefault(),
		peerName, key,
	)
	existing, err := tx.First(tableRegistrationRecords, indexID, lookup)
	if err != nil {
		return nil, fmt.Errorf("failed registration record lookup: %s", err)
	}
	if existing == nil {
		return nil, nil
	}
	rec, ok := existing.(*RegistrationRecord)
	if !ok {
		return nil, fmt.Errorf("registration record %q has invalid type: %T", key, existing)
	}
	return rec, nil
}

// GetRegistrationRecord returns the stored idempotency record for the given
// scoped key, or nil if no request with the key has been applied.
func (s *Store) GetRegistrationRecord(entMeta *acl.EnterpriseMeta, peerName, key string) (*RegistrationRecord, error) {
	tx := s.db.ReadTxn()
	defer tx.Abort()
	return txnRegistrationRecord(tx, entMeta, peerName, key)
}

func putRegistrationRecordTxn(tx WriteTxn, idx uint64, rec *RegistrationRecord) error {
	if err := tx.Insert(tableRegistrationRecords, rec); err != nil {
		return fmt.Errorf("failed inserting registration record: %s", err)
	}
	if err := indexUpdateMaxTxn(tx, idx, tableRegistrationRecords); err != nil {
		return fmt.Errorf("failed updating registration records index: %s", err)
	}
	return nil
}

// RegistrationRecords returns all stored idempotency records, for snapshots.
func (s *Snapshot) RegistrationRecords() (memdb.ResultIterator, error) {
	return s.tx.Get(tableRegistrationRecords, indexID)
}

// RegistrationRecord restores an idempotency record from a snapshot.
func (s *Restore) RegistrationRecord(rec *RegistrationRecord) error {
	if rec.LookupKey == "" {
		rec.LookupKey = registrationRecordLookupKey(rec.Partition, rec.Namespace, rec.PeerName, rec.Key)
	}
	if err := s.tx.Insert(tableRegistrationRecords, rec); err != nil {
		return fmt.Errorf("failed restoring registration record: %s", err)
	}
	if err := indexUpdateMaxTxn(s.tx, rec.ModifyIndex, tableRegistrationRecords); err != nil {
		return fmt.Errorf("failed updating registration records index: %s", err)
	}
	return nil
}
