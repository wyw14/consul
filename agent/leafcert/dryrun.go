// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package leafcert

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-metrics"

	"github.com/hashicorp/consul/agent/connect"
	"github.com/hashicorp/consul/agent/structs"
)

// Stage identifies one discrete stage of a dry-run leaf certificate rotation
// check. Stages always execute in the order declared here.
type Stage string

const (
	// StageCacheCheck inspects the locally cached active certificate and
	// reports whether a normal Get would be served from cache.
	StageCacheCheck Stage = "cache_check"

	// StageRootsOverlap loads the current CA roots, detects whether a root
	// rotation is in progress and verifies the explicit old/new root overlap
	// window.
	StageRootsOverlap Stage = "roots_overlap"

	// StageIssue prepares a fresh key and CSR and has it really signed by the
	// CA. The resulting candidate certificate is never installed.
	StageIssue Stage = "issue"

	// StageChainVerify verifies the candidate certificate chain against the
	// current roots and asserts the chain terminates at the active root.
	StageChainVerify Stage = "chain_verify"
)

// dryRunStages is the fixed stage execution order.
var dryRunStages = []Stage{
	StageCacheCheck,
	StageRootsOverlap,
	StageIssue,
	StageChainVerify,
}

// StageStatus is the outcome of a single stage.
type StageStatus string

const (
	StagePass    StageStatus = "pass"
	StageFail    StageStatus = "fail"
	StageSkipped StageStatus = "skipped"
)

// Stable, machine-readable failure reason codes emitted by dry-run stages.
const (
	// roots_overlap stage
	ReasonRootsUnavailable   = "roots_unavailable"
	ReasonNoActiveRoot       = "no_active_root"
	ReasonTrustDomainMissing = "trust_domain_missing"
	ReasonOverlapViolation   = "root_overlap_violation"
	ReasonActiveChainInvalid = "active_cert_chain_invalid"

	// issue stage
	ReasonCSRPrepareFailed = "csr_prepare_failed"
	ReasonSigningFailed    = "signing_failed"

	// chain_verify stage
	ReasonCertParseFailed    = "cert_parse_failed"
	ReasonCandidateExpired   = "candidate_expired"
	ReasonChainVerifyFailed  = "chain_verify_failed"
	ReasonActiveRootMismatch = "active_root_mismatch"
)

// DryRunOptions controls a dry-run rotation check.
type DryRunOptions struct {
	// Resume continues the most recent dry-run for the same key from the stage
	// after the last completed one, reusing the captured artifacts (root
	// snapshot, key/CSR, candidate cert): the resumed check validates against
	// the root snapshot captured by the first attempt, so a resume long after
	// roots changed should be avoided or followed by a non-resume run. When
	// false, or when no checkpoint exists, every stage runs from scratch and
	// any prior checkpoint is discarded.
	Resume bool
}

// RotationReport is the structured, observable result of a dry-run rotation
// check. It never contains a reference to the mutable cache state, so it is
// safe for callers to retain.
type RotationReport struct {
	// Key is the leaf cert cache key the check ran for.
	Key string

	// Resumed is true when the check continued a prior in-memory checkpoint.
	Resumed bool

	// StartedAt is when the first (possibly earlier, resumed) check started.
	StartedAt time.Time

	// Duration is the wall time spent by this invocation.
	Duration time.Duration

	// Stages holds per-stage outcomes in execution order.
	Stages []StageResult

	// CacheCheck describes the cached active certificate observed by the
	// cache_check stage.
	CacheCheck CacheCheckInfo

	// Overlap describes the root rotation overlap window observed by the
	// roots_overlap stage. Its zero value means no rotation was detected.
	Overlap RootOverlapInfo

	// Candidate summarizes the freshly issued candidate. It remains nil
	// until the issue/chain_verify stages complete and is never installed as
	// the active certificate.
	Candidate *CandidateCertInfo
}

// Succeeded reports whether the check reached every stage without a failure.
// Stages reported as skipped on a resume (already completed in the checkpoint)
// count as success: a nil error from DryRunRotate always means Succeeded.
func (r *RotationReport) Succeeded() bool {
	if r == nil || len(r.Stages) != len(dryRunStages) {
		return false
	}
	for _, s := range r.Stages {
		if s.Status == StageFail {
			return false
		}
	}
	return true
}

// StageResult records the outcome of one stage.
type StageResult struct {
	Stage    Stage
	Status   StageStatus
	Duration time.Duration
	// Reason is a stable code, populated when Status is StageFail.
	Reason string
	// Detail is a human-readable explanation.
	Detail string
}

// CacheCheckInfo describes the cached active certificate.
type CacheCheckInfo struct {
	// HasActiveCert is true when a cert is cached for the key.
	HasActiveCert bool
	// CacheHit is true when a usable active cert is present and a non-blocking
	// lookup would be served from cache.
	CacheHit bool
	// Index is the cached raft index.
	Index uint64
	// SerialNumber is the serial of the active certificate, when present.
	SerialNumber string
	// AuthorityKeyID identifies the CA signing key of the active cert.
	AuthorityKeyID string
	ValidAfter     time.Time
	ValidBefore    time.Time
	// Expired is true when the active cert is past ValidBefore.
	Expired bool
}

// RootOverlapInfo describes the old/new root coexistence window during a root
// CA rolling update.
type RootOverlapInfo struct {
	// RotationInProgress is true when the active cert's signing root differs
	// from the current active root.
	RotationInProgress bool
	// ActiveRootID is the currently active root ID.
	ActiveRootID string
	// SigningRootID is the ID of the root that signed the active cert. It is
	// empty when the signing root is no longer present in the bundle.
	SigningRootID string
	// OldRootPresent reports whether the active cert's signing root is still
	// present (trusted) in the roots bundle.
	OldRootPresent bool
	// Window is the explicitly configured overlap duration.
	Window time.Duration
	// StartsAt/EndsAt bound the overlap window.
	StartsAt time.Time
	EndsAt   time.Time
	// WithinWindow is true while the check time is before EndsAt.
	WithinWindow bool
	// Remaining is EndsAt minus the check time; it is negative once elapsed.
	Remaining time.Duration
}

// CandidateCertInfo summarizes the certificate issued by the dry-run.
type CandidateCertInfo struct {
	SerialNumber       string
	AuthorityKeyID     string
	ValidAfter         time.Time
	ValidBefore        time.Time
	RootID             string
	ChainsToActiveRoot bool
}

// DryRunError is returned when a dry-run stage fails. It exposes the failed
// Stage and a stable Reason code programmatically and wraps the underlying
// error.
type DryRunError struct {
	Stage  Stage
	Reason string
	Err    error
}

func (e *DryRunError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("dry-run rotation stage %q failed: %s", e.Stage, e.Reason)
	}
	return fmt.Sprintf("dry-run rotation stage %q failed (%s): %v", e.Stage, e.Reason, e.Err)
}

func (e *DryRunError) Unwrap() error { return e.Err }

func dryRunFailure(stage Stage, reason string, err error) *DryRunError {
	if err == nil {
		err = errors.New(reason)
	}
	return &DryRunError{Stage: stage, Reason: reason, Err: err}
}

// dryRunCheckpoint is the per-key, in-memory state that lets a failed or
// partial dry-run be resumed from the last completed stage. Apart from
// abandoned it is only touched by the single goroutine that executes the check
// (same-key concurrent checks are coalesced by dryRunGroup).
type dryRunCheckpoint struct {
	startedAt time.Time

	// completed lists stages that already succeeded, in execution order.
	completed []Stage

	// Artifacts captured as stages complete.
	roots         *structs.IndexedCARoots
	activeCert    *structs.IssuedCert
	pkPEM         string
	csrPEM        string
	candidate     *structs.IssuedCert
	candidateInfo *CandidateCertInfo
	rotationStart time.Time

	cache   CacheCheckInfo
	overlap RootOverlapInfo

	// abandoned is set by ClearDryRunState so a check that is finishing when
	// its checkpoint is cleared does not reinstall itself.
	abandoned atomic.Bool
}

func (cp *dryRunCheckpoint) isCompleted(stage Stage) bool {
	for _, s := range cp.completed {
		if s == stage {
			return true
		}
	}
	return false
}

func (cp *dryRunCheckpoint) markCompleted(stage Stage) {
	cp.completed = append(cp.completed, stage)
}

// DryRunRotate performs an observable dry-run of leaf certificate rotation for
// req without replacing the active certificate. It really exercises signing
// and chain validation, reports cache-hit and root-overlap state, and (on
// failure) can be resumed from the last completed stage with
// DryRunOptions.Resume.
//
// The active certificate (certData value/index/state) and the real rotation
// path (Get/Notify) are never mutated. The only bookkeeping side effect is
// that, as with a normal Get, looking up the key lazily creates a certData
// entry with a TTL-heap timer; that entry also bounds the lifetime of the
// in-memory checkpoint and is reaped after LastGetTTL.
//
// Concurrent calls for the same key are coalesced into a single check; when
// callers pass different DryRunOptions, the first caller's options drive the
// shared execution and all callers receive the same report.
func (m *Manager) DryRunRotate(
	ctx context.Context,
	req *ConnectCALeafRequest,
	opts DryRunOptions,
) (*RotationReport, error) {
	if req == nil {
		return nil, errors.New("a request is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Lightweight copy so manipulating the request can't race the caller.
	dup := *req
	req = &dup
	if req.MaxQueryTime <= 0 {
		req.MaxQueryTime = DefaultQueryTimeout
	}

	key := req.Key()
	if key == "" {
		return nil, fmt.Errorf("a key is required")
	}

	v, err, _ := m.dryRunGroup.Do(key, func() (any, error) {
		report, runErr := m.runDryRotate(ctx, req, opts)
		return report, runErr
	})

	report, _ := v.(*RotationReport)
	return report, err
}

// ClearDryRunState discards the in-memory dry-run checkpoint for key. It does
// not affect the active certificate.
func (m *Manager) ClearDryRunState(key string) {
	m.lock.RLock()
	cd := m.certs[key]
	m.lock.RUnlock()
	if cd == nil {
		return
	}
	if cp := cd.getDryRunCheckpoint(); cp != nil {
		cp.abandoned.Store(true)
	}
	cd.setDryRunCheckpoint(nil)
}

func (m *Manager) runDryRotate(
	ctx context.Context,
	req *ConnectCALeafRequest,
	opts DryRunOptions,
) (report *RotationReport, retErr error) {
	key := req.Key()
	cd := m.getCertData(key)

	var cp *dryRunCheckpoint
	resumedFromCheckpoint := false
	if opts.Resume {
		cp = cd.getDryRunCheckpoint()
	}
	if cp != nil {
		// Continue the prior checkpoint.
		resumedFromCheckpoint = true
	} else {
		cp = &dryRunCheckpoint{startedAt: m.now()}
		// Install up front so the checkpoint survives a failure and can be
		// resumed. It never touches the active cert.
		cd.setDryRunCheckpoint(cp)
	}

	invocationStart := m.now()
	report = &RotationReport{
		Key:       key,
		Resumed:   resumedFromCheckpoint,
		StartedAt: cp.startedAt,
	}

	defer func() {
		// Duration always measures this invocation, even when resuming an
		// earlier checkpoint; StartedAt retains the original check start.
		report.Duration = m.now().Sub(invocationStart)

		result := "success"
		if retErr != nil {
			result = "failure"
		}
		m.emitDryRunMetric("run", result, req)
		m.logger.Info("leaf certificate rotation dry-run finished",
			"service", req.Service,
			"kind", req.Kind,
			"agent", req.Agent,
			"resumed", report.Resumed,
			"result", result,
			"duration", report.Duration,
		)

		// The (test-only) hook is invoked inside the commit critical section,
		// between its abandoned check and the pointer assignment.
		cd.commitDryRunCheckpoint(cp, m.testDryRunBeforeCommit)
	}()

	for _, stage := range dryRunStages {
		if cp.isCompleted(stage) {
			result := StageResult{
				Stage:  stage,
				Status: StageSkipped,
				Detail: "stage already completed in checkpoint; reusing captured artifacts",
			}
			report.Stages = append(report.Stages, result)
			hydrateSkippedStageReport(stage, cp, report)
			m.recordDryRunStage(req, result)
			continue
		}

		start := m.now()
		m.logger.Debug("leaf certificate rotation dry-run stage starting",
			"service", req.Service,
			"kind", string(req.Kind),
			"agent", req.Agent,
			"stage", string(stage),
		)
		var stageErr error
		switch stage {
		case StageCacheCheck:
			stageErr = m.dryRunCacheCheck(cd, cp, report)
		case StageRootsOverlap:
			stageErr = m.dryRunRootsOverlap(cp, report)
		case StageIssue:
			stageErr = m.dryRunIssue(ctx, req, cp)
		case StageChainVerify:
			stageErr = m.dryRunChainVerify(cp, report)
		default:
			stageErr = dryRunFailure(stage, "unknown_stage", nil)
		}

		result := StageResult{
			Stage:    stage,
			Duration: m.now().Sub(start),
		}
		if stageErr != nil {
			var de *DryRunError
			if errors.As(stageErr, &de) {
				result.Status = StageFail
				result.Reason = de.Reason
				if de.Err != nil {
					result.Detail = de.Err.Error()
				}
			} else {
				result.Status = StageFail
				result.Reason = string(stage)
				result.Detail = stageErr.Error()
			}
			report.Stages = append(report.Stages, result)
			m.recordDryRunStage(req, result)
			// Checkpoint stays in place with the completed stages so a Resume
			// can continue here.
			return report, stageErr
		}

		result.Status = StagePass
		report.Stages = append(report.Stages, result)
		cp.markCompleted(stage)
		m.recordDryRunStage(req, result)
	}

	return report, nil
}

// hydrateSkippedStageReport fills report sections from checkpoint artifacts
// when a stage is skipped during a resume.
func hydrateSkippedStageReport(stage Stage, cp *dryRunCheckpoint, report *RotationReport) {
	switch stage {
	case StageCacheCheck:
		report.CacheCheck = cp.cache
	case StageRootsOverlap:
		report.Overlap = cp.overlap
	case StageChainVerify:
		report.Candidate = cp.candidateInfo
	}
}

// dryRunCacheCheck observes (never mutates) the cached active certificate.
func (m *Manager) dryRunCacheCheck(cd *certData, cp *dryRunCheckpoint, report *RotationReport) error {
	cd.lock.Lock()
	var (
		existing = cd.value
		index    = cd.index
		state    = cd.state
	)
	cd.lock.Unlock()

	info := CacheCheckInfo{}
	if existing != nil {
		info.HasActiveCert = true
		info.CacheHit = true
		info.Index = index
		info.SerialNumber = existing.SerialNumber
		info.AuthorityKeyID = state.authorityKeyID
		info.ValidAfter = existing.ValidAfter
		info.ValidBefore = existing.ValidBefore
		info.Expired = !existing.ValidBefore.IsZero() && m.now().After(existing.ValidBefore)

		// Keep a read-only snapshot for the roots stage, which verifies the
		// active cert still chains during a root rotation.
		cp.activeCert = existing
	}

	cp.cache = info
	report.CacheCheck = info
	return nil
}

// dryRunRootsOverlap reads roots, checks the rotation overlap window and, when
// a rotation is in progress, verifies the active cert still chains under the
// current (old + new) roots bundle.
func (m *Manager) dryRunRootsOverlap(cp *dryRunCheckpoint, report *RotationReport) error {
	roots, err := m.rootsReader.Get()
	if err != nil {
		return dryRunFailure(StageRootsOverlap, ReasonRootsUnavailable, err)
	}
	if roots == nil {
		return dryRunFailure(StageRootsOverlap, ReasonRootsUnavailable, errors.New("no CA roots"))
	}
	if roots.TrustDomain == "" {
		return dryRunFailure(StageRootsOverlap, ReasonTrustDomainMissing,
			errors.New("cluster has no CA bootstrapped yet"))
	}
	active := roots.Active()
	if active == nil {
		return dryRunFailure(StageRootsOverlap, ReasonNoActiveRoot,
			errors.New("roots bundle has no active root"))
	}

	cp.roots = roots
	ov := RootOverlapInfo{
		ActiveRootID: active.ID,
		Window:       m.config.RootRotationOverlapWindow,
	}

	// Detect a root rotation by comparing the root that signed the active
	// cert with the currently active root. This mirrors activeRootHasKey in
	// the real rotation path.
	if cp.cache.HasActiveCert && cp.cache.AuthorityKeyID != "" {
		signing := findRootBySigningKeyID(roots, cp.cache.AuthorityKeyID)
		if signing != nil && signing.ID != active.ID {
			ov.RotationInProgress = true
			ov.SigningRootID = signing.ID
			ov.OldRootPresent = true
		} else if signing == nil {
			ov.RotationInProgress = true
			ov.OldRootPresent = false
		}
	}

	if ov.RotationInProgress {
		// Anchor the window at the root's RotatedOutAt when the server records
		// it; otherwise use the first in-process observation time.
		now := m.now()
		start := now
		if signing := findRootBySigningKeyID(roots, cp.cache.AuthorityKeyID); signing != nil &&
			!signing.RotatedOutAt.IsZero() {
			start = signing.RotatedOutAt
		} else if !cp.rotationStart.IsZero() {
			start = cp.rotationStart
		} else {
			cp.rotationStart = start
		}

		ov.StartsAt = start
		ov.EndsAt = start.Add(ov.Window)
		ov.WithinWindow = now.Before(ov.EndsAt)
		ov.Remaining = ov.EndsAt.Sub(now)

		if ov.WithinWindow && !ov.OldRootPresent {
			cp.overlap = ov
			report.Overlap = ov
			return dryRunFailure(StageRootsOverlap, ReasonOverlapViolation, fmt.Errorf(
				"root CA rotation to active root %q detected, but the root that signed the "+
					"active certificate (signing key id %q) is no longer present in the "+
					"trusted roots bundle before the explicit overlap window ends at %s "+
					"(remaining %s); proxies holding certs signed by the old root may fail "+
					"TLS verification",
				ov.ActiveRootID, cp.cache.AuthorityKeyID, ov.EndsAt, ov.Remaining))
		}

		// While roots overlap, the active cert must still verify under the
		// current bundle (which includes the old root).
		if cp.activeCert != nil {
			leaf, intermediates, err := connect.ParseLeafCerts(cp.activeCert.CertPEM)
			if err != nil {
				cp.overlap = ov
				report.Overlap = ov
				return dryRunFailure(StageRootsOverlap, ReasonActiveChainInvalid, err)
			}
			if _, err := verifyLeafAgainstRoots(leaf, intermediates, roots); err != nil {
				cp.overlap = ov
				report.Overlap = ov
				return dryRunFailure(StageRootsOverlap, ReasonActiveChainInvalid, err)
			}
		}
	}

	cp.overlap = ov
	report.Overlap = ov
	return nil
}

// dryRunIssue really signs a fresh candidate certificate without installing it.
func (m *Manager) dryRunIssue(
	ctx context.Context,
	req *ConnectCALeafRequest,
	cp *dryRunCheckpoint,
) error {
	pkPEM, csrPEM, err := m.prepareCertificateRequest(req, cp.roots)
	if err != nil {
		return dryRunFailure(StageIssue, ReasonCSRPrepareFailed, err)
	}
	cp.pkPEM = pkPEM
	cp.csrPEM = csrPEM

	args := &structs.CASignRequest{
		WriteRequest: structs.WriteRequest{Token: req.Token},
		Datacenter:   req.Datacenter,
		CSR:          csrPEM,
	}

	reply, err := m.certSigner.SignCert(ctx, args)
	if err != nil {
		return dryRunFailure(StageIssue, ReasonSigningFailed, err)
	}
	if reply == nil {
		return dryRunFailure(StageIssue, ReasonSigningFailed,
			errors.New("signer returned an empty certificate"))
	}
	reply.PrivateKeyPEM = pkPEM
	cp.candidate = reply
	return nil
}

// dryRunChainVerify verifies the freshly issued candidate against the current
// roots bundle and asserts the chain terminates at the active root.
func (m *Manager) dryRunChainVerify(cp *dryRunCheckpoint, report *RotationReport) error {
	candidate := cp.candidate

	leaf, intermediates, err := connect.ParseLeafCerts(candidate.CertPEM)
	if err != nil {
		return dryRunFailure(StageChainVerify, ReasonCertParseFailed, err)
	}

	now := m.now()
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return dryRunFailure(StageChainVerify, ReasonCandidateExpired, fmt.Errorf(
			"newly issued candidate certificate is not currently valid: not_before=%s not_after=%s now=%s",
			leaf.NotBefore, leaf.NotAfter, now))
	}

	chainRoot, err := verifyLeafAgainstRoots(leaf, intermediates, cp.roots)
	if err != nil {
		return dryRunFailure(StageChainVerify, ReasonChainVerifyFailed, err)
	}

	active := cp.roots.Active()
	activeRootCert, err := connect.ParseCert(active.RootCert)
	if err != nil {
		return dryRunFailure(StageChainVerify, ReasonCertParseFailed, fmt.Errorf(
			"failed to parse active root %q: %w", active.ID, err))
	}

	info := &CandidateCertInfo{
		SerialNumber:   candidate.SerialNumber,
		AuthorityKeyID: connect.EncodeSigningKeyID(leaf.AuthorityKeyId),
		ValidAfter:     candidate.ValidAfter,
		ValidBefore:    candidate.ValidBefore,
		RootID:         active.ID,
	}

	if !bytes.Equal(chainRoot.Raw, activeRootCert.Raw) {
		cp.candidateInfo = info
		report.Candidate = info
		return dryRunFailure(StageChainVerify, ReasonActiveRootMismatch, fmt.Errorf(
			"candidate certificate chains to root %x but the active root is %q (%x)",
			chainRoot.RawSubject, active.ID, activeRootCert.RawSubject))
	}
	info.ChainsToActiveRoot = true

	cp.candidateInfo = info
	report.Candidate = info
	return nil
}

// findRootBySigningKeyID returns the root whose SigningKeyID matches, mirroring
// activeRootHasKey in the real rotation path.
func findRootBySigningKeyID(roots *structs.IndexedCARoots, signingKeyID string) *structs.CARoot {
	for _, r := range roots.Roots {
		if r.SigningKeyID == signingKeyID {
			return r
		}
	}
	return nil
}

// verifyLeafAgainstRoots builds a trust pool from every root in the bundle
// plus intermediates advertised by the roots themselves, then verifies leaf.
// It returns the root certificate the chosen chain terminates at.
func verifyLeafAgainstRoots(
	leaf *x509.Certificate,
	intermediates *x509.CertPool,
	roots *structs.IndexedCARoots,
) (*x509.Certificate, error) {
	if intermediates == nil {
		intermediates = x509.NewCertPool()
	}
	for _, r := range roots.Roots {
		for _, pemStr := range r.IntermediateCerts {
			if c, err := connect.ParseCert(pemStr); err == nil {
				intermediates.AddCert(c)
			}
		}
	}

	pool := x509.NewCertPool()
	for _, r := range roots.Roots {
		if !pool.AppendCertsFromPEM([]byte(r.RootCert)) {
			return nil, fmt.Errorf("failed to parse CA root %q from bundle", r.ID)
		}
	}

	chains, err := leaf.Verify(x509.VerifyOptions{
		Roots:         pool,
		Intermediates: intermediates,
	})
	if err != nil {
		return nil, err
	}
	if len(chains) == 0 || len(chains[0]) == 0 {
		return nil, errors.New("certificate verification returned no chain")
	}
	return chains[0][len(chains[0])-1], nil
}

// recordDryRunStage emits the per-stage metric and structured log line.
func (m *Manager) recordDryRunStage(req *ConnectCALeafRequest, result StageResult) {
	m.emitDryRunStageMetric(result, req)

	args := []any{
		"service", req.Service,
		"kind", string(req.Kind),
		"agent", req.Agent,
		"stage", string(result.Stage),
		"result", string(result.Status),
		"duration", result.Duration,
	}
	if result.Reason != "" {
		args = append(args, "reason", result.Reason)
	}
	if result.Detail != "" {
		args = append(args, "detail", result.Detail)
	}

	switch result.Status {
	case StageFail:
		m.logger.Warn("leaf certificate rotation dry-run stage failed", args...)
	default:
		m.logger.Info("leaf certificate rotation dry-run stage complete", args...)
	}
}

func (m *Manager) emitDryRunStageMetric(result StageResult, req *ConnectCALeafRequest) {
	metrics.IncrCounterWithLabels(
		[]string{"leaf-certs", "dry_run", "stage"},
		1,
		[]metrics.Label{
			{Name: "datacenter", Value: req.Datacenter},
			{Name: "service", Value: req.Service},
			{Name: "kind", Value: string(req.Kind)},
			{Name: "stage", Value: string(result.Stage)},
			{Name: "result", Value: string(result.Status)},
		},
	)
}

func (m *Manager) emitDryRunMetric(outcome, result string, req *ConnectCALeafRequest) {
	metrics.IncrCounterWithLabels(
		[]string{"leaf-certs", "dry_run", outcome},
		1,
		[]metrics.Label{
			{Name: "datacenter", Value: req.Datacenter},
			{Name: "service", Value: req.Service},
			{Name: "kind", Value: string(req.Kind)},
			{Name: "result", Value: result},
		},
	)
}
