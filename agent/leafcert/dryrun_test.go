// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package leafcert

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/connect"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/sdk/testutil"
)

func dryRunReq(service string) *ConnectCALeafRequest {
	return &ConnectCALeafRequest{
		Datacenter:    "dc1",
		Service:       service,
		MaxQueryTime:  5 * time.Second,
		MinQueryIndex: 0,
	}
}

// activeCertSnapshot reads the currently installed active cert for the key
// without driving a refresh.
func activeCertSnapshot(t *testing.T, m *Manager, req *ConnectCALeafRequest) *structs.IssuedCert {
	t.Helper()
	cd := m.getCertData(req.Key())
	cd.lock.Lock()
	defer cd.lock.Unlock()
	require.NotNil(t, cd.value, "expected an active cert for %s", req.Service)
	return cd.value
}

func requireDryRunError(t *testing.T, err error, stage Stage, reason string) *DryRunError {
	t.Helper()
	require.Error(t, err)
	var de *DryRunError
	require.True(t, errors.As(err, &de), "expected *DryRunError, got %T: %v", err, err)
	require.Equal(t, stage, de.Stage, "unexpected failure stage: %v", de)
	require.Equal(t, reason, de.Reason, "unexpected failure reason: %v", de)
	return de
}

func requireAllStagesPass(t *testing.T, report *RotationReport) {
	t.Helper()
	require.NotNil(t, report)
	require.Len(t, report.Stages, len(dryRunStages))
	for _, s := range report.Stages {
		require.Equal(t, StagePass, s.Status, "stage %s not pass: %+v", s.Stage, s)
	}
	require.True(t, report.Succeeded())
}

// AC-1: a happy-path dry run executes all stages, really signs once, reports
// cache-hit state and never replaces the active certificate.
func TestManager_DryRunRotate_happyPath(t *testing.T) {
	t.Parallel()

	m, signer := NewTestManager(t, nil)
	ca := signer.UpdateCA(t, nil)
	req := dryRunReq("web")

	// Prime the real cache.
	issued, meta, err := m.Get(context.Background(), req)
	require.NoError(t, err)
	require.False(t, meta.Hit)
	require.NotNil(t, issued)
	requireLeafValidUnderCA(t, issued, ca)

	signCallsBefore := signer.SignCallCount()

	report, err := m.DryRunRotate(context.Background(), req, DryRunOptions{})
	require.NoError(t, err)
	requireAllStagesPass(t, report)
	require.False(t, report.Resumed)

	// The signer was really exercised...
	require.Equal(t, signCallsBefore+1, signer.SignCallCount())

	// ...and the cache check reports a hit for the active cert.
	require.True(t, report.CacheCheck.HasActiveCert)
	require.True(t, report.CacheCheck.CacheHit)
	require.Equal(t, issued.SerialNumber, report.CacheCheck.SerialNumber)
	require.NotEmpty(t, report.CacheCheck.AuthorityKeyID)
	require.False(t, report.CacheCheck.Expired)

	// The candidate is a different, freshly issued cert that chains to the
	// active root.
	require.NotNil(t, report.Candidate)
	require.NotEqual(t, issued.SerialNumber, report.Candidate.SerialNumber)
	require.True(t, report.Candidate.ChainsToActiveRoot)
	require.Equal(t, ca.ID, report.Candidate.RootID)

	// No root rotation was detected.
	require.False(t, report.Overlap.RotationInProgress)

	// The active cert is untouched and a subsequent non-blocking Get is served
	// the same cached cert without any new signing RPC.
	activeAfter := activeCertSnapshot(t, m, req)
	require.Equal(t, issued.SerialNumber, activeAfter.SerialNumber)

	issuedAgain, _, err := m.Get(context.Background(), dryRunReq("web"))
	require.NoError(t, err)
	require.Equal(t, issued.SerialNumber, issuedAgain.SerialNumber)
	require.Equal(t, signCallsBefore+1, signer.SignCallCount())
}

// AC-2: a cold key reports a cache miss but issuing and chain verification
// still run successfully.
func TestManager_DryRunRotate_coldKeyReportsCacheMiss(t *testing.T) {
	t.Parallel()

	m, signer := NewTestManager(t, nil)
	ca := signer.UpdateCA(t, nil)
	req := dryRunReq("cold")

	report, err := m.DryRunRotate(context.Background(), req, DryRunOptions{})
	require.NoError(t, err)
	requireAllStagesPass(t, report)

	require.False(t, report.CacheCheck.HasActiveCert)
	require.False(t, report.CacheCheck.CacheHit)
	require.Empty(t, report.CacheCheck.SerialNumber)
	require.False(t, report.Overlap.RotationInProgress)
	require.True(t, report.Candidate.ChainsToActiveRoot)
	require.Equal(t, ca.ID, report.Candidate.RootID)

	// Dry run must not have installed anything as the active cert.
	cd := m.getCertData(req.Key())
	cd.lock.Lock()
	require.Nil(t, cd.value)
	cd.lock.Unlock()
}

// AC-3: during a root CA rolling update both roots remain trusted inside the
// explicit overlap window; the candidate chains to the new root while the
// active cert still verifies under the old root.
func TestManager_DryRunRotate_rootRotationOverlap(t *testing.T) {
	t.Parallel()

	m, signer := NewTestManager(t, nil)
	ca1 := signer.UpdateCA(t, nil)
	req := dryRunReq("web")

	issued1, _, err := m.Get(context.Background(), req)
	require.NoError(t, err)
	requireLeafValidUnderCA(t, issued1, ca1)

	// Rotate to a new active root; the test signer keeps the old root in the
	// bundle (Active=false), exactly like a real rolling update overlap.
	ca2 := signer.UpdateCA(t, nil)

	report, err := m.DryRunRotate(context.Background(), req, DryRunOptions{})
	require.NoError(t, err)
	requireAllStagesPass(t, report)

	require.True(t, report.Overlap.RotationInProgress)
	require.Equal(t, ca1.ID, report.Overlap.SigningRootID)
	require.Equal(t, ca2.ID, report.Overlap.ActiveRootID)
	require.True(t, report.Overlap.OldRootPresent)
	require.Equal(t, DefaultRootRotationOverlapWindow, report.Overlap.Window)
	require.True(t, report.Overlap.WithinWindow)
	require.True(t, report.Overlap.Remaining > 0)
	require.False(t, report.Overlap.StartsAt.IsZero())
	require.True(t, report.Overlap.EndsAt.After(report.Overlap.StartsAt))

	require.True(t, report.Candidate.ChainsToActiveRoot)
	require.Equal(t, ca2.ID, report.Candidate.RootID)
	require.NotEqual(t, issued1.SerialNumber, report.Candidate.SerialNumber)

	// Active cert is untouched and still valid under the (still trusted) old
	// root.
	activeAfter := activeCertSnapshot(t, m, req)
	require.Equal(t, issued1.SerialNumber, activeAfter.SerialNumber)
	requireLeafValidUnderCA(t, activeAfter, ca1)
}

// AC-4: when the old root disappears from the bundle while still inside the
// overlap window the roots_overlap stage fails with a stable reason.
func TestManager_DryRunRotate_overlapViolationOldRootMissing(t *testing.T) {
	t.Parallel()

	m, signer := NewTestManager(t, nil)
	ca1 := signer.UpdateCA(t, nil)
	req := dryRunReq("web")

	issued1, _, err := m.Get(context.Background(), req)
	require.NoError(t, err)
	requireLeafValidUnderCA(t, issued1, ca1)

	ca2 := signer.UpdateCA(t, nil)

	// Simulate an operator/server mistake: only the new root is distributed.
	signer.RootsReader.Set(&structs.IndexedCARoots{
		ActiveRootID: ca2.ID,
		TrustDomain:  connect.TestTrustDomain,
		Roots:        []*structs.CARoot{ca2},
		QueryMeta:    structs.QueryMeta{Index: 999},
	})

	report, err := m.DryRunRotate(context.Background(), req, DryRunOptions{})
	requireDryRunError(t, err, StageRootsOverlap, ReasonOverlapViolation)
	require.NotNil(t, report)

	require.True(t, report.Overlap.RotationInProgress)
	require.False(t, report.Overlap.OldRootPresent)
	require.True(t, report.Overlap.WithinWindow)

	require.Equal(t, StagePass, report.Stages[0].Status)
	require.Equal(t, StageFail, report.Stages[1].Status)
	require.Len(t, report.Stages, 2, "later stages must not run after a failure")
	require.Nil(t, report.Candidate)

	// The active cert was not replaced.
	require.Equal(t, issued1.SerialNumber,
		activeCertSnapshot(t, m, req).SerialNumber)
}

// Once the explicit overlap window has elapsed, the overlap constraint no
// longer applies (the rotation is considered complete), so a dry-run passes.
// The window is anchored deterministically at the old root's RotatedOutAt.
func TestManager_DryRunRotate_overlapWindowElapsed(t *testing.T) {
	t.Parallel()

	m, signer := NewTestManager(t, func(cfg *Config) {
		cfg.RootRotationOverlapWindow = time.Hour
	})
	ca1 := signer.UpdateCA(t, nil)
	req := dryRunReq("web")

	issued1, _, err := m.Get(context.Background(), req)
	require.NoError(t, err)
	requireLeafValidUnderCA(t, issued1, ca1)

	// Rotate, then stamp the old root copy that is actually held in the
	// roots bundle (UpdateCA keeps a clone, not the live CA pointer) as rotated
	// out well before the (1 hour) overlap window.
	ca2 := signer.UpdateCA(t, nil)
	bundle, err := signer.RootsReader.Get()
	require.NoError(t, err)
	for _, r := range bundle.Roots {
		if r.ID == ca1.ID {
			r.RotatedOutAt = time.Now().Add(-2 * time.Hour)
		}
	}
	signer.RootsReader.Set(bundle)

	report, err := m.DryRunRotate(context.Background(), req, DryRunOptions{})
	require.NoError(t, err)
	requireAllStagesPass(t, report)

	require.True(t, report.Overlap.RotationInProgress)
	require.True(t, report.Overlap.OldRootPresent)
	require.False(t, report.Overlap.WithinWindow)
	require.True(t, report.Overlap.Remaining < 0)
	require.Equal(t, ca1.ID, report.Overlap.SigningRootID)
	require.True(t, report.Candidate.ChainsToActiveRoot)
	require.Equal(t, ca2.ID, report.Candidate.RootID)
}

// AC-5: roots unavailable fails the roots_overlap stage before any signing.
func TestManager_DryRunRotate_rootsUnavailable(t *testing.T) {
	t.Parallel()

	m, signer := NewTestManager(t, nil)
	// Deliberately do not call UpdateCA: roots are nil.

	before := signer.SignCallCount()
	report, err := m.DryRunRotate(context.Background(), dryRunReq("web"), DryRunOptions{})
	requireDryRunError(t, err, StageRootsOverlap, ReasonRootsUnavailable)
	require.NotNil(t, report)
	require.False(t, report.CacheCheck.HasActiveCert)
	require.Equal(t, before, signer.SignCallCount(), "no signing must happen before roots load")
}

// AC-5: a signer RPC error fails the issue stage with a stable reason.
func TestManager_DryRunRotate_signingFailure(t *testing.T) {
	t.Parallel()

	m, signer := NewTestManager(t, nil)
	signer.UpdateCA(t, nil)
	signer.SetSignCallErrors(errors.New("CA unavailable"))

	before := signer.SignCallCount()
	report, err := m.DryRunRotate(context.Background(), dryRunReq("web"), DryRunOptions{})
	requireDryRunError(t, err, StageIssue, ReasonSigningFailed)
	require.NotNil(t, report)
	require.Equal(t, before+1, signer.SignCallCount())

	require.Len(t, report.Stages, 3)
	require.Equal(t, StagePass, report.Stages[0].Status)
	require.Equal(t, StagePass, report.Stages[1].Status)
	require.Equal(t, StageFail, report.Stages[2].Status)
	require.Nil(t, report.Candidate)
}

// AC-5: an expired candidate is caught by the chain_verify stage even though
// the signer "successfully" returned a certificate.
func TestManager_DryRunRotate_expiredCandidateFailsChainVerify(t *testing.T) {
	t.Parallel()

	m, signer := NewTestManager(t, nil)
	signer.UpdateCA(t, nil)
	signer.SetSignCallErrors(ReplyWithExpiredCert)

	before := signer.SignCallCount()
	report, err := m.DryRunRotate(context.Background(), dryRunReq("web"), DryRunOptions{})
	requireDryRunError(t, err, StageChainVerify, ReasonCandidateExpired)
	require.NotNil(t, report)
	require.Equal(t, before+1, signer.SignCallCount())

	require.Len(t, report.Stages, 4)
	require.Equal(t, StagePass, report.Stages[2].Status, "issue stage did sign")
	require.Equal(t, StageFail, report.Stages[3].Status)
}

// AC-6: after a failure, Resume skips completed stages and re-runs the failed
// stage; a fresh run re-executes everything. A completed checkpoint is fully
// skippable, and a chain-verify failure deterministically repeats while the
// same candidate checkpoint is reused.
func TestManager_DryRunRotate_resume(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m, signer := NewTestManager(t, nil)
	signer.UpdateCA(t, nil)
	req := dryRunReq("web")

	// First attempt fails at issue.
	signer.SetSignCallErrors(errors.New("boom"))
	r1, err := m.DryRunRotate(ctx, req, DryRunOptions{})
	requireDryRunError(t, err, StageIssue, ReasonSigningFailed)
	require.Len(t, r1.Stages, 3)
	require.Equal(t, 1, signer.SignCallCount(), "exactly one failed sign so far")
	failedSigns := signer.SignCallCount()

	// Resume after the failure is cleared: roots/cache stages are skipped.
	r2, err := m.DryRunRotate(ctx, req, DryRunOptions{Resume: true})
	require.NoError(t, err)
	require.True(t, r2.Resumed)
	require.Equal(t, failedSigns+1, signer.SignCallCount(), "only one new signing attempt")

	statuses := map[Stage]StageStatus{}
	for _, s := range r2.Stages {
		statuses[s.Stage] = s.Status
	}
	require.Equal(t, StageSkipped, statuses[StageCacheCheck])
	require.Equal(t, StageSkipped, statuses[StageRootsOverlap])
	require.Equal(t, StagePass, statuses[StageIssue])
	require.Equal(t, StagePass, statuses[StageChainVerify])
	require.NotNil(t, r2.Candidate)
	candidateSerial := r2.Candidate.SerialNumber

	// Resuming a fully completed run skips every stage and signs nothing; a
	// nil error still means the overall check succeeded.
	r3, err := m.DryRunRotate(ctx, req, DryRunOptions{Resume: true})
	require.NoError(t, err)
	require.True(t, r3.Succeeded())
	require.True(t, r3.Resumed)
	require.Equal(t, failedSigns+1, signer.SignCallCount())
	for _, s := range r3.Stages {
		require.Equal(t, StageSkipped, s.Status)
	}
	require.Equal(t, candidateSerial, r3.Candidate.SerialNumber)

	// A non-resume run discards the checkpoint and executes everything again.
	r4, err := m.DryRunRotate(ctx, req, DryRunOptions{})
	require.NoError(t, err)
	require.False(t, r4.Resumed)
	requireAllStagesPass(t, r4)
	require.Equal(t, failedSigns+2, signer.SignCallCount())
	require.NotEqual(t, candidateSerial, r4.Candidate.SerialNumber)

	// ClearDryRunState makes a subsequent resume start from scratch.
	m.ClearDryRunState(req.Key())
	r5, err := m.DryRunRotate(ctx, req, DryRunOptions{Resume: true})
	require.NoError(t, err)
	require.False(t, r5.Resumed, "cleared checkpoint cannot be resumed")
	requireAllStagesPass(t, r5)
}

func TestManager_DryRunRotate_resumeChainFailureIsDeterministic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m, signer := NewTestManager(t, nil)
	signer.UpdateCA(t, nil)
	req := dryRunReq("web")

	signer.SetSignCallErrors(ReplyWithExpiredCert)
	_, err := m.DryRunRotate(ctx, req, DryRunOptions{})
	requireDryRunError(t, err, StageChainVerify, ReasonCandidateExpired)
	signsAfterFirst := signer.SignCallCount()

	// Resume reuses the (expired) candidate, so chain verification fails again
	// without another signing attempt.
	r2, err := m.DryRunRotate(ctx, req, DryRunOptions{Resume: true})
	requireDryRunError(t, err, StageChainVerify, ReasonCandidateExpired)
	require.True(t, r2.Resumed)
	require.Equal(t, signsAfterFirst, signer.SignCallCount())
	for _, s := range r2.Stages {
		require.NotEqual(t, StagePass, s.Status)
	}

	// A fresh run issues a new (valid) candidate and passes.
	r3, err := m.DryRunRotate(ctx, req, DryRunOptions{})
	require.NoError(t, err)
	requireAllStagesPass(t, r3)
}

// AC-7: dry-run state lives only in the Manager process. A restarted manager
// never inherits the candidate or checkpoint, and the previously active cert
// was never replaced.
func TestManager_DryRunRotate_restartAcrossManagers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	signer := newTestSigner(t, nil, nil)
	signer.UpdateCA(t, nil)

	depsFor := func() Deps {
		return Deps{
			Logger:      testutil.Logger(t),
			Datacenter:  "dc1",
			RootsReader: signer.RootsReader,
			CertSigner:  signer,
			Config: Config{
				TestOverrideCAChangeInitialDelay: time.Microsecond,
			},
		}
	}

	m1 := NewManager(depsFor())
	req := dryRunReq("web")

	issued1, _, err := m1.Get(ctx, req)
	require.NoError(t, err)

	r1, err := m1.DryRunRotate(ctx, req, DryRunOptions{})
	require.NoError(t, err)
	requireAllStagesPass(t, r1)
	require.NotEqual(t, issued1.SerialNumber, r1.Candidate.SerialNumber)

	// The active cert on m1 is still the original.
	require.Equal(t, issued1.SerialNumber, activeCertSnapshot(t, m1, req).SerialNumber)

	m1.Stop()

	// A brand new manager shares the signer/roots (as after an agent restart it
	// would re-discover them) but has no in-memory dry-run checkpoint.
	m2 := NewManager(depsFor())
	t.Cleanup(m2.Stop)

	r2, err := m2.DryRunRotate(ctx, req, DryRunOptions{Resume: true})
	require.NoError(t, err)
	require.False(t, r2.Resumed, "dry-run checkpoint must not survive a manager restart")
	requireAllStagesPass(t, r2)

	// Normal rotation on the restarted manager works independently.
	issued2, _, err := m2.Get(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, issued1.SerialNumber, issued2.SerialNumber)
	require.Equal(t, issued2.SerialNumber, activeCertSnapshot(t, m2, req).SerialNumber)
}

// AC-8: with an expired active cert cached, the dry run reports expiry, issues
// and verifies a valid candidate, but neither replaces nor repairs the active
// cert; the real Get path subsequently performs the replacement.
func TestManager_DryRunRotate_expiredActiveCert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m, signer := NewTestManager(t, nil)
	ca := signer.UpdateCA(t, nil)
	req := dryRunReq("web")

	// Prime the cache with an already-expired cert.
	signer.SetSignCallErrors(ReplyWithExpiredCert)
	expired, _, err := m.Get(ctx, req)
	require.NoError(t, err)
	require.True(t, time.Now().After(expired.ValidBefore))

	report, err := m.DryRunRotate(ctx, req, DryRunOptions{})
	require.NoError(t, err)
	requireAllStagesPass(t, report)

	require.True(t, report.CacheCheck.HasActiveCert)
	require.True(t, report.CacheCheck.Expired)
	require.Equal(t, expired.SerialNumber, report.CacheCheck.SerialNumber)
	require.True(t, report.Candidate.ValidBefore.After(time.Now()))

	// Dry run did not repair anything: the expired cert is still active.
	stillActive := activeCertSnapshot(t, m, req)
	require.Equal(t, expired.SerialNumber, stillActive.SerialNumber)
	require.True(t, time.Now().After(stillActive.ValidBefore))

	// The real rotation path replaces it with a valid cert.
	renewed, _, err := m.Get(ctx, dryRunReq("web"))
	require.NoError(t, err)
	require.NotEqual(t, expired.SerialNumber, renewed.SerialNumber)
	require.True(t, renewed.ValidBefore.After(time.Now()))
	requireLeafValidUnderCA(t, renewed, ca)
}

// delayingSigner adds a fixed delay to each signing RPC. In the concurrency
// test this widens the window during which same-key dry-runs enter the
// singleflight group so coalescing can be asserted deterministically without
// relying on scheduler timing.
type delayingSigner struct {
	inner CertSigner
	delay time.Duration
}

func (s *delayingSigner) SignCert(ctx context.Context, req *structs.CASignRequest) (*structs.IssuedCert, error) {
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.inner.SignCert(ctx, req)
}

// AC-9: many proxies (distinct service keys) run concurrent dry-runs; same-key
// dry-runs coalesce into a single signing, distinct keys stay independent, and
// concurrent non-blocking Gets keep serving the unaffected active certs.
func TestManager_DryRunRotate_multipleProxiesConcurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	signer := newTestSigner(t, nil, nil)
	signer.UpdateCA(t, nil)
	m := NewManager(Deps{
		Logger:      testutil.Logger(t),
		Datacenter:  "dc1",
		RootsReader: signer.RootsReader,
		CertSigner:  &delayingSigner{inner: signer, delay: 25 * time.Millisecond},
		Config:      Config{TestOverrideCAChangeInitialDelay: time.Microsecond},
	})
	t.Cleanup(m.Stop)

	const numProxies = 10
	const concurrentChecksPerProxy = 5

	// Warm a real active cert for every proxy first.
	reqs := make([]*ConnectCALeafRequest, numProxies)
	serials := make([]string, numProxies)
	for i := 0; i < numProxies; i++ {
		reqs[i] = dryRunReq(fmt.Sprintf("web-%d", i))
		issued, _, err := m.Get(ctx, reqs[i])
		require.NoError(t, err)
		serials[i] = issued.SerialNumber
	}

	signCallsAfterWarmup := signer.SignCallCount()

	var (
		wg      sync.WaitGroup
		gate    = make(chan struct{})
		reports = make(chan *RotationReport, numProxies*concurrentChecksPerProxy)
		errs    = make(chan error, cap(reports))
	)
	wg.Add(numProxies*concurrentChecksPerProxy + numProxies*2)

	for i := 0; i < numProxies; i++ {
		for j := 0; j < concurrentChecksPerProxy; j++ {
			go func(req *ConnectCALeafRequest) {
				defer wg.Done()
				<-gate
				report, err := m.DryRunRotate(ctx, req, DryRunOptions{})
				if err != nil {
					errs <- err
					return
				}
				reports <- report
			}(reqs[i])
		}
		// Mix in concurrent non-blocking Gets, which must keep serving the
		// pre-dry-run cached cert without re-signing.
		for j := 0; j < 2; j++ {
			go func(req *ConnectCALeafRequest, wantSerial string) {
				defer wg.Done()
				<-gate
				issued, _, err := m.Get(ctx, &ConnectCALeafRequest{
					Datacenter:     req.Datacenter,
					Service:        req.Service,
					EnterpriseMeta: req.EnterpriseMeta,
					MinQueryIndex:  0,
					MaxQueryTime:   5 * time.Second,
				})
				if err != nil {
					errs <- err
					return
				}
				if issued == nil || issued.SerialNumber != wantSerial {
					errs <- fmt.Errorf("concurrent Get for %s did not serve the stable cached cert",
						req.Service)
				}
			}(reqs[i], serials[i])
		}
	}

	close(gate)
	wg.Wait()
	close(reports)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	// Exactly one dry-run signing per proxy key.
	require.Equal(t, signCallsAfterWarmup+numProxies, signer.SignCallCount())

	count := 0
	for report := range reports {
		requireAllStagesPass(t, report)
		require.True(t, report.CacheCheck.CacheHit)
		count++
	}
	require.Equal(t, numProxies*concurrentChecksPerProxy, count)

	// All active certs remain the pre-dry-run ones.
	for i, req := range reqs {
		require.Equal(t, serials[i], activeCertSnapshot(t, m, req).SerialNumber)
	}
}

// AC-10: stage results are available on a failure and every stage carries a
// status and, on failure, a stable reason.
func TestManager_DryRunRotate_reportShape(t *testing.T) {
	t.Parallel()

	m, signer := NewTestManager(t, nil)
	signer.UpdateCA(t, nil)
	signer.SetSignCallErrors(errors.New("nope"))

	report, err := m.DryRunRotate(
		context.Background(), dryRunReq("web"), DryRunOptions{})
	require.Error(t, err)
	require.NotNil(t, report)
	require.False(t, report.StartedAt.IsZero())
	require.NotEmpty(t, report.Key)

	var sawFail bool
	for _, s := range report.Stages {
		require.NotEmpty(t, s.Stage)
		require.NotEmpty(t, s.Status)
		if s.Status == StageFail {
			sw := sawFail
			require.False(t, sw, "only one failing stage expected")
			sawFail = true
			require.NotEmpty(t, s.Reason)
			require.NotEmpty(t, s.Detail)
			require.Equal(t, StageIssue, s.Stage)
		}
	}
	require.True(t, sawFail)
}

// A roots bundle without any active root fails the roots stage before
// signing.
func TestManager_DryRunRotate_noActiveRoot(t *testing.T) {
	t.Parallel()

	m, signer := NewTestManager(t, nil)
	ca := signer.UpdateCA(t, nil)
	ca.Active = false
	signer.RootsReader.Set(&structs.IndexedCARoots{
		// ActiveRootID intentionally does not match the single inactive root.
		ActiveRootID: "does-not-exist",
		TrustDomain:  connect.TestTrustDomain,
		Roots:        []*structs.CARoot{ca},
	})

	before := signer.SignCallCount()
	_, err := m.DryRunRotate(context.Background(), dryRunReq("web"), DryRunOptions{})
	requireDryRunError(t, err, StageRootsOverlap, ReasonNoActiveRoot)
	require.Equal(t, before, signer.SignCallCount())
}

// Drives the overlap boundary with an injected clock (Config.TestOverrideNow):
// while inside the window a removed old root is an overlap violation; once the
// window has elapsed (resumed run reusing the first-observation anchor) that
// specific violation is gone, but the active cert can no longer chain because
// the only trusted root is the new one.
func TestManager_DryRunRotate_overlapElapsedOldRootRemovedActiveChainInvalid(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now()
	cur := now
	m, signer := NewTestManager(t, func(cfg *Config) {
		cfg.RootRotationOverlapWindow = time.Hour
		cfg.TestOverrideNow = func() time.Time { return cur }
	})
	ca1 := signer.UpdateCA(t, nil)
	req := dryRunReq("web")

	issued1, _, err := m.Get(ctx, req)
	require.NoError(t, err)
	requireLeafValidUnderCA(t, issued1, ca1)

	ca2 := signer.UpdateCA(t, nil)
	signer.RootsReader.Set(&structs.IndexedCARoots{
		ActiveRootID: ca2.ID,
		TrustDomain:  connect.TestTrustDomain,
		Roots:        []*structs.CARoot{ca2},
		QueryMeta:    structs.QueryMeta{Index: 999},
	})

	// First attempt, inside the window: overlap violation with positive
	// remaining time.
	r1, err := m.DryRunRotate(ctx, req, DryRunOptions{})
	requireDryRunError(t, err, StageRootsOverlap, ReasonOverlapViolation)
	require.True(t, r1.Overlap.WithinWindow)
	require.True(t, r1.Overlap.Remaining > 0)

	// Advance beyond the window and resume, which re-evaluates the roots stage
	// but keeps the original rotation-start anchor.
	cur = now.Add(2 * time.Hour)
	r2, err := m.DryRunRotate(ctx, req, DryRunOptions{Resume: true})
	requireDryRunError(t, err, StageRootsOverlap, ReasonActiveChainInvalid)
	require.False(t, r2.Overlap.WithinWindow)
	require.True(t, r2.Overlap.Remaining < 0)
	require.True(t, r2.Resumed)
}

// Clearing a dry-run checkpoint concurrently with running checks must never
// corrupt the active cert and must not leave a "resurrected" checkpoint that a
// later resume can continue from.
func TestManager_DryRunRotate_clearConcurrentWithRuns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m, signer := NewTestManager(t, nil)
	signer.UpdateCA(t, nil)
	req := dryRunReq("web")

	issued, _, err := m.Get(ctx, req)
	require.NoError(t, err)

	var wg sync.WaitGroup
	gate := make(chan struct{})
	key := req.Key()

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			_, _ = m.DryRunRotate(ctx, req, DryRunOptions{})
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			m.ClearDryRunState(key)
		}()
	}

	close(gate)
	wg.Wait()

	// The active cert is whatever it was, never a dry-run candidate.
	require.Equal(t, issued.SerialNumber, activeCertSnapshot(t, m, req).SerialNumber)

	// After the storm settles, clear once more and resume: there must be no
	// checkpoint to continue from, so the run starts fresh.
	m.ClearDryRunState(key)
	report, err := m.DryRunRotate(ctx, req, DryRunOptions{Resume: true})
	require.NoError(t, err)
	require.False(t, report.Resumed)
	requireAllStagesPass(t, report)
	require.Equal(t, issued.SerialNumber, activeCertSnapshot(t, m, req).SerialNumber)
}

// Deterministically drives the exact I-3 interleaving: the finishing dry-run
// holds cd.lock and is parked between its abandoned check and the pointer
// assignment; ClearDryRunState must then block on that lock and nil the
// pointer only after commit releases it, so a cleared checkpoint can never be
// resurrected.
func TestManager_DryRunRotate_clearDuringCommitDoesNotResurrect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m, signer := NewTestManager(t, nil)
	signer.UpdateCA(t, nil)
	req := dryRunReq("web")

	issued, _, err := m.Get(ctx, req)
	require.NoError(t, err)

	atCommit := make(chan struct{})
	proceed := make(chan struct{})
	m.testDryRunBeforeCommit = func(cp *dryRunCheckpoint) {
		atCommit <- struct{}{}
		<-proceed
	}

	type runResult struct {
		report *RotationReport
		err    error
	}
	runDone := make(chan runResult, 1)
	go func() {
		report, err := m.DryRunRotate(ctx, req, DryRunOptions{})
		runDone <- runResult{report, err}
	}()

	<-atCommit
	// Commit holds cd.lock here. Start a concurrent Clear: it must block until
	// commit finishes, then observe the abandoned flag/nil sequence.
	clearDone := make(chan struct{})
	go func() {
		m.ClearDryRunState(req.Key())
		close(clearDone)
	}()

	// Give the Clear goroutine a chance to block on cd.lock (it cannot commit
	// until proceed because the lock is held by the parked run).
	select {
	case <-clearDone:
		t.Fatal("Clear completed while the commit still held the lock")
	case <-time.After(20 * time.Millisecond):
	}

	close(proceed)
	<-clearDone
	res := <-runDone
	require.NoError(t, res.err)
	requireAllStagesPass(t, res.report)

	// Uninstall the hook before any further runs.
	m.testDryRunBeforeCommit = nil

	// The Clear ran after commit, so the committed checkpoint must end up nil
	// rather than resurrected.
	cd := m.getCertData(req.Key())
	require.Nil(t, cd.getDryRunCheckpoint())

	// A resume therefore starts from scratch...
	resumed, err := m.DryRunRotate(ctx, req, DryRunOptions{Resume: true})
	require.NoError(t, err)
	require.False(t, resumed.Resumed)
	requireAllStagesPass(t, resumed)

	// ...and the active cert was never replaced.
	require.Equal(t, issued.SerialNumber, activeCertSnapshot(t, m, req).SerialNumber)
}
