package workflowprobe

import (
	"net/http"
	"sync"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func withMaxLeaseTotal(value time.Duration) func(*harnessOption) {
	return func(option *harnessOption) {
		option.maxLeaseTotal = value
	}
}

func runLeaseRenewal() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-renew-01", "renew"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "renewal.example", 3001, 1, 1, 100)
	if err != nil {
		return err
	}

	group, err := allocateLease(harness, plan.ID, "renew-alloc-0001", "renew", 300, 1)
	if err != nil {
		return err
	}
	leaseID := group.LeaseIDs[0]

	// A claimed lease can be extended before the deadline.
	firstDeadline := harness.clock.Now().Add(300 * time.Second)
	renewed, err := renewLease(harness, leaseID, "renew-request-0001", 120, "probe still running")
	if err != nil {
		return err
	}
	if renewed.State != domain.LeaseClaimed || renewed.RenewalCount != 1 {
		return ensure(false, "first renewal was not recorded")
	}
	wantDeadline := firstDeadline.Add(120 * time.Second)
	if !renewed.ExpiresAt.Equal(wantDeadline) {
		return ensure(false, "renewal did not extend the current deadline")
	}
	if len(renewed.Renewals) != 1 {
		return ensure(false, "renewal history must contain one record")
	}
	record := renewed.Renewals[0]
	if record.Sequence != 1 || !record.PreviousExpiresAt.Equal(firstDeadline) ||
		!record.ExpiresAt.Equal(wantDeadline) || record.Basis != "probe still running" ||
		record.RequestKey != "renew-request-0001" || record.RequestFingerprint == "" {
		return ensure(false, "renewal record did not preserve count, previous deadline, or basis")
	}

	// Replaying the same request key with the same content is a safe no-op.
	replay, err := renewLease(harness, leaseID, "renew-request-0001", 120, "probe still running")
	if err != nil {
		return err
	}
	if replay.RenewalCount != 1 || !replay.ExpiresAt.Equal(wantDeadline) {
		return ensure(false, "renewal replay must not extend the lease again")
	}

	// The same request key with different parameters conflicts.
	_, err = renewLease(harness, leaseID, "renew-request-0001", 60, "probe still running")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}

	// A second, distinct request extends again and appends history.
	renewed, err = renewLease(harness, leaseID, "renew-request-0002", 60, "waiting on resolver")
	if err != nil {
		return err
	}
	if renewed.RenewalCount != 2 || !renewed.ExpiresAt.Equal(wantDeadline.Add(60*time.Second)) {
		return ensure(false, "second renewal was not applied correctly")
	}
	if renewed.Renewals[1].Sequence != 2 || !renewed.Renewals[1].PreviousExpiresAt.Equal(wantDeadline) {
		return ensure(false, "second renewal must reference the previous deadline")
	}
	// Renewing after the deadline is rejected, even without a background sweep.
	harness.clock.Advance(481 * time.Second)
	_, err = renewLease(harness, leaseID, "renew-request-0003", 60, "too late")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseExpired)); err != nil {
		return err
	}
	// Once expired, the node is released and the lease can never be revived.
	_, err = renewLease(harness, leaseID, "renew-request-0004", 60, "still too late")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseExpired)); err != nil {
		return err
	}

	// Probe submission must judge the renewed deadline atomically: this lease
	// is expired, so reporting it must fail and must not resurrect it.
	_, err = submitProbe(harness, leaseID, "renew-probe-0001", true, revision.Serial, 10)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
		return err
	}

	// The expired node can be re-allocated, producing a new independent lease.
	nextGroup, err := allocateLease(harness, plan.ID, "renew-alloc-0002", "renew", 300, 1)
	if err != nil {
		return err
	}
	nextLeaseID := nextGroup.LeaseIDs[0]
	if nextLeaseID == leaseID {
		return ensure(false, "re-allocation must create a fresh lease")
	}
	// The old lease cannot be renewed after its node moved to a new lease.
	_, err = renewLease(harness, leaseID, "renew-request-0005", 60, "stale holder")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseExpired)); err != nil {
		return err
	}

	// Completing the plan blocks renewal of its remaining claimed lease.
	var completed domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": plan.Version,
	}, &completed); err != nil {
		return err
	}
	_, err = renewLease(harness, nextLeaseID, "renew-request-0006", 60, "plan completed")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// Released and reported leases cannot be renewed either.
	if err := verifyReportedAndReleasedRejectRenewal(); err != nil {
		return err
	}

	// The server-side total cap and concurrent renewals get their own harness.
	if err := verifyRenewalCapAndConcurrency(); err != nil {
		return err
	}
	return nil
}

func verifyReportedAndReleasedRejectRenewal() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-renew-02", "south"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-renew-03", "south"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "renewed.example", 3002, 2, 0.5, 100)
	if err != nil {
		return err
	}
	group, err := allocateLease(harness, plan.ID, "renew-alloc-0010", "south", 300, 2)
	if err != nil {
		return err
	}

	if _, err := submitProbe(harness, group.LeaseIDs[0], "renew-probe-0010", true, revision.Serial, 5); err != nil {
		return err
	}
	_, err = renewLease(harness, group.LeaseIDs[0], "renew-after-report-1", 60, "reported")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
		return err
	}

	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+group.LeaseIDs[1]+"/release", nil, &released); err != nil {
		return err
	}
	_, err = renewLease(harness, group.LeaseIDs[1], "renew-after-release-1", 60, "released")
	return expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed))
}

func verifyRenewalCapAndConcurrency() error {
	// Cap the whole lease (claim plus renewals) at 300 seconds.
	harness, err := newHarness(withMaxLeaseTotal(300 * time.Second))
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-renew-04", "west"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "cap.example", 3003, 1, 1, 100)
	if err != nil {
		return err
	}
	group, err := allocateLease(harness, plan.ID, "renew-alloc-0020", "west", 60, 1)
	if err != nil {
		return err
	}
	leaseID := group.LeaseIDs[0]
	claimedAt := harness.clock.Now()

	// Concurrent replays of one request key collapse to a single renewal.
	const replicas = 12
	var wg sync.WaitGroup
	results := make([]domain.NodeLease, replicas)
	errorsList := make([]error, replicas)
	start := make(chan struct{})
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			results[index], errorsList[index] = renewLease(
				harness, leaseID, "renew-parallel-same", 60, "parallel replay")
		}(i)
	}
	close(start)
	wg.Wait()
	for _, callErr := range errorsList {
		if callErr != nil {
			return callErr
		}
	}
	for _, lease := range results {
		if lease.RenewalCount != 1 {
			return ensure(false, "concurrent identical renewals created more than one renewal")
		}
		if !lease.ExpiresAt.Equal(claimedAt.Add(120 * time.Second)) {
			return ensure(false, "concurrent identical renewals disagree on the deadline")
		}
	}

	// Concurrent distinct request keys serialize and each extends once, but
	// they cannot push the lease past the server-side maximum total duration.
	const distinct = 8
	parallelResults := make([]domain.NodeLease, distinct)
	parallelErrors := make([]error, distinct)
	parallelStart := make(chan struct{})
	for i := 0; i < distinct; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-parallelStart
			key := "renew-parallel-" + string(rune('a'+index)) + "-000000"
			parallelResults[index], parallelErrors[index] = renewLease(
				harness, leaseID, key, 60, "parallel extension")
		}(i)
	}
	close(parallelStart)
	wg.Wait()
	accepted := 0
	for _, callErr := range parallelErrors {
		if callErr == nil {
			accepted++
			continue
		}
		if err := expectFailure(callErr, http.StatusConflict, string(domain.CodeLeaseRenewalLimit)); err != nil {
			return err
		}
	}
	if accepted != 3 {
		return ensure(false, "expected exactly three concurrent renewals to fit under the total cap")
	}
	// 60s claim + one replay renewal + three distinct renewals == the 300s cap.
	final := parallelResults[0]
	for _, lease := range parallelResults {
		if lease.RenewalCount > final.RenewalCount {
			final = lease
		}
	}
	if final.RenewalCount != 4 || !final.ExpiresAt.Equal(claimedAt.Add(300*time.Second)) {
		return ensure(false, "concurrent renewals did not stop exactly at the total cap")
	}

	// Reaching the cap exactly is fine; one more second must be refused.
	_, err = renewLease(harness, leaseID, "renew-over-cap-00001", 30, "beyond cap")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseRenewalLimit)); err != nil {
		return err
	}

	// The lease is still claimed and usable right up to its capped deadline.
	var fetched domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+leaseID+"/release", nil, &fetched); err != nil {
		return err
	}
	if fetched.State != domain.LeaseReleased {
		return ensure(false, "capped lease could not be released after renewal")
	}
	return nil
}
