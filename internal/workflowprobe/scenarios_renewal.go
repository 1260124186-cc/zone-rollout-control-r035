package workflowprobe

import (
	"fmt"
	"net/http"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runLeaseRenewal() error {
	if err := runLeaseRenewalBasics(); err != nil {
		return err
	}
	if err := runLeaseRenewalCeiling(); err != nil {
		return err
	}
	if err := runLeaseRenewalConcurrency(); err != nil {
		return err
	}
	if err := runLeaseRenewalPlanState(); err != nil {
		return err
	}
	return nil
}

func runLeaseRenewalBasics() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-renew-01", "east"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "renew.example", 3001, 1, 0.5, 100)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "renew-lease-0001", "east", 1)
	if err != nil {
		return err
	}
	leaseID := group.LeaseIDs[0]

	// A fresh lease has no renewals.
	lease, err := getLease(harness, leaseID)
	if err != nil {
		return err
	}
	if lease.RenewalCount != 0 {
		return ensure(false, "fresh lease must have zero renewals")
	}
	originalExpiry := lease.ExpiresAt

	// First renewal records the full history.
	renewed, err := renewLease(harness, leaseID, "renew-request-0001", 600)
	if err != nil {
		return err
	}
	if renewed.Renewal.RenewalNumber != 1 {
		return ensure(false, "first renewal must be number 1")
	}
	if renewed.Renewal.PreviousExpiresAt != originalExpiry {
		return ensure(false, "renewal must record the previous expiry")
	}
	if renewed.Renewal.NewExpiresAt != originalExpiry.Add(600*time.Second) {
		return ensure(false, "renewal must extend by the requested duration")
	}
	if renewed.Renewal.ExtensionSeconds != 600 {
		return ensure(false, "renewal must record the granted extension")
	}
	if renewed.Renewal.Basis != domain.LeaseRenewalBasis {
		return ensure(false, "renewal must record the basis")
	}
	if renewed.Lease.RenewalCount != 1 {
		return ensure(false, "lease must show one renewal")
	}
	if renewed.Lease.ExpiresAt != originalExpiry.Add(600*time.Second) {
		return ensure(false, "lease expiry must match the renewal")
	}
	if renewed.Lease.LastRenewedAt == nil {
		return ensure(false, "lease must record the renewal time")
	}

	// Idempotent replay returns the same record without changing the lease.
	replay, err := renewLease(harness, leaseID, "renew-request-0001", 600)
	if err != nil {
		return err
	}
	if replay.Renewal.ID != renewed.Renewal.ID {
		return ensure(false, "renewal replay must return the same record")
	}
	if replay.Lease.RenewalCount != 1 {
		return ensure(false, "renewal replay must not increment the count")
	}

	// Same key with different parameters is a conflict.
	_, err = renewLease(harness, leaseID, "renew-request-0001", 300)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}

	// Renewal history is readable in renewal-number order.
	history, err := listRenewals(harness, leaseID)
	if err != nil {
		return err
	}
	if len(history) != 1 || history[0].ID != renewed.Renewal.ID {
		return ensure(false, "renewal history must contain the recorded renewal")
	}

	// Advance past the original expiry but before the renewed expiry.
	harness.clock.Advance(900 * time.Second)

	// Probe after renewal uses the new deadline.
	if _, err := submitProbe(harness, leaseID, "renew-probe-0001", true, revision.Serial, 20); err != nil {
		return err
	}

	// Lease read is consistent with the probe deadline.
	lease, err = getLease(harness, leaseID)
	if err != nil {
		return err
	}
	if lease.State != domain.LeaseReported {
		return ensure(false, "probed lease must be reported")
	}
	if lease.ExpiresAt != originalExpiry.Add(600*time.Second) {
		return ensure(false, "lease expiry must be unchanged after the probe")
	}

	// Reported lease cannot be renewed.
	_, err = renewLease(harness, leaseID, "renew-request-0002", 600)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// Released lease cannot be renewed.
	group2, err := allocate(harness, plan.ID, "renew-lease-0002", "east", 1)
	if err != nil {
		return err
	}
	lease2ID := group2.LeaseIDs[0]
	if err := harness.call(http.MethodPatch, "/v1/leases/"+lease2ID+"/release", nil, nil); err != nil {
		return err
	}
	_, err = renewLease(harness, lease2ID, "renew-request-0003", 600)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// Expired lease cannot be renewed; node reassignment does not revive it.
	group3, err := allocate(harness, plan.ID, "renew-lease-0003", "east", 1)
	if err != nil {
		return err
	}
	lease3ID := group3.LeaseIDs[0]
	harness.clock.Advance(1200 * time.Second)
	_, err = renewLease(harness, lease3ID, "renew-request-0004", 600)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseExpired)); err != nil {
		return err
	}
	// Reassign the node: the sweep frees the expired lease and allocates a new one.
	group4, err := allocate(harness, plan.ID, "renew-lease-0004", "east", 1)
	if err != nil {
		return err
	}
	if group4.ID == group3.ID {
		return ensure(false, "node reassignment must create a new lease group")
	}
	lease3, err := getLease(harness, lease3ID)
	if err != nil {
		return err
	}
	if lease3.State != domain.LeaseExpired {
		return ensure(false, "old lease must be expired after reassignment")
	}
	_, err = renewLease(harness, lease3ID, "renew-request-0005", 600)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseExpired)); err != nil {
		return err
	}
	return nil
}

func runLeaseRenewalCeiling() error {
	harness, err := newHarnessWithCeiling(3600 * time.Second)
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-ceil-01", "east"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "ceil.example", 4001, 1, 0.5, 100)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "ceil-lease-0001", "east", 1)
	if err != nil {
		return err
	}
	leaseID := group.LeaseIDs[0]
	lease, err := getLease(harness, leaseID)
	if err != nil {
		return err
	}
	ceiling := lease.MaxExpiresAt
	if ceiling.IsZero() {
		return ensure(false, "lease must record the server ceiling")
	}

	// Renewal within the ceiling extends in full.
	renewed, err := renewLease(harness, leaseID, "ceil-renew-0001", 1800)
	if err != nil {
		return err
	}
	if renewed.Lease.ExpiresAt != lease.ExpiresAt.Add(1800*time.Second) {
		return ensure(false, "renewal within ceiling must extend fully")
	}
	if renewed.Renewal.ExtensionSeconds != 1800 {
		return ensure(false, "renewal within ceiling must grant the full extension")
	}

	// Renewal that would overshoot is clamped to the ceiling.
	clamped, err := renewLease(harness, leaseID, "ceil-renew-0002", 1800)
	if err != nil {
		return err
	}
	if clamped.Lease.ExpiresAt != ceiling {
		return ensure(false, "renewal must be clamped to the ceiling")
	}
	if clamped.Renewal.ExtensionSeconds != 1200 {
		return ensure(false, "clamped renewal must grant the remaining seconds")
	}

	// Renewal already at the ceiling is rejected.
	_, err = renewLease(harness, leaseID, "ceil-renew-0003", 1800)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseRenewalLimit)); err != nil {
		return err
	}

	lease, err = getLease(harness, leaseID)
	if err != nil {
		return err
	}
	if lease.RenewalCount != 2 {
		return ensure(false, "only two renewals must be recorded")
	}
	return nil
}

func runLeaseRenewalConcurrency() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-conc-01", "east"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "conc.example", 5001, 1, 0.5, 100)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "conc-lease-0001", "east", 1)
	if err != nil {
		return err
	}
	leaseID := group.LeaseIDs[0]

	// Concurrent renewals with distinct keys: no lost updates.
	const distinctCount = 5
	errs := make(chan error, distinctCount)
	for i := 0; i < distinctCount; i++ {
		go func(i int) {
			_, err := renewLease(harness, leaseID, fmt.Sprintf("conc-distinct-%04d", i), 600)
			errs <- err
		}(i)
	}
	for i := 0; i < distinctCount; i++ {
		if err := <-errs; err != nil {
			return err
		}
	}
	lease, err := getLease(harness, leaseID)
	if err != nil {
		return err
	}
	if lease.RenewalCount != distinctCount {
		return ensure(false, "concurrent distinct renewals must not lose updates")
	}
	history, err := listRenewals(harness, leaseID)
	if err != nil {
		return err
	}
	if len(history) != distinctCount {
		return ensure(false, "all distinct renewals must be recorded")
	}

	// Concurrent renewals with the same key: idempotent replay.
	const sameCount = 5
	errs = make(chan error, sameCount)
	for i := 0; i < sameCount; i++ {
		go func() {
			_, err := renewLease(harness, leaseID, "conc-same-0001", 600)
			errs <- err
		}()
	}
	for i := 0; i < sameCount; i++ {
		if err := <-errs; err != nil {
			return err
		}
	}
	lease, err = getLease(harness, leaseID)
	if err != nil {
		return err
	}
	if lease.RenewalCount != distinctCount+1 {
		return ensure(false, "same-key concurrent renewals must add only one renewal")
	}
	history, err = listRenewals(harness, leaseID)
	if err != nil {
		return err
	}
	if len(history) != distinctCount+1 {
		return ensure(false, "same-key replay must not create duplicate records")
	}
	return nil
}

func runLeaseRenewalPlanState() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-plan-01", "east"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "planstate.example", 6001, 1, 0.5, 100)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "planstate-lease-0001", "east", 1)
	if err != nil {
		return err
	}
	leaseID := group.LeaseIDs[0]

	// Completing the plan blocks renewal.
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": plan.Version,
	}, &plan); err != nil {
		return err
	}
	_, err = renewLease(harness, leaseID, "planstate-renew-0001", 600)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	return nil
}
