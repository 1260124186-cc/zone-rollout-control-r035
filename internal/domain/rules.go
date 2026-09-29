package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"
)

func ActivatePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanDraft {
		return InvalidState("plan can be activated only from draft", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	if plan.LatestRevisionID == "" {
		return InvalidState("plan needs a zone revision before activation", map[string]any{
			"plan_id": plan.ID,
		})
	}
	plan.State = PlanActive
	plan.ActivatedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func CompletePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanActive {
		return InvalidState("plan can be completed only from active", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	plan.State = PlanCompleted
	plan.CompletedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func LeaseFingerprint(planID string, input LeaseInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%s|%d|%d",
		planID,
		input.RequestKey,
		input.Region,
		input.LeaseSeconds,
		input.MaxNodes,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// LeaseRenewalBasis is the stable reason code recorded on every renewal.
const LeaseRenewalBasis = "lease_renewal_request"

func LeaseRenewalFingerprint(leaseID string, input LeaseRenewalInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%d",
		leaseID,
		input.RequestKey,
		input.ExtensionSeconds,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// LeaseTotalDeadline returns the server-side ceiling for a lease's expiry.
// Leases created before the ceiling was recorded fall back to claimedAt plus
// the configured maximum total duration.
func LeaseTotalDeadline(lease NodeLease, maxTotalDuration time.Duration) time.Time {
	if !lease.MaxExpiresAt.IsZero() {
		return lease.MaxExpiresAt
	}
	return lease.ClaimedAt.Add(maxTotalDuration)
}

// CalculateLeaseRenewal decides the new expiry for a renewal request.
// The server never extends a lease beyond its total deadline: a request that
// would overshoot is clamped to the ceiling and reports clamped=true. A lease
// already sitting at the ceiling is rejected with CodeLeaseRenewalLimit.
func CalculateLeaseRenewal(
	lease NodeLease,
	input LeaseRenewalInput,
	maxTotalDuration time.Duration,
) (newExpiresAt time.Time, grantedSeconds int, clamped bool, err error) {
	ceiling := LeaseTotalDeadline(lease, maxTotalDuration)
	if !lease.ExpiresAt.Before(ceiling) {
		return time.Time{}, 0, false, NewError(CodeLeaseRenewalLimit, "lease has reached its maximum total duration", map[string]any{
			"lease_id":       lease.ID,
			"expires_at":     lease.ExpiresAt,
			"max_expires_at": ceiling,
			"renewal_count":  lease.RenewalCount,
		})
	}
	requested := lease.ExpiresAt.Add(time.Duration(input.ExtensionSeconds) * time.Second)
	if !requested.Before(ceiling) {
		granted := int(ceiling.Sub(lease.ExpiresAt).Seconds())
		return ceiling, granted, true, nil
	}
	return requested, input.ExtensionSeconds, false, nil
}

func ProbeFingerprint(leaseID string, input ProbeInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%t|%d|%d|%s",
		leaseID,
		input.RequestKey,
		input.Success,
		input.ObservedSerial,
		input.LatencyMS,
		input.ObservedAt.UTC().Format(time.RFC3339Nano),
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func BuildAssessment(
	id string,
	plan RolloutPlan,
	revision ZoneRevision,
	probes []Probe,
	now time.Time,
) RolloutAssessment {
	reasons := make(map[string]struct{})
	if len(probes) == 0 {
		reasons["no_probes"] = struct{}{}
	}
	probeCount := len(probes)
	successCount := 0
	maximumLatency := 0
	nodes := make(map[string]struct{})
	for _, probe := range probes {
		nodes[probe.NodeID] = struct{}{}
		if probe.LatencyMS > maximumLatency {
			maximumLatency = probe.LatencyMS
		}
		if probe.Success {
			successCount++
			if probe.ObservedSerial != revision.Serial {
				reasons["serial_mismatch"] = struct{}{}
			}
		}
	}
	if len(nodes) < plan.ExpectedNodes {
		reasons["node_not_observed"] = struct{}{}
	}
	successRatio := 0.0
	if probeCount > 0 {
		successRatio = round(float64(successCount)/float64(probeCount), 6)
	}
	if successRatio < plan.MinimumSuccessRatio {
		reasons["success_ratio_below_minimum"] = struct{}{}
	}
	if maximumLatency > plan.MaximumLatencyMS {
		reasons["latency_limit_exceeded"] = struct{}{}
	}
	reasonList := make([]string, 0, len(reasons))
	for reason := range reasons {
		reasonList = append(reasonList, reason)
	}
	sort.Strings(reasonList)
	verdict := VerdictConsistent
	if len(reasonList) > 0 {
		verdict = VerdictReviewRequired
	}
	return RolloutAssessment{
		ID:               id,
		PlanID:           plan.ID,
		RevisionID:       revision.ID,
		Verdict:          verdict,
		Reasons:          reasonList,
		ProbeCount:       probeCount,
		SuccessCount:     successCount,
		SuccessRatio:     successRatio,
		MaximumLatencyMS: maximumLatency,
		CreatedAt:        now.UTC(),
	}
}

func round(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}

func timePointer(value time.Time) *time.Time {
	return &value
}
