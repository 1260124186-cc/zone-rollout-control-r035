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

// LeaseRenewalFingerprint binds a renewal request to one lease and its exact
// extension parameters, including the caller-supplied basis.
func LeaseRenewalFingerprint(leaseID string, input LeaseRenewalInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%d|%s",
		leaseID,
		input.RequestKey,
		input.RenewalSeconds,
		input.Basis,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

// ExpireLease moves a due claimed lease to expired and returns whether the
// transition was applied. It never changes leases already in a final state.
func ExpireLease(lease *NodeLease, now time.Time) bool {
	if lease.State != LeaseClaimed || lease.ExpiresAt.After(now) {
		return false
	}
	finishedAt := now.UTC()
	lease.State = LeaseExpired
	lease.FinishedAt = &finishedAt
	return true
}

// RenewLease extends a claimed lease relative to its current deadline and
// appends an immutable renewal record. It is a pure state transition: callers
// are responsible for every precondition and the maximum-total-duration cap.
func RenewLease(lease *NodeLease, input LeaseRenewalInput, now time.Time) LeaseRenewal {
	previousExpiresAt := lease.ExpiresAt
	newExpiresAt := previousExpiresAt.Add(time.Duration(input.RenewalSeconds) * time.Second)
	renewal := LeaseRenewal{
		Sequence:           lease.RenewalCount + 1,
		RequestKey:         input.RequestKey,
		RequestFingerprint: LeaseRenewalFingerprint(lease.ID, input),
		Basis:              input.Basis,
		PreviousExpiresAt:  previousExpiresAt,
		ExpiresAt:          newExpiresAt,
		RenewedAt:          now.UTC(),
	}
	lease.ExpiresAt = newExpiresAt
	lease.RenewalCount++
	lease.Renewals = append(lease.Renewals, renewal)
	return renewal
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
