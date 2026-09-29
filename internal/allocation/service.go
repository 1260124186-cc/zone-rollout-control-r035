package allocation

import (
	"context"
	"sort"
	"strings"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

const DefaultMaxLeaseTotalDuration = 7 * 24 * time.Hour

type Service struct {
	store                 *store.Store
	clock                 platform.Clock
	ids                   platform.IDGenerator
	maxLeaseTotalDuration time.Duration
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator) *Service {
	return &Service{
		store:                 stateStore,
		clock:                 clock,
		ids:                   ids,
		maxLeaseTotalDuration: DefaultMaxLeaseTotalDuration,
	}
}

// SetMaxLeaseTotalDuration overrides the server-side ceiling on a lease's
// total lifetime. A non-positive value restores the default.
func (s *Service) SetMaxLeaseTotalDuration(value time.Duration) {
	if value > 0 {
		s.maxLeaseTotalDuration = value
		return
	}
	s.maxLeaseTotalDuration = DefaultMaxLeaseTotalDuration
}

func (s *Service) Allocate(
	ctx context.Context,
	planID string,
	input domain.LeaseInput,
) (domain.LeaseGroup, error) {
	if err := domain.ValidateLeaseInput(input); err != nil {
		return domain.LeaseGroup{}, err
	}
	input.Region = strings.TrimSpace(strings.ToLower(input.Region))
	requestIndex := planID + "\x00" + input.RequestKey
	fingerprint := domain.LeaseFingerprint(planID, input)
	now := s.clock.Now().UTC()
	var group domain.LeaseGroup

	err := s.store.Write(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active for node leasing", map[string]any{
				"plan_id": planID,
				"state":   plan.State,
			})
		}
		if existingID, exists := state.LeaseRequestKeys[requestIndex]; exists {
			existing, found := state.LeaseGroups[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "lease request index is inconsistent", map[string]any{
					"lease_group_id": existingID,
				})
			}
			if existing.RequestFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different lease parameters", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			group = existing
			return nil
		}

		expireLeases(state, now)
		candidates := make([]domain.ResolverNode, 0)
		for _, node := range state.Nodes {
			if node.State != domain.NodeReady {
				continue
			}
			if input.Region != "" && node.Region != input.Region {
				continue
			}
			candidates = append(candidates, node)
		}
		if len(candidates) == 0 {
			return domain.NewError(domain.CodeNodeUnavailable, "no eligible node is ready", map[string]any{
				"plan_id": planID,
				"region":  input.Region,
			})
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].Region != candidates[j].Region {
				return candidates[i].Region < candidates[j].Region
			}
			return candidates[i].ID < candidates[j].ID
		})
		if len(candidates) > input.MaxNodes {
			candidates = candidates[:input.MaxNodes]
		}

		groupID := s.ids.New("grp")
		leaseIDs := make([]string, 0, len(candidates))
		expiresAt := now.Add(time.Duration(input.LeaseSeconds) * time.Second)
		maxExpiresAt := now.Add(s.maxLeaseTotalDuration)
		for _, candidate := range candidates {
			leaseID := s.ids.New("lse")
			candidate.State = domain.NodeLeased
			candidate.Version++
			candidate.UpdatedAt = now
			state.Nodes[candidate.ID] = candidate
			state.Leases[leaseID] = domain.NodeLease{
				ID:           leaseID,
				PlanID:       planID,
				NodeID:       candidate.ID,
				AllocationID: groupID,
				State:        domain.LeaseClaimed,
				ClaimedAt:    now,
				ExpiresAt:    expiresAt,
				MaxExpiresAt: maxExpiresAt,
			}
			leaseIDs = append(leaseIDs, leaseID)
		}
		group = domain.LeaseGroup{
			ID:                 groupID,
			PlanID:             planID,
			RequestKey:         input.RequestKey,
			RequestFingerprint: fingerprint,
			Region:             input.Region,
			LeaseIDs:           leaseIDs,
			CreatedAt:          now,
		}
		state.LeaseGroups[group.ID] = group
		state.LeaseRequestKeys[requestIndex] = group.ID
		store.AddAudit(state, s.ids, "lease_group", group.ID, "created", now, map[string]any{
			"plan_id":     planID,
			"lease_count": len(leaseIDs),
		})
		return nil
	})
	return group, err
}

func (s *Service) Release(ctx context.Context, leaseID string) (domain.NodeLease, error) {
	now := s.clock.Now().UTC()
	var result domain.NodeLease
	err := s.store.Write(ctx, func(state *domain.State) error {
		lease, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}
		if lease.State == domain.LeaseReleased || lease.State == domain.LeaseExpired {
			result = lease
			return nil
		}
		if lease.State != domain.LeaseClaimed {
			return domain.InvalidState("only a claimed lease can be released", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		}
		node, ok := state.Nodes[lease.NodeID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": lease.NodeID,
			})
		}
		finishedAt := now
		lease.State = domain.LeaseReleased
		lease.FinishedAt = &finishedAt
		state.Leases[leaseID] = lease
		if node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		store.AddAudit(state, s.ids, "lease", leaseID, "released", now, nil)
		result = lease
		return nil
	})
	return result, err
}

func (s *Service) Get(ctx context.Context, leaseID string) (domain.NodeLease, error) {
	var lease domain.NodeLease
	err := s.store.Read(ctx, func(state *domain.State) error {
		found, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}
		lease = found
		return nil
	})
	return lease, err
}

// ListRenewals returns the renewal records for a lease in renewal-number
// order, so callers can read the same history the deadline checks rely on.
func (s *Service) ListRenewals(ctx context.Context, leaseID string) ([]domain.LeaseRenewal, error) {
	var renewals []domain.LeaseRenewal
	err := s.store.Read(ctx, func(state *domain.State) error {
		if _, ok := state.Leases[leaseID]; !ok {
			return domain.NotFound("lease", leaseID)
		}
		for _, renewal := range state.LeaseRenewals {
			if renewal.LeaseID != leaseID {
				continue
			}
			renewals = append(renewals, renewal)
		}
		sort.Slice(renewals, func(i, j int) bool {
			return renewals[i].RenewalNumber < renewals[j].RenewalNumber
		})
		return nil
	})
	return renewals, err
}

// Renew extends an active lease's expiry by request key. The request key is
// idempotent: replaying the same key returns the recorded renewal without
// changing the lease. All mutations happen inside the store's write lock, so
// concurrent renewals and node reassignments cannot produce two valid leases.
func (s *Service) Renew(
	ctx context.Context,
	leaseID string,
	input domain.LeaseRenewalInput,
) (domain.NodeLease, domain.LeaseRenewal, error) {
	if err := domain.ValidateLeaseRenewalInput(input); err != nil {
		return domain.NodeLease{}, domain.LeaseRenewal{}, err
	}
	input.RequestKey = strings.TrimSpace(input.RequestKey)
	requestIndex := leaseID + "\x00" + input.RequestKey
	fingerprint := domain.LeaseRenewalFingerprint(leaseID, input)
	now := s.clock.Now().UTC()
	var lease domain.NodeLease
	var renewal domain.LeaseRenewal

	err := s.store.Write(ctx, func(state *domain.State) error {
		// Sweep first, matching Allocate: a lease past its deadline is moved
		// to expired and its node is freed before any renewal decision.
		expireLeases(state, now)
		found, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}
		lease = found

		// Idempotent replay: a recorded renewal for this lease and key is
		// returned unchanged, regardless of the lease's current state.
		if existingID, exists := state.LeaseRenewalRequestKeys[requestIndex]; exists {
			existing, found := state.LeaseRenewals[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "lease renewal request index is inconsistent", map[string]any{
					"renewal_id": existingID,
				})
			}
			if existing.RequestFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different renewal parameters", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			renewal = existing
			return nil
		}

		plan, ok := state.Plans[lease.PlanID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing plan", map[string]any{
				"plan_id": lease.PlanID,
			})
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active to renew leases", map[string]any{
				"plan_id": lease.PlanID,
				"state":   plan.State,
			})
		}
		if lease.State == domain.LeaseExpired {
			return domain.NewError(domain.CodeLeaseExpired, "lease has expired and cannot be renewed", map[string]any{
				"lease_id":   leaseID,
				"expires_at": lease.ExpiresAt,
			})
		}
		if lease.State != domain.LeaseClaimed {
			return domain.InvalidState("only a claimed lease can be renewed", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		}
		// Renewal uses the same boundary as probe submission: the lease is
		// expired once now reaches expires_at.
		if !now.Before(lease.ExpiresAt) {
			return domain.NewError(domain.CodeLeaseExpired, "lease has expired and cannot be renewed", map[string]any{
				"lease_id":   leaseID,
				"expires_at": lease.ExpiresAt,
			})
		}
		node, ok := state.Nodes[lease.NodeID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": lease.NodeID,
			})
		}
		if node.State != domain.NodeLeased {
			return domain.NewError(domain.CodeConflict, "node is not leased to this lease", map[string]any{
				"lease_id": leaseID,
				"node_id":  lease.NodeID,
				"state":    node.State,
			})
		}

		newExpiresAt, grantedSeconds, clamped, err := domain.CalculateLeaseRenewal(lease, input, s.maxLeaseTotalDuration)
		if err != nil {
			return err
		}
		previousExpiresAt := lease.ExpiresAt
		renewalNumber := lease.RenewalCount + 1
		renewal = domain.LeaseRenewal{
			ID:                 s.ids.New("rnw"),
			LeaseID:            leaseID,
			PlanID:             lease.PlanID,
			NodeID:             lease.NodeID,
			RequestKey:         input.RequestKey,
			RequestFingerprint: fingerprint,
			RenewalNumber:      renewalNumber,
			PreviousExpiresAt:  previousExpiresAt,
			NewExpiresAt:       newExpiresAt,
			ExtensionSeconds:   grantedSeconds,
			Basis:              domain.LeaseRenewalBasis,
			CreatedAt:          now,
		}
		lease.ExpiresAt = newExpiresAt
		lease.RenewalCount = renewalNumber
		lastRenewedAt := now
		lease.LastRenewedAt = &lastRenewedAt
		state.Leases[leaseID] = lease
		state.LeaseRenewals[renewal.ID] = renewal
		state.LeaseRenewalRequestKeys[requestIndex] = renewal.ID
		store.AddAudit(state, s.ids, "lease", leaseID, "renewed", now, map[string]any{
			"renewal_id":          renewal.ID,
			"renewal_number":      renewalNumber,
			"previous_expires_at": previousExpiresAt,
			"new_expires_at":      newExpiresAt,
			"extension_seconds":   grantedSeconds,
			"request_key":         input.RequestKey,
			"clamped":             clamped,
		})
		return nil
	})
	return lease, renewal, err
}

func (s *Service) ExpireDue(ctx context.Context) (int, error) {
	now := s.clock.Now().UTC()
	expired := 0
	err := s.store.Write(ctx, func(state *domain.State) error {
		expired = expireLeases(state, now)
		return nil
	})
	return expired, err
}

func expireLeases(state *domain.State, now time.Time) int {
	count := 0
	for leaseID, lease := range state.Leases {
		if lease.State != domain.LeaseClaimed || lease.ExpiresAt.After(now) {
			continue
		}
		finishedAt := now
		lease.State = domain.LeaseExpired
		lease.FinishedAt = &finishedAt
		state.Leases[leaseID] = lease
		if node, ok := state.Nodes[lease.NodeID]; ok && node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		count++
	}
	return count
}
