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

// DefaultMaxLeaseTotal is the server-side cap on the total time a single
// lease can cover from claim through every accepted renewal.
const DefaultMaxLeaseTotal = 7 * 24 * time.Hour

type Service struct {
	store         *store.Store
	clock         platform.Clock
	ids           platform.IDGenerator
	maxLeaseTotal time.Duration
}

// Option customizes an allocation service.
type Option func(*Service)

// WithMaxLeaseTotal overrides the server-side maximum total lease duration.
func WithMaxLeaseTotal(value time.Duration) Option {
	return func(service *Service) {
		if value > 0 {
			service.maxLeaseTotal = value
		}
	}
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator, options ...Option) *Service {
	service := &Service{
		store:         stateStore,
		clock:         clock,
		ids:           ids,
		maxLeaseTotal: DefaultMaxLeaseTotal,
	}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *Service) Allocate(
	ctx context.Context,
	planID string,
	input domain.LeaseInput,
) (domain.LeaseGroup, error) {
	if err := domain.ValidateLeaseInput(input); err != nil {
		return domain.LeaseGroup{}, err
	}
	if int64(input.LeaseSeconds) > int64(s.maxLeaseTotal.Seconds()) {
		return domain.LeaseGroup{}, domain.NewError(domain.CodeLeaseRenewalLimit, "lease duration exceeds the maximum total lease duration", map[string]any{
			"lease_seconds": input.LeaseSeconds,
			"maximum_total": int64(s.maxLeaseTotal.Seconds()),
		})
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

// Renew extends an active claimed lease by request key before its deadline.
// Renewal is idempotent on (lease, request_key), records every accepted
// extension, and refuses to push the lease beyond the server-side total cap.
func (s *Service) Renew(
	ctx context.Context,
	leaseID string,
	input domain.LeaseRenewalInput,
) (domain.NodeLease, error) {
	input, err := domain.ValidateLeaseRenewalInput(input)
	if err != nil {
		return domain.NodeLease{}, err
	}
	requestIndex := leaseID + "\x00" + input.RequestKey
	fingerprint := domain.LeaseRenewalFingerprint(leaseID, input)
	now := s.clock.Now().UTC()
	var result domain.NodeLease

	err = s.store.Write(ctx, func(state *domain.State) error {
		if replayedID, exists := state.LeaseRenewalKeys[requestIndex]; exists {
			replayed, found := state.Leases[replayedID]
			if !found || replayedID != leaseID {
				return domain.NewError(domain.CodeStorageFailure, "lease renewal index is inconsistent", map[string]any{
					"lease_id": leaseID,
				})
			}
			matched := -1
			for index := range replayed.Renewals {
				if replayed.Renewals[index].RequestKey == input.RequestKey {
					matched = index
					break
				}
			}
			if matched < 0 {
				return domain.NewError(domain.CodeStorageFailure, "lease renewal index is inconsistent", map[string]any{
					"lease_id": leaseID,
				})
			}
			if replayed.Renewals[matched].RequestFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different renewal parameters", map[string]any{
					"lease_id":    leaseID,
					"request_key": input.RequestKey,
				})
			}
			result = replayed
			return nil
		}

		// Sweep due leases inside the same lock so a concurrent expiry and a
		// concurrent re-allocation can never race the renewal decision.
		expireLeases(state, now)

		lease, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}
		switch lease.State {
		case domain.LeaseExpired:
			return domain.NewError(domain.CodeLeaseExpired, "lease has expired and cannot be renewed", map[string]any{
				"lease_id":   leaseID,
				"expires_at": lease.ExpiresAt,
			})
		case domain.LeaseReleased:
			return domain.NewError(domain.CodeLeaseNotClaimed, "lease has been released and cannot be renewed", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		case domain.LeaseReported:
			return domain.NewError(domain.CodeLeaseNotClaimed, "lease has already reported and cannot be renewed", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		case domain.LeaseClaimed:
		default:
			return domain.InvalidState("lease cannot be renewed from its current state", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		}
		if !now.Before(lease.ExpiresAt) {
			return domain.NewError(domain.CodeLeaseExpired, "lease has expired and cannot be renewed", map[string]any{
				"lease_id":   leaseID,
				"expires_at": lease.ExpiresAt,
			})
		}
		plan, ok := state.Plans[lease.PlanID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing plan", map[string]any{
				"plan_id": lease.PlanID,
			})
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active to renew a lease", map[string]any{
				"plan_id": lease.PlanID,
				"state":   plan.State,
			})
		}
		node, ok := state.Nodes[lease.NodeID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": lease.NodeID,
			})
		}
		if node.State != domain.NodeLeased {
			return domain.InvalidState("node is no longer held by this lease", map[string]any{
				"node_id": lease.NodeID,
				"state":   node.State,
			})
		}

		newExpiresAt := lease.ExpiresAt.Add(time.Duration(input.RenewalSeconds) * time.Second)
		if total := newExpiresAt.Sub(lease.ClaimedAt); total > s.maxLeaseTotal {
			return domain.NewError(domain.CodeLeaseRenewalLimit, "renewal would exceed the maximum total lease duration", map[string]any{
				"lease_id":             leaseID,
				"requested_expires_at": newExpiresAt,
				"maximum_total":        int64(s.maxLeaseTotal.Seconds()),
				"remaining_seconds":    int64((lease.ClaimedAt.Add(s.maxLeaseTotal).Sub(lease.ExpiresAt)).Seconds()),
			})
		}

		renewal := domain.RenewLease(&lease, input, now)
		state.Leases[leaseID] = lease
		state.LeaseRenewalKeys[requestIndex] = leaseID
		store.AddAudit(state, s.ids, "lease", leaseID, "renewed", now, map[string]any{
			"plan_id":             lease.PlanID,
			"renewal_sequence":    renewal.Sequence,
			"previous_expires_at": renewal.PreviousExpiresAt,
			"expires_at":          renewal.ExpiresAt,
			"request_key":         renewal.RequestKey,
			"basis":               renewal.Basis,
		})
		result = lease
		return nil
	})
	return result, err
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
		transitioned := domain.ExpireLease(&lease, now)
		if !transitioned {
			continue
		}
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
