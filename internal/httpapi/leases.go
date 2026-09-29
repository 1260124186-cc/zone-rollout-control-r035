package httpapi

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func (s *Server) allocateLeases(w http.ResponseWriter, r *http.Request) {
	var input domain.LeaseInput
	if err := decode(w, r, &input); err != nil {
		sendError(w, r, err)
		return
	}
	group, err := s.allocations.Allocate(r.Context(), r.PathValue("id"), input)
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusCreated, group)
}

func (s *Server) releaseLease(w http.ResponseWriter, r *http.Request) {
	lease, err := s.allocations.Release(r.Context(), r.PathValue("id"))
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, lease)
}

func (s *Server) getLease(w http.ResponseWriter, r *http.Request) {
	lease, err := s.allocations.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, lease)
}

func (s *Server) listLeaseRenewals(w http.ResponseWriter, r *http.Request) {
	renewals, err := s.allocations.ListRenewals(r.Context(), r.PathValue("id"))
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, renewals)
}

func (s *Server) renewLease(w http.ResponseWriter, r *http.Request) {
	var input domain.LeaseRenewalInput
	if err := decode(w, r, &input); err != nil {
		sendError(w, r, err)
		return
	}
	lease, renewal, err := s.allocations.Renew(r.Context(), r.PathValue("id"), input)
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, map[string]any{
		"lease":   lease,
		"renewal": renewal,
	})
}
