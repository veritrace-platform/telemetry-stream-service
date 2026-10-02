// Package access decides who may see a shipment's telemetry (access-control.md §2, "View shipment, events, and
// telemetry"; ADR-0008): users of a participant tenant, except drivers, who see only the shipments assigned to
// them. The read API and the notification hub apply the same rule.
package access

import (
	"errors"
	"slices"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/auth"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection"
)

// Denials. A shipment in which the caller's tenant takes no part is not found, so its existence is not revealed.
var (
	ErrNotFound  = errors.New("shipment not found")
	ErrForbidden = errors.New("drivers may view only the shipments assigned to them")
)

// View returns nil if p may view the shipment's telemetry, ErrNotFound if p's tenant takes no part in it, and
// ErrForbidden if p is a driver of a participant tenant who is not assigned to it.
func View(p auth.Principal, s projection.Shipment) error {
	if !slices.Contains(s.ParticipantTenantIDs, p.TenantID) {
		return ErrNotFound
	}
	if p.Role == auth.RoleDriver && (s.AssignedDriverID == nil || *s.AssignedDriverID != p.UserID) {
		return ErrForbidden
	}
	return nil
}

// AssignedTo returns the driver whose assignments limit p's lists: p itself if it is a driver, otherwise nil.
func AssignedTo(p auth.Principal) *uuid.UUID {
	if p.Role != auth.RoleDriver {
		return nil
	}
	id := p.UserID
	return &id
}
