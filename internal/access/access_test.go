package access_test

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/access"
	"github.com/veritrace-platform/telemetry-stream-service/internal/auth"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection"
)

func TestView(t *testing.T) {
	owner, carrier, driver := uuid.New(), uuid.New(), uuid.New()
	s := projection.Shipment{ParticipantTenantIDs: []uuid.UUID{owner, carrier}, AssignedDriverID: &driver}
	unassigned := s
	unassigned.AssignedDriverID = nil
	tests := []struct {
		name     string
		p        auth.Principal
		shipment projection.Shipment
		want     error
	}{
		{"owner admin", auth.Principal{TenantID: owner, Role: auth.RoleAdmin}, s, nil},
		{"carrier warehouse manager", auth.Principal{TenantID: carrier, Role: auth.RoleWarehouseManager}, s, nil},
		{"carrier inspector", auth.Principal{TenantID: carrier, Role: auth.RoleInspector}, s, nil},
		{"assigned driver", auth.Principal{UserID: driver, TenantID: carrier, Role: auth.RoleDriver}, s, nil},
		{"another driver", auth.Principal{UserID: uuid.New(), TenantID: carrier, Role: auth.RoleDriver}, s, access.ErrForbidden},
		{"driver without an assignment", auth.Principal{UserID: driver, TenantID: carrier, Role: auth.RoleDriver}, unassigned, access.ErrForbidden},
		{"another tenant", auth.Principal{TenantID: uuid.New(), Role: auth.RoleAdmin}, s, access.ErrNotFound},
		{"another tenant's driver", auth.Principal{UserID: driver, TenantID: uuid.New(), Role: auth.RoleDriver}, s, access.ErrNotFound},
	}
	for _, tt := range tests {
		if err := access.View(tt.p, tt.shipment); !errors.Is(err, tt.want) {
			t.Errorf("%s: View() = %v, want %v", tt.name, err, tt.want)
		}
	}
	if id := access.AssignedTo(auth.Principal{UserID: driver, Role: auth.RoleDriver}); id == nil || *id != driver {
		t.Errorf("AssignedTo(driver) = %v", id)
	}
	if id := access.AssignedTo(auth.Principal{UserID: driver, Role: auth.RoleAdmin}); id != nil {
		t.Errorf("AssignedTo(admin) = %v", id)
	}
}
