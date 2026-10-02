package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/veritrace-platform/telemetry-stream-service/internal/gs1"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection/queries"
)

// Event types whose data the projection reads (messaging.md §3). Every other event only advances the status.
const (
	typeCreated          = "shipment.created"
	typeParticipantAdded = "shipment.participant_added"
	typeDriverAssigned   = "shipment.driver_assigned"
)

// eventVersion is the version of the event payloads that the projection reads.
const eventVersion = 1

// roleCarrier is the participant role that takes over transport from the owner.
const roleCarrier = "CARRIER"

// errOtherVersion reports an event of a payload version that the projection does not read. During a version
// migration both versions are published (messaging.md), so the projection keeps reading its own.
var errOtherVersion = errors.New("event version is not read")

// event is the part of the domain event envelope (messaging.md §2.1) that the projection reads, with the data
// of the event types that change more than the status.
type event struct {
	EventID      string          `json:"event_id"`
	EventType    string          `json:"event_type"`
	EventVersion int             `json:"event_version"`
	Subject      subject         `json:"subject"`
	Sequence     int32           `json:"sequence"`
	Data         json.RawMessage `json:"data"`

	created *createdData
	joined  *participant
	driver  *driverData
}

type subject struct {
	ShipmentID uuid.UUID `json:"shipment_id"`
	SSCC       string    `json:"sscc"`
	Status     string    `json:"status"`
}

type createdData struct {
	OwnerTenantID    uuid.UUID     `json:"owner_tenant_id"`
	GTIN             string        `json:"gtin"`
	ProductName      string        `json:"product_name"`
	LotNumber        string        `json:"lot_number"`
	MinTempCelsius   *float64      `json:"min_temp_celsius"`
	MaxTempCelsius   *float64      `json:"max_temp_celsius"`
	Participants     []participant `json:"participants"`
	AssignedDriverID *uuid.UUID    `json:"assigned_driver_id"`
}

type participant struct {
	TenantID uuid.UUID `json:"tenant_id"`
	Role     string    `json:"role"`
}

type driverData struct {
	DriverUserID uuid.UUID `json:"driver_user_id"`
}

// decode reads and checks an event envelope and the data that the projection uses.
func decode(value []byte) (event, error) {
	var e event
	if err := json.Unmarshal(value, &e); err != nil {
		return event{}, fmt.Errorf("decode envelope: %w", err)
	}
	if e.EventVersion != eventVersion {
		return event{}, fmt.Errorf("%w: %s version %d", errOtherVersion, e.EventType, e.EventVersion)
	}
	switch {
	case e.EventType == "":
		return event{}, errors.New("event_type is missing")
	case e.Subject.ShipmentID == uuid.Nil:
		return event{}, errors.New("subject.shipment_id is missing")
	case !gs1.ValidSSCC(e.Subject.SSCC):
		return event{}, fmt.Errorf("subject.sscc %q is not an SSCC", e.Subject.SSCC)
	case !statuses[e.Subject.Status]:
		return event{}, fmt.Errorf("subject.status %q is not a shipment status", e.Subject.Status)
	case e.Sequence < 1:
		return event{}, fmt.Errorf("sequence %d is not positive", e.Sequence)
	}

	var missing string
	switch e.EventType {
	case typeCreated:
		e.created = &createdData{}
		if err := json.Unmarshal(e.Data, e.created); err != nil {
			return event{}, fmt.Errorf("decode %s data: %w", e.EventType, err)
		}
		switch {
		case e.created.OwnerTenantID == uuid.Nil:
			missing = "owner_tenant_id"
		case e.created.MinTempCelsius == nil || e.created.MaxTempCelsius == nil:
			missing = "min_temp_celsius or max_temp_celsius"
		}
	case typeParticipantAdded:
		e.joined = &participant{}
		if err := json.Unmarshal(e.Data, e.joined); err != nil {
			return event{}, fmt.Errorf("decode %s data: %w", e.EventType, err)
		}
		if e.joined.TenantID == uuid.Nil {
			missing = "tenant_id"
		}
	case typeDriverAssigned:
		e.driver = &driverData{}
		if err := json.Unmarshal(e.Data, e.driver); err != nil {
			return event{}, fmt.Errorf("decode %s data: %w", e.EventType, err)
		}
		if e.driver.DriverUserID == uuid.Nil {
			missing = "driver_user_id"
		}
	}
	if missing != "" {
		return event{}, fmt.Errorf("%s data lacks %s", e.EventType, missing)
	}
	return e, nil
}

// result is what applying an event did.
type result int

const (
	applied result = iota
	// duplicate: the event was applied before.
	duplicate
	// unknownShipment: the projection has no shipment for the event to change.
	unknownShipment
)

// apply writes the event to the projection.
func (e event) apply(ctx context.Context, q *queries.Queries) (result, error) {
	if e.created != nil {
		return e.create(ctx, q)
	}
	params := queries.AdvanceParams{Sscc: e.Subject.SSCC, Status: e.Subject.Status, Sequence: e.Sequence}
	switch {
	case e.joined != nil:
		params.JoinedTenantID = &e.joined.TenantID
		// A new carrier replaces the owner as carrier, which ends the assignment of the owner's driver.
		params.ChangeDriver = e.joined.Role == roleCarrier
	case e.driver != nil:
		params.ChangeDriver, params.AssignedDriverID = true, &e.driver.DriverUserID
	}
	n, err := q.Advance(ctx, params)
	if err != nil {
		return 0, fmt.Errorf("apply %s: %w", e.EventType, err)
	}
	if n == 1 {
		return applied, nil
	}
	return e.unapplied(ctx, q)
}

// create inserts the shipment that a shipment.created event describes.
func (e event) create(ctx context.Context, q *queries.Queries) (result, error) {
	data := e.created
	tenants := []uuid.UUID{data.OwnerTenantID}
	for _, p := range data.Participants {
		if p.TenantID != uuid.Nil && !slices.Contains(tenants, p.TenantID) {
			tenants = append(tenants, p.TenantID)
		}
	}
	n, err := q.CreateShipment(ctx, queries.CreateShipmentParams{
		Sscc: e.Subject.SSCC, ShipmentID: e.Subject.ShipmentID, OwnerTenantID: data.OwnerTenantID,
		ParticipantTenantIds: tenants, AssignedDriverID: data.AssignedDriverID, Status: e.Subject.Status,
		Gtin: data.GTIN, ProductName: data.ProductName, LotNumber: data.LotNumber,
		MinTempCelsius: *data.MinTempCelsius, MaxTempCelsius: *data.MaxTempCelsius, Sequence: e.Sequence,
	})
	if err != nil {
		return 0, fmt.Errorf("apply %s: %w", e.EventType, err)
	}
	if n == 1 {
		return applied, nil
	}
	return duplicate, nil
}

// unapplied tells why an event changed nothing.
func (e event) unapplied(ctx context.Context, q *queries.Queries) (result, error) {
	_, err := q.LastEventSequence(ctx, e.Subject.SSCC)
	if errors.Is(err, pgx.ErrNoRows) {
		return unknownShipment, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read last event sequence: %w", err)
	}
	return duplicate, nil
}
