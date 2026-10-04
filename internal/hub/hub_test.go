package hub_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/veritrace-platform/telemetry-stream-service/internal/auth"
	"github.com/veritrace-platform/telemetry-stream-service/internal/auth/authtest"
	"github.com/veritrace-platform/telemetry-stream-service/internal/gs1"
	"github.com/veritrace-platform/telemetry-stream-service/internal/httpapi"
	"github.com/veritrace-platform/telemetry-stream-service/internal/hub"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection"
)

const (
	sscc      = "089300010000000018"
	otherSSCC = "089300010000000025"
)

// shipments is a fixed projection.
type shipments map[string]projection.Shipment

func (s shipments) Get(_ context.Context, sscc string) (projection.Shipment, error) {
	if sh, ok := s[sscc]; ok {
		return sh, nil
	}
	return projection.Shipment{}, projection.ErrNotFound
}

type fixture struct {
	hub      *hub.Hub
	issuer   *authtest.Issuer
	url      string
	owner    uuid.UUID
	carrier  uuid.UUID
	driver   uuid.UUID
	shipment projection.Shipment
}

// newFixture starts a hub whose projection knows one shipment of an owner, carried by a carrier, with an assigned
// driver, plus extra shipments of the owner with the SSCCs of ssccOf(0), ssccOf(1), ….
func newFixture(t *testing.T, extra int) *fixture {
	t.Helper()
	f := &fixture{issuer: authtest.NewIssuer(t), owner: uuid.New(), carrier: uuid.New(), driver: uuid.New()}
	f.shipment = projection.Shipment{
		SSCC: sscc, ShipmentID: uuid.New(), OwnerTenantID: f.owner, ParticipantTenantIDs: []uuid.UUID{f.owner, f.carrier},
		AssignedDriverID: &f.driver, Status: projection.StatusInTransit, GTIN: "08930001000018", ProductName: "Fresh milk 1 L",
	}
	known := shipments{sscc: f.shipment}
	for i := range extra {
		known[ssccOf(i)] = projection.Shipment{SSCC: ssccOf(i), ShipmentID: uuid.New(), OwnerTenantID: f.owner,
			ParticipantTenantIDs: []uuid.UUID{f.owner}, Status: projection.StatusInTransit}
	}
	verifier := auth.NewVerifier(auth.NewJWKS(f.issuer.URL, http.DefaultClient, slog.New(slog.DiscardHandler), time.Now), time.Now)
	f.hub = hub.New(known, verifier, []string{"https://app.example.com"}, slog.New(slog.DiscardHandler),
		prometheus.NewRegistry(), time.Now)
	server := httptest.NewServer(httpapi.NewRouter(slog.New(slog.DiscardHandler), prometheus.NewRegistry(),
		httpapi.Mounts{WebSocket: []httpapi.Routes{f.hub.Routes}}))
	t.Cleanup(server.Close)
	f.url = "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/v1/notifications"
	return f
}

// dial connects with the given subprotocols and origin. It returns the HTTP status of a refused handshake.
func (f *fixture) dial(t *testing.T, protocols []string, origin string) (*websocket.Conn, int, error) {
	t.Helper()
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, f.url, &websocket.DialOptions{Subprotocols: protocols, HTTPHeader: header})
	status := 0
	if resp != nil {
		status = resp.StatusCode
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
	}
	if conn != nil {
		t.Cleanup(func() { _ = conn.CloseNow() })
	}
	return conn, status, err
}

// connect connects as p.
func (f *fixture) connect(t *testing.T, p auth.Principal) *websocket.Conn {
	t.Helper()
	conn, _, err := f.dial(t, []string{hub.Subprotocol, "bearer." + f.issuer.Token(t, p)}, "")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if conn.Subprotocol() != hub.Subprotocol {
		t.Fatalf("subprotocol = %q", conn.Subprotocol())
	}
	return conn
}

type received struct {
	Type   string          `json:"type"`
	ID     uuid.UUID       `json:"id"`
	SentAt string          `json:"sent_at"`
	Data   json.RawMessage `json:"data"`
}

// next reads the next message, or returns the close status if the connection closes.
func next(t *testing.T, conn *websocket.Conn) (received, websocket.StatusCode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		status := websocket.CloseStatus(err)
		if status == -1 {
			t.Fatalf("read: %v", err)
		}
		return received{}, status
	}
	var m received
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("decode %s: %v", data, err)
	}
	return m, -1
}

func expect(t *testing.T, conn *websocket.Conn, typ string) received {
	t.Helper()
	m, status := next(t, conn)
	if status != -1 || m.Type != typ {
		t.Fatalf("got %+v (close %d), want %s", m, status, typ)
	}
	if _, err := time.Parse(time.RFC3339Nano, m.SentAt); err != nil || m.ID == uuid.Nil {
		t.Errorf("message envelope = %+v", m)
	}
	return m
}

func expectClose(t *testing.T, conn *websocket.Conn, want websocket.StatusCode) {
	t.Helper()
	for {
		m, status := next(t, conn)
		if status == -1 {
			t.Logf("skipped %s before the close", m.Type)
			continue
		}
		if status != want {
			t.Fatalf("close status = %d, want %d", status, want)
		}
		return
	}
}

func send(t *testing.T, conn *websocket.Conn, msg string) {
	t.Helper()
	if err := conn.Write(t.Context(), websocket.MessageText, []byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func subscribe(sscc string) string {
	return `{"type":"subscribe","channel":"telemetry","sscc":"` + sscc + `"}`
}

func TestHandshake(t *testing.T) {
	f := newFixture(t, 0)
	valid := f.issuer.Token(t, auth.Principal{UserID: uuid.New(), TenantID: f.owner, Role: auth.RoleAdmin})

	t.Run("without the protocol", func(t *testing.T) {
		_, status, err := f.dial(t, []string{"bearer." + valid}, "")
		if err == nil || status != http.StatusBadRequest {
			t.Errorf("dial error = %v, status %d; want 400", err, status)
		}
	})

	t.Run("from a foreign origin", func(t *testing.T) {
		_, status, err := f.dial(t, []string{hub.Subprotocol, "bearer." + valid}, "https://evil.example")
		if err == nil || status != http.StatusForbidden {
			t.Errorf("dial error = %v, status %d; want 403", err, status)
		}
	})

	t.Run("from an allowed origin", func(t *testing.T) {
		if _, _, err := f.dial(t, []string{hub.Subprotocol, "bearer." + valid}, "https://app.example.com"); err != nil {
			t.Errorf("dial: %v", err)
		}
	})

	expired := f.issuer.Token(t, auth.Principal{UserID: uuid.New(), TenantID: f.owner, Role: auth.RoleAdmin,
		ExpiresAt: time.Now().Add(-time.Minute)})
	for name, protocols := range map[string][]string{
		"without a token":     {hub.Subprotocol},
		"with an invalid one": {hub.Subprotocol, "bearer.abc.def.ghi"},
		"with an expired one": {hub.Subprotocol, "bearer." + expired},
	} {
		t.Run(name, func(t *testing.T) {
			conn, _, err := f.dial(t, protocols, "")
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			expectClose(t, conn, hub.CloseUnauthenticated)
		})
	}

	t.Run("while the keys cannot be loaded", func(t *testing.T) {
		g := newFixture(t, 0)
		token := g.issuer.Token(t, auth.Principal{UserID: uuid.New(), TenantID: g.owner, Role: auth.RoleAdmin})
		g.issuer.Unavailable.Store(true)
		conn, _, err := g.dial(t, []string{"bearer." + token, hub.Subprotocol}, "")
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		expectClose(t, conn, websocket.StatusTryAgainLater)
	})
}

func TestSubscriptions(t *testing.T) {
	f := newFixture(t, 0)
	conn := f.connect(t, auth.Principal{UserID: uuid.New(), TenantID: f.carrier, Role: auth.RoleWarehouseManager})

	send(t, conn, subscribe(otherSSCC))
	if m := expect(t, conn, hub.TypeError); !strings.Contains(string(m.Data), `"code":"NOT_FOUND"`) {
		t.Errorf("error = %s", m.Data)
	}
	send(t, conn, `{"type":"watch","channel":"telemetry","sscc":"`+sscc+`"}`)
	if m := expect(t, conn, hub.TypeError); !strings.Contains(string(m.Data), `"code":"INVALID_MESSAGE"`) {
		t.Errorf("error = %s", m.Data)
	}
	send(t, conn, `{"type":"subscribe","channel":"telemetry","sscc":"089300010000000019"}`)
	expect(t, conn, hub.TypeError)

	send(t, conn, subscribe(sscc))
	if m := expect(t, conn, hub.TypeSubscribed); string(m.Data) != `{"channel":"telemetry","sscc":"`+sscc+`"}` {
		t.Errorf("subscribed = %s", m.Data)
	}
	if !f.hub.Subscribed(sscc) || f.hub.Subscribed(otherSSCC) {
		t.Error("Subscribed() does not match the subscriptions")
	}
	f.hub.Publish(sscc, map[string]any{"sscc": sscc, "temperature_celsius": 5.5})
	if m := expect(t, conn, hub.TypeReading); !strings.Contains(string(m.Data), `"temperature_celsius":5.5`) {
		t.Errorf("reading = %s", m.Data)
	}

	send(t, conn, `{"type":"unsubscribe","channel":"telemetry","sscc":"`+sscc+`"}`)
	expect(t, conn, hub.TypeUnsubscribed)
	f.hub.Publish(sscc, map[string]any{"sscc": sscc})
	// Nothing arrives for the SSCC any more: the next message is the answer to the next request.
	send(t, conn, subscribe(otherSSCC))
	expect(t, conn, hub.TypeError)
}

func TestSubscriptionLimitsAndProtocolErrors(t *testing.T) {
	f := newFixture(t, 0)
	admin := auth.Principal{UserID: uuid.New(), TenantID: f.owner, Role: auth.RoleAdmin}

	t.Run("an unassigned driver", func(t *testing.T) {
		conn := f.connect(t, auth.Principal{UserID: uuid.New(), TenantID: f.carrier, Role: auth.RoleDriver})
		send(t, conn, subscribe(sscc))
		if m := expect(t, conn, hub.TypeError); !strings.Contains(string(m.Data), `"code":"FORBIDDEN"`) {
			t.Errorf("error = %s", m.Data)
		}
		expectClose(t, conn, hub.CloseForbidden)
	})

	t.Run("the assigned driver", func(t *testing.T) {
		conn := f.connect(t, auth.Principal{UserID: f.driver, TenantID: f.carrier, Role: auth.RoleDriver})
		send(t, conn, subscribe(sscc))
		expect(t, conn, hub.TypeSubscribed)
	})

	t.Run("too many subscriptions", func(t *testing.T) {
		g := newFixture(t, hub.MaxSubscriptions+1)
		conn := g.connect(t, auth.Principal{UserID: uuid.New(), TenantID: g.owner, Role: auth.RoleAdmin})
		for i := range hub.MaxSubscriptions {
			send(t, conn, subscribe(ssccOf(i)))
			expect(t, conn, hub.TypeSubscribed)
		}
		send(t, conn, subscribe(ssccOf(0))) // following an SSCC again is not one more
		expect(t, conn, hub.TypeSubscribed)
		send(t, conn, subscribe(ssccOf(hub.MaxSubscriptions)))
		expectClose(t, conn, hub.CloseTooManySubscriptions)
	})

	t.Run("a message that is not JSON", func(t *testing.T) {
		conn := f.connect(t, admin)
		send(t, conn, `subscribe me`)
		expectClose(t, conn, hub.CloseBadMessage)
	})

	t.Run("a binary message", func(t *testing.T) {
		conn := f.connect(t, admin)
		if err := conn.Write(t.Context(), websocket.MessageBinary, []byte(subscribe(sscc))); err != nil {
			t.Fatal(err)
		}
		expectClose(t, conn, hub.CloseBadMessage)
	})

	t.Run("a message over 4 KiB", func(t *testing.T) {
		conn := f.connect(t, admin)
		send(t, conn, `{"type":"subscribe","channel":"telemetry","sscc":"`+strings.Repeat("0", 5000)+`"}`)
		expectClose(t, conn, hub.CloseBadMessage)
	})
}

// ssccOf returns a valid SSCC with serial 100 + i.
func ssccOf(i int) string {
	payload := fmt.Sprintf("08930001%09d", 100+i)
	return payload + strconv.Itoa(gs1.CheckDigit(payload))
}

func TestDeliver(t *testing.T) {
	f := newFixture(t, 0)
	type listener struct {
		name    string
		conn    *websocket.Conn
		receive bool
	}
	listeners := []listener{
		{"owner admin", f.connect(t, auth.Principal{UserID: uuid.New(), TenantID: f.owner, Role: auth.RoleAdmin}), true},
		{"carrier manager", f.connect(t, auth.Principal{UserID: uuid.New(), TenantID: f.carrier, Role: auth.RoleWarehouseManager}), true},
		{"assigned driver", f.connect(t, auth.Principal{UserID: f.driver, TenantID: f.carrier, Role: auth.RoleDriver}), true},
		{"another driver", f.connect(t, auth.Principal{UserID: uuid.New(), TenantID: f.carrier, Role: auth.RoleDriver}), false},
		{"another tenant", f.connect(t, auth.Principal{UserID: uuid.New(), TenantID: uuid.New(), Role: auth.RoleAdmin}), false},
	}
	// A connection answers requests once it is registered.
	for _, l := range listeners {
		send(t, l.conn, `{"type":"unsubscribe","channel":"telemetry","sscc":"`+sscc+`"}`)
		expect(t, l.conn, hub.TypeUnsubscribed)
	}
	id := uuid.New()
	f.hub.Deliver(f.shipment, hub.TypeShipmentRecalled, id, map[string]string{"sscc": sscc})
	for _, l := range listeners {
		send(t, l.conn, `{"type":"unsubscribe","channel":"telemetry","sscc":"`+sscc+`"}`)
		m := expect(t, l.conn, map[bool]string{true: hub.TypeShipmentRecalled, false: hub.TypeUnsubscribed}[l.receive])
		if l.receive && m.ID != id {
			t.Errorf("%s: id = %s, want %s", l.name, m.ID, id)
		}
	}
}

func TestTokenExpiryAndShutdown(t *testing.T) {
	f := newFixture(t, 0)
	soon := f.connect(t, auth.Principal{UserID: uuid.New(), TenantID: f.owner, Role: auth.RoleAdmin,
		ExpiresAt: time.Now().Add(2 * time.Second)})
	later := f.connect(t, auth.Principal{UserID: uuid.New(), TenantID: f.owner, Role: auth.RoleAdmin})
	expectClose(t, soon, hub.CloseUnauthenticated)

	send(t, later, subscribe(sscc))
	expect(t, later, hub.TypeSubscribed)
	f.hub.Shutdown()
	expectClose(t, later, websocket.StatusGoingAway)
	// The hub accepts no new connection.
	expectClose(t, f.connect(t, auth.Principal{UserID: uuid.New(), TenantID: f.owner, Role: auth.RoleAdmin}),
		websocket.StatusGoingAway)
}
