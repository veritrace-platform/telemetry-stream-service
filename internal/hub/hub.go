// Package hub is the real-time notification hub (ADR-0008; messaging.md §6): one WebSocket endpoint that delivers
// cold-chain breaches and recalls to the users who may view the affected shipment, and live readings to the
// connections that subscribe to a shipment's telemetry. Every instance delivers to its own connections; Fanout
// feeds it from Kafka.
package hub

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/veritrace-platform/telemetry-stream-service/internal/access"
	"github.com/veritrace-platform/telemetry-stream-service/internal/auth"
	"github.com/veritrace-platform/telemetry-stream-service/internal/gs1"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/httpx"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection"
)

// Protocol (messaging.md §6).
const (
	// Subprotocol is the protocol that the client offers and the server selects.
	Subprotocol  = "veritrace.v1"
	bearerPrefix = "bearer."
	// ChannelTelemetry is the channel of live readings.
	ChannelTelemetry = "telemetry"
	// MaxSubscriptions is the number of SSCCs a connection may follow at once.
	MaxSubscriptions = 20
	pingInterval     = 30 * time.Second
	pongTimeout      = 10 * time.Second
	// maxClientMessage bounds a client message; subscriptions are a few dozen bytes.
	maxClientMessage = 4096
	// sendBuffer is how many messages a connection may fall behind before it is closed.
	sendBuffer   = 256
	writeTimeout = 10 * time.Second
)

// Close codes (messaging.md §6).
const (
	CloseBadMessage           websocket.StatusCode = 4400
	CloseUnauthenticated      websocket.StatusCode = 4401
	CloseForbidden            websocket.StatusCode = 4403
	CloseTooManySubscriptions websocket.StatusCode = 4408
)

// Server message types (messaging.md §6.1).
const (
	TypeShipmentRecalled = "shipment.recalled"
	TypeReading          = "telemetry.reading"
	TypeSubscribed       = "subscribed"
	TypeUnsubscribed     = "unsubscribed"
	TypeError            = "error"
)

// Error codes of error messages.
const (
	ErrorForbidden      = "FORBIDDEN"
	ErrorNotFound       = "NOT_FOUND"
	ErrorInvalidMessage = "INVALID_MESSAGE"
)

// sentAtLayout formats sent_at.
const sentAtLayout = "2006-01-02T15:04:05.000Z07:00"

// Message is a server → client message.
type Message struct {
	Type   string    `json:"type"`
	ID     uuid.UUID `json:"id"`
	SentAt string    `json:"sent_at"`
	Data   any       `json:"data"`
}

// Shipments returns the projection of an SSCC, as *projection.Store does.
type Shipments interface {
	Get(ctx context.Context, sscc string) (projection.Shipment, error)
}

// Verifier checks access tokens, as *auth.Verifier does.
type Verifier interface {
	Verify(ctx context.Context, token string) (auth.Principal, error)
}

// Hub keeps the connections of this instance and delivers notifications to them.
type Hub struct {
	shipments   Shipments
	verifier    Verifier
	origins     []string
	logger      *slog.Logger
	now         func() time.Time
	connections prometheus.Gauge

	mu      sync.Mutex
	clients map[*client]struct{}
	closed  bool
}

// New returns a hub. origins lists the origins, besides the request's own host, from which browsers may connect
// (WS_ALLOWED_ORIGINS), for example https://app.example.com. Its metrics are registered with registerer.
func New(shipments Shipments, verifier Verifier, origins []string, logger *slog.Logger,
	registerer prometheus.Registerer, now func() time.Time,
) *Hub {
	connections := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "veritrace_telemetry_websocket_connections",
		Help: "Open WebSocket notification connections.",
	})
	registerer.MustRegister(connections)
	return &Hub{
		shipments: shipments, verifier: verifier, origins: origins, logger: logger, now: now,
		connections: connections, clients: map[*client]struct{}{},
	}
}

// Routes registers the endpoint under /ws/v1.
func (h *Hub) Routes(r chi.Router) {
	r.Get("/notifications", h.serve)
}

// serve upgrades a request to a notification connection. The client offers the subprotocols veritrace.v1 and
// bearer.<access token>, because browsers cannot set headers on WebSocket requests (ADR-0007). An unacceptable
// origin is refused before the upgrade; a missing, invalid, or expired token closes the connection with 4401, so
// that the client refreshes its token.
func (h *Hub) serve(w http.ResponseWriter, r *http.Request) {
	offered := subprotocols(r)
	if !slices.Contains(offered, Subprotocol) {
		httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusBadRequest, httpx.CodeValidationFailed,
			"offer the WebSocket subprotocols veritrace.v1 and bearer.<access token>"))
		return
	}
	var token string
	for _, p := range offered {
		if t, ok := strings.CutPrefix(p, bearerPrefix); ok {
			token = t
		}
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{Subprotocol}, OriginPatterns: h.origins,
	})
	if err != nil {
		// Accept has answered, for example 403 for a foreign origin.
		h.logger.InfoContext(r.Context(), "WebSocket upgrade refused", slog.Any("error", err))
		return
	}
	// The request context must not be used once the connection is hijacked.
	ctx := context.WithoutCancel(r.Context())
	conn.SetReadLimit(64 << 10)

	principal, err := h.verifier.Verify(ctx, token)
	switch {
	case token == "":
		_ = conn.Close(CloseUnauthenticated, "an access token is required")
		return
	case errors.Is(err, auth.ErrKeysUnavailable):
		_ = conn.Close(websocket.StatusTryAgainLater, "access tokens cannot be verified right now")
		return
	case errors.Is(err, auth.ErrTokenExpired):
		_ = conn.Close(CloseUnauthenticated, "the access token has expired")
		return
	case err != nil:
		_ = conn.Close(CloseUnauthenticated, "the access token is invalid")
		return
	}

	c := &client{
		hub: h, conn: conn, principal: principal, send: make(chan []byte, sendBuffer), closing: make(chan closeRequest, 1),
		subs: map[string]struct{}{},
	}
	if !h.register(c) {
		_ = conn.Close(websocket.StatusGoingAway, "server shutdown")
		return
	}
	defer h.unregister(c)
	c.run(ctx)
}

// subprotocols returns the subprotocols that the client offers.
func subprotocols(r *http.Request) []string {
	var offered []string
	for _, header := range r.Header.Values("Sec-WebSocket-Protocol") {
		for p := range strings.SplitSeq(header, ",") {
			if p = strings.TrimSpace(p); p != "" {
				offered = append(offered, p)
			}
		}
	}
	return offered
}

func (h *Hub) register(c *client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.clients[c] = struct{}{}
	h.connections.Inc()
	return true
}

func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		h.connections.Dec()
	}
}

// Shutdown closes every connection with 1001, and the hub accepts no more. The HTTP server calls it when it shuts
// down, because it does not track hijacked connections.
func (h *Hub) Shutdown() {
	h.mu.Lock()
	h.closed = true
	clients := make([]*client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		c.close(websocket.StatusGoingAway, "server shutdown")
	}
}

// message encodes a server message.
func (h *Hub) message(typ string, id uuid.UUID, data any) ([]byte, error) {
	return json.Marshal(Message{Type: typ, ID: id, SentAt: h.now().UTC().Format(sentAtLayout), Data: data})
}

// Deliver sends a message to every connection whose user may view the shipment (access.View): users of a
// participant tenant, and of drivers only the assigned one.
func (h *Hub) Deliver(s projection.Shipment, typ string, id uuid.UUID, data any) {
	payload, err := h.message(typ, id, data)
	if err != nil {
		h.logger.Error("encode notification", slog.String("type", typ), slog.Any("error", err))
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if access.View(c.principal, s) == nil {
			c.push(payload)
		}
	}
}

// Subscribed reports whether a connection follows the SSCC's telemetry.
func (h *Hub) Subscribed(sscc string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if _, ok := c.subs[sscc]; ok {
			return true
		}
	}
	return false
}

// Publish sends a reading to the connections that follow the SSCC.
func (h *Hub) Publish(sscc string, data any) {
	payload, err := h.message(TypeReading, uuid.Must(uuid.NewV7()), data)
	if err != nil {
		h.logger.Error("encode reading", slog.Any("error", err))
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if _, ok := c.subs[sscc]; ok {
			c.push(payload)
		}
	}
}

// subscribe adds a telemetry subscription after checking that the caller may view the shipment.
func (h *Hub) subscribe(ctx context.Context, c *client, sscc string) {
	s, err := h.shipments.Get(ctx, sscc)
	switch {
	case errors.Is(err, projection.ErrNotFound):
		c.reply(TypeError, errorData{Code: ErrorNotFound, Message: "shipment not found"})
		return
	case err != nil:
		h.logger.ErrorContext(ctx, "subscribe", slog.String("sscc", sscc), slog.Any("error", err))
		c.close(websocket.StatusInternalError, "internal error")
		return
	}
	switch err := access.View(c.principal, s); {
	case errors.Is(err, access.ErrNotFound):
		c.reply(TypeError, errorData{Code: ErrorNotFound, Message: "shipment not found"})
		return
	case errors.Is(err, access.ErrForbidden):
		c.reply(TypeError, errorData{Code: ErrorForbidden, Message: err.Error()})
		c.close(CloseForbidden, "forbidden subscription")
		return
	}
	h.mu.Lock()
	_, already := c.subs[sscc]
	tooMany := !already && len(c.subs) >= MaxSubscriptions
	if !tooMany {
		c.subs[sscc] = struct{}{}
	}
	h.mu.Unlock()
	if tooMany {
		c.close(CloseTooManySubscriptions, "too many subscriptions")
		return
	}
	c.reply(TypeSubscribed, channelData{Channel: ChannelTelemetry, SSCC: sscc})
}

func (h *Hub) unsubscribe(c *client, sscc string) {
	h.mu.Lock()
	delete(c.subs, sscc)
	h.mu.Unlock()
	c.reply(TypeUnsubscribed, channelData{Channel: ChannelTelemetry, SSCC: sscc})
}

type channelData struct {
	Channel string `json:"channel"`
	SSCC    string `json:"sscc"`
}

type errorData struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// clientMessage is a client → server message (messaging.md §6.2).
type clientMessage struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	SSCC    string `json:"sscc"`
}

// client is one connection.
type client struct {
	hub       *Hub
	conn      *websocket.Conn
	principal auth.Principal
	send      chan []byte
	closing   chan closeRequest
	// subs is guarded by hub.mu.
	subs      map[string]struct{}
	closeOnce sync.Once
}

// closeRequest asks the writer to close the connection, after the queued messages if drain is set.
type closeRequest struct {
	code   websocket.StatusCode
	reason string
	drain  bool
}

// run serves the connection until it ends: it reads client messages, writes queued messages, pings the client,
// and closes the connection when the token expires.
func (c *client) run(ctx context.Context) {
	defer func() { _ = c.conn.CloseNow() }()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	written := make(chan struct{})
	go func() {
		defer close(written)
		c.write(ctx)
	}()
	go c.ping(ctx)
	expiry := time.AfterFunc(time.Until(c.principal.ExpiresAt), func() {
		c.close(CloseUnauthenticated, "the access token has expired")
	})
	defer expiry.Stop()
	c.read(ctx)

	// Reading has ended, because the client left or a close was requested. Let the writer send what is queued and
	// the close frame before it stops.
	c.close(websocket.StatusNormalClosure, "")
	select {
	case <-written:
	case <-time.After(2 * writeTimeout):
	}
}

func (c *client) read(ctx context.Context) {
	for {
		typ, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText || len(data) > maxClientMessage {
			c.close(CloseBadMessage, "messages are JSON text of at most 4 KiB")
			return
		}
		var m clientMessage
		if err := json.Unmarshal(data, &m); err != nil {
			c.close(CloseBadMessage, "messages are JSON objects")
			return
		}
		switch {
		case m.Type != "subscribe" && m.Type != "unsubscribe":
			c.reply(TypeError, errorData{Code: ErrorInvalidMessage, Message: "type must be subscribe or unsubscribe"})
		case m.Channel != ChannelTelemetry:
			c.reply(TypeError, errorData{Code: ErrorInvalidMessage, Message: "channel must be telemetry"})
		case !gs1.ValidSSCC(m.SSCC):
			c.reply(TypeError, errorData{Code: ErrorInvalidMessage, Message: "sscc must be a valid SSCC-18"})
		case m.Type == "subscribe":
			c.hub.subscribe(ctx, c, m.SSCC)
		default:
			c.hub.unsubscribe(c, m.SSCC)
		}
	}
}

// write sends the queued messages in order, and closes the connection when asked to.
func (c *client) write(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-c.closing:
			for req.drain && len(c.send) > 0 {
				if !c.writeOne(ctx, <-c.send) {
					return
				}
			}
			_ = c.conn.Close(req.code, req.reason)
			return
		case payload := <-c.send:
			if !c.writeOne(ctx, payload) {
				return
			}
		}
	}
}

func (c *client) writeOne(ctx context.Context, payload []byte) bool {
	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	if err := c.conn.Write(wctx, websocket.MessageText, payload); err != nil {
		_ = c.conn.CloseNow()
		return false
	}
	return true
}

// ping checks every 30 s that the client is there; one that does not answer within 10 s is disconnected.
func (c *client) ping(ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, pongTimeout)
			err := c.conn.Ping(pctx)
			cancel()
			if err != nil {
				_ = c.conn.CloseNow()
				return
			}
		}
	}
}

// push queues a message without blocking. A connection that has fallen too far behind is closed; the client
// reconnects and reads the current state through the REST API.
func (c *client) push(payload []byte) {
	select {
	case c.send <- payload:
	default:
		c.closeWith(closeRequest{code: websocket.StatusPolicyViolation, reason: "the connection is too slow"})
	}
}

// reply queues a message to this client.
func (c *client) reply(typ string, data any) {
	payload, err := c.hub.message(typ, uuid.Must(uuid.NewV7()), data)
	if err == nil {
		c.push(payload)
	}
}

// close closes the connection once, after the messages already queued, so that an error message reaches the
// client before the close frame.
func (c *client) close(code websocket.StatusCode, reason string) {
	c.closeWith(closeRequest{code: code, reason: reason, drain: true})
}

func (c *client) closeWith(req closeRequest) {
	c.closeOnce.Do(func() { c.closing <- req })
}
