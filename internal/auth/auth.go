// Package auth verifies the access tokens that core-business-service issues (ADR-0007): EdDSA-signed JWTs, checked
// against the public keys that core publishes at /.well-known/jwks.json. Telemetry never holds signing keys.
package auth

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Role is a user's role within its tenant (access-control.md §1).
type Role string

// Roles.
const (
	RoleAdmin            Role = "ADMIN"
	RoleWarehouseManager Role = "WAREHOUSE_MANAGER"
	RoleDriver           Role = "DRIVER"
	RoleInspector        Role = "INSPECTOR"
)

// Valid reports whether r is a known role.
func (r Role) Valid() bool {
	switch r {
	case RoleAdmin, RoleWarehouseManager, RoleDriver, RoleInspector:
		return true
	}
	return false
}

// Principal is the caller that an access token names.
type Principal struct {
	UserID   uuid.UUID
	TenantID uuid.UUID
	Role     Role
	// ExpiresAt is when the token expires; a WebSocket connection closes then.
	ExpiresAt time.Time
}

type contextKey struct{}

// NewContext returns a copy of ctx carrying p.
func NewContext(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, p)
}

// FromContext returns the principal stored in ctx, if any.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(contextKey{}).(Principal)
	return p, ok
}

// Verification failures.
var (
	ErrTokenExpired = errors.New("access token expired")
	ErrTokenInvalid = errors.New("access token invalid")
	// ErrKeysUnavailable reports that the keys could not be loaded, so no token can be verified for now.
	ErrKeysUnavailable = errors.New("token verification keys unavailable")
)

// leeway tolerates clock differences between core and telemetry.
const leeway = 5 * time.Second

// KeySource returns the public key with an ID.
type KeySource interface {
	Key(ctx context.Context, kid string) (ed25519.PublicKey, error)
}

// Verifier checks access tokens.
type Verifier struct {
	keys KeySource
	now  func() time.Time
}

// NewVerifier returns a verifier that reads keys from keys and the time from now.
func NewVerifier(keys KeySource, now func() time.Time) *Verifier {
	return &Verifier{keys: keys, now: now}
}

// claims are the access token claims of ADR-0007; telemetry does not use jti.
type claims struct {
	TenantID string `json:"tid"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

// Verify checks a token and returns its principal. It returns ErrTokenExpired for an expired token with a valid
// signature, ErrKeysUnavailable when no key can be loaded, and ErrTokenInvalid for anything else.
func (v *Verifier) Verify(ctx context.Context, token string) (Principal, error) {
	var c claims
	_, err := jwt.ParseWithClaims(token, &c, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		return v.keys.Key(ctx, kid)
	},
		jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()}),
		jwt.WithTimeFunc(v.now),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(leeway),
	)
	switch {
	case errors.Is(err, ErrKeysUnavailable):
		return Principal{}, ErrKeysUnavailable
	case errors.Is(err, jwt.ErrTokenExpired):
		return Principal{}, ErrTokenExpired
	case err != nil:
		return Principal{}, fmt.Errorf("%w: %w", ErrTokenInvalid, err)
	}

	p := Principal{Role: Role(c.Role), ExpiresAt: c.ExpiresAt.Time}
	var errs [2]error
	p.UserID, errs[0] = uuid.Parse(c.Subject)
	p.TenantID, errs[1] = uuid.Parse(c.TenantID)
	if err := errors.Join(errs[:]...); err != nil || !p.Role.Valid() {
		return Principal{}, fmt.Errorf("%w: malformed claims", ErrTokenInvalid)
	}
	return p, nil
}

// JWKS refresh policy.
const (
	// refreshAfter is how long a key set is used before it is fetched again.
	refreshAfter = 10 * time.Minute
	// refetchAfter limits how often keys are fetched while some are loaded: for an unknown key ID, for example
	// right after a key rotation, or after a refresh that failed.
	refetchAfter = 30 * time.Second
	// retryAfter limits how often a key set that could not be loaded at all is fetched again.
	retryAfter = 2 * time.Second
	// fetchTimeout bounds one fetch.
	fetchTimeout = 5 * time.Second
	// maxJWKSBytes bounds the key set document.
	maxJWKSBytes = 64 << 10
)

// JWKS fetches core's public keys and caches them. A fetch that fails keeps the keys loaded before.
type JWKS struct {
	url    string
	client *http.Client
	logger *slog.Logger
	now    func() time.Time

	mu        sync.Mutex
	keys      map[string]ed25519.PublicKey
	loadedAt  time.Time
	attemptAt time.Time
}

// NewJWKS returns a key source for the JWKS document at url. Keys are fetched on first use.
func NewJWKS(url string, client *http.Client, logger *slog.Logger, now func() time.Time) *JWKS {
	return &JWKS{url: url, client: client, logger: logger, now: now}
}

// Key returns the Ed25519 public key with the ID kid. The key set is fetched on first use, again once it is 10
// minutes old, and again for a key ID it lacks, so that a rotated key is found. A fetch that fails keeps the keys
// loaded before, and the next attempt waits 2 s while no key is loaded and 30 s otherwise, so that an unreachable
// core does not slow down every request.
func (j *JWKS) Key(ctx context.Context, kid string) (ed25519.PublicKey, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	now := j.now()
	key, known := j.keys[kid]
	if !known || now.Sub(j.loadedAt) >= refreshAfter {
		j.refreshIfDue(ctx, now)
		key, known = j.keys[kid]
	}
	switch {
	case j.keys == nil:
		return nil, ErrKeysUnavailable
	case !known:
		return nil, fmt.Errorf("unknown key %q", kid)
	}
	return key, nil
}

// Ready loads the keys if none are loaded yet and reports whether some are.
func (j *JWKS) Ready(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.keys == nil {
		j.refreshIfDue(ctx, j.now())
	}
	if j.keys == nil {
		return ErrKeysUnavailable
	}
	return nil
}

// refreshIfDue fetches the key set unless the last attempt is too recent. On failure it keeps the keys it has.
func (j *JWKS) refreshIfDue(ctx context.Context, now time.Time) {
	wait := refetchAfter
	if j.keys == nil {
		wait = retryAfter
	}
	if !j.attemptAt.IsZero() && now.Sub(j.attemptAt) < wait {
		return
	}
	j.attemptAt = now
	keys, err := j.fetch(ctx)
	if err != nil {
		j.logger.WarnContext(ctx, "token verification keys not loaded", slog.String("url", j.url), slog.Any("error", err))
		return
	}
	j.keys, j.loadedAt = keys, now
}

func (j *JWKS) fetch(ctx context.Context) (map[string]ed25519.PublicKey, error) {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.url, nil) //nolint:gosec // JWKS_URL is configuration
	if err != nil {
		return nil, fmt.Errorf("build JWKS request: %w", err)
	}
	resp, err := j.client.Do(req) //nolint:gosec // JWKS_URL is configuration
	if err != nil {
		return nil, fmt.Errorf("fetch JWKS: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch JWKS: status %d", resp.StatusCode)
	}
	var doc struct {
		Keys []struct {
			KeyType string `json:"kty"`
			Curve   string `json:"crv"`
			KeyID   string `json:"kid"`
			X       string `json:"x"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxJWKSBytes)).Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode JWKS: %w", err)
	}
	keys := map[string]ed25519.PublicKey{}
	for _, k := range doc.Keys {
		if k.KeyType != "OKP" || k.Curve != "Ed25519" || k.KeyID == "" {
			continue
		}
		x, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil || len(x) != ed25519.PublicKeySize {
			continue
		}
		keys[k.KeyID] = ed25519.PublicKey(x)
	}
	if len(keys) == 0 {
		return nil, errors.New("JWKS has no Ed25519 key")
	}
	return keys, nil
}
