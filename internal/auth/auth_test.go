package auth_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/veritrace-platform/telemetry-stream-service/internal/auth"
	"github.com/veritrace-platform/telemetry-stream-service/internal/auth/authtest"
)

// clock is a settable time source.
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newVerifier(url string, c *clock) (*auth.Verifier, *auth.JWKS) {
	keys := auth.NewJWKS(url, http.DefaultClient, slog.New(slog.DiscardHandler), c.now)
	return auth.NewVerifier(keys, func() time.Time { return time.Now() }), keys
}

func TestVerify(t *testing.T) {
	iss := authtest.NewIssuer(t)
	v, _ := newVerifier(iss.URL, &clock{t: time.Now()})
	p := authtest.Principal(auth.RoleWarehouseManager)

	got, err := v.Verify(t.Context(), iss.Token(t, p))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if got.UserID != p.UserID || got.TenantID != p.TenantID || got.Role != p.Role || got.ExpiresAt.IsZero() {
		t.Errorf("Verify() = %+v, want %+v", got, p)
	}

	expired := p
	expired.ExpiresAt = time.Now().Add(-time.Minute)
	if _, err := v.Verify(t.Context(), iss.Token(t, expired)); !errors.Is(err, auth.ErrTokenExpired) {
		t.Errorf("expired token: error = %v", err)
	}

	unknownRole := p
	unknownRole.Role = "AUDITOR"
	if _, err := v.Verify(t.Context(), iss.Token(t, unknownRole)); !errors.Is(err, auth.ErrTokenInvalid) {
		t.Errorf("unknown role: error = %v", err)
	}

	// A token signed by a key that the JWKS does not publish, with another algorithm, or tampered with.
	_, stranger, _ := ed25519.GenerateKey(rand.Reader)
	forged := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
		"sub": p.UserID.String(), "tid": p.TenantID.String(), "role": "ADMIN", "exp": time.Now().Add(time.Hour).Unix(),
	})
	forged.Header["kid"] = "test-1"
	forgedToken, _ := forged.SignedString(stranger)
	hmac := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": p.UserID.String()})
	hmac.Header["kid"] = "test-1"
	hmacToken, _ := hmac.SignedString([]byte("secret"))
	// The first character of the signature carries six significant bits, unlike the last one.
	parts := strings.Split(iss.Token(t, p), ".")
	flipped := "A"
	if parts[2][0] == 'A' {
		flipped = "B"
	}
	tampered := parts[0] + "." + parts[1] + "." + flipped + parts[2][1:]
	for name, token := range map[string]string{
		"forged": forgedToken, "HS256": hmacToken, "tampered": tampered, "garbage": "not-a-jwt",
	} {
		if _, err := v.Verify(t.Context(), token); !errors.Is(err, auth.ErrTokenInvalid) {
			t.Errorf("%s token: error = %v", name, err)
		}
	}
}

func TestJWKSCachesAndRefetchesForNewKeys(t *testing.T) {
	iss := authtest.NewIssuer(t)
	c := &clock{t: time.Now()}
	v, _ := newVerifier(iss.URL, c)
	p := authtest.Principal(auth.RoleAdmin)
	for range 3 {
		if _, err := v.Verify(t.Context(), iss.Token(t, p)); err != nil {
			t.Fatalf("Verify() error = %v", err)
		}
	}
	if n := iss.Fetches.Load(); n != 1 {
		t.Errorf("fetches = %d, want 1", n)
	}

	// A rotated key is fetched when a token names it, at most once per 30 s.
	iss.Rotate(t, "test-2")
	c.t = c.t.Add(31 * time.Second)
	if _, err := v.Verify(t.Context(), iss.Token(t, p)); err != nil {
		t.Fatalf("Verify() after rotation error = %v", err)
	}
	if n := iss.Fetches.Load(); n != 2 {
		t.Errorf("fetches = %d, want 2", n)
	}
	iss.Rotate(t, "test-3")
	c.t = c.t.Add(time.Second)
	if _, err := v.Verify(t.Context(), iss.Token(t, p)); !errors.Is(err, auth.ErrTokenInvalid) {
		t.Errorf("Verify() right after another rotation error = %v, want invalid until the next fetch", err)
	}
	// The key set is fetched again after 10 minutes anyway.
	c.t = c.t.Add(10 * time.Minute)
	if _, err := v.Verify(t.Context(), iss.Token(t, p)); err != nil {
		t.Errorf("Verify() after the refresh interval error = %v", err)
	}
}

func TestJWKSUnavailable(t *testing.T) {
	iss := authtest.NewIssuer(t)
	c := &clock{t: time.Now()}
	iss.Unavailable.Store(true)
	v, keys := newVerifier(iss.URL, c)
	token := iss.Token(t, authtest.Principal(auth.RoleAdmin))
	if _, err := v.Verify(t.Context(), token); !errors.Is(err, auth.ErrKeysUnavailable) {
		t.Errorf("Verify() error = %v, want ErrKeysUnavailable", err)
	}
	if err := keys.Ready(t.Context()); !errors.Is(err, auth.ErrKeysUnavailable) {
		t.Errorf("Ready() error = %v", err)
	}

	// Without keys, the next attempt waits 2 s.
	iss.Unavailable.Store(false)
	if err := keys.Ready(t.Context()); !errors.Is(err, auth.ErrKeysUnavailable) {
		t.Errorf("Ready() right after a failed attempt error = %v, want ErrKeysUnavailable", err)
	}
	c.t = c.t.Add(2 * time.Second)
	if err := keys.Ready(t.Context()); err != nil {
		t.Fatalf("Ready() error = %v", err)
	}
	// Keys loaded once stay in use while the JWKS cannot be fetched.
	iss.Unavailable.Store(true)
	c.t = c.t.Add(time.Hour)
	if _, err := v.Verify(t.Context(), token); err != nil {
		t.Errorf("Verify() with cached keys error = %v", err)
	}
}

func TestJWKSDoesNotFetchOnEveryCallWhileCoreIsDown(t *testing.T) {
	iss := authtest.NewIssuer(t)
	c := &clock{t: time.Now()}
	v, _ := newVerifier(iss.URL, c)
	token := iss.Token(t, authtest.Principal(auth.RoleWarehouseManager))
	if _, err := v.Verify(t.Context(), token); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	// The keys are due for a refresh, and core does not answer: the cached keys keep working, and only the first
	// call tries to fetch.
	iss.Unavailable.Store(true)
	c.t = c.t.Add(11 * time.Minute)
	for range 5 {
		if _, err := v.Verify(t.Context(), token); err != nil {
			t.Fatalf("Verify() with a cached key while core is down error = %v", err)
		}
	}
	if n := iss.Fetches.Load(); n != 2 {
		t.Errorf("fetches = %d, want 2: the first load and one failed refresh", n)
	}

	// The next attempt comes 30 s later, and succeeds once core is back.
	iss.Unavailable.Store(false)
	c.t = c.t.Add(29 * time.Second)
	if _, err := v.Verify(t.Context(), token); err != nil || iss.Fetches.Load() != 2 {
		t.Errorf("Verify() 29 s after the failed refresh = %v, fetches %d; want no new fetch", err, iss.Fetches.Load())
	}
	c.t = c.t.Add(time.Second)
	if _, err := v.Verify(t.Context(), token); err != nil || iss.Fetches.Load() != 3 {
		t.Errorf("Verify() 30 s after the failed refresh = %v, fetches %d; want a new fetch", err, iss.Fetches.Load())
	}
	c.t = c.t.Add(5 * time.Minute)
	if _, err := v.Verify(t.Context(), token); err != nil || iss.Fetches.Load() != 3 {
		t.Errorf("Verify() with fresh keys = %v, fetches %d; want no new fetch", err, iss.Fetches.Load())
	}
}

func TestMiddleware(t *testing.T) {
	iss := authtest.NewIssuer(t)
	v, _ := newVerifier(iss.URL, &clock{t: time.Now()})
	p := authtest.Principal(auth.RoleInspector)
	handler := v.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller, ok := auth.Caller(w, r)
		if ok {
			_ = json.NewEncoder(w).Encode(map[string]string{"tenant": caller.TenantID.String()})
		}
	}))
	expired := p
	expired.ExpiresAt = time.Now().Add(-time.Minute)
	tests := []struct {
		name          string
		authorization string
		status        int
		code          string
	}{
		{"valid", "Bearer " + iss.Token(t, p), http.StatusOK, ""},
		{"lowercase scheme", "bearer " + iss.Token(t, p), http.StatusOK, ""},
		{"missing", "", http.StatusUnauthorized, "UNAUTHENTICATED"},
		{"other scheme", "Basic dXNlcjpwYXNz", http.StatusUnauthorized, "UNAUTHENTICATED"},
		{"expired", "Bearer " + iss.Token(t, expired), http.StatusUnauthorized, "TOKEN_EXPIRED"},
		{"invalid", "Bearer abc.def.ghi", http.StatusUnauthorized, "UNAUTHENTICATED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/v1/telemetry/incidents", nil)
			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tt.status, rec.Body)
			}
			if tt.code != "" {
				var problem struct{ Code string }
				_ = json.Unmarshal(rec.Body.Bytes(), &problem)
				if problem.Code != tt.code || rec.Header().Get("WWW-Authenticate") == "" {
					t.Errorf("problem code = %s, WWW-Authenticate = %q", problem.Code, rec.Header().Get("WWW-Authenticate"))
				}
			}
		})
	}

	t.Run("keys unavailable", func(t *testing.T) {
		down := httptest.NewServer(http.NotFoundHandler())
		defer down.Close()
		v, _ := newVerifier(down.URL, &clock{t: time.Now()})
		req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+iss.Token(t, p))
		rec := httptest.NewRecorder()
		v.Middleware(http.NotFoundHandler()).ServeHTTP(rec, req)
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", rec.Code)
		}
	})

	t.Run("no principal fails closed", func(t *testing.T) {
		rec := httptest.NewRecorder()
		if _, ok := auth.Caller(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)); ok ||
			rec.Code != http.StatusUnauthorized {
			t.Errorf("Caller() ok = %v, status %d", ok, rec.Code)
		}
	})
}
