// Package authtest issues access tokens for tests the way core-business-service does, and serves its JWKS.
package authtest

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/veritrace-platform/telemetry-stream-service/internal/auth"
)

// Issuer signs access tokens and serves the JWKS that verifies them.
type Issuer struct {
	// URL is the JWKS document's address.
	URL string
	// Fetches counts the JWKS requests, answered or not.
	Fetches atomic.Int32
	// Unavailable makes the JWKS server answer 503.
	Unavailable atomic.Bool

	mu   sync.Mutex
	keys map[string]ed25519.PrivateKey
	kid  string
}

// NewIssuer starts a JWKS server with one key, which the test closes when it finishes.
func NewIssuer(t testing.TB) *Issuer {
	t.Helper()
	iss := &Issuer{keys: map[string]ed25519.PrivateKey{}}
	iss.Rotate(t, "test-1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		iss.Fetches.Add(1)
		if iss.Unavailable.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		iss.mu.Lock()
		defer iss.mu.Unlock()
		type jwk struct {
			KeyType string `json:"kty"`
			Curve   string `json:"crv"`
			KeyID   string `json:"kid"`
			X       string `json:"x"`
			Alg     string `json:"alg"`
			Use     string `json:"use"`
		}
		doc := struct {
			Keys []jwk `json:"keys"`
		}{}
		for kid, key := range iss.keys {
			doc.Keys = append(doc.Keys, jwk{KeyType: "OKP", Curve: "Ed25519", KeyID: kid, Alg: "EdDSA", Use: "sig",
				X: base64.RawURLEncoding.EncodeToString(key.Public().(ed25519.PublicKey))})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	t.Cleanup(srv.Close)
	iss.URL = srv.URL + "/.well-known/jwks.json"
	return iss
}

// Rotate adds a key with the ID kid, which signs from now on.
func (iss *Issuer) Rotate(t testing.TB, kid string) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	iss.mu.Lock()
	defer iss.mu.Unlock()
	iss.keys[kid], iss.kid = key, kid
}

// Token signs an access token for p that expires at p.ExpiresAt, or in 15 minutes if it is zero.
func (iss *Issuer) Token(t testing.TB, p auth.Principal) string {
	t.Helper()
	iss.mu.Lock()
	defer iss.mu.Unlock()
	expires := p.ExpiresAt
	if expires.IsZero() {
		expires = time.Now().Add(15 * time.Minute)
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
		"sub": p.UserID.String(), "tid": p.TenantID.String(), "role": string(p.Role),
		"iat": time.Now().Add(-time.Minute).Unix(), "exp": expires.Unix(), "jti": uuid.NewString(),
	})
	token.Header["kid"] = iss.kid
	signed, err := token.SignedString(iss.keys[iss.kid])
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}

// Principal returns a principal with new IDs.
func Principal(role auth.Role) auth.Principal {
	return auth.Principal{UserID: uuid.New(), TenantID: uuid.New(), Role: role}
}
