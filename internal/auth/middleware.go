package auth

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/httpx"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/logging"
	"github.com/veritrace-platform/telemetry-stream-service/internal/rest"
)

// Middleware requires a valid bearer token on the requests it wraps and answers as core does: 401 TOKEN_EXPIRED
// for an expired token, which the client refreshes, and 401 UNAUTHENTICATED otherwise. While no key can be
// loaded it answers 503 SERVICE_UNAVAILABLE. It stores the principal in the request context and adds the tenant
// and user to every log record of the request.
func (v *Verifier) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r)
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="veritrace"`)
			httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusUnauthorized, httpx.CodeUnauthenticated,
				"a bearer access token is required"))
			return
		}
		p, err := v.Verify(r.Context(), token)
		switch {
		case errors.Is(err, ErrKeysUnavailable):
			httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusServiceUnavailable, httpx.CodeServiceUnavailable,
				"access tokens cannot be verified right now; retry shortly"))
			return
		case errors.Is(err, ErrTokenExpired):
			w.Header().Set("WWW-Authenticate", `Bearer realm="veritrace", error="invalid_token", error_description="expired"`)
			httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusUnauthorized, rest.CodeTokenExpired,
				"the access token has expired; refresh it and retry"))
			return
		case err != nil:
			w.Header().Set("WWW-Authenticate", `Bearer realm="veritrace", error="invalid_token"`)
			httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusUnauthorized, httpx.CodeUnauthenticated,
				"the access token is invalid"))
			return
		}
		ctx := NewContext(r.Context(), p)
		ctx = logging.WithAttrs(ctx, slog.String("tenant_id", p.TenantID.String()), slog.String("user_id", p.UserID.String()))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func bearerToken(r *http.Request) (string, bool) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	token = strings.TrimSpace(token)
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", false
	}
	return token, true
}

// Caller returns the authenticated caller of r. Handlers behind the middleware always have one; without it, the
// request fails closed with 401 UNAUTHENTICATED and ok is false.
func Caller(w http.ResponseWriter, r *http.Request) (p Principal, ok bool) {
	if p, ok = FromContext(r.Context()); !ok {
		httpx.WriteProblem(w, r, httpx.NewProblem(http.StatusUnauthorized, httpx.CodeUnauthenticated,
			"a bearer access token is required"))
	}
	return p, ok
}
