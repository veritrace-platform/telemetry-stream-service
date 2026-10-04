package httpx_test

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/httpx"
)

type payload struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
	Inner struct {
		Flag bool `json:"flag"`
	} `json:"inner"`
}

func TestDecodeJSON(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int // 0 means success
		wantCode    string
		wantField   string
	}{
		{name: "valid", contentType: "application/json", body: `{"name":"a","count":2,"inner":{"flag":true}}`},
		{name: "charset parameter", contentType: "application/json; charset=utf-8", body: `{"name":"a"}`},
		{name: "merge patch", contentType: "application/merge-patch+json", body: `{"name":"a"}`},
		{name: "trailing whitespace", contentType: "application/json", body: "{\"name\":\"a\"}\n  "},
		{name: "missing content type", body: `{}`, wantStatus: 415, wantCode: httpx.CodeUnsupportedMediaType},
		{name: "form content type", contentType: "application/x-www-form-urlencoded", body: `a=b`, wantStatus: 415, wantCode: httpx.CodeUnsupportedMediaType},
		{name: "empty body", contentType: "application/json", wantStatus: 400, wantCode: httpx.CodeValidationFailed},
		{name: "malformed", contentType: "application/json", body: `{"name":`, wantStatus: 400, wantCode: httpx.CodeValidationFailed},
		{name: "two values", contentType: "application/json", body: `{} {}`, wantStatus: 400, wantCode: httpx.CodeValidationFailed},
		{name: "unknown field", contentType: "application/json", body: `{"nme":"a"}`, wantStatus: 400, wantCode: httpx.CodeValidationFailed, wantField: "nme"},
		{name: "wrong type", contentType: "application/json", body: `{"count":"two"}`, wantStatus: 400, wantCode: httpx.CodeValidationFailed, wantField: "count"},
		{name: "wrong nested type", contentType: "application/json", body: `{"inner":{"flag":"yes"}}`, wantStatus: 400, wantCode: httpx.CodeValidationFailed, wantField: "inner.flag"},
		{name: "too large", contentType: "application/json", body: `{"name":"` + strings.Repeat("x", httpx.MaxJSONBodyBytes) + `"}`, wantStatus: 413, wantCode: httpx.CodePayloadTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/items", strings.NewReader(tt.body))
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			var dst payload
			p := httpx.DecodeJSON(httptest.NewRecorder(), req, &dst)

			if tt.wantStatus == 0 {
				if p != nil {
					t.Fatalf("DecodeJSON() = %+v, want success", p)
				}
				if dst.Name != "a" {
					t.Errorf("decoded name = %q, want a", dst.Name)
				}
				return
			}
			if p == nil {
				t.Fatalf("DecodeJSON() succeeded, want %d %s", tt.wantStatus, tt.wantCode)
			}
			if p.Status != tt.wantStatus || p.Code != tt.wantCode {
				t.Errorf("problem = %d %s, want %d %s", p.Status, p.Code, tt.wantStatus, tt.wantCode)
			}
			if tt.wantField != "" && (len(p.Errors) != 1 || p.Errors[0].Field != tt.wantField) {
				t.Errorf("field errors = %+v, want one for %q", p.Errors, tt.wantField)
			}
		})
	}
}

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.WriteJSON(rec, httptest.NewRequest(http.MethodGet, "/items/1", nil), http.StatusCreated, map[string]int{"n": 1})

	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
	var got map[string]int
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil || got["n"] != 1 {
		t.Errorf("body = %v, %v; want {n: 1}", got, err)
	}
}

func TestWriteJSONReportsUnencodableValues(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.WriteJSON(rec, httptest.NewRequest(http.MethodGet, "/items/1", nil), http.StatusOK, math.NaN())

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if p := decodeProblem(t, rec); p.Code != httpx.CodeInternalError {
		t.Errorf("code = %q, want %q", p.Code, httpx.CodeInternalError)
	}
}

func TestValidationProblem(t *testing.T) {
	errs := []httpx.FieldError{{Field: "name", Code: httpx.FieldRequired, Message: "is required"}}
	p := httpx.ValidationProblem(errs)
	if p.Status != http.StatusBadRequest || p.Code != httpx.CodeValidationFailed || len(p.Errors) != 1 {
		t.Errorf("ValidationProblem() = %+v", p)
	}
}
