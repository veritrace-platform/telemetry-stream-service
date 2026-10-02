package pgerror_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/veritrace-platform/telemetry-stream-service/internal/pgerror"
)

func TestIsDataError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: "23514"}, true},                           // check_violation
		{fmt.Errorf("insert: %w", &pgconn.PgError{Code: "22003"}), true}, // numeric_value_out_of_range
		{&pgconn.PgError{Code: "42501"}, false},                          // insufficient_privilege
		{&pgconn.PgError{Code: "57P01"}, false},                          // admin_shutdown
		{&pgconn.PgError{Code: "40001"}, false},                          // serialization_failure
		{context.DeadlineExceeded, false},
		{errors.New("connection refused"), false},
		{nil, false},
	}
	for _, tt := range tests {
		if got := pgerror.IsDataError(tt.err); got != tt.want {
			t.Errorf("IsDataError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}
