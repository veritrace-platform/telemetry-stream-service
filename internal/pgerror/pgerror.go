// Package pgerror classifies PostgreSQL errors for consumers, which must tell a record that the database refuses
// from a database that is unavailable.
package pgerror

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// IsDataError reports whether PostgreSQL refused the data of a statement: a data exception (SQLSTATE class 22)
// or an integrity constraint violation (class 23). The same data fails again, so a consumer sets the record aside
// instead of retrying it forever. Any other error, such as a lost connection, may pass on a retry.
func IsDataError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23"))
}
