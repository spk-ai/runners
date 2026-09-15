package server

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdmissionDatabaseStatusErrors(t *testing.T) {
	for _, tt := range []struct {
		name       string
		sqlstate   string
		constraint string
		want       codes.Code
	}{
		{"admission", "55000", "runtime_volume_admission", codes.FailedPrecondition},
		{"preparation-revocation", "55000", "preparation_revocation_lifecycle", codes.FailedPrecondition},
		{"serialization", "40001", "", codes.Aborted},
		{"deadlock", "40P01", "", codes.Aborted},
		{"unrelated-constraint", "55000", "unrelated", codes.Internal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := fmt.Errorf("storage write: %w", &pgconn.PgError{Code: tt.sqlstate, ConstraintName: tt.constraint})
			if got := status.Code(toStatusError(err)); got != tt.want {
				t.Fatalf("code=%s want=%s", got, tt.want)
			}
			if got := status.Code(volumeLifecycleStatusError(err)); got != tt.want {
				t.Fatalf("checked volume code=%s want=%s", got, tt.want)
			}
		})
	}
}
