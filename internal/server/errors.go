package server

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NotFoundError struct {
	Resource string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s not found", e.Resource)
}

type AlreadyExistsError struct {
	Resource string
}

func (e *AlreadyExistsError) Error() string {
	return fmt.Sprintf("%s already exists", e.Resource)
}

type PermissionDeniedError struct{}

func (e *PermissionDeniedError) Error() string {
	return "permission denied"
}

type InvalidPageTokenError struct {
	Err error
}

func (e *InvalidPageTokenError) Error() string {
	return fmt.Sprintf("invalid page token: %v", e.Err)
}

func (e *InvalidPageTokenError) Unwrap() error {
	return e.Err
}

func NotFound(resource string) error {
	return &NotFoundError{Resource: resource}
}

func AlreadyExists(resource string) error {
	return &AlreadyExistsError{Resource: resource}
}

func PermissionDenied() error {
	return &PermissionDeniedError{}
}

func InvalidPageToken(err error) error {
	return &InvalidPageTokenError{Err: err}
}

func toStatusError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if (pgErr.Code == "55000" || pgErr.Code == "23514") && pgErr.ConstraintName == "volume_anchor_migration" {
			return status.Error(codes.FailedPrecondition, "volume_anchor_migration_conflict")
		}
		if (pgErr.Code == "55000" || pgErr.Code == "23514") &&
			(pgErr.ConstraintName == "prepared_workload_lifecycle" || pgErr.ConstraintName == "preparation_revocation_lifecycle" || pgErr.ConstraintName == "workloads_preparation_shape" || pgErr.ConstraintName == "runtime_prepared_pin" || pgErr.ConstraintName == "resource_anchor_lifecycle" || pgErr.ConstraintName == "workloads_resource_anchors_shape" || pgErr.ConstraintName == "volumes_resource_anchor_shape" || pgErr.ConstraintName == "runtime_resource_anchor_pin") {
			return status.Error(codes.FailedPrecondition, "prepared_workload_lifecycle_conflict")
		}
		if pgErr.Code == "55000" && pgErr.ConstraintName == "runtime_volume_admission" {
			return status.Error(codes.FailedPrecondition, "runtime_volume_admission_conflict")
		}
		if pgErr.Code == "40001" || pgErr.Code == "40P01" {
			return status.Error(codes.Aborted, "transaction_conflict")
		}
	}
	var notFound *NotFoundError
	if errors.As(err, &notFound) {
		return status.Error(codes.NotFound, notFound.Error())
	}
	var exists *AlreadyExistsError
	if errors.As(err, &exists) {
		return status.Error(codes.AlreadyExists, exists.Error())
	}
	var denied *PermissionDeniedError
	if errors.As(err, &denied) {
		return status.Error(codes.PermissionDenied, denied.Error())
	}
	return status.Errorf(codes.Internal, "internal error: %v", err)
}
