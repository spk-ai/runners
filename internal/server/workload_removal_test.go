package server

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	runnersv1 "github.com/agynio/runners/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func removalTestServer(t *testing.T) (*Server, pgxmock.PgxPoolIface, workloadRecord) {
	t.Helper()
	pool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := pool.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		pool.Close()
	})
	record := defaultWorkloadRecord(uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), time.Now().UTC())
	return New(Options{Pool: pool}), pool, record
}

func TestWorkloadRemovalRequiresExplicitConfirmation(t *testing.T) {
	for _, terminal := range []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
	} {
		t.Run(terminal.String(), func(t *testing.T) {
			s, pool, record := removalTestServer(t)
			record.Status, _ = workloadStatusToString(terminal)
			meteredAt := record.Meta.UpdatedAt
			record.RemovedAt = &meteredAt
			query := fmt.Sprintf("UPDATE workloads SET status = $1, removed_at = COALESCE(removed_at, NOW()), updated_at = NOW() WHERE id = $2 RETURNING %s", workloadColumns)
			pool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(record.Status, record.Meta.ID).WillReturnRows(workloadRows(t, record))
			result, err := s.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{Id: record.Meta.ID.String(), Status: &terminal})
			if err != nil {
				t.Fatal(err)
			}
			if result.Workload.RemovedAt == nil || result.Workload.RemovalConfirmedAt != nil {
				t.Fatal("terminal status must end metering without asserting physical removal")
			}

			confirmation := meteredAt.Add(time.Minute)
			for attempt := 0; attempt < 2; attempt++ {
				pool.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns))).
					WithArgs(record.Meta.ID).WillReturnRows(workloadRows(t, record))
				record.RemovalConfirmedAt = &confirmation
				supplied := confirmation.Add(time.Duration(attempt) * time.Minute)
				query = fmt.Sprintf("UPDATE workloads SET removal_confirmed_at = COALESCE(removal_confirmed_at, $1), updated_at = NOW() WHERE id = $2 RETURNING %s", workloadColumns)
				pool.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(supplied, record.Meta.ID).WillReturnRows(workloadRows(t, record))
				result, err = s.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{Id: record.Meta.ID.String(), RemovalConfirmedAt: timestamppb.New(supplied)})
				if err != nil {
					t.Fatal(err)
				}
				if result.Workload.RemovalConfirmedAt == nil || !result.Workload.RemovalConfirmedAt.AsTime().Equal(confirmation) {
					t.Fatal("first confirmation must survive readback and retries")
				}
				if !result.Workload.RemovedAt.AsTime().Equal(meteredAt) {
					t.Fatal("confirmation changed metering")
				}
			}
		})
	}
}

func TestWorkloadRemovalConfirmationValidation(t *testing.T) {
	for _, state := range []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
	} {
		t.Run(state.String(), func(t *testing.T) {
			s, pool, record := removalTestServer(t)
			_, err := s.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
				Id: record.Meta.ID.String(), Status: &state, RemovalConfirmedAt: timestamppb.Now(),
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("nonterminal request: %v", err)
			}
			record.Status, _ = workloadStatusToString(state)
			pool.ExpectQuery(regexp.QuoteMeta(fmt.Sprintf("SELECT %s FROM workloads WHERE id = $1", workloadColumns))).
				WithArgs(record.Meta.ID).WillReturnRows(workloadRows(t, record))
			_, err = s.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
				Id: record.Meta.ID.String(), RemovalConfirmedAt: timestamppb.Now(),
			})
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("nonterminal record: %v", err)
			}
		})
	}
	s, _, record := removalTestServer(t)
	_, err := s.UpdateWorkload(context.Background(), &runnersv1.UpdateWorkloadRequest{
		Id: record.Meta.ID.String(), RemovalConfirmedAt: &timestamppb.Timestamp{Nanos: -1},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid timestamp: %v", err)
	}
}
