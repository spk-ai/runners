-- Metering end and physical removal are independent. Historical terminal rows
-- must be inspected by lifecycle reconciliation, never inferred as absent.
ALTER TABLE workloads ADD COLUMN removal_confirmed_at TIMESTAMPTZ;

ALTER TABLE workloads ADD CONSTRAINT workload_removal_confirmation_terminal
CHECK (removal_confirmed_at IS NULL OR status IN ('stopped', 'failed'));
