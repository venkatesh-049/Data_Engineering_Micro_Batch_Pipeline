# Data Dictionary

## Source Entity: `raw_order_event` (JSONL)

Grain: one order event per line.

Primary business key: `event_id`.

Fields:

- `event_id` (string, required, unique candidate): stable event identifier.
- `event_time` (ISO-8601 timestamp, required): event occurrence time.
- `customer_id` (string, required): must exist in `dim_customers.customer_id`.
- `order_id` (string, required): order reference ID.
- `currency` (string, required): one of `USD|EUR|GBP|JPY`.
- `gross_amount` (float, required): non-negative.
- `order_status` (string enum, required): `CREATED|PAID|CANCELLED|REFUNDED`.
- `client_version` (string, optional).

Strict schema rule: unknown fields are rejected (schema-drift detection).

## Reference Entity: `dim_customers`

Grain: one row per customer.

Primary key: `customer_id`.

Fields:

- `customer_id` (string, PK, non-null).
- `customer_tier` (string, non-null).
- `created_at` (timestamp, non-null).

## Target Fact: `fact_order_events`

Grain: one accepted event row.

Primary key: `event_id`.

Fields:

- `event_id` (PK)
- `event_time`
- `customer_id` (FK-like validated against `dim_customers` before insert)
- `order_id`
- `currency`
- `gross_amount`
- `order_status`
- `client_version`
- `is_late` (boolean)
- `batch_id` (ingestion offset)
- `source_file` (lineage)
- `run_id` (lineage)
- `ingested_at`

## Aggregate: `agg_tumbling_sales_1m`

Grain: 1-minute window x currency.

Primary key: (`window_start`, `currency`).

Fields:

- `window_start` (timestamp bucket)
- `currency`
- `total_gross_amount` (sum of paid, non-late fact rows)
- `order_count` (count of paid, non-late fact rows)
- `last_updated_at`

## Operational / Audit: `pipeline_audit_ledger`

Grain: one row per run attempt.

Primary key: `run_id`.

Fields:

- `run_id`, `batch_id`, `status`, `source_file`
- `input_records`, `valid_records`, `quarantined_records`, `duplicate_records`, `late_records`
- `started_at`, `completed_at`, `duration_ms`
- `error_message`

## Quarantine: `audit_quarantine`

Grain: one rejected source line.

Primary key: `quarantine_id`.

Fields:

- `quarantine_id`, `batch_id`, `source_file`
- `raw_payload`
- `error_reason`
- `detected_at`
