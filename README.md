# Data Engineering Micro-Batch Pipeline

## 1. Objective and Scope

This project delivers a local, production-style Data Engineering solution for continuously arriving events processed in micro-batches.

It is designed to prioritize:

- Correctness and deterministic behavior
- Data quality and traceability
- Idempotency and safe reruns
- Operational visibility (metrics and failure reporting)
- Maintainable code structure suitable for 0-3 years experience level

The pipeline simulates incoming order events, applies strict validation and business rules, deduplicates records, handles late arrivals with watermark logic, and publishes near-real-time aggregates.

## 2. Primary Actors and Systems

- Data Engineer: owns schema, validation rules, and quality controls.
- Source Producer: writes JSONL event files as micro-batches.
- Processing Layer: Python pipeline with DuckDB transaction engine.
- Storage Layer: DuckDB tables for facts, aggregates, checkpoints, and audits.
- Analytical Consumer: reads near-real-time aggregate table.
- Pipeline Operator: monitors run status, quality issues, and reconciliation.

## 3. Repository Structure

- config.py: paths and runtime parameters
- run_pipeline.py: generation + processing CLI
- engine/models.py: strict source schema and field validators
- engine/generator.py: reproducible synthetic data generation
- engine/pipeline.py: ingestion, quarantine, dedup, watermarking, publish
- tests/test_pipeline.py: automated happy-path + negative + edge tests
- scripts/validate_outputs.py: expected-vs-actual validation report
- scripts/operator_dashboard.py: lightweight operational dashboard report
- docs/data_dictionary.md: entities, keys, grain, constraints
- docs/architecture.md: flow and component interactions
- docs/design_tradeoffs.md: design decisions and limitations
- data/sample_input: reproducible sample micro-batches
- data/sample_labels: expected answer labels (kept separate from input)
- reports: stored output artifacts (generated at runtime)
- .vscode/tasks.json: one-click pipeline runner

## 4. Business Workflow and Rules

### 4.1 Source and Target Schemas

- Source contract is strict and rejects unknown fields (schema drift detection).
- Source business key: event_id.
- Required identifiers: event_id, customer_id, order_id (non-empty).
- Allowed currencies: USD, EUR, GBP, JPY.
- gross_amount must be non-negative.
- customer_id must exist in reference table dim_customers.

Target storage includes:

- fact_order_events: accepted event history
- agg_tumbling_sales_1m: 1-minute aggregate by currency
- audit_quarantine: rejected records with reason and payload
- pipeline_audit_ledger: run-level metrics, status, and errors
- sys_watermarks: checkpoint and watermark state

### 4.2 End-to-End Pipeline Steps

1. Parse JSONL records from batch_XXXX.json
2. Validate schema + business rules + reference integrity
3. Quarantine invalid records with detailed reasons
4. Deduplicate valid records (intra-batch and inter-batch)
5. Mark late arrivals using watermark
6. Insert valid rows into fact table
7. Upsert paid + non-late rows into aggregate table
8. Advance checkpoint and watermark atomically
9. Record run metrics in audit ledger

### 4.3 Idempotency and Recovery Rules

- Checkpoint offset is derived from batch filename number.
- If batch_id <= last_processed_batch, processing is skipped.
- Transaction rollback prevents partial writes on failure.
- Retrying failed runs is safe.
- Backfill with older batch ids is intentionally blocked for monotonic offset safety.

## 5. Entities, Keys, Relationships, and Grain

See detailed definitions in:

- docs/data_dictionary.md

Highlights:

- Grain of source and fact: one event per row
- Fact key: event_id (unique)
- Aggregate grain: (window_start, currency)
- Referential rule: fact customer_id must exist in dim_customers
- Traceability: source_file and run_id persisted in fact and audits

## 6. Naming and ID Conventions

- snake_case for table and column names
- Deterministic event IDs from source (event_id)
- ISO-8601 timestamps for event_time and runtime metadata
- Explicit run_id, batch_id, source_file lineage fields

## 7. Setup Instructions

### 7.1 Environment

- Python 3.11+
- Install dependencies:

```bash
pip install -r requirements.txt
```

### 7.2 Quick Functional Run

```bash
python run_pipeline.py --generate --reset-data --batches 5 --batch-size 200
python run_pipeline.py --process
python -m pytest -v tests/test_pipeline.py
python scripts/validate_outputs.py
python scripts/operator_dashboard.py
```

## 8. One-Click Task Runner

A one-click orchestrated task runner is provided in:

- .vscode/tasks.json

Run this VS Code task:

- Pipeline: Full one-click run

It executes in sequence:

1. Generate sample batches
2. Process batches
3. Run tests
4. Validate expected outputs
5. Generate operator dashboard

## 8.1 Windows Double-Click Launcher

A Windows batch launcher is provided at:

- run_full_workflow.bat

Usage:

1. Double-click run_full_workflow.bat in File Explorer.
2. It runs the same full workflow sequence:
	- generate sample batches
	- process batches
	- run tests
	- validate expected outputs
	- generate operator dashboard
3. On success, it automatically opens:
	- reports/validation_report.json
	- reports/operator_dashboard.json

If any step fails, the launcher stops immediately and displays the failing status.

## 9. Reproducible Data

### 9.1 Small Starter Dataset

- data/sample_input/batch_0001.json
- data/sample_input/batch_0002.json

### 9.2 Expected Answers (separate from input)

- data/sample_labels/expected_metrics.json

### 9.3 Larger Scalable Dataset

Example 100,000+ source records:

```bash
python run_pipeline.py --generate --reset-data --batches 20 --batch-size 6000
python run_pipeline.py --process
```

## 10. Required Edge Cases Covered

Automated tests include:

- Malformed JSON
- Missing required field
- Null required field
- Duplicate event key (intra-batch and inter-batch)
- Unexpected schema change (extra field)
- Empty batch input
- Partial failure and safe retry
- Late-arriving events
- Out-of-order on-time events
- Invalid reference key
- Rerun of same input batch
- Domain-specific aggregate rule: cancelled events excluded

## 11. Validation and Reconciliation

### 11.1 Runtime Reconciliation

run_pipeline.py --process prints:

- inserted, quarantined, duplicates
- elapsed and throughput
- aggregate summary
- reconciliation diff between fact-based total and aggregate total

Expected condition:

- diff = 0

### 11.2 Label-Based Validation Report

```bash
python scripts/validate_outputs.py
```

Output is stored at:

- reports/validation_report.json

## 12. Lightweight Operator Dashboard

```bash
python scripts/operator_dashboard.py
```

Output is stored at:

- reports/operator_dashboard.json

Dashboard includes:

- latest checkpoint state
- run success/failure totals
- aggregate snapshot metrics
- top quarantine reasons
- recent run history with durations and row counts

## 13. Operational Metrics Captured

Per run in pipeline_audit_ledger:

- run_id
- batch_id
- source_file
- status
- input_records
- valid_records (inserted)
- quarantined_records
- duplicate_records
- late_records
- started_at
- completed_at
- duration_ms
- error_message

## 14. Performance Measurement and Optimization

Current measured metrics:

- elapsed wall-clock time for processing
- inserted rows per second throughput

Optimization implemented:

- Incremental upsert of aggregates from current batch only (avoids full-table recomputation)

Reasonable next optimization:

- Replace per-record customer reference lookup with preloaded in-memory set for higher-volume runs

## 15. Testing Summary

Run:

```bash
python -m pytest -v tests/test_pipeline.py
```

Expected:

- All tests pass

## 16. Deliverables Checklist

Included in this repository:

- Source code modules
- Automated tests
- Sample input and separate expected labels
- README and detailed docs
- Validation report script
- Operator dashboard script
- One-click task runner

## 17. Notes and Limitations

- Batch naming convention is required: batch_XXXX.json
- Backfill for older batch IDs is intentionally not allowed to preserve strict offset monotonicity
- Solution is local-first with DuckDB for reproducibility and easy evaluation

## 18. Additional Documentation

- docs/architecture.md
- docs/data_dictionary.md
- docs/design_tradeoffs.md
