# Design Trade-Offs

## Why DuckDB

- Local-first analytics database with SQL features needed for dedup windows and upserts.
- No external service dependency for assignment evaluation.

## Why Batch-ID Checkpointing

- Simple and robust for micro-batch files.
- Assumes monotonic batch naming and append-only ingestion pattern.
- Trade-off: backfills with older batch IDs are skipped by design.

## Late Data Handling Choice

- Late events are preserved in fact history (`is_late = TRUE`) for traceability.
- Aggregates exclude late rows to protect finalized windows.
- Trade-off: aggregate represents finalized view, not full historical restatement.

## Validation Strictness

- `extra=forbid` explicitly detects upstream schema drift.
- Trade-off: stricter contracts can increase quarantine rates during upstream changes.

## Optimization Implemented

- Incremental aggregate updates from current batch only (not full refresh).

## Known Limitations

- Batch offset parser assumes `batch_XXXX.json` naming.
- Reference integrity is checked through SQL lookups per record (good enough at this scale).
- Single-process execution; no distributed execution semantics.
