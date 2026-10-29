import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import DATABASE_PATH


def build_dashboard(conn: duckdb.DuckDBPyConnection) -> dict:
    checkpoint = conn.execute(
        """
        SELECT pipeline_name, last_processed_batch, high_event_watermark, updated_at
        FROM sys_watermarks
        ORDER BY updated_at DESC
        LIMIT 1
        """
    ).fetchone()

    run_totals = conn.execute(
        """
        SELECT
            COUNT(*) AS total_runs,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_runs,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
            COALESCE(SUM(input_records), 0) AS input_records,
            COALESCE(SUM(valid_records), 0) AS inserted_records,
            COALESCE(SUM(quarantined_records), 0) AS quarantined_records,
            COALESCE(SUM(duplicate_records), 0) AS duplicate_records,
            COALESCE(SUM(late_records), 0) AS late_records,
            COALESCE(AVG(duration_ms), 0) AS avg_duration_ms
        FROM pipeline_audit_ledger
        """
    ).fetchone()

    recent_runs = conn.execute(
        """
        SELECT run_id, batch_id, status, source_file, input_records, valid_records,
               quarantined_records, duplicate_records, late_records, duration_ms, started_at, completed_at
        FROM pipeline_audit_ledger
        ORDER BY started_at DESC
        LIMIT 10
        """
    ).fetchall()

    top_quarantine = conn.execute(
        """
        SELECT error_reason, COUNT(*) AS rejected_count
        FROM audit_quarantine
        GROUP BY error_reason
        ORDER BY rejected_count DESC, error_reason ASC
        LIMIT 10
        """
    ).fetchall()

    aggregate_snapshot = conn.execute(
        """
        SELECT
            COALESCE(SUM(total_gross_amount), 0) AS aggregate_total,
            COALESCE(SUM(order_count), 0) AS aggregate_order_count,
            COUNT(*) AS aggregate_windows
        FROM agg_tumbling_sales_1m
        """
    ).fetchone()

    now_utc = datetime.now(timezone.utc).isoformat()

    return {
        "generated_at_utc": now_utc,
        "database_path": str(DATABASE_PATH),
        "checkpoint": {
            "pipeline_name": checkpoint[0] if checkpoint else None,
            "last_processed_batch": checkpoint[1] if checkpoint else None,
            "high_event_watermark": checkpoint[2].isoformat() if checkpoint and checkpoint[2] else None,
            "updated_at": checkpoint[3].isoformat() if checkpoint and checkpoint[3] else None,
        },
        "run_totals": {
            "total_runs": run_totals[0],
            "success_runs": run_totals[1] or 0,
            "failed_runs": run_totals[2] or 0,
            "input_records": run_totals[3] or 0,
            "inserted_records": run_totals[4] or 0,
            "quarantined_records": run_totals[5] or 0,
            "duplicate_records": run_totals[6] or 0,
            "late_records": run_totals[7] or 0,
            "avg_duration_ms": float(run_totals[8] or 0),
        },
        "aggregate_snapshot": {
            "aggregate_total": float(aggregate_snapshot[0] or 0),
            "aggregate_order_count": int(aggregate_snapshot[1] or 0),
            "aggregate_windows": int(aggregate_snapshot[2] or 0),
        },
        "recent_runs": [
            {
                "run_id": row[0],
                "batch_id": row[1],
                "status": row[2],
                "source_file": row[3],
                "input_records": row[4],
                "inserted_records": row[5],
                "quarantined_records": row[6],
                "duplicate_records": row[7],
                "late_records": row[8],
                "duration_ms": row[9],
                "started_at": row[10].isoformat() if row[10] else None,
                "completed_at": row[11].isoformat() if row[11] else None,
            }
            for row in recent_runs
        ],
        "top_quarantine_reasons": [
            {"error_reason": row[0], "rejected_count": row[1]}
            for row in top_quarantine
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate lightweight operator dashboard metrics from DuckDB.")
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "reports" / "operator_dashboard.json"),
        help="Output JSON path for dashboard report.",
    )
    args = parser.parse_args()

    conn = duckdb.connect(str(DATABASE_PATH))
    dashboard = build_dashboard(conn)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dashboard, indent=2), encoding="utf-8")

    print(json.dumps(dashboard, indent=2))
    print(f"Saved dashboard report: {output_path}")


if __name__ == "__main__":
    main()
