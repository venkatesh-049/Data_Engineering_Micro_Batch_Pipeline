import json
import argparse
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import DATABASE_PATH


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate pipeline outputs against expected labels.")
    parser.add_argument(
        "--labels",
        default=str(PROJECT_ROOT / "data" / "sample_labels" / "expected_metrics.json"),
        help="Path to expected labels JSON.",
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "reports" / "validation_report.json"),
        help="Path where validation report JSON will be stored.",
    )
    args = parser.parse_args()

    labels_path = Path(args.labels)
    if not labels_path.exists():
        raise FileNotFoundError(f"Expected labels file missing: {labels_path}")

    expected = json.loads(labels_path.read_text(encoding="utf-8"))
    conn = duckdb.connect(str(DATABASE_PATH))

    actual = {
        "actual_input_rows_total": conn.execute("SELECT COALESCE(SUM(input_records), 0) FROM pipeline_audit_ledger WHERE status = 'SUCCESS';").fetchone()[0],
        "actual_clean_inserted": conn.execute("SELECT COUNT(*) FROM fact_order_events;").fetchone()[0],
        "actual_quarantined": conn.execute("SELECT COUNT(*) FROM audit_quarantine;").fetchone()[0],
        "actual_duplicates_filtered": conn.execute("SELECT COALESCE(SUM(duplicate_records), 0) FROM pipeline_audit_ledger WHERE status = 'SUCCESS';").fetchone()[0],
        "actual_paid_non_late_aggregate_total": conn.execute("SELECT COALESCE(SUM(total_gross_amount), 0) FROM agg_tumbling_sales_1m;").fetchone()[0],
    }

    report = {
        "expected": expected,
        "actual": actual,
        "checks": {
            "clean_inserted_match": actual["actual_clean_inserted"] == expected["expected_clean_inserted"],
            "quarantined_match": actual["actual_quarantined"] == expected["expected_quarantined"],
            "duplicates_match": actual["actual_duplicates_filtered"] == expected["expected_duplicates_filtered"],
            "aggregate_match": abs(actual["actual_paid_non_late_aggregate_total"] - expected["expected_paid_non_late_aggregate_total"]) < 1e-9,
        },
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))
    print(f"Saved validation report: {output_path}")


if __name__ == "__main__":
    main()
