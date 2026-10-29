import argparse
import time

from config import DATA_DIR, DATABASE_PATH, DEFAULT_BATCH_SIZE, TOTAL_BATCHES_DEFAULT, RANDOM_SEED
from engine.generator import StreamDataGenerator
from engine.pipeline import StreamPipelineEngine


def main():
    parser = argparse.ArgumentParser(description="Micro-Batch Real-Time Pipeline Engine")
    parser.add_argument("--generate", action="store_true", help="Synthesize incoming batch files")
    parser.add_argument("--process", action="store_true", help="Process outstanding micro-batches")
    parser.add_argument("--reset-data", action="store_true", help="Delete existing batch files before generation")
    parser.add_argument("--batches", type=int, default=TOTAL_BATCHES_DEFAULT, help="Total batches to simulate")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Record volume per batch")
    args = parser.parse_args()

    generator = StreamDataGenerator(output_dir=DATA_DIR, seed=RANDOM_SEED)
    engine = StreamPipelineEngine(db_path=DATABASE_PATH)

    if args.generate:
        if args.reset_data and DATA_DIR.exists():
            for old_file in DATA_DIR.glob("batch_*.json"):
                old_file.unlink()
        print(f"Generating {args.batches} micro-batches with {args.batch_size} events each...")
        for b_id in range(1, args.batches + 1):
            fpath = generator.generate_batch(batch_id=b_id, record_count=args.batch_size)
            print(f"Generated: {fpath.name}")

    if args.process:
        batch_files = sorted(list(DATA_DIR.glob("batch_*.json")))
        print(f"Found {len(batch_files)} batches ready for ingestion.")

        t0 = time.perf_counter()
        processed = 0
        inserted = 0
        quarantined = 0
        duplicates = 0

        for b_file in batch_files:
            metrics = engine.process_micro_batch(b_file)
            if metrics.get("status") == "SKIPPED":
                continue
            processed += 1
            inserted += metrics["clean_inserted"]
            quarantined += metrics["quarantine_count"]
            duplicates += metrics["duplicates_filtered"]

        elapsed_seconds = max(time.perf_counter() - t0, 1e-9)
        throughput = inserted / elapsed_seconds

        print("\n--- Run Summary ---")
        print(f"Processed batches       : {processed}")
        print(f"Inserted rows          : {inserted}")
        print(f"Quarantined rows       : {quarantined}")
        print(f"Duplicate rows filtered: {duplicates}")
        print(f"Elapsed seconds        : {elapsed_seconds:.4f}")
        print(f"Insert throughput r/s  : {throughput:.2f}")

        print("\n--- Pipeline Aggregation Verification ---")
        aggregates = engine.conn.execute("""
            SELECT window_start, currency, total_gross_amount, order_count 
            FROM agg_tumbling_sales_1m 
            ORDER BY window_start ASC, currency ASC 
            LIMIT 5;
        """).fetchdf()
        print(aggregates.to_string(index=False))

        print("\n--- Quarantine / Dead-Letter Verification ---")
        quarantine = engine.conn.execute("""
            SELECT error_reason, count(*) as count 
            FROM audit_quarantine 
            GROUP BY error_reason;
        """).fetchdf()
        print(quarantine.to_string(index=False))

        print("\n--- Reconciliation ---")
        recon = engine.conn.execute("""
            WITH paid_non_late AS (
                SELECT COALESCE(SUM(gross_amount), 0) AS fact_paid_non_late_total
                FROM fact_order_events
                WHERE order_status = 'PAID' AND is_late = FALSE
            ),
            agg_total AS (
                SELECT COALESCE(SUM(total_gross_amount), 0) AS agg_total
                FROM agg_tumbling_sales_1m
            )
            SELECT 
                fact_paid_non_late_total,
                agg_total,
                (fact_paid_non_late_total - agg_total) AS diff
            FROM paid_non_late, agg_total;
        """).fetchdf()
        print(recon.to_string(index=False))


if __name__ == "__main__":
    main()