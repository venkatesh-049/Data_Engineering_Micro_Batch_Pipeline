import json
import logging
import uuid
from datetime import datetime, timezone
from datetime import timedelta
from pathlib import Path
from typing import Dict, Any, Tuple

import duckdb
from pydantic import ValidationError

from config import WATERMARK_DELAY_SECONDS
from engine.models import RawOrderEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MicroBatchEngine")


class StreamPipelineEngine:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self.pipeline_name = "order_micro_batch_stream"
        self._init_schemas()

    def _init_schemas(self) -> None:
        """Bootstraps idempotency, state checkpoint, quarantine, and aggregate schemas."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_customers (
                customer_id VARCHAR PRIMARY KEY,
                customer_tier VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sys_watermarks (
                pipeline_name VARCHAR PRIMARY KEY,
                last_processed_batch INTEGER NOT NULL,
                high_event_watermark TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pipeline_audit_ledger (
                run_id VARCHAR PRIMARY KEY,
                batch_id INTEGER NOT NULL,
                status VARCHAR NOT NULL,
                source_file VARCHAR NOT NULL,
                input_records INTEGER NOT NULL,
                valid_records INTEGER NOT NULL,
                quarantined_records INTEGER NOT NULL,
                duplicate_records INTEGER NOT NULL,
                late_records INTEGER NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP NOT NULL,
                duration_ms BIGINT NOT NULL,
                error_message VARCHAR
            );

            CREATE TABLE IF NOT EXISTS audit_quarantine (
                quarantine_id VARCHAR PRIMARY KEY,
                batch_id INTEGER NOT NULL,
                source_file VARCHAR NOT NULL,
                raw_payload VARCHAR NOT NULL,
                error_reason VARCHAR NOT NULL,
                detected_at TIMESTAMP NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fact_order_events (
                event_id VARCHAR PRIMARY KEY,
                event_time TIMESTAMP NOT NULL,
                customer_id VARCHAR NOT NULL,
                order_id VARCHAR NOT NULL,
                currency VARCHAR NOT NULL,
                gross_amount DOUBLE NOT NULL,
                order_status VARCHAR NOT NULL,
                client_version VARCHAR,
                is_late BOOLEAN NOT NULL,
                batch_id INTEGER NOT NULL,
                source_file VARCHAR NOT NULL,
                run_id VARCHAR NOT NULL,
                ingested_at TIMESTAMP NOT NULL
            );

            CREATE TABLE IF NOT EXISTS agg_tumbling_sales_1m (
                window_start TIMESTAMP NOT NULL,
                currency VARCHAR NOT NULL,
                total_gross_amount DOUBLE NOT NULL,
                order_count BIGINT NOT NULL,
                last_updated_at TIMESTAMP NOT NULL,
                PRIMARY KEY (window_start, currency)
            );
        """)
        self._seed_customer_dimension()

    def _seed_customer_dimension(self) -> None:
        """Populate a deterministic customer reference set for integrity checks."""
        existing = self.conn.execute("SELECT COUNT(*) FROM dim_customers;").fetchone()[0]
        if existing > 0:
            return

        now_utc = datetime.now(timezone.utc)
        tiers = ["bronze", "silver", "gold"]
        rows = []
        for idx in range(100, 1000):
            rows.append((f"cust_{idx}", tiers[idx % len(tiers)], now_utc))
        self.conn.executemany(
            """
            INSERT INTO dim_customers (customer_id, customer_tier, created_at)
            VALUES (?, ?, ?)
            """,
            rows,
        )

    def _is_known_customer(self, customer_id: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM dim_customers WHERE customer_id = ? LIMIT 1",
            [customer_id],
        ).fetchone() is not None

    def get_last_checkpoint(self) -> Tuple[int, datetime]:
        res = self.conn.execute("""
            SELECT last_processed_batch, high_event_watermark 
            FROM sys_watermarks 
            WHERE pipeline_name = ?
        """, [self.pipeline_name]).fetchone()
        
        if res is None:
            # Cold-start state
            return 0, datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        return res[0], res[1].replace(tzinfo=timezone.utc)

    def process_micro_batch(self, batch_file: Path, force_fail_after_validation: bool = False) -> Dict[str, Any]:
        """Atomically processes a single micro-batch under explicit checkpoint boundaries."""
        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        source_file = batch_file.name
        batch_id = int(batch_file.stem.split("_")[-1])

        last_batch, current_watermark = self.get_last_checkpoint()

        # Idempotency guard: skip already-processed micro-batches
        if batch_id <= last_batch:
            logger.warning(f"Batch {batch_id} already processed (checkpoint: {last_batch}). Skipping.")
            return {"status": "SKIPPED", "batch_id": batch_id, "reason": "ALREADY_COMMITTED"}

        logger.info(f"==> Ingesting Batch {batch_id} (run_id: {run_id})")

        total_input = 0
        valid_records = []
        quarantine_records = []
        max_batch_time = current_watermark

        # 1. Parse & Quarantine Pass (Record-level isolation)
        with open(batch_file, "r", encoding="utf-8") as f:
            for line_idx, line in enumerate(f):
                total_input += 1
                stripped = line.strip()
                if not stripped:
                    quarantine_records.append((str(uuid.uuid4()), batch_id, source_file, "", "EMPTY_LINE", datetime.now(timezone.utc)))
                    continue
                try:
                    raw_json = json.loads(stripped)
                except Exception as err:
                    quarantine_records.append((str(uuid.uuid4()), batch_id, source_file, stripped, f"ERR_MALFORMED_JSON: {str(err)}", datetime.now(timezone.utc)))
                    continue

                try:
                    event = RawOrderEvent(**raw_json)
                    if not self._is_known_customer(event.customer_id):
                        quarantine_records.append((
                            str(uuid.uuid4()),
                            batch_id,
                            source_file,
                            stripped,
                            f"ERR_REFERENCE: Unknown customer_id {event.customer_id}",
                            datetime.now(timezone.utc),
                        ))
                        continue
                    valid_records.append(event)
                    if event.event_time > max_batch_time:
                        max_batch_time = event.event_time
                except ValidationError as val_err:
                    quarantine_records.append((str(uuid.uuid4()), batch_id, source_file, stripped, f"ERR_VALIDATION: {str(val_err.errors()[0]['msg'])}", datetime.now(timezone.utc)))

        # 2. Transaction Execution
        self.conn.execute("BEGIN TRANSACTION;")
        try:
            if force_fail_after_validation:
                raise RuntimeError("SIMULATED_PARTIAL_FAILURE_AFTER_VALIDATION")

            # Stage quarantine entries
            if quarantine_records:
                self.conn.executemany("""
                    INSERT INTO audit_quarantine (quarantine_id, batch_id, source_file, raw_payload, error_reason, detected_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, quarantine_records)

            # Update engine watermark threshold
            new_watermark = max(current_watermark, max_batch_time - timedelta(seconds=WATERMARK_DELAY_SECONDS))

            # Ingest valid events using deterministic deduplication
            duplicates_detected = 0
            inserted_count = 0
            late_records_count = 0

            if valid_records:
                # Load valid records into a scratch temporary table
                self.conn.execute("CREATE TEMPORARY TABLE staging_events AS SELECT * FROM fact_order_events LIMIT 0;")
                
                rows_to_insert = []
                now_utc = datetime.now(timezone.utc)
                for item in valid_records:
                    is_late = item.event_time < new_watermark
                    rows_to_insert.append((
                        item.event_id,
                        item.event_time,
                        item.customer_id,
                        item.order_id,
                        item.currency,
                        item.gross_amount,
                        item.order_status.value,
                        item.client_version,
                        is_late,
                        batch_id,
                        source_file,
                        run_id,
                        now_utc
                    ))
                    if is_late:
                        late_records_count += 1

                self.conn.executemany("""
                    INSERT INTO staging_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows_to_insert)

                # Intra-batch & Inter-batch deduplication query
                # Uses ROW_NUMBER to preserve only the first appearance of duplicate event_ids
                dedup_res = self.conn.execute("""
                    WITH ranked_batch AS (
                        SELECT *,
                               ROW_NUMBER() OVER(PARTITION BY event_id ORDER BY event_time ASC) as rank_id
                        FROM staging_events
                    )
                    SELECT * FROM ranked_batch WHERE rank_id = 1
                    AND event_id NOT IN (SELECT event_id FROM fact_order_events);
                """).fetchall()

                duplicates_detected = len(valid_records) - len(dedup_res)
                inserted_count = len(dedup_res)

                if dedup_res:
                    self.conn.execute("""
                        INSERT INTO fact_order_events 
                        SELECT event_id, event_time, customer_id, order_id, currency, 
                               gross_amount, order_status, client_version, is_late, batch_id, source_file, run_id, ingested_at 
                        FROM (
                            SELECT *, ROW_NUMBER() OVER(PARTITION BY event_id ORDER BY event_time ASC) as rank_id
                            FROM staging_events
                        )
                        WHERE rank_id = 1
                        AND event_id NOT IN (SELECT event_id FROM fact_order_events);
                    """)

                self.conn.execute("DROP TABLE staging_events;")

            # 3. Publish Incremental 1-Minute Aggregates
            self.conn.execute("""
                INSERT INTO agg_tumbling_sales_1m
                SELECT 
                    time_bucket(INTERVAL '1 minute', event_time) AS window_start,
                    currency,
                    SUM(gross_amount) AS total_gross_amount,
                    COUNT(event_id) AS order_count,
                    CURRENT_TIMESTAMP AS last_updated_at
                FROM fact_order_events
                                WHERE batch_id = ?
                                    AND is_late = FALSE
                                    AND order_status = 'PAID'
                GROUP BY 1, 2
                ON CONFLICT (window_start, currency) DO UPDATE SET
                    total_gross_amount = agg_tumbling_sales_1m.total_gross_amount + EXCLUDED.total_gross_amount,
                    order_count = agg_tumbling_sales_1m.order_count + EXCLUDED.order_count,
                    last_updated_at = EXCLUDED.last_updated_at;
            """, [batch_id])

            # 4. Advance State Checkpoint and Watermark
            self.conn.execute("""
                INSERT INTO sys_watermarks (pipeline_name, last_processed_batch, high_event_watermark, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(pipeline_name) DO UPDATE SET
                    last_processed_batch = EXCLUDED.last_processed_batch,
                    high_event_watermark = EXCLUDED.high_event_watermark,
                    updated_at = EXCLUDED.updated_at;
            """, [self.pipeline_name, batch_id, new_watermark, datetime.now(timezone.utc)])

            # 5. Commit Operational Audit Ledger
            completed_at = datetime.now(timezone.utc)
            duration_ms = int((completed_at - started_at).total_seconds() * 1000)
            self.conn.execute("""
                INSERT INTO pipeline_audit_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, [
                run_id,
                batch_id,
                "SUCCESS",
                source_file,
                total_input,
                inserted_count,
                len(quarantine_records),
                duplicates_detected,
                late_records_count,
                started_at,
                completed_at,
                duration_ms,
                None,
            ])

            self.conn.execute("COMMIT;")

            metrics = {
                "run_id": run_id,
                "batch_id": batch_id,
                "source_file": source_file,
                "input_count": total_input,
                "clean_inserted": inserted_count,
                "quarantine_count": len(quarantine_records),
                "duplicates_filtered": duplicates_detected,
                "late_records": late_records_count,
                "duration_ms": duration_ms,
                "watermark": new_watermark.isoformat()
            }
            logger.info(f"SUCCESS Batch {batch_id}: {metrics}")
            return metrics

        except Exception as ex:
            self.conn.execute("ROLLBACK;")
            logger.error(f"FAILURE Batch {batch_id}. Rolled back transaction. Error: {str(ex)}")
            # Record failed state
            self.conn.execute("""
                INSERT INTO pipeline_audit_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, [
                run_id,
                batch_id,
                "FAILED",
                source_file,
                total_input,
                0,
                len(quarantine_records),
                0,
                0,
                started_at,
                datetime.now(timezone.utc),
                int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000),
                str(ex),
            ])
            raise ex