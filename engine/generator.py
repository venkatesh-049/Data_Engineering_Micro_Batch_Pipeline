import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List


class StreamDataGenerator:
    """Simulates an external ingestion bus delivering micro-batched JSON events."""

    def __init__(self, output_dir: Path, seed: int = 42):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.rng = random.Random(seed)
        self.base_time = datetime(2026, 3, 30, 12, 0, 0, tzinfo=timezone.utc)

    def generate_batch(
        self,
        batch_id: int,
        record_count: int = 1000,
        inject_duplicates: bool = True,
        inject_corrupt: bool = True,
        inject_late_records: bool = True,
        inject_invalid_reference: bool = True,
        inject_schema_drift: bool = True,
    ) -> Path:
        batch_file = self.output_dir / f"batch_{batch_id:04d}.json"
        records: List[Dict[str, Any]] = []

        pool_of_ids = [str(uuid.UUID(int=self.rng.getrandbits(128))) for _ in range(50)]

        for i in range(record_count):
            # Normal distribution: increment timeline forward
            offset_seconds = (batch_id * 300) + (i % 300)
            event_time = self.base_time + timedelta(seconds=offset_seconds)

            # Domain edge cases
            if inject_late_records and self.rng.random() < 0.05:
                # Arrives 2 hours behind the watermark
                event_time = event_time - timedelta(seconds=7200)

            rec = {
                "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
                "event_time": event_time.isoformat(),
                "customer_id": f"cust_{self.rng.randint(100, 999)}",
                "order_id": f"ord_{self.rng.randint(1000, 9999)}",
                "currency": self.rng.choice(["USD", "EUR", "GBP", "JPY"]),
                "gross_amount": round(self.rng.uniform(5.0, 500.0), 2),
                "order_status": self.rng.choice(["CREATED", "PAID", "CANCELLED"]),
                "client_version": "v1.2.0",
            }

            # 1. Intentional duplicate injection
            if inject_duplicates and self.rng.random() < 0.03:
                rec["event_id"] = self.rng.choice(pool_of_ids)

            # 2. Intentional validation failures: Negative amounts
            if inject_corrupt and self.rng.random() < 0.02:
                rec["gross_amount"] = -1.0 * abs(rec["gross_amount"])

            # 3. Intentional validation failures: Corrupted schema/missing fields
            if inject_corrupt and self.rng.random() < 0.01:
                del rec["order_id"]

            # 4. Intentional invalid reference key
            if inject_invalid_reference and self.rng.random() < 0.01:
                rec["customer_id"] = "unknown_customer"

            # 5. Intentional schema drift / unknown field
            if inject_schema_drift and self.rng.random() < 0.01:
                rec["promotion_code"] = "NEW_FIELD"

            records.append(rec)

        # 6. Intentional malformed JSON at physical line-level
        with open(batch_file, "w", encoding="utf-8") as f:
            for idx, r in enumerate(records):
                if inject_corrupt and idx == len(records) - 1 and self.rng.random() < 0.5:
                    f.write('{"event_id": "malformed_json", "broken":\n')
                else:
                    f.write(json.dumps(r) + "\n")

        return batch_file