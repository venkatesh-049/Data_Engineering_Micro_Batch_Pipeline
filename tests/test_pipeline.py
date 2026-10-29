import json
import pytest
from datetime import datetime, timezone, timedelta
from engine.pipeline import StreamPipelineEngine


@pytest.fixture
def test_env(tmp_path):
    db_file = tmp_path / "test_warehouse.duckdb"
    engine = StreamPipelineEngine(db_path=db_file)
    data_dir = tmp_path / "incoming"
    data_dir.mkdir()
    return engine, data_dir


def test_happy_path(test_env):
    engine, data_dir = test_env
    batch_file = data_dir / "batch_0001.json"

    records = [
        {
            "event_id": f"evt_{i}",
            "event_time": (datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=i)).isoformat(),
            "customer_id": f"cust_{100 + i}",
            "order_id": f"ord_{i}",
            "currency": "USD",
            "gross_amount": 100.0 + i,
            "order_status": "PAID"
        }
        for i in range(10)
    ]
    batch_file.write_text("\n".join(json.dumps(r) for r in records))

    res = engine.process_micro_batch(batch_file)
    assert res["input_count"] == 10
    assert res["clean_inserted"] == 10
    assert res["quarantine_count"] == 0
    assert res["duplicates_filtered"] == 0

    # Ensure facts exist
    stored_count = engine.conn.execute("SELECT COUNT(*) FROM fact_order_events;").fetchone()[0]
    assert stored_count == 10


def test_empty_batch_file(test_env):
    engine, data_dir = test_env
    batch = data_dir / "batch_0001.json"
    batch.write_text("")

    res = engine.process_micro_batch(batch)
    assert res["input_count"] == 0
    assert res["clean_inserted"] == 0
    assert res["quarantine_count"] == 0


def test_deduplication_intra_and_inter_batch(test_env):
    engine, data_dir = test_env

    # 1. Intra-batch duplicate
    batch1 = data_dir / "batch_0001.json"
    rec1 = {
        "event_id": "duplicate_id",
        "event_time": datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc).isoformat(),
        "customer_id": "cust_101",
        "order_id": "ord_1",
        "currency": "USD",
        "gross_amount": 50.0,
        "order_status": "CREATED"
    }
    rec2 = dict(rec1)  # Identical event_id
    batch1.write_text(f"{json.dumps(rec1)}\n{json.dumps(rec2)}")

    res1 = engine.process_micro_batch(batch1)
    assert res1["input_count"] == 2
    assert res1["clean_inserted"] == 1
    assert res1["duplicates_filtered"] == 1

    # 2. Inter-batch duplicate (Event re-submitted in batch 2)
    batch2 = data_dir / "batch_0002.json"
    rec3 = dict(rec1)
    batch2.write_text(json.dumps(rec3))

    res2 = engine.process_micro_batch(batch2)
    assert res2["input_count"] == 1
    assert res2["clean_inserted"] == 0
    assert res2["duplicates_filtered"] == 1


def test_malformed_json_and_validation_quarantine(test_env):
    engine, data_dir = test_env
    batch = data_dir / "batch_0001.json"

    lines = [
        '{"event_id": "bad_json", "broken": ',  # Malformed
        '{"event_id": "e_2", "event_time": "2026-03-30T10:00:00Z", "customer_id": "cust_102", "order_id": "o2", "currency": "AAA", "gross_amount": 10.0, "order_status": "PAID"}',  # Invalid currency
        '{"event_id": "e_3", "event_time": "2026-03-30T10:00:00Z", "customer_id": "cust_103", "order_id": "o3", "currency": "USD", "gross_amount": -10.0, "order_status": "PAID"}'  # Negative gross_amount
    ]
    batch.write_text("\n".join(lines))

    res = engine.process_micro_batch(batch)
    assert res["input_count"] == 3
    assert res["clean_inserted"] == 0
    assert res["quarantine_count"] == 3

    reasons = engine.conn.execute("SELECT error_reason FROM audit_quarantine").fetchall()
    reasons_str = " ".join([r[0] for r in reasons])
    assert "ERR_MALFORMED_JSON" in reasons_str
    assert "Unsupported currency" in reasons_str
    assert "gross_amount cannot be negative" in reasons_str


def test_null_and_missing_required_fields_are_quarantined(test_env):
    engine, data_dir = test_env
    batch = data_dir / "batch_0001.json"

    lines = [
        '{"event_id": "e_null", "event_time": "2026-03-30T10:00:00Z", "customer_id": null, "order_id": "o2", "currency": "USD", "gross_amount": 10.0, "order_status": "PAID"}',
        '{"event_id": "e_missing", "event_time": "2026-03-30T10:00:00Z", "customer_id": "cust_104", "currency": "USD", "gross_amount": 10.0, "order_status": "PAID"}'
    ]
    batch.write_text("\n".join(lines))

    res = engine.process_micro_batch(batch)
    assert res["clean_inserted"] == 0
    assert res["quarantine_count"] == 2


def test_idempotent_replay(test_env):
    engine, data_dir = test_env
    batch = data_dir / "batch_0001.json"
    rec = {
        "event_id": "idemp_1",
        "event_time": datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc).isoformat(),
        "customer_id": "cust_105",
        "order_id": "ord_1",
        "currency": "EUR",
        "gross_amount": 75.0,
        "order_status": "PAID"
    }
    batch.write_text(json.dumps(rec))

    # First run
    res1 = engine.process_micro_batch(batch)
    assert res1["clean_inserted"] == 1

    # Exact same file replay
    res2 = engine.process_micro_batch(batch)
    assert res2["status"] == "SKIPPED"
    assert res2["reason"] == "ALREADY_COMMITTED"


def test_watermark_late_arriving_events(test_env):
    """Verifies that late records are preserved but flagged with is_late = TRUE."""
    engine, data_dir = test_env

    # 1. Establish watermark by providing an event at T
    batch1 = data_dir / "batch_0001.json"
    t_now = datetime(2026, 3, 30, 12, 0, 0, tzinfo=timezone.utc)
    rec1 = {
        "event_id": "e_base",
        "event_time": t_now.isoformat(),
        "customer_id": "cust_106",
        "order_id": "ord_1",
        "currency": "USD",
        "gross_amount": 100.0,
        "order_status": "PAID"
    }
    batch1.write_text(json.dumps(rec1))
    engine.process_micro_batch(batch1)

    # Watermark is now 12:00:00 - 600 seconds = 11:50:00

    # 2. Ingest an event from 11:40:00 (arrives 10 minutes past the watermark)
    batch2 = data_dir / "batch_0002.json"
    rec_late = {
        "event_id": "e_late",
        "event_time": (t_now - timedelta(minutes=20)).isoformat(),
        "customer_id": "cust_107",
        "order_id": "ord_2",
        "currency": "USD",
        "gross_amount": 50.0,
        "order_status": "PAID"
    }
    batch2.write_text(json.dumps(rec_late))
    engine.process_micro_batch(batch2)

    # 3. Assert late event is marked accordingly
    is_late = engine.conn.execute("""
        SELECT is_late FROM fact_order_events WHERE event_id = 'e_late';
    """).fetchone()[0]
    assert is_late is True

    # 4. Assert aggregate table DID NOT increment from the late record (protects finalized windows)
    agg_sum = engine.conn.execute("SELECT SUM(total_gross_amount) FROM agg_tumbling_sales_1m;").fetchone()[0]
    assert agg_sum == 100.0  # Only the original paid + non-late record is present


def test_unexpected_schema_change_is_quarantined(test_env):
    engine, data_dir = test_env
    batch = data_dir / "batch_0001.json"
    rec = {
        "event_id": "evt_schema_drift",
        "event_time": datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc).isoformat(),
        "customer_id": "cust_108",
        "order_id": "ord_108",
        "currency": "USD",
        "gross_amount": 25.0,
        "order_status": "PAID",
        "new_upstream_field": "unexpected",
    }
    batch.write_text(json.dumps(rec))

    res = engine.process_micro_batch(batch)
    assert res["clean_inserted"] == 0
    assert res["quarantine_count"] == 1

    reason = engine.conn.execute("SELECT error_reason FROM audit_quarantine").fetchone()[0]
    assert "Extra inputs are not permitted" in reason


def test_invalid_reference_key_is_quarantined(test_env):
    engine, data_dir = test_env
    batch = data_dir / "batch_0001.json"
    rec = {
        "event_id": "evt_bad_ref",
        "event_time": datetime(2026, 3, 30, 10, 0, 0, tzinfo=timezone.utc).isoformat(),
        "customer_id": "unknown_customer",
        "order_id": "ord_109",
        "currency": "USD",
        "gross_amount": 20.0,
        "order_status": "PAID",
    }
    batch.write_text(json.dumps(rec))

    res = engine.process_micro_batch(batch)
    assert res["clean_inserted"] == 0
    assert res["quarantine_count"] == 1

    reason = engine.conn.execute("SELECT error_reason FROM audit_quarantine").fetchone()[0]
    assert "ERR_REFERENCE" in reason


def test_partial_failure_then_retry_is_safe(test_env):
    engine, data_dir = test_env
    batch = data_dir / "batch_0001.json"
    rec = {
        "event_id": "evt_retry",
        "event_time": datetime(2026, 3, 30, 10, 1, 0, tzinfo=timezone.utc).isoformat(),
        "customer_id": "cust_110",
        "order_id": "ord_110",
        "currency": "USD",
        "gross_amount": 45.0,
        "order_status": "PAID",
    }
    batch.write_text(json.dumps(rec))

    with pytest.raises(RuntimeError):
        engine.process_micro_batch(batch, force_fail_after_validation=True)

    checkpoint = engine.get_last_checkpoint()[0]
    assert checkpoint == 0
    stored_count = engine.conn.execute("SELECT COUNT(*) FROM fact_order_events;").fetchone()[0]
    assert stored_count == 0

    res = engine.process_micro_batch(batch)
    assert res["clean_inserted"] == 1
    assert engine.get_last_checkpoint()[0] == 1


def test_cancelled_events_do_not_contribute_to_sales_aggregate(test_env):
    engine, data_dir = test_env
    batch = data_dir / "batch_0001.json"
    paid = {
        "event_id": "evt_paid",
        "event_time": datetime(2026, 3, 30, 11, 0, 0, tzinfo=timezone.utc).isoformat(),
        "customer_id": "cust_111",
        "order_id": "ord_111",
        "currency": "USD",
        "gross_amount": 120.0,
        "order_status": "PAID",
    }
    cancelled = {
        "event_id": "evt_cancelled",
        "event_time": datetime(2026, 3, 30, 11, 0, 5, tzinfo=timezone.utc).isoformat(),
        "customer_id": "cust_112",
        "order_id": "ord_112",
        "currency": "USD",
        "gross_amount": 999.0,
        "order_status": "CANCELLED",
    }
    batch.write_text(f"{json.dumps(paid)}\n{json.dumps(cancelled)}")

    engine.process_micro_batch(batch)
    total = engine.conn.execute("SELECT SUM(total_gross_amount) FROM agg_tumbling_sales_1m;").fetchone()[0]
    assert total == 120.0


def test_out_of_order_but_on_time_events_are_aggregated(test_env):
    engine, data_dir = test_env
    batch = data_dir / "batch_0001.json"
    t = datetime(2026, 3, 30, 13, 0, 0, tzinfo=timezone.utc)
    rec_newer = {
        "event_id": "evt_newer",
        "event_time": (t + timedelta(seconds=10)).isoformat(),
        "customer_id": "cust_113",
        "order_id": "ord_113",
        "currency": "EUR",
        "gross_amount": 30.0,
        "order_status": "PAID",
    }
    rec_older = {
        "event_id": "evt_older",
        "event_time": t.isoformat(),
        "customer_id": "cust_114",
        "order_id": "ord_114",
        "currency": "EUR",
        "gross_amount": 20.0,
        "order_status": "PAID",
    }
    batch.write_text(f"{json.dumps(rec_newer)}\n{json.dumps(rec_older)}")

    res = engine.process_micro_batch(batch)
    assert res["late_records"] == 0
    total = engine.conn.execute("SELECT SUM(total_gross_amount) FROM agg_tumbling_sales_1m WHERE currency = 'EUR';").fetchone()[0]
    assert total == 50.0