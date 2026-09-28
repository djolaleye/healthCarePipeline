"""
Transform Lambda tests: validation, stripping, and the S3 write contract.
"""
 
from __future__ import annotations
 
import hashlib
import hmac
import json
import logging
import re
 
import pytest
 
from conftest import ARRIVAL, DELETE, FIRST_SEQUENCE, SHARD_ID, TEST_BUCKET, TEST_KEY, midpoint
from core.schema import (
    CLEAN_COLUMNS,
    IDENTIFIER_FIELDS,
    VITALS,
    ZONE_CLEAN,
    ZONE_QUARANTINE,
    QuarantineReason,
    to_iso,
)
from transform import app
 
CLEAN_FIELDS = {name for name, _ in CLEAN_COLUMNS}
DT_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
 
 
def only(outputs: list[dict]) -> dict:
    assert len(outputs) == 1
    return outputs[0]
 
 
# --- Clean path -------------------------------------------------------------
def test_clean_record_has_exactly_the_schema_columns(raw_event, run_handler):
    out = only(run_handler([raw_event()]))
    assert out["zone"] == ZONE_CLEAN
    assert set(out["row"]) == CLEAN_FIELDS
 
 
def test_patient_key_is_deterministic_hmac(raw_event, run_handler):
    event = raw_event()
    outputs = run_handler([event, event])
    expected = hmac.new(TEST_KEY, event["patient_id"].encode(), hashlib.sha256).hexdigest()
    assert {o["row"]["patient_key"] for o in outputs} == {expected}
    assert event["patient_id"] not in expected
 
 
def test_timestamps_are_normalized(raw_event, run_handler):
    row = only(run_handler([raw_event(event_timestamp="2026-09-15T20:30:00+02:00")]))["row"]
    assert row["event_timestamp"] == to_iso(ARRIVAL)  # same instant, canonical form
    assert row["ingest_ts"] == to_iso(ARRIVAL)        # from seconds-based arrival time
 
 
@pytest.mark.parametrize("spec", VITALS, ids=lambda s: s.name)
def test_null_vital_is_clean(spec, raw_event, run_handler):
    out = only(run_handler([raw_event(**{spec.name: None})]))
    assert out["zone"] == ZONE_CLEAN
    assert out["row"][spec.name] is None
 
 
@pytest.mark.parametrize("spec", VITALS, ids=lambda s: s.name)
def test_range_bounds_are_inclusive(spec, raw_event, run_handler):
    cast = int if spec.is_integer else float
    records = [raw_event(**{spec.name: cast(b)}) for b in (spec.valid_min, spec.valid_max)]
    assert [o["zone"] for o in run_handler(records)] == [ZONE_CLEAN, ZONE_CLEAN]
 
 
# --- Quarantine path --------------------------------------------------------
@pytest.mark.parametrize(
    "overrides,reason",
    [
        ({"patient_id": DELETE}, QuarantineReason.MISSING_REQUIRED_FIELD),
        ({"event_id": DELETE}, QuarantineReason.MISSING_REQUIRED_FIELD),
        ({"event_timestamp": None}, QuarantineReason.MISSING_REQUIRED_FIELD),
        ({"patient_id": ""}, QuarantineReason.INVALID_TYPE),
        ({"event_id": 12345}, QuarantineReason.INVALID_TYPE),
        ({"event_timestamp": "15/09/2026"}, QuarantineReason.INVALID_TIMESTAMP),
        ({"event_timestamp": "2026-09-15T18:30:00"}, QuarantineReason.INVALID_TIMESTAMP),
    ],
)
def test_quarantine_reasons(overrides, reason, raw_event, run_handler):
    out = only(run_handler([raw_event(**overrides)]))
    assert out["zone"] == ZONE_QUARANTINE
    assert out["row"]["reason"] == str(reason)
 
 
@pytest.mark.parametrize("spec", VITALS, ids=lambda s: s.name)
def test_out_of_range_is_quarantined(spec, raw_event, run_handler):
    below = int(spec.valid_min - 1) if spec.is_integer else spec.valid_min - 0.5
    above = int(spec.valid_max + 1) if spec.is_integer else spec.valid_max + 0.5
    for out in run_handler([raw_event(**{spec.name: v}) for v in (below, above)]):
        assert out["zone"] == ZONE_QUARANTINE
        assert out["row"]["reason"] == str(QuarantineReason.OUT_OF_RANGE)
 
 
@pytest.mark.parametrize("spec", VITALS, ids=lambda s: s.name)
def test_boolean_vital_is_invalid_type(spec, raw_event, run_handler):
    out = only(run_handler([raw_event(**{spec.name: True})]))
    assert out["row"]["reason"] == str(QuarantineReason.INVALID_TYPE)
 
 
def test_float_in_integer_vital_is_invalid_type(raw_event, run_handler):
    spec = next(s for s in VITALS if s.is_integer)
    out = only(run_handler([raw_event(**{spec.name: midpoint(spec) + 0.5})]))
    assert out["row"]["reason"] == str(QuarantineReason.INVALID_TYPE)
 
 
def test_unparseable_record_carries_no_payload(run_handler):
    out = only(run_handler([b"{not json"]))
    assert out["zone"] == ZONE_QUARANTINE
    assert out["row"] == {
        "event_id": None,
        "reason": str(QuarantineReason.UNPARSEABLE),
        "payload": "",
    }
 
 
def test_json_array_is_unparseable(run_handler):
    out = only(run_handler([b'[{"event_id": "EVT-1"}]']))
    assert out["row"]["reason"] == str(QuarantineReason.UNPARSEABLE)
 
 
# --- Identifier stripping ---------------------------------------------------
def test_clean_output_drops_identifiers_and_unknown_fields(raw_event, run_handler):
    event = raw_event(zip="60435", middle_name="Q")  # fields the schema never listed
    row = only(run_handler([event]))["row"]
    assert set(row) == CLEAN_FIELDS
    assert not (IDENTIFIER_FIELDS | {"patient_id", "zip", "middle_name"}) & set(row)
 
 
def test_quarantine_payload_is_stripped_but_traceable(raw_event, run_handler):
    event = raw_event(heart_rate=-5, zip="60435")
    row = only(run_handler([event]))["row"]
    payload = json.loads(row["payload"])
    assert not (IDENTIFIER_FIELDS | {"patient_id", "zip"}) & set(payload)
    assert payload["patient_key"] == hmac.new(
        TEST_KEY, event["patient_id"].encode(), hashlib.sha256
    ).hexdigest()
    assert payload["heart_rate"] == -5  # the offending value is kept for debugging
    assert row["event_id"] == event["event_id"]
 
 
def test_quarantine_without_patient_id_has_no_patient_key(raw_event, run_handler):
    row = only(run_handler([raw_event(patient_id=DELETE)]))["row"]
    assert "patient_key" not in json.loads(row["payload"])
 
 
def test_no_identifier_value_appears_in_any_written_byte(raw_event, run_handler):
    events = [raw_event(), raw_event(heart_rate=-5), raw_event(patient_id=DELETE)]
    written = "".join(o["line"] for o in run_handler(events))
    for field in IDENTIFIER_FIELDS:
        assert f"value-{field}" not in written
    assert events[0]["patient_id"] not in written
 
 
# --- S3 write contract ------------------------------------------------------
def test_one_newline_terminated_line_per_input_record(raw_event, run_handler):
    records = [raw_event(event_id=f"EVT-{i:06d}") for i in range(5)] + [b"{bad"]
    outputs = run_handler(records)
    assert len(outputs) == len(records)
    assert all(o["line"].endswith("\n") and o["line"].count("\n") == 1 for o in outputs)
 
 
def test_objects_are_grouped_by_zone_and_dt(raw_event, run_handler, fake_s3):
    late = raw_event(event_id="EVT-000002", event_timestamp="2026-09-14T23:10:00Z")
    run_handler([raw_event(), raw_event(heart_rate=-5), late])
    suffix = f"{SHARD_ID}-{FIRST_SEQUENCE}.json.gz"
    assert set(fake_s3.objects) == {
        f"{ZONE_CLEAN}/dt=2026-09-15/{suffix}",
        f"{ZONE_QUARANTINE}/dt=2026-09-15/{suffix}",
        f"{ZONE_CLEAN}/dt=2026-09-14/{suffix}",
    }
    assert fake_s3.buckets == {TEST_BUCKET}
 
 
def test_retried_batch_overwrites_instead_of_duplicating(raw_event, kinesis_event, fake_s3):
    event = kinesis_event([raw_event(), raw_event(heart_rate=-5)])
    app.handler(event)
    first = dict(fake_s3.objects)
    app.handler(event)
    assert fake_s3.objects == first
 
 
def test_dt_is_always_well_formed(raw_event, run_handler):
    for out in run_handler([raw_event(), raw_event(heart_rate=-5), b"{bad"]):
        assert DT_PATTERN.match(out["dt"])
 
 
def test_dt_uses_event_date_not_arrival_date(raw_event, run_handler):
    late = raw_event(event_timestamp="2026-09-14T23:10:00Z")  # arrival is the 15th
    assert only(run_handler([late]))["dt"] == "2026-09-14"
 
 
def test_dt_falls_back_to_arrival_when_timestamp_is_unusable(raw_event, run_handler):
    for out in run_handler([raw_event(event_timestamp="15/09/2026"), b"{bad"]):
        assert out["dt"] == ARRIVAL.strftime("%Y-%m-%d")
 
 
# --- Failure handling -------------------------------------------------------
def test_unexpected_record_error_is_quarantined_without_payload(
    raw_event, run_handler, monkeypatch, caplog
):
    def boom(*args, **kwargs):
        raise RuntimeError("leaky message value-ssn")
 
    monkeypatch.setattr(app, "build_clean", boom)
    with caplog.at_level(logging.INFO):
        out = only(run_handler([raw_event()]))
 
    assert out["zone"] == ZONE_QUARANTINE
    assert out["dt"] == ARRIVAL.strftime("%Y-%m-%d")
    assert out["row"] == {
        "event_id": None,
        "reason": str(QuarantineReason.PROCESSING_ERROR),
        "payload": "",
    }
    assert "RuntimeError" in caplog.text
    assert "value-ssn" not in caplog.text  # exception messages are never logged
 
 
def test_key_load_failure_raises_and_writes_nothing(raw_event, kinesis_event, fake_s3, monkeypatch):
    def denied():
        raise RuntimeError("AccessDenied")
 
    monkeypatch.setattr(app, "_load_hmac_key", denied)
    with pytest.raises(RuntimeError):
        app.handler(kinesis_event([raw_event()]))
    assert fake_s3.objects == {}
 
 
def test_missing_bucket_raises_and_writes_nothing(raw_event, kinesis_event, fake_s3, monkeypatch):
    monkeypatch.setattr(app, "ANALYTICS_BUCKET", "")
    with pytest.raises(RuntimeError):
        app.handler(kinesis_event([raw_event()]))
    assert fake_s3.objects == {}
 
 
def test_logs_contain_counts_only(raw_event, run_handler, caplog):
    event = raw_event()
    with caplog.at_level(logging.INFO):
        run_handler([event, raw_event(heart_rate=-5), b"{bad"])
    assert '"processed": 3' in caplog.text
    for value in [event["patient_id"], *(f"value-{f}" for f in IDENTIFIER_FIELDS)]:
        assert value not in caplog.text