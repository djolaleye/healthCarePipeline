"""
Transform Lambda tests: validation, stripping, and the Firehose contract.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re

import pytest

from conftest import ARRIVAL, DELETE, TEST_KEY, midpoint
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

CLEAN_FIELDS = { name for name, _ in CLEAN_COLUMNS }
DT_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def run(firehose_event, records, **kwargs) -> list[dict]:
    return app.handler(firehose_event(records, **kwargs))["records"]


def zone_of(result: dict) -> str:
    return result["metadata"]["partitionKeys"]["zone"]


# --- Clean path -------------------------------------------------------------
def test_clean_record_has_exactly_the_schema_columns(raw_event, firehose_event, decode):
    result = run(firehose_event, [raw_event()])[0]
    assert result["result"] == "Ok"
    assert zone_of(result) == ZONE_CLEAN
    assert set(decode(result)) == CLEAN_FIELDS


def test_patient_key_is_deterministic_hmac(raw_event, firehose_event, decode):
    event = raw_event()
    results = run(firehose_event, [event, event])
    expected = hmac.new(TEST_KEY, event["patient_id"].encode(), hashlib.sha256).hexdigest()
    keys = {decode(r)["patient_key"] for r in results}
    assert keys == {expected}
    assert event["patient_id"] not in expected


def test_timestamps_are_normalized(raw_event, firehose_event, decode):
    result = run(firehose_event, [raw_event(event_timestamp="2026-09-15T20:30:00+02:00")])[0]
    output = decode(result)
    assert output["event_timestamp"] == to_iso(ARRIVAL)  # same instant, canonical form
    assert output["ingest_ts"] == to_iso(ARRIVAL)


@pytest.mark.parametrize("spec", VITALS, ids=lambda s: s.name)
def test_null_vital_is_clean(spec, raw_event, firehose_event, decode):
    result = run(firehose_event, [raw_event(**{spec.name: None})])[0]
    assert zone_of(result) == ZONE_CLEAN
    assert decode(result)[spec.name] is None


@pytest.mark.parametrize("spec", VITALS, ids=lambda s: s.name)
def test_range_bounds_are_inclusive(spec, raw_event, firehose_event):
    records = [raw_event(**{spec.name: bound}) for bound in (spec.valid_min, spec.valid_max)]
    if spec.is_integer:
        records = [raw_event(**{spec.name: int(b)}) for b in (spec.valid_min, spec.valid_max)]
    assert [zone_of(r) for r in run(firehose_event, records)] == [ZONE_CLEAN, ZONE_CLEAN]


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
def test_quarantine_reasons(overrides, reason, raw_event, firehose_event, decode):
    result = run(firehose_event, [raw_event(**overrides)])[0]
    assert zone_of(result) == ZONE_QUARANTINE
    assert decode(result)["reason"] == str(reason)


@pytest.mark.parametrize("spec", VITALS, ids=lambda s: s.name)
def test_out_of_range_is_quarantined(spec, raw_event, firehose_event, decode):
    below = int(spec.valid_min - 1) if spec.is_integer else spec.valid_min - 0.5
    above = int(spec.valid_max + 1) if spec.is_integer else spec.valid_max + 0.5
    results = run(firehose_event, [raw_event(**{spec.name: v}) for v in (below, above)])
    for result in results:
        assert zone_of(result) == ZONE_QUARANTINE
        assert decode(result)["reason"] == str(QuarantineReason.OUT_OF_RANGE)


@pytest.mark.parametrize("spec", VITALS, ids=lambda s: s.name)
def test_boolean_vital_is_invalid_type(spec, raw_event, firehose_event, decode):
    result = run(firehose_event, [raw_event(**{spec.name: True})])[0]
    assert decode(result)["reason"] == str(QuarantineReason.INVALID_TYPE)


def test_float_in_integer_vital_is_invalid_type(raw_event, firehose_event, decode):
    spec = next(s for s in VITALS if s.is_integer)
    result = run(firehose_event, [raw_event(**{spec.name: midpoint(spec) + 0.5})])[0]
    assert decode(result)["reason"] == str(QuarantineReason.INVALID_TYPE)


def test_unparseable_record_carries_no_payload(firehose_event, decode):
    result = run(firehose_event, [b"{not json"])[0]
    assert result["result"] == "Ok"  # quarantined, not sent to errors/
    assert zone_of(result) == ZONE_QUARANTINE
    output = decode(result)
    assert output == {
        "event_id": None,
        "reason": str(QuarantineReason.UNPARSEABLE),
        "payload": "",
    }


def test_json_array_is_unparseable(firehose_event, decode):
    result = run(firehose_event, [b'[{"event_id": "EVT-1"}]'])[0]
    assert decode(result)["reason"] == str(QuarantineReason.UNPARSEABLE)


# --- Identifier stripping ---------------------------------------------------
def test_clean_output_drops_identifiers_and_unknown_fields(raw_event, firehose_event, decode):
    event = raw_event(zip="60435", middle_name="Q")  # fields the schema never listed
    output = decode(run(firehose_event, [event])[0])
    assert set(output) == CLEAN_FIELDS
    assert not (IDENTIFIER_FIELDS | {"patient_id", "zip", "middle_name"}) & set(output)


def test_quarantine_payload_is_stripped_but_traceable(raw_event, firehose_event, decode):
    event = raw_event(heart_rate=-5, zip="60435")
    output = decode(run(firehose_event, [event])[0])
    payload = json.loads(output["payload"])
    assert not (IDENTIFIER_FIELDS | {"patient_id", "zip"}) & set(payload)
    assert payload["patient_key"] == hmac.new(
        TEST_KEY, event["patient_id"].encode(), hashlib.sha256
    ).hexdigest()
    assert payload["heart_rate"] == -5  # the offending value is kept for debugging
    assert output["event_id"] == event["event_id"]


def test_quarantine_without_patient_id_has_no_patient_key(raw_event, firehose_event, decode):
    output = decode(run(firehose_event, [raw_event(patient_id=DELETE)])[0])
    assert "patient_key" not in json.loads(output["payload"])


# --- Firehose contract ------------------------------------------------------
def test_every_record_is_returned_once_in_order(raw_event, firehose_event):
    records = [raw_event(event_id=f"EVT-{i:06d}") for i in range(5)] + [b"{bad"]
    event = firehose_event(records)
    results = app.handler(event)["records"]
    assert [r["recordId"] for r in results] == [r["recordId"] for r in event["records"]]


def test_output_records_are_newline_terminated(raw_event, firehose_event):
    import base64

    results = run(firehose_event, [raw_event(), b"{bad"])
    assert all(base64.b64decode(r["data"]).endswith(b"\n") for r in results)


def test_partition_keys_are_always_present_and_well_formed(raw_event, firehose_event):
    results = run(firehose_event, [raw_event(), raw_event(heart_rate=-5), b"{bad"])
    for result in results:
        keys = result["metadata"]["partitionKeys"]
        assert set(keys) == {"zone", "dt"}
        assert keys["zone"] in {ZONE_CLEAN, ZONE_QUARANTINE}
        assert DT_PATTERN.match(keys["dt"])


def test_dt_uses_event_date_not_arrival_date(raw_event, firehose_event):
    late = raw_event(event_timestamp="2026-09-14T23:10:00Z")  # arrival is the 15th
    assert run(firehose_event, [late])[0]["metadata"]["partitionKeys"]["dt"] == "2026-09-14"


def test_dt_falls_back_to_arrival_when_timestamp_is_unusable(raw_event, firehose_event):
    records = [raw_event(event_timestamp="15/09/2026"), b"{bad"]
    for result in run(firehose_event, records):
        assert result["metadata"]["partitionKeys"]["dt"] == ARRIVAL.strftime("%Y-%m-%d")


def test_unexpected_error_becomes_processing_failed(raw_event, firehose_event, monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(app, "build_clean", boom)
    result = run(firehose_event, [raw_event()])[0]
    assert result["result"] == "ProcessingFailed"
    assert "data" not in result