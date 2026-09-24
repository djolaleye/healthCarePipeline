"""
Shared fixtures.

hmac_key fixture is autouse.
"""

from __future__ import annotations

import base64
import csv
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from core.schema import IDENTIFIER_FIELDS, VITALS, to_iso
from transform import app

                 # YYYY MM  DD  HH  MM
ARRIVAL = datetime(2026, 9, 15, 18, 30, tzinfo=timezone.utc)
TEST_KEY = b"testing-key"
REPO_ROOT = Path(__file__).resolve().parents[1]

# Sentinel for raw_event(field=DELETE) --> drop the field.
DELETE = object()


def midpoint(spec) -> float | int:
    value = (spec.valid_min + spec.valid_max) / 2

    return round(value) if spec.is_integer else round(value, 1)


@pytest.fixture(autouse=True)
def hmac_key(monkeypatch) -> bytes:
    monkeypatch.setattr(app, "_hmac_key", TEST_KEY)
    return TEST_KEY


@pytest.fixture
def raw_event():
    """
    A single valid event built from the schema.
    """

    def build(**overrides) -> dict:
        event = {
            "event_id": "EVT-000001",
            "patient_id": "0c4b1b8a-9f4e-4a1b-9d2e-000000000001",
            "event_timestamp": to_iso(ARRIVAL),
            **{field: f"value-{field}" for field in sorted(IDENTIFIER_FIELDS)},
            **{spec.name: midpoint(spec) for spec in VITALS},
        }
        event.update(overrides)

        return {k: v for k, v in event.items() if v is not DELETE}

    return build


@pytest.fixture
def firehose_event():
    """
    Wrap records as Firehose input.
    """

    def build(records, arrival: datetime = ARRIVAL) -> dict:
        ms = int(arrival.timestamp() * 1000)
        wrapped = []

        for i, record in enumerate(records):
            data = record if isinstance(record, bytes) else json.dumps(record).encode()

            wrapped.append({
                "recordId": f"rec-{i}",
                "data": base64.b64encode(data).decode(),
                "kinesisRecordMetadata": {"approximateArrivalTimestamp": ms},
            })

        return {"invocationId": "test-invocation", "records": wrapped}

    return build


@pytest.fixture
def decode():
    def _decode(result_record: dict) -> dict:
        return json.loads(base64.b64decode(result_record["data"]))

    return _decode


@pytest.fixture
def synthea_csv(tmp_path: Path) -> Path:
    """
    20 record standin for synthetic patient data.
    """
    path = tmp_path / "patients.csv"

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Id", "BIRTHDATE", "DEATHDATE", "SSN", "FIRST", "LAST"])

        for i in range(20):
            writer.writerow([
                str(uuid.UUID(int=i)),
                "1980-01-01",
                "2021-05-05" if i < 4 else "",  # deceased == skipped
                f"999-{i:02d}-0000",
                f"Palter{i}",
                f"Wayton{i}",
            ])

    return path