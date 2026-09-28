"""
Shared fixtures.

hmac_key fixture is autouse.
"""

from __future__ import annotations

import base64
import csv
import json
import uuid
import gzip
from datetime import datetime, timezone
from pathlib import Path

import pytest

from core.schema import IDENTIFIER_FIELDS, VITALS, to_iso
from transform import app

                 # YYYY MM  DD  HH  MM
ARRIVAL = datetime(2026, 9, 15, 18, 30, tzinfo=timezone.utc)
TEST_KEY = b"testing-key"
TEST_BUCKET = "testing-bucket"
SHARD_ID = "shardId-000000000000"
FIRST_SEQUENCE = 20_500_000_000_000_000_000
REPO_ROOT = Path(__file__).resolve().parents[1]

# Sentinel for raw_event(field=DELETE) --> drop the field.
DELETE = object()


def midpoint(spec) -> float | int:
    value = (spec.valid_min + spec.valid_max) / 2

    return round(value) if spec.is_integer else round(value, 1)


class FakeS3:
    """
    Captures put_object calls in insertion order.
    """
 
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.buckets: set[str] = set()
 
    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **_):
        self.buckets.add(Bucket)
        self.objects[Key] = Body
 
    def lines(self, key: str) -> list[str]:
        return gzip.decompress(self.objects[key]).decode().splitlines(keepends=True)


@pytest.fixture(autouse=True)
def hmac_key(monkeypatch) -> bytes:
    monkeypatch.setattr(app, "_hmac_key", TEST_KEY)
    return TEST_KEY


@pytest.fixture(autouse=True)
def fake_s3(monkeypatch) -> FakeS3:
    fake = FakeS3()
    monkeypatch.setattr(app, "_s3_client", fake)
    monkeypatch.setattr(app, "ANALYTICS_BUCKET", TEST_BUCKET)
    return fake


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
def kinesis_event():
    """
    Wrap records as a Kinesis event-source-mapping batch.
    """
 
    def build(records, arrival: datetime = ARRIVAL) -> dict:
        wrapped = []
 
        for i, record in enumerate(records):
            data = record if isinstance(record, bytes) else json.dumps(record).encode()
            sequence = str(FIRST_SEQUENCE + i)
 
            wrapped.append({
                "eventSource": "aws:kinesis",
                "eventID": f"{SHARD_ID}:{sequence}",
                "kinesis": {
                    "partitionKey": "pk",
                    "sequenceNumber": sequence,
                    "data": base64.b64encode(data).decode(),
                    "approximateArrivalTimestamp": arrival.timestamp(),  # seconds
                },
            })
 
        return {"Records": wrapped}
 
    return build


@pytest.fixture
def run_handler(kinesis_event, fake_s3):
    """
    Run the handler on records; return one output per written line, in order:
    {"key", "zone", "dt", "row", "line"}.
    """
 
    def run(records, **kwargs) -> list[dict]:
        before = set(fake_s3.objects)
        app.handler(kinesis_event(records, **kwargs))
        outputs = []
 
        for key in fake_s3.objects:
            if key in before:
                continue
 
            zone, dt_part, _name = key.split("/", 2)
 
            for line in fake_s3.lines(key):
                outputs.append({
                    "key": key,
                    "zone": zone,
                    "dt": dt_part.removeprefix("dt="),
                    "row": json.loads(line),
                    "line": line,
                })
 
        return outputs
 
    return run


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