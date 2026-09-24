"""
Firehose transform Lambda.

For each record: decode, validate, replace patient_id with an HMAC patient_key,
rebuild the output fields from an allow-list, and return partition keys (zone, datetime).

Env:
  HMAC_PARAM_NAME  SSM SecureString holding the pseudonym key (default /vitals/hmac-key)

NEVER log record payloads, since raw records carry identifier fields.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
from collections import Counter
from datetime import datetime, timezone

from core.schema import (
    CLEAN_COLUMNS,
    DT_FORMAT,
    REQUIRED_FIELDS,
    VITALS,
    ZONE_CLEAN,
    ZONE_QUARANTINE,
    QuarantineReason,
    parse_iso,
    to_iso,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HMAC_PARAM_NAME = os.environ.get("HMAC_PARAM_NAME", "/vitals/hmac-key")
_hmac_key: bytes | None = None  # Cached for the life of the container

_CLEAN_FIELDS = tuple(name for name, _ in CLEAN_COLUMNS)


def _load_hmac_key() -> bytes:
    global _hmac_key

    if _hmac_key is None:
        import boto3

        response = boto3.client("ssm").get_parameter(Name=HMAC_PARAM_NAME, WithDecryption=True)

        _hmac_key = response["Parameter"]["Value"].encode()
    
    return _hmac_key


def patient_key(patient_id: str, key: bytes) -> str:
    """
    Deterministic pseudonym.
    """
    return hmac.new(key, patient_id.encode(), hashlib.sha256).hexdigest()


def validate(record: dict) -> QuarantineReason | None:
    """
    No failues ensures a clean record.
    """
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None:
            return QuarantineReason.MISSING_REQUIRED_FIELD
        
        if not isinstance(record[field], str) or not record[field].strip():
            return QuarantineReason.INVALID_TYPE

    try:
        parse_iso(record["event_timestamp"])
    except (ValueError, TypeError):
        return QuarantineReason.INVALID_TIMESTAMP

    for spec in VITALS:
        value = record.get(spec.name)

        if value is None:
            continue
        
        if isinstance(value, bool):
            return QuarantineReason.INVALID_TYPE
        
        if spec.is_integer:
            if not isinstance(value, int):
                return QuarantineReason.INVALID_TYPE  
        elif not isinstance(value, (int, float)):
            return QuarantineReason.INVALID_TYPE
        
        if not spec.valid_min <= value <= spec.valid_max:
            return QuarantineReason.OUT_OF_RANGE

    return None


def build_clean(record: dict, key: bytes, ingest_ts: str) -> dict:
    record_values = {
        "event_id": record["event_id"],
        "patient_key": patient_key(record["patient_id"], key),
        "event_timestamp": to_iso(parse_iso(record["event_timestamp"])),
        "ingest_ts": ingest_ts,
    }

    for spec in VITALS:
        value = record.get(spec.name)

        if value is not None:
            value = int(value) if spec.is_integer else float(value)
        
        record_values[spec.name] = value

    return {field: record_values[field] for field in _CLEAN_FIELDS}


def build_quarantine(record: dict | None, reason: QuarantineReason, key: bytes) -> dict:
    if record is None:
        return {"event_id": None, "reason": str(reason), "payload": ""}

    event_id = record.get("event_id")
    payload = {"event_id": event_id, "event_timestamp": record.get("event_timestamp")}
    patient_id = record.get("patient_id")

    if isinstance(patient_id, str) and patient_id:
        payload["patient_key"] = patient_key(patient_id, key)
    
    for spec in VITALS:
        payload[spec.name] = record.get(spec.name)

    return {
        "event_id": event_id if isinstance(event_id, str) else None,
        "reason": str(reason),
        "payload": json.dumps(payload),
    }


def _ingest_time(record: dict) -> datetime:
    ms = record.get("kinesisRecordMetadata", {}).get("approximateArrivalTimestamp") or record.get("approximateArrivalTimestamp")

    if ms is None:
        return datetime.now(timezone.utc)
    
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _partition_date(record: dict | None, arrival: datetime) -> str:
    """
    Event date when usable, arrival date otherwise. For Firehose dynamic partitioning.
    """
    if record is not None:
        try:
            return parse_iso(record["event_timestamp"]).strftime(DT_FORMAT)
        except (ValueError, TypeError, KeyError):
            pass
        
    return arrival.strftime(DT_FORMAT)


def _process(record: dict, key: bytes) -> tuple[dict, str]:
    """
    data processing per record
        INPUT: Base64 data --> Bytes --> JSON --> Python dict
        OUTPUT: Python dict --> JSON --> Bytes --> Base64 data
    """
    arrival = _ingest_time(record)

    # Parse & Validate
    try:
        parsed = json.loads(base64.b64decode(record["data"]))
        if not isinstance(parsed, dict):
            raise ValueError("record is not a JSON object")
    except (ValueError, binascii.Error):
        parsed, reason = None, QuarantineReason.UNPARSEABLE
    else:
        reason = validate(parsed)

    # Route to correct zone
    if reason is None:
        zone, output = ZONE_CLEAN, build_clean(parsed, key, to_iso(arrival))
    else:
        zone, output = ZONE_QUARANTINE, build_quarantine(parsed, reason, key)

    data = base64.b64encode((json.dumps(output) + "\n").encode()).decode()

    response = {
        "recordId": record["recordId"],
        "result": "Ok",
        "data": data,
        "metadata": {
            "partitionKeys": {
                "zone": zone,
                "dt": _partition_date(parsed, arrival)
                }
            },
    }, zone

    return response


def handler(event: dict, context=None) -> dict:
    key = _load_hmac_key()
    counts: Counter[str] = Counter()
    records = []

    for record in event["records"]:
        try:
            output, zone = _process(record, key)
            counts[zone] += 1
        except Exception:
            # Data quality problem. Send record to Firehose 'errors/' prefix.
            logger.exception("record %s failed", record.get("recordId"))
            output = { "recordId": record["recordId"], "result": "ProcessingFailed" }
            counts["failed"] += 1
        
        records.append(output)

    logger.info(json.dumps({"processed": len(records), **counts}))
    return {"records": records}