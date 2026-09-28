"""
Shared data contract for the pipeline.

Glue table columns in template.yaml will mirror CLEAN_COLUMNS and
QUARANTINE_COLUMNS.
"""
 
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum


def to_iso(dt: datetime) -> str:
    utc = dt.astimezone(timezone.utc)

    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"
  
def parse_iso(iso: str) -> datetime:
    dt = datetime.fromisoformat(iso)

    if dt.tzinfo is None:
        raise ValueError("timestamp must include a UTC offset")
    
    return dt.astimezone(timezone.utc)


# Partitioning
ZONE_CLEAN = "clean"
ZONE_QUARANTINE = "quarantine"
DT_FORMAT = "%Y-%m-%d"  ## Glue projection.dt.format = yyyy-MM-dd.
PROJECTION_START = "2026-09-01"

@dataclass(frozen=True)
class VitalSpec:
    name: str
    loinc: str
    unit: str  # UCUM
    is_integer: bool
    valid_min: float  # inclusive 
    valid_max: float  # inclusive
 
VITALS = (
    VitalSpec("heart_rate", "8867-4", "/min", True, 20, 300),
    VitalSpec("systolic", "8480-6", "mm[Hg]", True, 50, 300),
    VitalSpec("diastolic", "8462-4", "mm[Hg]", True, 20, 200),
    VitalSpec("spo2", "59408-5", "%", False, 50, 100),
    VitalSpec("temperature_c", "8310-5", "Cel", False, 30, 45),
)

VITALS_BY_NAME = {vital.name: vital for vital in VITALS}

# Missing or non-string values for these == event to quarantine.
REQUIRED_FIELDS = ("event_id", "patient_id", "event_timestamp")
 
# Carried in raw events, never written to the analytics bucket. 
IDENTIFIER_FIELDS = frozenset(
    {"first_name", "last_name", "birth_date", "ssn", "mrn", "device_id"}
)
 

# Errors
class InjectedError(StrEnum):
    """ Generator corrupts -> written to the sidecar. """
 
    MISSING_VALUE = "missing_value"
    OUT_OF_RANGE = "out_of_range"
    MISSING_PATIENT_ID = "missing_patient_id"
    DUPLICATE = "duplicate"
    LATE_EVENT = "late_event"

class QuarantineReason(StrEnum):
    """ Reason why transform rejected a record. """
 
    UNPARSEABLE = "unparseable"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    INVALID_TYPE = "invalid_type"
    INVALID_TIMESTAMP = "invalid_timestamp"
    OUT_OF_RANGE = "out_of_range"
    PROCESSING_ERROR = "processing_error"
 

## Where each injected error should land. Used to verify results in Athena.
EXPECTED_ZONE = {
    InjectedError.MISSING_VALUE: ZONE_CLEAN,
    InjectedError.OUT_OF_RANGE: ZONE_QUARANTINE,
    InjectedError.MISSING_PATIENT_ID: ZONE_QUARANTINE,
    InjectedError.DUPLICATE: ZONE_CLEAN, 
    InjectedError.LATE_EVENT: ZONE_CLEAN,
}

# Output columns
CLEAN_COLUMNS = (
    ("event_id", "string"),
    ("patient_key", "string"),
    ("event_timestamp", "string"),
    ("ingest_ts", "string"),
    *((vital.name, "int" if vital.is_integer else "double") for vital in VITALS),
)
 
QUARANTINE_COLUMNS = (
    ("event_id", "string"),
    ("reason", "string"),
    ("payload", "string"),  # identifier-stripped, JSON
)


if __name__ == "__main__":
    pass