"""
Loads Synthea patient data and injects data-quality errors, before
sending events to Kinesis.
 
Run (from repo root):
  PYTHONPATH=src python3 generator/generator.py --mode file
  PYTHONPATH=src python3 generator/generator.py --mode kinesis --stream-name <name>
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core.schema import (
    EXPECTED_ZONE,
    IDENTIFIER_FIELDS,
    PROJECTION_START,
    VITALS,
    VITALS_BY_NAME,
    ZONE_QUARANTINE,
    InjectedError,
    parse_iso,
    to_iso,
)

KINESIS_MAX_BATCH = 500
KINESIS_MAX_ATTEMPTS = 5


#    Vital-sign simulation  
# ----------------------------
@dataclass(frozen=True)
class VitalModel:
    mean: float 
    patient_stdv: float  
    step_stdv: float  # noise
    episode_shift: float  # target offset

VITAL_MODELS = {
    "heart_rate": VitalModel(75, 8, 1.5, +30),
    "systolic": VitalModel(120, 12, 2.0, -15),
    "diastolic": VitalModel(78, 7, 1.5, -8),
    "spo2": VitalModel(97.5, 1.0, 0.3, -6),
    "temperature_c": VitalModel(36.8, 0.2, 0.05, +1.2),
}

# Schema match check
if set(VITAL_MODELS) != set(VITALS_BY_NAME):
    raise RuntimeError("VITAL_MODELS is out of sync with schema.VITALS")
 
MEAN_REVERSION = 0.1  # fraction of the gap to target closed per reading
EPISODE_START_PROB = 0.002  # per reading
EPISODE_LENGTH = (20, 60)  # readings
 
 
#        Patients 
# --------------------------
@dataclass
class PatientState:
    identity: dict[str, str]  # patient_id + IDENTIFIER_FIELDS
    baseline: dict[str, float] # base for patient vitals
    current: dict[str, float]
    episode_left: int = 0
 
    @classmethod
    def create(cls, identity: dict[str, str], rng: random.Random) -> PatientState:
        """
        Draws the baseline for each patient
        """
        baseline = {vital : rng.gauss(model.mean, model.patient_stdv) for vital, model in VITAL_MODELS.items()}

        baseline["diastolic"] = min(baseline["diastolic"], baseline["systolic"] - 25)
        baseline["spo2"] = min(baseline["spo2"], 99.5)

        return cls(identity, baseline, dict(baseline))
 
    def next_vitals(self, rng: random.Random) -> dict[str, float | int]:
        """
        Moves each vital value towards a target, with some added noise.
        """
        if self.episode_left == 0 and rng.random() < EPISODE_START_PROB:
            self.episode_left = rng.randint(*EPISODE_LENGTH)

        in_episode = self.episode_left > 0

        if in_episode:
            self.episode_left -= 1
 
        vitals: dict[str, float | int] = {}

        for vital, model in VITAL_MODELS.items():
            target = self.baseline[vital] + (model.episode_shift if in_episode else 0)
            value = self.current[vital]
            value += MEAN_REVERSION * (target - value) + rng.gauss(0, model.step_stdv)

            if vital == "spo2":
                value = min(value, 100.0)

            self.current[vital] = value
            vitals[vital] = round(value) if VITALS_BY_NAME[vital].is_integer else round(value, 1)

        return vitals

  
def load_patients(csv_path: Path, rng: random.Random, limit: int = 100) -> list[PatientState]:
    """
    Prepares patient data for loading:
            Keep living patients, sort by id, sample (seeded RNG),
            generate identifiers (mrn, device_id, patient_id).
    """

    with csv_path.open(newline="", encoding="utf-8") as file:
        rows = [row for row in csv.DictReader(file) if not row.get("DEATHDATE")]

    if len(rows) < limit:
        raise SystemExit(f"{csv_path} has less than required number of living patients. {len(rows)} / {limit}.\n")
    
    rows.sort(key=lambda r: r["Id"])  # stable order so the seed determines the sample
 
    patients = []
    for i, row in enumerate(rng.sample(rows, limit), start=1):
        identity = {
            "patient_id": row["Id"],
            "first_name": row["FIRST"],
            "last_name": row["LAST"],
            "birth_date": row["BIRTHDATE"],
            "ssn": row["SSN"],
            "mrn": f"MRN-{100000 + i}",
            "device_id": f"DEV-{i:04d}",
        }

        patients.append(PatientState.create(identity, rng))
 
    if set(patients[0].identity) - {"patient_id"} != IDENTIFIER_FIELDS:
        raise RuntimeError("identity fields are out of sync with schema.IDENTIFIER_FIELDS")
    
    return patients
 

#     Error injection
# ----------------------------
@dataclass
class Injector:
    rates: dict[InjectedError, float]
    late_max_hours: float
    rng: random.Random
 
    def pick(self) -> InjectedError | None:
        """
        Max one error per event.
        """
        random_value, cumulative = self.rng.random(), 0.0

        for error, rate in self.rates.items():
            cumulative += rate

            if random_value < cumulative:
                return error
        
        return None
 
    def apply(self, error: InjectedError, event: dict) -> str:
        """
        Apply error mutation to an event in place.
        """
        if error is InjectedError.MISSING_VALUE:
            # Null one random vital
            name = self.rng.choice(VITALS).name
            event[name] = None

            return name
        
        if error is InjectedError.OUT_OF_RANGE:
            # Push one random vital out of range
            spec = self.rng.choice(VITALS)
            offset = self.rng.uniform(1, 25)
            value = spec.valid_min - offset if self.rng.random() < 0.5 else spec.valid_max + offset
            event[spec.name] = round(value) if spec.is_integer else round(value, 1)

            return f"{spec.name}={event[spec.name]}"
        
        if error is InjectedError.MISSING_PATIENT_ID:
            # Delete patient id 
            del event["patient_id"]

            return ""
        
        if error is InjectedError.LATE_EVENT:
            # move event timestamp back 1 hour
            late_ts = parse_iso(event["event_timestamp"]) - timedelta(hours=self.rng.uniform(1, self.late_max_hours))

            event["event_timestamp"] = to_iso(late_ts)

            return event["event_timestamp"]
        
        return ""  # DUPLICATE
 
 
#      Sinks
# --------------------------
class FileSink:
    def __init__(self, path: Path):
        self._file = path.open("w", encoding="utf-8")
 
    def send(self, event: dict) -> None:
        self._file.write(json.dumps(event) + "\n")
 
    def close(self) -> None:
        self._file.close()
 
 
class KinesisSink:
    def __init__(self, stream_name: str, region: str | None, batch_delay: float):
        import boto3
 
        self._client = boto3.client("kinesis", region_name=region)
        self._stream = stream_name
        self._batch_delay = batch_delay
        self._buffer: list[dict] = []
 
    def send(self, event: dict) -> None:
        key = event.get("patient_id") or event["event_id"]

        self._buffer.append({"Data": json.dumps(event).encode(), "PartitionKey": key})

        # send records to Firehose only when batch size reached
        if len(self._buffer) == KINESIS_MAX_BATCH:
            self._flush()
 
    def close(self) -> None:
        if self._buffer:
            self._flush()
 
    def _flush(self) -> None:
        """
        Put_records call.
        """
        records, self._buffer = self._buffer, []

        for attempt in range(KINESIS_MAX_ATTEMPTS):
            response = self._client.put_records(StreamName=self._stream, Records=records)

            if response["FailedRecordCount"] == 0:
                break
            
            # Retry failures
            records = [ rec for rec, res in zip(records, response["Records"]) if "ErrorCode" in res ]

            time.sleep(0.2 * 2**attempt)

        else:
            raise RuntimeError(f"{len(records)} records still failing after {KINESIS_MAX_ATTEMPTS} attempts")

        time.sleep(self._batch_delay) # remain under 1000/s per shard pace
 
 
#       Run
# --------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    p.add_argument("--mode", choices=["file", "kinesis"], required=True)
    p.add_argument("--patients-csv", type=Path, default=Path("synthea/output/csv/patients.csv"))
    p.add_argument("--patient-limit", type=int, default=100)
    p.add_argument("--events", type=int, default=10_000, help="distinct events (duplicates are extra)")
    p.add_argument("--seed", type=int, default=20)
    p.add_argument("--start", type=parse_iso, help="first event time, ISO 8601 with offset; default ends the run at now")
    p.add_argument("--tick-seconds", type=float, default=1.0)
    p.add_argument("--out-dir", type=Path, default=Path("data"))
    p.add_argument("--stream-name")
    p.add_argument("--region")
    p.add_argument("--batch-delay", type=float, default=0.6)
    p.add_argument("--missing-value-rate", type=float, default=0.03)
    p.add_argument("--out-of-range-rate", type=float, default=0.02)
    p.add_argument("--missing-patient-id-rate", type=float, default=0.01)
    p.add_argument("--duplicate-rate", type=float, default=0.02)
    p.add_argument("--late-rate", type=float, default=0.02)
    p.add_argument("--late-max-hours", type=float, default=6.0)

    args = p.parse_args()
 
    if args.mode == "kinesis" and not args.stream_name:
        p.error("kinesis requires a stream name")

    if args.late_max_hours < 1:
        p.error("--late-max-hours must be at least 1")

    args.rates = {
        InjectedError.MISSING_VALUE: args.missing_value_rate,
        InjectedError.OUT_OF_RANGE: args.out_of_range_rate,
        InjectedError.MISSING_PATIENT_ID: args.missing_patient_id_rate,
        InjectedError.DUPLICATE: args.duplicate_rate,
        InjectedError.LATE_EVENT: args.late_rate,
    }

    if sum(args.rates.values()) > 1:
        p.error("error rates must sum to 1 or less")

    if args.start is None:
        args.start = datetime.now(timezone.utc) - timedelta(seconds=args.events * args.tick_seconds)

    earliest = args.start - timedelta(hours=args.late_max_hours)

    if earliest.strftime("%Y-%m-%d") < PROJECTION_START:
        p.error(f"late events could land before PROJECTION_START ({PROJECTION_START}) and be invisible in Athena")

    return args
 
 
def main() -> None:
    args = parse_args()
    
    # error and vitals have separate rng sources.
    vitals_rng = random.Random(args.seed)
    injector = Injector(args.rates, args.late_max_hours, random.Random(args.seed + 1))

    patients = load_patients(args.patients_csv, vitals_rng, args.patient_limit)
 
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.mode == "file":
        sink = FileSink(args.out_dir / "events.jsonl")
    else:
        sink = KinesisSink(args.stream_name, args.region, args.batch_delay)
 
    counts: Counter[str] = Counter()
    truth: list[dict] = []

    try:
        for seq in range(args.events):
            patient = patients[seq % len(patients)]
            identity = patient.identity

            # Build Event
            event = {
                "event_id": f"EVT-{seq + 1:06d}",
                "patient_id": identity["patient_id"],
                "event_timestamp": to_iso(args.start + timedelta(seconds=seq * args.tick_seconds)),
                **{k: v for k, v in identity.items() if k != "patient_id"},
                **patient.next_vitals(vitals_rng),
            }

            # Apply error
            error = injector.pick()
            if error is not None:
                detail = injector.apply(error, event)

                truth.append({
                    "event_id": event["event_id"],
                    "error": error,
                    "expected_zone": EXPECTED_ZONE[error],
                    "detail": detail,
                })

                counts[error] += 1

            # Send event
            sink.send(event)
            counts["records_sent"] += 1

            if error is InjectedError.DUPLICATE:
                sink.send(event)
                counts["records_sent"] += 1
        
    finally:
        sink.close()
 
    expected_quarantine = sum(counts[err] for err, zone in EXPECTED_ZONE.items() if zone == ZONE_QUARANTINE)

    summary = {
        "mode": args.mode,
        "seed": args.seed,
        "patients": len(patients),
        "start": to_iso(args.start),
        "distinct_events": args.events,
        "records_sent": counts["records_sent"],
        "injected": {err.value: counts[err] for err in InjectedError},
        "expected_quarantine_rows": expected_quarantine,
        "expected_clean_rows_min": counts["records_sent"] - expected_quarantine,
        "expected_dedup_rows": args.events - expected_quarantine,
    }
 
    with (args.out_dir / "ground_truth.jsonl").open("w", encoding="utf-8") as file:
        file.writelines(json.dumps(row) + "\n" for row in truth)

    (args.out_dir / "run_summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()