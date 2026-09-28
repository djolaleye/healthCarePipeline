"""
Generator tests, plus one end-to-end pass of generated events through the transform.
"""
 
from __future__ import annotations
 
import json
import subprocess
import sys
import uuid
from collections import Counter
from pathlib import Path
 
import pytest
 
from conftest import REPO_ROOT
from core.schema import ZONE_CLEAN, ZONE_QUARANTINE
 
GENERATOR = REPO_ROOT / "generator" / "generator.py"
START = "2026-09-15T12:00:00Z"
 
 
def run_generator(csv_path: Path, out_dir: Path, *extra: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(GENERATOR), "--mode", "file",
         "--patients-csv", str(csv_path), "--patient-limit", "5",
         "--events", "300", "--start", START, "--out-dir", str(out_dir), *extra],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src", "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
 
 
@pytest.fixture
def generated(synthea_csv, tmp_path):
    out_dir = tmp_path / "data"
    result = run_generator(synthea_csv, out_dir)
    assert result.returncode == 0, result.stderr
    return {
        "events": [json.loads(line) for line in (out_dir / "events.jsonl").open()],
        "truth": [json.loads(line) for line in (out_dir / "ground_truth.jsonl").open()],
        "summary": json.loads((out_dir / "run_summary.json").read_text()),
    }
 
 
def test_summary_matches_the_events_written(generated):
    summary, events = generated["summary"], generated["events"]
    assert summary["records_sent"] == len(events)
    assert summary["distinct_events"] == len({e["event_id"] for e in events})
    assert summary["patients"] == 5
 
 
def test_deceased_patients_are_excluded(generated, synthea_csv):
    # The fixture marks the first four patients deceased.
    excluded = {str(uuid.UUID(int=i)) for i in range(4)}
    assert not excluded & {e.get("patient_id") for e in generated["events"]}
 
 
def test_same_seed_reproduces_the_run(synthea_csv, tmp_path):
    first, second = tmp_path / "a", tmp_path / "b"
    assert run_generator(synthea_csv, first).returncode == 0
    assert run_generator(synthea_csv, second).returncode == 0
    assert (first / "events.jsonl").read_bytes() == (second / "events.jsonl").read_bytes()
 
 
def test_error_rates_do_not_disturb_the_vitals(synthea_csv, tmp_path):
    """The two RNGs are separate, so a rate change leaves clean events identical."""
    base, changed = tmp_path / "base", tmp_path / "changed"
    run_generator(synthea_csv, base)
    run_generator(synthea_csv, changed, "--duplicate-rate", "0.0")
 
    def uncorrupted(out_dir):
        corrupted = {json.loads(l)["event_id"] for l in (out_dir / "ground_truth.jsonl").open()}
        return {
            e["event_id"]: e
            for e in (json.loads(l) for l in (out_dir / "events.jsonl").open())
            if e["event_id"] not in corrupted
        }
 
    shared = uncorrupted(base).keys() & uncorrupted(changed).keys()
    assert shared  # sanity: the runs overlap
    assert all(uncorrupted(base)[k] == uncorrupted(changed)[k] for k in shared)
 
 
def test_projection_guard_rejects_an_early_start(synthea_csv, tmp_path):
    result = run_generator(synthea_csv, tmp_path / "early", "--start", "2026-09-01T02:00:00Z")
    assert result.returncode != 0
    assert "PROJECTION_START" in result.stderr
 
 
def test_generated_events_land_where_the_summary_predicts(generated, run_handler):
    outputs = run_handler(generated["events"])
    zones = Counter(o["zone"] for o in outputs)
 
    assert zones[ZONE_QUARANTINE] == generated["summary"]["expected_quarantine_rows"]
    assert zones[ZONE_CLEAN] == generated["summary"]["expected_clean_rows_min"]
 
    clean_ids = [o["row"]["event_id"] for o in outputs if o["zone"] == ZONE_CLEAN]
    # What the vitals_dedup view should collapse to.
    assert len(set(clean_ids)) == generated["summary"]["expected_dedup_rows"]
 
 
def test_each_injected_error_lands_in_its_expected_zone(generated, run_handler):
    zone_by_event: dict[str, str] = {}
    for output in run_handler(generated["events"]):
        if output["row"].get("event_id"):
            zone_by_event[output["row"]["event_id"]] = output["zone"]
 
    for row in generated["truth"]:
        if row["error"] == "missing_patient_id":
            continue  # quarantined with event_id retained; covered by the count test
        assert zone_by_event[row["event_id"]] == row["expected_zone"], row