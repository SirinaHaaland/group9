import csv
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

import clean
import ingest
import run_pipeline

FLIGHTS = [
    {"FlightDate": "2025-01-01", "Reporting_Airline": "AA",
     "Flight_Number_Reporting_Airline": "100", "Origin": "JFK", "Dest": "LAX",
     "CRSDepTime": "0930", "DepTime": "0935", "DepDelay": "5",
     "CRSArrTime": "1245", "ArrTime": "1255", "ArrDelay": "10",
     "Cancelled": "0", "Diverted": "0", "CRSElapsedTime": "375",
     "Distance": "2475"},
    {"FlightDate": "2025-01-02", "Reporting_Airline": "AA",
     "Flight_Number_Reporting_Airline": "101", "Origin": "LAX", "Dest": "JFK",
     "CRSDepTime": "0800", "DepTime": "0800", "DepDelay": "0",
     "CRSArrTime": "1630", "ArrTime": "1640", "ArrDelay": "10",
     "Cancelled": "0", "Diverted": "0", "CRSElapsedTime": "330",
     "Distance": "2475"},
    {"FlightDate": "2025-01-03", "Reporting_Airline": "DL",
     "Flight_Number_Reporting_Airline": "200", "Origin": "JFK", "Dest": "LAX",
     "CRSDepTime": "1000", "DepTime": "", "DepDelay": "",
     "CRSArrTime": "1315", "ArrTime": "", "ArrDelay": "",
     "Cancelled": "1", "Diverted": "0", "CRSElapsedTime": "375",
     "Distance": "2475"},
]

AIRPORTS = [
    ["1", "John F Kennedy International Airport", "New York", "United States",
     "JFK", "KJFK", "40.64", "-73.78", "13", "-5", "A", "America/New_York",
     "airport", "OurAirports"],
    ["2", "Los Angeles International Airport", "Los Angeles", "United States",
     "LAX", "KLAX", "33.94", "-118.40", "125", "-8", "A", "America/Los_Angeles",
     "airport", "OurAirports"],
]


def write_raw(raw, columns=ingest.REQUIRED_COLUMNS):
    year_dir = raw / "2025"
    year_dir.mkdir(parents=True)

    path = year_dir / "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2025_1.csv"
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(FLIGHTS)

    with (raw / "openflights_airports.dat").open("w", newline="") as f:
        csv.writer(f).writerows(AIRPORTS)


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    raw, out = tmp_path / "raw", tmp_path / "out"
    monkeypatch.setattr(run_pipeline, "RAW_DIR", str(raw))
    monkeypatch.setattr(run_pipeline, "OUTPUT_DIR", str(out))
    monkeypatch.setattr(run_pipeline, "YEARS", [])            
    monkeypatch.setattr(run_pipeline, "PARTIAL_YEARS", [2025])  
    return raw, out


def read_pointer(path):
    return json.loads((path / "_READY.json").read_text())


def read_history(out):
    lines = (out / "silver" / "quality_history.jsonl").read_text().splitlines()
    return [json.loads(line) for line in lines]


def test_failed_silver_keeps_old_runs(pipeline, monkeypatch):
    raw, out = pipeline
    write_raw(raw)

    results = run_pipeline.run_stages(["bronze", "silver", "gold"])

    assert [r["status"] for r in results.values()] == ["success"] * 3

    silver_ready = read_pointer(out / "silver")
    gold_ready = read_pointer(out / "gold")
    assert gold_ready["silver_run_id"] == silver_ready["run_id"]

    report = json.loads(
        (out / "silver" / "runs" / silver_ready["run_id"] / "quality_report.json").read_text()
    )
    assert report["quality_gate"]["status"] == "PASS"
    assert report["accepted_records"] == 3
    assert report["missing_utc"] == 0

    strict = dict(clean.QUALITY_LIMITS, max_rejected_pct=-1)
    monkeypatch.setattr(clean, "QUALITY_LIMITS", strict)

    results = run_pipeline.run_stages(["silver", "gold"])

    assert results["silver"]["status"] == "failed"
    assert "quality gate failed" in results["silver"]["error"]
    assert results["gold"]["status"] == "skipped"

    assert read_pointer(out / "silver")["run_id"] == silver_ready["run_id"]
    assert read_pointer(out / "gold")["run_id"] == gold_ready["run_id"]

    runs = {p.name for p in (out / "silver" / "runs").iterdir()}
    failed_run = (runs - {silver_ready["run_id"]}).pop()
    failed_report = json.loads(
        (out / "silver" / "runs" / failed_run / "quality_report.json").read_text()
    )
    assert failed_report["quality_gate"]["status"] == "FAIL"
    assert [h["gate_status"] for h in read_history(out)][:2] == ["PASS", "FAIL"]

    monkeypatch.setattr(clean, "QUALITY_LIMITS", dict(strict, max_rejected_pct=1.0))

    def crash(*args, **kwargs):
        raise RuntimeError("simulated crash before publishing")

    monkeypatch.setattr(clean, "publish_run", crash)
    runs_before = {p.name for p in (out / "silver" / "runs").iterdir()}

    results = run_pipeline.run_stages(["silver", "gold"])

    assert results["silver"]["status"] == "failed"
    assert results["gold"]["status"] == "skipped"
    assert read_pointer(out / "silver")["run_id"] == silver_ready["run_id"]

    crashed = ({p.name for p in (out / "silver" / "runs").iterdir()} - runs_before).pop()
    assert (out / "silver" / "runs" / crashed / "flights").is_dir()

    summary = "\n".join(run_pipeline.summary_lines())
    assert "Bronze 2025: PARTIAL, 1 months, 3 records" in summary
    assert "Silver last run (PASS)" in summary
    assert "Gold daily_trends: 3 rows" in summary


def test_missing_column_stops_pipeline(pipeline, monkeypatch):
    raw, out = pipeline
    without_arr_delay = [c for c in ingest.REQUIRED_COLUMNS if c != "ArrDelay"]
    write_raw(raw, columns=without_arr_delay)

    monkeypatch.setattr(sys, "argv", ["run_pipeline.py", "all"])
    exit_code = run_pipeline.main()

    assert exit_code == 1
    assert not (out / "silver").exists()
    assert not (out / "gold").exists()

    log = json.loads(next((out / "logs").glob("pipeline_*.json")).read_text())
    assert log["stages"]["bronze"]["status"] == "failed"
    assert "ArrDelay" in log["stages"]["bronze"]["error"]
    assert log["stages"]["silver"]["status"] == "skipped"
    assert log["stages"]["gold"]["status"] == "skipped"
