#!/usr/bin/env python3

import argparse
import json
import logging
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import aggregate
import clean
import ingest

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = os.getenv("DAT535_OUTPUT_DIR")
RAW_DIR = os.getenv("DAT535_RAW_DIR")


def parse_years(value):
    return [int(year) for year in (value or "").split(",") if year.strip()]


YEARS = parse_years(os.getenv("DAT535_YEARS"))
PARTIAL_YEARS = parse_years(os.getenv("DAT535_PARTIAL_YEARS"))

STAGES = {
    "bronze": lambda: ingest.run(RAW_DIR, OUTPUT_DIR, YEARS or None, PARTIAL_YEARS or None),
    "silver": lambda: clean.run(OUTPUT_DIR),
    "gold": lambda: aggregate.run(OUTPUT_DIR),
}


def data_dir():
    return Path(OUTPUT_DIR) if OUTPUT_DIR else ROOT / "data"


def log_dir():
    return Path(OUTPUT_DIR) / "logs" if OUTPUT_DIR else ROOT / "logs" / "pipeline"


def read_json(path):
    return json.loads(path.read_text()) if path.is_file() else None


def run_stages(names):
    results = {}
    start = time.time()

    for name in names:
        if any(r["status"] != "success" for r in results.values()):
            logger.warning(f"Skipping {name} because an earlier stage failed")
            results[name] = {"status": "skipped"}
            continue

        logger.info(f"===== {name.upper()} =====")
        stage_start = time.time()
        try:
            results[name] = STAGES[name]()
        except Exception as e:
            logger.exception(f"{name} failed")
            results[name] = {"status": "failed", "error": str(e)}
        results[name]["elapsed_time"] = time.time() - stage_start

    total = time.time() - start

    logger.info("===== SUMMARY =====")
    for name, result in results.items():
        logger.info(f"  {name}: {result['status']} ({result.get('elapsed_time', 0):.2f}s)")
    logger.info(f"  Total time: {total:.2f}s")
    for line in summary_lines():
        logger.info(line)

    write_run_log(results, total)
    return results


def summary_lines():
    root = data_dir()
    lines = []

    for path in sorted((root / "bronze" / "manifests").glob("delivery_*.json")):
        r = read_json(path)
        lines.append(
            f"  Bronze {r['year']}: {r['status']}, {r['received_count']} months, "
            f"{r.get('source_records', 0):,} records"
        )

    history = root / "silver" / "quality_history.jsonl"
    if history.is_file() and history.read_text().strip():
        last = json.loads(history.read_text().splitlines()[-1])
        lines.append(
            f"  Silver last run ({last['gate_status']}): {last['source_records']:,} in -> "
            f"{last['accepted_records']:,} accepted, {last['rejected_records']:,} rejected, "
            f"{last['duplicates_removed']:,} duplicates removed"
        )

    ready = read_json(root / "gold" / "_READY.json")
    if ready:
        manifest = read_json(Path(ready["run_path"]) / "manifest.json") or {}
        for name, info in manifest.get("datasets", {}).items():
            lines.append(f"  Gold {name}: {info['aggregate_rows']:,} rows")
        lines.append(f"  Gold run {ready['run_id']} built from Silver {ready.get('silver_run_id')}")

    return lines


def write_run_log(results, total):
    folder = log_dir()
    folder.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")

    stages = {
        name: {k: v for k, v in r.items() if k in ("status", "error", "elapsed_time")}
        for name, r in results.items()
    }
    log = {"finished_utc": stamp, "total_time": total, "stages": stages}
    (folder / f"pipeline_{stamp}.json").write_text(json.dumps(log, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pipeline", choices=[*STAGES, "all"])
    args = parser.parse_args()

    names = list(STAGES) if args.pipeline == "all" else [args.pipeline]
    results = run_stages(names)

    return 0 if all(r["status"] == "success" for r in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
