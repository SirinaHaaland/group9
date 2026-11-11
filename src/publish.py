import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2))
    temporary.replace(path)


def read_ready(layer_dir):
    pointer = Path(layer_dir) / "_READY.json"
    return json.loads(pointer.read_text()) if pointer.is_file() else None


def publish_run(layer_dir, run_id, manifest, **details):
    run_dir = Path(layer_dir) / "runs" / run_id
    if not (run_dir / manifest).is_file():
        raise RuntimeError(f"Run {run_id} has no {manifest}, not publishing")

    write_json(Path(layer_dir) / "_READY.json", {
        "run_id": run_id,
        "status": "READY",
        "run_path": str(run_dir.resolve()),
        "published_utc": datetime.now(UTC).isoformat(),
        **details,
    })


def current_run(layer_dir):
    ready = read_ready(layer_dir)
    if ready is None:
        raise RuntimeError(f"Nothing published yet in {layer_dir}")

    run_dir = Path(layer_dir) / "runs" / ready["run_id"]
    if ready.get("status") != "READY" or not run_dir.is_dir():
        raise RuntimeError(f"Published run in {layer_dir} is not usable: {ready}")
    return run_dir


def code_version():
    if os.getenv("GITHUB_SHA"):
        return os.getenv("GITHUB_SHA")
    try:
        git = ["git", "-C", str(ROOT)]
        sha = subprocess.run([*git, "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()
        dirty = subprocess.run([*git, "status", "--porcelain", "--untracked-files=no"],
                               capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    return f"{sha}-dirty" if dirty else sha
