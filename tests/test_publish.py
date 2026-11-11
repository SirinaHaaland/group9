import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from publish import current_run, publish_run


def make_run(layer, run_id, age, finished=True):
    run_dir = layer / "runs" / run_id
    run_dir.mkdir(parents=True)
    if finished:
        (run_dir / "report.json").write_text("{}")
    os.utime(run_dir, (age, age))
    return run_dir


def test_nothing_published(tmp_path):
    with pytest.raises(RuntimeError, match="Nothing published"):
        current_run(tmp_path)


def test_publish_updates_pointer(tmp_path):
    first = make_run(tmp_path, "run-1", 1)
    publish_run(tmp_path, "run-1", "report.json")
    assert current_run(tmp_path) == first

    make_run(tmp_path, "run-2", 2)
    assert current_run(tmp_path) == first
    assert json.loads((tmp_path / "_READY.json").read_text())["run_id"] == "run-1"


def test_unfinished_run_not_published(tmp_path):
    make_run(tmp_path, "run-1", 1, finished=False)

    with pytest.raises(RuntimeError, match="not publishing"):
        publish_run(tmp_path, "run-1", "report.json")

    assert not (tmp_path / "_READY.json").exists()
