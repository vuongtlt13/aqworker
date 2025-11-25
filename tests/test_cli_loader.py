from pathlib import Path

import pytest

from aqworker.cli.loader import get_aqworker_file, load_aqworker_from_file
from aqworker.core import AQWorker

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_WORKER_FILE = PROJECT_ROOT / "tests" / "examples" / "worker_app.py"
NON_WORKER_FILE = PROJECT_ROOT / "examples" / "simple_fastapi" / "client.py"
NON_PY_FILE = PROJECT_ROOT / "README.md"


def test_load_aqworker_from_absolute_path():
    instance = load_aqworker_from_file(str(EXAMPLE_WORKER_FILE))
    assert isinstance(instance, AQWorker)


def test_load_aqworker_from_relative_path(monkeypatch):
    monkeypatch.chdir(EXAMPLE_WORKER_FILE.parent)
    instance = load_aqworker_from_file(EXAMPLE_WORKER_FILE.name)
    assert isinstance(instance, AQWorker)


def test_load_aqworker_raises_when_missing():
    missing_file = PROJECT_ROOT / "examples" / "missing_worker.py"
    with pytest.raises(FileNotFoundError):
        load_aqworker_from_file(str(missing_file))


def test_load_aqworker_requires_python_extension():
    with pytest.raises(ValueError):
        load_aqworker_from_file(str(NON_PY_FILE))


def test_load_aqworker_errors_when_no_instance():
    with pytest.raises(ValueError):
        load_aqworker_from_file(str(NON_WORKER_FILE))


def test_get_aqworker_file_reads_environment(monkeypatch):
    monkeypatch.setenv("AQWORKER_FILE", "/tmp/worker.py")
    assert get_aqworker_file() == "/tmp/worker.py"
