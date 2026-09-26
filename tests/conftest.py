import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import monitoring  # noqa: E402
import validation  # noqa: E402
from data_ingestion import build_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = build_spark()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def platform(tmp_path, monkeypatch):
    """Point the quarantine and monitoring tables at a temporary directory."""
    monkeypatch.setattr(validation, "QUARANTINE_DIR", str(tmp_path / "quarantine"))
    monkeypatch.setattr(monitoring, "MONITORING_DIR", str(tmp_path / "monitoring"))
    monitoring.start_run("test")
    return tmp_path
