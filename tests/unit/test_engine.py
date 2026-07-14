"""Tests for the snapshot extraction engine."""

import tempfile
from pathlib import Path

import pytest

from dagster_codekit.core.engine import create_snapshot_payload, extract_repository_data
from dagster_codekit import DeploymentEvent


@pytest.fixture
def fixture_definitions():
    """Create a temporary Dagster definitions file."""
    code = """
from dagster import Definitions, job, op

@op
def hello():
    return "world"

@job
def my_job():
    hello()

defs = Definitions(jobs=[my_job])
"""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False, prefix="defs_"
    ) as f:
        f.write(code)

    yield f.name
    Path(f.name).unlink()


def test_extract_repository_data(fixture_definitions):
    """Snapshot extraction should succeed for a valid definitions file."""
    result = extract_repository_data(fixture_definitions, "test-location")
    assert result is not None
    assert len(result) > 100  # Non-trivial JSON


def test_create_snapshot_payload(fixture_definitions):
    """Snapshot payload should include location name and image tag."""
    payload = create_snapshot_payload("test-location", fixture_definitions, "image:v1")
    assert isinstance(payload, DeploymentEvent)
    assert payload.location_name == "test-location"
    assert payload.image_tag == "image:v1"
    assert payload.snapshot_json is not None


def test_extract_missing_file():
    """Extraction should raise FileNotFoundError for missing files."""
    with pytest.raises(FileNotFoundError):
        extract_repository_data("/nonexistent/file.py", "test")


def test_extract_empty_file():
    """Extraction should raise for files without Definitions."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write("# empty file")
    try:
        with pytest.raises(ValueError, match="Definitions"):
            extract_repository_data(f.name, "test")
    finally:
        Path(f.name).unlink()
