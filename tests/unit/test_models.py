"""Tests for API schemas."""

import pytest

from dagster_codekit.api.schemas import DeploymentEvent, ValidationResult


class TestDeploymentEvent:
    def test_valid(self):
        event = DeploymentEvent(location_name="analytics", image_tag="img:v1")
        assert event.location_name == "analytics"
        assert event.image_tag == "img:v1"
        assert event.snapshot_json is None
        assert event.commit_hash is None

    def test_with_snapshot(self):
        event = DeploymentEvent(
            location_name="analytics",
            image_tag="img:v1",
            snapshot_json='{"foo": "bar"}',
            commit_hash="abc123",
        )
        assert event.snapshot_json == '{"foo": "bar"}'
        assert event.commit_hash == "abc123"

    def test_with_k8s_config(self):
        event = DeploymentEvent(
            location_name="analytics",
            image_tag="img:v1",
            k8s_config={"service_account": "gpu-sa"},
        )
        assert event.k8s_config == {"service_account": "gpu-sa"}

    def test_default_k8s_config(self):
        event = DeploymentEvent(location_name="analytics", image_tag="img:v1")
        assert event.k8s_config == {}

    def test_default_metadata(self):
        event = DeploymentEvent(location_name="analytics", image_tag="img:v1")
        assert event.metadata == {}

    def test_empty_location_name(self):
        with pytest.raises(ValueError, match="location_name is required"):
            DeploymentEvent(location_name="", image_tag="img:v1")


    def test_commit_hash_persisted(self):
        """Regression: commit_hash must survive _register_deployment."""
        import os
        import tempfile

        from dagster_codekit.api.app import _register_deployment
        from dagster_codekit.db.models import Snapshot, db_session, init_db

        db_path = tempfile.mktemp(suffix=".db")
        db_url = f"sqlite:///{db_path}"

        try:
            init_db(db_url)

            event = DeploymentEvent(
                location_name="regression-test",
                image_tag="img:v1",
                snapshot_json='{"test": true}',
                commit_hash="abc123def456",
            )
            _register_deployment(event)

            with db_session():
                snap = Snapshot.select().order_by(Snapshot.created_at.desc()).first()
                assert snap is not None
                assert snap.commit_hash == "abc123def456", (
                    f"Expected commit_hash='abc123def456', got {snap.commit_hash!r}"
                )
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass


class TestValidationResult:
    def test_success(self):
        result = ValidationResult(success=True, message="All checks passed")
        assert result.success is True
        assert result.message == "All checks passed"

    def test_failure(self):
        result = ValidationResult(success=False, message="Missing assets")
        assert result.success is False
        assert result.message == "Missing assets"

    def test_empty_message(self):
        result = ValidationResult(success=True, message="")
        assert result.success is True
        assert result.message == ""
