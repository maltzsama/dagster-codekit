"""Tests for core models."""

import pytest
from dagster_codekit.models import DeploymentEvent, ValidationResult


def test_deployment_event_valid():
    """Test creating valid DeploymentEvent."""
    event = DeploymentEvent(
        location_name="analytics", grpc_host="analytics.dagster.svc", grpc_port=4000
    )

    assert event.location_name == "analytics"
    assert event.grpc_host == "analytics.dagster.svc"
    assert event.grpc_port == 4000
    assert event.deployment_type == "grpc-service"


def test_deployment_event_with_optionals():
    """Test DeploymentEvent with optional fields."""
    event = DeploymentEvent(
        location_name="ml-models",
        grpc_host="ml-models.svc",
        grpc_port=4000,
        commit_hash="abc123",
        image="registry.io/ml-models:abc123",
        namespace="dagster",
        metadata={"argocd_app": "ml-models-dagster"},
    )

    assert event.commit_hash == "abc123"
    assert event.image == "registry.io/ml-models:abc123"
    assert event.namespace == "dagster"
    assert event.metadata["argocd_app"] == "ml-models-dagster"


def test_deployment_event_empty_location_name():
    """Test that empty location_name raises error."""
    with pytest.raises(ValueError, match="location_name cannot be empty"):
        DeploymentEvent(location_name="", grpc_host="analytics.svc", grpc_port=4000)


def test_deployment_event_none_location_name():
    """Test that None location_name is caught."""
    event = DeploymentEvent(location_name="test", grpc_host="test.svc", grpc_port=4000)
    # Manually set to empty and re-validate
    event.location_name = ""
    with pytest.raises(ValueError, match="location_name cannot be empty"):
        event.__post_init__()


def test_deployment_event_empty_grpc_host():
    """Test that empty grpc_host raises error."""
    with pytest.raises(ValueError, match="grpc_host cannot be empty"):
        DeploymentEvent(location_name="analytics", grpc_host="", grpc_port=4000)


def test_deployment_event_none_grpc_host():
    """Test that None grpc_host is caught."""
    event = DeploymentEvent(location_name="test", grpc_host="test.svc", grpc_port=4000)
    # Manually set to empty and re-validate
    event.grpc_host = ""
    with pytest.raises(ValueError, match="grpc_host cannot be empty"):
        event.__post_init__()


def test_deployment_event_invalid_port_zero():
    """Test that port 0 raises error."""
    with pytest.raises(ValueError, match="Invalid grpc_port: 0"):
        DeploymentEvent(location_name="analytics", grpc_host="analytics.svc", grpc_port=0)


def test_deployment_event_invalid_port_negative():
    """Test that negative port raises error."""
    with pytest.raises(ValueError, match="Invalid grpc_port: -1"):
        DeploymentEvent(location_name="analytics", grpc_host="analytics.svc", grpc_port=-1)


def test_deployment_event_invalid_port_too_high():
    """Test that port > 65535 raises error."""
    with pytest.raises(ValueError, match="Invalid grpc_port: 99999"):
        DeploymentEvent(location_name="analytics", grpc_host="analytics.svc", grpc_port=99999)


def test_deployment_event_port_boundary_values():
    """Test valid boundary port values."""
    # Port 1 (minimum valid)
    event = DeploymentEvent(location_name="test", grpc_host="test.svc", grpc_port=1)
    assert event.grpc_port == 1

    # Port 65535 (maximum valid)
    event = DeploymentEvent(location_name="test", grpc_host="test.svc", grpc_port=65535)
    assert event.grpc_port == 65535

    # Port 4000 (typical)
    event = DeploymentEvent(location_name="test", grpc_host="test.svc", grpc_port=4000)
    assert event.grpc_port == 4000


def test_validation_result():
    """Test ValidationResult model."""
    result = ValidationResult(success=True, message="All checks passed")
    assert result.success is True
    assert result.message == "All checks passed"

    result = ValidationResult(success=False, message="Missing assets")
    assert result.success is False
    assert result.message == "Missing assets"


def test_validation_result_empty_message():
    """Test ValidationResult accepts empty message."""
    result = ValidationResult(success=True, message="")
    assert result.success is True
    assert result.message == ""
