"""Tests for custom exceptions."""

import pytest
from dagster_codekit.exceptions import (
    CodekitError,
    ConfigurationError,
    ValidationError,
    AuthenticationError,
    TimeoutError,
    ConnectionError,
)


def test_exceptions_hierarchy():
    """Test that all exceptions inherit from CodekitError."""
    assert issubclass(ConfigurationError, CodekitError)
    assert issubclass(ValidationError, CodekitError)
    assert issubclass(AuthenticationError, CodekitError)
    assert issubclass(TimeoutError, CodekitError)
    assert issubclass(ConnectionError, CodekitError)


def test_exceptions_can_be_raised():
    """Test that exceptions can be raised with messages."""
    with pytest.raises(ConfigurationError, match="Missing config"):
        raise ConfigurationError("Missing config")

    with pytest.raises(ValidationError, match="Invalid payload"):
        raise ValidationError("Invalid payload")

    with pytest.raises(AuthenticationError, match="Invalid signature"):
        raise AuthenticationError("Invalid signature")


def test_catch_all_codekit_errors():
    """Test that CodekitError catches all custom exceptions."""
    with pytest.raises(CodekitError):
        raise ConfigurationError("test")

    with pytest.raises(CodekitError):
        raise ValidationError("test")

    with pytest.raises(CodekitError):
        raise TimeoutError("test")
