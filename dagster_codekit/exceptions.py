"""
Custom exceptions for dagster-codekit.

All exceptions inherit from CodekitError for easy catching.
"""


class CodekitError(Exception):
    """Base exception for all dagster-codekit errors."""

    pass


class ConfigurationError(CodekitError):
    """
    Raised when configuration is invalid or incomplete.

    Examples:
    - Missing required fields in config.yaml
    - Backend enabled but not configured
    - Invalid repository format
    """

    pass


class ValidationError(CodekitError):
    """
    Raised when runtime validation fails.

    Examples:
    - Invalid webhook payload
    - Repository not in config
    - Missing required labels (ArgoCD)
    """

    pass


class AuthenticationError(CodekitError):
    """
    Raised when authentication fails.

    Examples:
    - Invalid webhook signature (HMAC)
    - Dagster webserver rejected reload (401/403)
    """

    pass


class TimeoutError(CodekitError):
    """
    Raised when operation times out.

    Examples:
    - gRPC server never became healthy
    - Dagster reload timeout
    """

    pass


class ConnectionError(CodekitError):
    """
    Raised when cannot connect to external service.

    Examples:
    - Cannot reach Dagster webserver
    - Cannot reach gRPC server
    """

    pass
