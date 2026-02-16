"""
Built-in backend implementations.

Backends convert CI/CD webhooks into DeploymentEvents.
"""

from dagster_codekit.backends.argocd import ArgoCDBackend

__all__ = ["ArgoCDBackend"]
