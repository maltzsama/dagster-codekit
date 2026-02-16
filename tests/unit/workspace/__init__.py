"""
Workspace management for dagster-codekit.

Provides managers for updating Dagster workspace configuration.
"""

from dagster_codekit.workspace.file import FileWorkspaceManager

# Lazy import K8s - only if kubernetes package installed
_K8sWorkspaceManager = None

try:
    from dagster_codekit.workspace.k8s import K8sWorkspaceManager as _K8sWorkspaceManager
except ImportError:
    pass

# Export what's available
if _K8sWorkspaceManager is not None:
    K8sWorkspaceManager = _K8sWorkspaceManager
    __all__ = ["FileWorkspaceManager", "K8sWorkspaceManager"]
else:
    __all__ = ["FileWorkspaceManager"]
