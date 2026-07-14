from dagster_codekit.workspace.base import WorkspaceManager
from dagster_codekit.workspace.file import FileWorkspaceManager

__all__ = ["WorkspaceManager", "FileWorkspaceManager"]

try:
    from dagster_codekit.workspace.k8s import K8sWorkspaceManager  # noqa: F401

    __all__.append("K8sWorkspaceManager")
except ImportError:
    pass
