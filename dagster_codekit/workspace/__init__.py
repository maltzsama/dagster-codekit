from dagster_codekit.workspace.base import WorkspaceManager
from dagster_codekit.workspace.file import FileWorkspaceManager


def _get_k8s_workspace_manager():
    from dagster_codekit.workspace.k8s import K8sWorkspaceManager
    return K8sWorkspaceManager


__all__ = ["WorkspaceManager", "FileWorkspaceManager"]

try:
    from dagster_codekit.workspace.k8s import K8sWorkspaceManager
    __all__.append("K8sWorkspaceManager")
except ImportError:
    pass
