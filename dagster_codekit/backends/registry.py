"""Backend plugin registry.

Discovers and instantiates CI/CD backend plugins from configuration.
"""

from dagster_codekit.config import BackendsConfig
from dagster_codekit.core.interfaces import BackendPlugin
from dagster_codekit.core.exceptions import ConfigurationError


class BackendRegistry:
    def __init__(self, config: BackendsConfig):
        self._backends: dict[str, BackendPlugin] = {}
        self._init_backends(config)

    def _init_backends(self, config: BackendsConfig) -> None:
        if config.argocd.enabled:
            from dagster_codekit.backends.argocd import ArgoCDBackend
            self._backends["argocd"] = ArgoCDBackend(config.argocd)

        if hasattr(config, "forgejo") and config.forgejo.enabled:
            from dagster_codekit.backends.forgejo import ForgejoBackend
            self._backends["forgejo"] = ForgejoBackend(config.forgejo)

        if hasattr(config, "azure_devops") and config.azure_devops.enabled:
            from dagster_codekit.backends.azure_devops import AzureDevOpsBackend
            self._backends["azure_devops"] = AzureDevOpsBackend(config.azure_devops)

    def get(self, name: str) -> BackendPlugin | None:
        return self._backends.get(name)

    @property
    def names(self) -> list[str]:
        return list(self._backends.keys())
