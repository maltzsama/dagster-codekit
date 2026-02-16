"""
Kubernetes ConfigMap-based workspace manager.
Manages workspace via Kubernetes ConfigMap with optimistic locking.
"""

import io
import time
from typing import Optional, Any

import structlog
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from ruamel.yaml import YAML

from dagster_codekit.workspace.base import WorkspaceManager
from dagster_codekit.core.exceptions import ConfigurationError, ValidationError

logger = structlog.get_logger()


class K8sWorkspaceManager(WorkspaceManager):
    """
    Manage workspace via Kubernetes ConfigMap.

    Features:
    - Optimistic locking (resourceVersion)
    - Automatic retry on conflicts
    - Preserves YAML formatting
    - No shared volume required

    Usage:
        manager = K8sWorkspaceManager("dagster", "dagster-workspace")
        manager.add_or_update("analytics", {
            "grpc_server": {
                "host": "analytics.svc",
                "port": 4000,
                "location_name": "analytics"
            }
        })
    """

    def __init__(self, namespace: str, configmap_name: str, max_retries: int = 5):
        self.namespace = namespace
        self.configmap_name = configmap_name
        self.max_retries = max_retries
        self._initialized = False
        self.k8s_api = None

        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.default_flow_style = False

    def _load_config(self) -> None:
        """Load K8s config from cluster or local kubeconfig."""
        try:
            config.load_incluster_config()
        except Exception:
            try:
                config.load_kube_config()
            except Exception as e:
                raise ConfigurationError(
                    f"Cannot load Kubernetes config: {e}\n"
                    f"Ensure running in K8s cluster or have valid kubeconfig."
                )

        self.k8s_api = client.CoreV1Api()
        self._initialized = True

        logger.info(
            "workspace_manager_initialized",
            mode="configmap",
            namespace=self.namespace,
            configmap=self.configmap_name,
        )

    async def validate(self) -> None:
        """Pre-flight check to verify ConfigMap accessibility."""
        if not self._initialized:
            self._load_config()

        logger.debug(
            "k8s_validation_started", configmap=self.configmap_name, namespace=self.namespace
        )

        try:
            self.k8s_api.read_namespaced_config_map(
                name=self.configmap_name, namespace=self.namespace
            )
            logger.info("k8s_validation_success", configmap=self.configmap_name)
        except ApiException as e:
            if e.status == 404:
                raise ConfigurationError(
                    f"ConfigMap '{self.configmap_name}' not found in namespace '{self.namespace}'.\n\n"
                    f"Create it with:\n\n"
                    f"kubectl create configmap {self.configmap_name} \\\n"
                    f"  --from-literal=workspace.yaml='load_from: []' \\\n"
                    f"  -n {self.namespace}"
                )
            raise ConfigurationError(f"Kubernetes API Error: {e.status} {e.reason}")

    def add_or_update(self, location_name: str, location_config: dict[str, Any]) -> None:
        """Update workspace in ConfigMap with optimistic locking and original retry logic."""
        if not self._initialized:
            self._load_config()

        self._validate_location_config(location_config)
        retry_count = 0

        while retry_count < self.max_retries:
            try:
                # 1. Read ConfigMap (captures resourceVersion)
                cm = self.k8s_api.read_namespaced_config_map(
                    name=self.configmap_name, namespace=self.namespace
                )
                current_version = cm.metadata.resource_version

                logger.debug("k8s_configmap_read", version=current_version, attempt=retry_count + 1)

                # 2. Validate ConfigMap has workspace.yaml
                if "workspace.yaml" not in cm.data:
                    raise ConfigurationError(
                        f"ConfigMap '{self.configmap_name}' missing 'workspace.yaml' key.\n\n"
                        f"Add it with:\n\n"
                        f"kubectl patch configmap {self.configmap_name} -n {self.namespace} \\\n"
                        f'  --patch \'{{"data": {{"workspace.yaml": "load_from: []"}}}}\n'
                    )

                # 3. Parse YAML
                try:
                    workspace = self.yaml.load(cm.data["workspace.yaml"])
                except Exception as e:
                    raise ValidationError(f"Invalid YAML in ConfigMap '{self.configmap_name}': {e}")

                workspace = workspace or {"load_from": []}

                # 4. Find/update location
                existing_idx = self._find_location_index(workspace, location_name)

                if existing_idx is not None:
                    workspace["load_from"][existing_idx] = location_config
                else:
                    workspace["load_from"].append(location_config)

                # 5. Serialize
                stream = io.StringIO()
                self.yaml.dump(workspace, stream)
                cm.data["workspace.yaml"] = stream.getvalue()

                # 6. Replace with optimistic lock check
                cm.metadata.resource_version = current_version
                self.k8s_api.replace_namespaced_config_map(
                    name=self.configmap_name, namespace=self.namespace, body=cm
                )

                logger.info("k8s_configmap_updated", location=location_name)
                return

            except ApiException as e:
                if e.status == 404:
                    # Re-raise for clarity if deleted mid-flight
                    raise ConfigurationError(f"ConfigMap {self.configmap_name} was deleted.")

                if e.status == 409:  # Conflict
                    retry_count += 1
                    if retry_count >= self.max_retries:
                        raise ConfigurationError(
                            f"Conflict updating ConfigMap after {self.max_retries} retries."
                        )

                    backoff = 0.1 * (2**retry_count)
                    logger.warning(
                        "k8s_configmap_conflict",
                        location=location_name,
                        attempt=retry_count,
                        max_retries=self.max_retries,
                        backoff=backoff,
                        help="Another deployment is updating workspace simultaneously, retrying...",
                    )
                    time.sleep(backoff)
                    continue

                raise ConfigurationError(f"K8s API error: {e.status} {e.reason}")

    def _find_location_index(self, workspace: dict, location_name: str) -> Optional[int]:
        """Find location index by name, checking all Dagster source types."""
        for i, loc in enumerate(workspace.get("load_from", [])):
            loc_data = loc.get("grpc_server") or loc.get("python_package") or loc.get("python_file")
            if loc_data and loc_data.get("location_name") == location_name:
                return i
        return None

    def _validate_location_config(self, location_config: dict) -> None:
        """Validate location config structure exactly as original."""
        if not isinstance(location_config, dict):
            raise ValidationError(f"location_config must be a dict, got {type(location_config)}")

        if (
            "grpc_server" not in location_config
            and "python_package" not in location_config
            and "python_file" not in location_config
        ):
            raise ValidationError(
                f"Invalid location_config: must contain either 'grpc_server', "
                f"'python_package', or 'python_file'.\nGot: {location_config}"
            )
