"""
Kubernetes ConfigMap-based workspace manager.

Manages workspace via Kubernetes ConfigMap with optimistic locking.
"""

import asyncio
import io
import time
from typing import Optional

import structlog
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from ruamel.yaml import YAML

from dagster_codekit.exceptions import ConfigurationError, ValidationError

logger = structlog.get_logger()


class K8sWorkspaceManager:
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
        """
        Initialize Kubernetes workspace manager.

        Args:
            namespace: Kubernetes namespace
            configmap_name: ConfigMap name
            max_retries: Max retries on conflict (default 5)

        Raises:
            ConfigurationError: If cannot connect to Kubernetes
        """
        self.namespace = namespace
        self.configmap_name = configmap_name
        self.max_retries = max_retries

        # Load K8s config
        try:
            # Try in-cluster config first
            config.load_incluster_config()
        except:
            # Fall back to kubeconfig
            try:
                config.load_kube_config()
            except Exception as e:
                raise ConfigurationError(
                    f"Cannot load Kubernetes config: {e}\n"
                    f"Ensure running in K8s cluster or have valid kubeconfig."
                )

        self.k8s_api = client.CoreV1Api()
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.default_flow_style = False

        logger.info(
            "workspace_manager_initialized",
            mode="configmap",
            namespace=namespace,
            configmap=configmap_name,
        )

    def add_or_update(self, location_name: str, location_config: dict) -> None:
        """
        Update workspace in ConfigMap with optimistic locking.

        Retries on conflict (409) - handles concurrent updates.

        Args:
            location_name: Name of the code location
            location_config: Location configuration dict

        Raises:
            ConfigurationError: If ConfigMap not found or invalid
            ValidationError: If location_config is invalid
            Exception: If update fails after max retries
        """
        # Validate location_config
        self._validate_location_config(location_config)

        retry_count = 0

        while retry_count < self.max_retries:
            try:
                # 1. Read ConfigMap (captures resourceVersion)
                try:
                    cm = self.k8s_api.read_namespaced_config_map(
                        name=self.configmap_name, namespace=self.namespace
                    )
                except ApiException as e:
                    if e.status == 404:
                        raise ConfigurationError(
                            f"ConfigMap '{self.configmap_name}' not found in namespace '{self.namespace}'.\n\n"
                            f"Create it with:\n\n"
                            f"kubectl create configmap {self.configmap_name} \\\n"
                            f"  --from-literal=workspace.yaml='load_from: []' \\\n"
                            f"  -n {self.namespace}\n"
                        )
                    raise

                # Store current resourceVersion
                current_version = cm.metadata.resource_version

                logger.debug(
                    "k8s_configmap_read",
                    version=current_version,
                    attempt=retry_count + 1,
                )

                # 2. Validate ConfigMap has workspace.yaml
                if "workspace.yaml" not in cm.data:
                    raise ConfigurationError(
                        f"ConfigMap '{self.configmap_name}' exists but missing 'workspace.yaml' key.\n\n"
                        f"Add it with:\n\n"
                        f"kubectl patch configmap {self.configmap_name} -n {self.namespace} \\\n"
                        f'  --patch \'{{"data": {{"workspace.yaml": "load_from: []"}}}}\n'
                    )

                # 3. Parse YAML
                try:
                    workspace = self.yaml.load(cm.data["workspace.yaml"])
                except Exception as e:
                    raise ValidationError(
                        f"Invalid YAML in ConfigMap '{self.configmap_name}':\n"
                        f"{e}\n\n"
                        f"View with: kubectl get configmap {self.configmap_name} -n {self.namespace} -o yaml\n"
                    )

                if workspace is None or "load_from" not in workspace:
                    workspace = {"load_from": []}

                # 4. Find/update location
                existing_idx = self._find_location_index(workspace, location_name)

                if existing_idx is not None:
                    logger.info(
                        "k8s_location_updating",
                        name=location_name,
                        index=existing_idx,
                        total_locations=len(workspace["load_from"]),
                    )
                    workspace["load_from"][existing_idx] = location_config
                else:
                    logger.info(
                        "k8s_location_adding",
                        name=location_name,
                        new_total=len(workspace["load_from"]) + 1,
                    )
                    workspace["load_from"].append(location_config)

                # 5. Serialize workspace back to YAML
                stream = io.StringIO()
                self.yaml.dump(workspace, stream)
                cm.data["workspace.yaml"] = stream.getvalue()

                # 6. Update ConfigMap with resourceVersion check
                # K8s will reject if version changed (409 Conflict)
                cm.metadata.resource_version = current_version

                self.k8s_api.replace_namespaced_config_map(
                    name=self.configmap_name, namespace=self.namespace, body=cm
                )

                logger.info(
                    "k8s_configmap_updated",
                    location=location_name,
                    total_locations=len(workspace["load_from"]),
                    version=current_version,
                )

                # Success - exit retry loop
                return

            except ApiException as e:
                if e.status == 409:
                    # Conflict - ConfigMap was modified by another process
                    retry_count += 1

                    if retry_count >= self.max_retries:
                        raise Exception(
                            f"Failed to update ConfigMap after {self.max_retries} retries due to conflicts.\n"
                            f"This usually means multiple deployments are happening simultaneously.\n"
                            f"Location '{location_name}' may not be registered.\n"
                            f"Check workspace manually: kubectl get configmap {self.configmap_name} -n {self.namespace}"
                        )

                    # Exponential backoff
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
                else:
                    # Other K8s error - fail immediately
                    raise Exception(f"K8s API error: {e.status} {e.reason}")

            except Exception as e:
                logger.error("k8s_configmap_update_failed", error=str(e))
                raise

    def _find_location_index(self, workspace: dict, location_name: str) -> Optional[int]:
        """Find location index by name."""
        for i, loc in enumerate(workspace.get("load_from", [])):
            loc_name = (
                loc.get("grpc_server", {}).get("location_name")
                or loc.get("python_package", {}).get("location_name")
                or loc.get("python_file", {}).get("location_name")
            )

            if loc_name == location_name:
                return i

        return None

    def _validate_location_config(self, location_config: dict) -> None:
        """Validate location config structure."""
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
