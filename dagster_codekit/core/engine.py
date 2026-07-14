import os
import importlib.util

import structlog

from dagster_codekit.core.dagster_facade import Definitions, RepositorySnap, serialize_value
from dagster_codekit.api.schemas import DeploymentEvent

logger = structlog.get_logger(__name__)


def extract_repository_data(file_path: str, location_name: str) -> str:
    """
    Load the Python file, extract the RepositorySnap, and serialize to JSON
    using Dagster's native protocol (1.12+).
    """
    logger.info("extracting repository snap", file=file_path, location=location_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Python file not found: {file_path}")

    try:
        # 1. Module Loading (Standard Python, no tricks)
        spec = importlib.util.spec_from_file_location("dagster_target_module", file_path)
        if not spec or not spec.loader:
            raise ImportError(f"Could not load spec from {file_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # 2. Find the Definitions object
        defs = next((obj for obj in vars(module).values() if isinstance(obj, Definitions)), None)

        if not defs:
            raise ValueError(f"No 'Definitions' object found in {file_path}")

        # 3. Transform into RepositoryDefinition
        repository_def = defs.get_repository_def()

        # 4. Generate the Snapshot (The Clever Bit)
        # We use defer_snapshots=False to ensure the payload is complete (jobs, sensors, assets)
        # Signature confirmed in output.md: classmethod from_def(cls, repository_def, defer_snapshots=False)
        repository_snap = RepositorySnap.from_def(repository_def, defer_snapshots=False)

        # 5. Serialize (The Contract)
        # Transforms the RepositorySnap object into the "ExternalRepositoryData" JSON string
        # Signature confirmed: serialize_value(val)
        return serialize_value(repository_snap)

    except Exception as e:
        logger.error("failed to extract metadata", error=str(e))
        raise


def create_snapshot_payload(location_name: str, file_path: str, image_tag: str) -> DeploymentEvent:
    """Assembles the final payload for the Codekit API."""
    snapshot_json = extract_repository_data(file_path, location_name)

    return DeploymentEvent(
        location_name=location_name,
        image_tag=image_tag,
        snapshot_json=snapshot_json,
        # Attempts to get metadata from CI environment, falls back to null if not available
        commit_hash=os.getenv("GITHUB_SHA") or os.getenv("CI_COMMIT_SHA"),
    )
