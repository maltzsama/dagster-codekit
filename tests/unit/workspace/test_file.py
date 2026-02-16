import tempfile
from pathlib import Path
import pytest
import yaml
from dagster_codekit.workspace.file import FileWorkspaceManager


@pytest.fixture
def workspace_file():
    """Create temporary workspace.yaml file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("load_from: []\n")
        f.flush()
        yield f.name

    # Cleanup
    Path(f.name).unlink(missing_ok=True)
    Path(f.name).with_suffix(".tmp").unlink(missing_ok=True)


def test_file_manager_init(workspace_file):
    manager = FileWorkspaceManager(workspace_file)
    assert manager.path == Path(workspace_file)


def test_file_manager_add_new_location(workspace_file):
    manager = FileWorkspaceManager(workspace_file)
    location_config = {
        "grpc_server": {
            "host": "analytics.svc",
            "port": 4000,
            "location_name": "analytics",
        }
    }
    manager.add_or_update("analytics", location_config)

    with open(workspace_file) as f:
        workspace = yaml.safe_load(f)

    assert len(workspace["load_from"]) == 1
    assert workspace["load_from"][0]["grpc_server"]["location_name"] == "analytics"


def test_file_manager_update_existing_location(workspace_file):
    manager = FileWorkspaceManager(workspace_file)

    # Add initial
    config_v1 = {"grpc_server": {"host": "v1.svc", "port": 4000, "location_name": "analytics"}}
    manager.add_or_update("analytics", config_v1)

    # Update
    config_v2 = {"grpc_server": {"host": "v2.svc", "port": 4000, "location_name": "analytics"}}
    manager.add_or_update("analytics", config_v2)

    with open(workspace_file) as f:
        workspace = yaml.safe_load(f)

    assert len(workspace["load_from"]) == 1
    assert workspace["load_from"][0]["grpc_server"]["host"] == "v2.svc"
