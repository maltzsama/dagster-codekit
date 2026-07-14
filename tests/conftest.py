import pytest
from dagster_codekit.config import Config, ServerConfig


@pytest.fixture
def basic_config():
    return Config(
        server=ServerConfig(),
    )
