"""Tests for gRPC health check utility."""

from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from dagster_codekit.core.exceptions import TimeoutError
from dagster_codekit.utils.health import wait_for_grpc_server


@pytest.mark.asyncio
async def test_returns_true_when_serving():
    mock_stub = AsyncMock()
    mock_response = MagicMock()
    mock_response.status = 1  # SERVING
    mock_stub.Check.return_value = mock_response

    with (
        patch("grpc.aio.insecure_channel") as mock_channel,
        patch("grpc_health.v1.health_pb2_grpc.HealthStub", return_value=mock_stub),
    ):
        mock_channel.return_value.__aenter__.return_value = None
        result = await wait_for_grpc_server("localhost", 4000, timeout=5, check_interval=0.1)
        assert result is True


@pytest.mark.asyncio
async def test_times_out_when_never_serving():
    mock_stub = AsyncMock()
    mock_response = MagicMock()
    mock_response.status = 0  # UNKNOWN - never becomes SERVING
    mock_stub.Check.return_value = mock_response

    with (
        patch("grpc.aio.insecure_channel") as mock_channel,
        patch("grpc_health.v1.health_pb2_grpc.HealthStub", return_value=mock_stub),
    ):
        mock_channel.return_value.__aenter__.return_value = None

        with pytest.raises(TimeoutError, match="did not become ready"):
            await wait_for_grpc_server("localhost", 4000, timeout=0.2, check_interval=0.1)


@pytest.mark.asyncio
async def test_returns_false_on_unimplemented():
    mock_stub = AsyncMock()
    mock_stub.Check.side_effect = grpc.RpcError()
    mock_stub.Check.side_effect._code = grpc.StatusCode.UNIMPLEMENTED

    def fake_code():
        return grpc.StatusCode.UNIMPLEMENTED

    mock_stub.Check.side_effect.code = fake_code

    with (
        patch("grpc.aio.insecure_channel") as mock_channel,
        patch("grpc_health.v1.health_pb2_grpc.HealthStub", return_value=mock_stub),
    ):
        mock_channel.return_value.__aenter__.return_value = None
        result = await wait_for_grpc_server("localhost", 4000, timeout=5, check_interval=0.1)
        assert result is False
