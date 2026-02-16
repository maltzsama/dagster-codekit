"""
Health check utilities.

Handles gRPC Health Checking Protocol (service mesh standard).
"""

import asyncio
import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc
from structlog.stdlib import BoundLogger

from dagster_codekit.exceptions import TimeoutError


async def wait_for_grpc_server(
    host: str, port: int, timeout: int, location_name: str, logger: BoundLogger
) -> bool:
    """
    Polls a gRPC server until it reports SERVING or timeout expires.

    Args:
        host: Hostname or IP
        port: Port number
        timeout: Max seconds to wait
        location_name: Context for logging
        logger: Structlog logger instance

    Returns:
        True if healthy

    Raises:
        TimeoutError: If timeout reached
    """
    target = f"{host}:{port}"
    deadline = asyncio.get_event_loop().time() + timeout
    attempt = 0

    logger.info("grpc_health_check_start", target=target, timeout=timeout, location=location_name)

    while asyncio.get_event_loop().time() < deadline:
        attempt += 1
        try:
            # Create async channel
            async with grpc.aio.insecure_channel(target) as channel:
                stub = health_pb2_grpc.HealthStub(channel)

                # Call Check()
                response = await stub.Check(
                    health_pb2.HealthCheckRequest(service=""),
                    timeout=2,  # Short timeout for individual check
                )

                if response.status == health_pb2.HealthCheckResponse.SERVING:
                    logger.info(
                        "grpc_health_check_passed", location=location_name, attempts=attempt
                    )
                    return True
                else:
                    logger.debug("grpc_not_serving", status=response.status, location=location_name)

        except grpc.RpcError as e:
            # UNAVAILABLE is normal during startup (pod not ready)
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                pass
            elif e.code() == grpc.StatusCode.UNIMPLEMENTED:
                logger.error(
                    "grpc_health_protocol_missing",
                    help="Server does not implement grpc.health.v1.Health",
                    location=location_name,
                )
                return False
            else:
                logger.debug("grpc_check_error", code=e.code(), location=location_name)
        except Exception as e:
            logger.warning("grpc_check_exception", error=str(e))

        # Wait before retry
        await asyncio.sleep(2)

    # If we got here, we timed out
    raise TimeoutError(f"gRPC server {target} did not become ready within {timeout}s")
