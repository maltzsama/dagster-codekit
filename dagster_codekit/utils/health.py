import asyncio

import grpc
import structlog
from grpc_health.v1 import health_pb2, health_pb2_grpc

from dagster_codekit.exceptions import TimeoutError

logger = structlog.get_logger()


async def wait_for_grpc_server(
    host: str, port: int, timeout: int, use_tls: bool = False, check_interval: float = 2.0
) -> bool:
    """
    Polls a gRPC server until it reports SERVING or timeout expires.
    """
    target = f"{host}:{port}"
    deadline = asyncio.get_event_loop().time() + timeout
    attempt = 0

    logger.info("grpc_health_check_start", target=target, timeout=timeout, tls=use_tls)

    while asyncio.get_event_loop().time() < deadline:
        attempt += 1
        try:

            if use_tls:
                credentials = grpc.ssl_channel_credentials()
                channel = grpc.aio.secure_channel(target, credentials)
            else:
                channel = grpc.aio.insecure_channel(target)

            async with channel:
                stub = health_pb2_grpc.HealthStub(channel)

                # Call Check()
                response = await stub.Check(
                    health_pb2.HealthCheckRequest(service=""),
                    timeout=check_interval,
                )

                if response.status == health_pb2.HealthCheckResponse.SERVING:
                    logger.info("grpc_health_check_passed", attempts=attempt)
                    return True

                logger.debug("grpc_not_serving", status=response.status)

        except grpc.RpcError as e:
            code = e.code()

            if code == grpc.StatusCode.UNAVAILABLE:
                pass

            elif code == grpc.StatusCode.UNIMPLEMENTED:
                logger.error(
                    "grpc_health_protocol_missing",
                    help="Server does not implement grpc.health.v1.Health",
                )
                return False

            elif code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED):
                logger.error(
                    "grpc_access_denied",
                    code=code.name,
                    help="Verify TLS certificates, mTLS configuration or authentication tokens.",
                )
                return False

            elif "ssl" in str(e).lower() or "handshake" in str(e).lower():
                logger.error(
                    "grpc_tls_handshake_failed",
                    error=str(e),
                    help="TLS handshake failed. Verify CA certificates and grpc_tls configuration.",
                )
                return False

            else:
                logger.debug("grpc_check_retry", code=code.name, details=str(e))

        except Exception as e:

            logger.warning("grpc_check_exception", error=str(e))

        await asyncio.sleep(check_interval)

    raise TimeoutError(f"gRPC server {target} did not become ready within {timeout}s")
