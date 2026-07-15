"""Kubernetes launcher integration tests.

Requires a real K8s cluster with the codekit proxy and RBAC already deployed.
The CI workflow deploys these via ./examples/kubernetes/manifests.yaml.
"""

import os
import time

import grpc
import pytest
from dagster._grpc.__generated__ import dagster_api_pb2 as api_pb2
from dagster._grpc.__generated__ import dagster_api_pb2_grpc as api_pb2_grpc
from dagster._grpc.types import (
    CancelExecutionRequest,
    CancelExecutionResult,
    ExecuteExternalJobArgs,
    JobPythonOrigin,
    RemoteJobOrigin,
    RemoteRepositoryOrigin,
    StartRunResult,
)
from dagster._serdes import deserialize_value, serialize_value

CODEKIT_HOST = os.environ.get("CODEKIT_HOST", "localhost")
CODEKIT_GRPC_PORT = int(os.environ.get("CODEKIT_GRPC_PORT", "4000"))
CODEKIT_NAMESPACE = os.environ.get("CODEKIT_NAMESPACE", "dagster")


@pytest.fixture(scope="module")
def grpc_stub():
    channel = grpc.insecure_channel(f"{CODEKIT_HOST}:{CODEKIT_GRPC_PORT}")
    stub = api_pb2_grpc.DagsterApiStub(channel)
    yield stub
    channel.close()


def _make_run_args(location_name: str, job_name: str, run_id: str) -> ExecuteExternalJobArgs:
    clo = {
        "__class__": "CodeLocationOrigin",
        "location_name": location_name,
    }
    rro = RemoteRepositoryOrigin(code_location_origin=clo, repository_name="")
    jpo = JobPythonOrigin(job_name=job_name, repository_origin=rro)
    rjo = RemoteJobOrigin(repository_origin=rro, job_name=job_name)
    return ExecuteExternalJobArgs(job_origin=rjo, run_id=run_id, instance_ref=None)


@pytest.mark.skipif(
    not os.environ.get("CODEKIT_HOST"),
    reason="K8s tests require a real cluster (set CODEKIT_HOST to enable)",
)
class TestK8sLauncher:
    """Tests that require a real K8s cluster with codekit proxy deployed."""

    def test_health(self, grpc_stub):
        response = grpc_stub.Ping(api_pb2.PingRequest(echo="ping"))
        assert response.echo == "ping"

    def test_start_run_creates_job(self, grpc_stub):
        run_id = f"e2e-test-{int(time.time())}"

        args = _make_run_args("e2e-test-location", "test-job", run_id)
        request = api_pb2.StartRunRequest(
            serialized_execute_run_args=serialize_value(args),
        )
        response = grpc_stub.StartRun(request)
        result = deserialize_value(
            response.serialized_start_run_result, StartRunResult
        )
        assert result.success, f"StartRun failed: {result.message}"

        time.sleep(5)

        from kubernetes import client, config
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()

        batch_api = client.BatchV1Api()
        job_name = f"dagster-run-{run_id}"

        job = batch_api.read_namespaced_job(name=job_name, namespace=CODEKIT_NAMESPACE)
        assert job is not None
        assert job.metadata.labels.get("dagster/run-id") == run_id

    def test_cancel_deletes_job(self, grpc_stub):
        run_id = f"e2e-cancel-{int(time.time())}"

        args = _make_run_args("e2e-test-location", "test-job", run_id)
        request = api_pb2.StartRunRequest(
            serialized_execute_run_args=serialize_value(args),
        )
        grpc_stub.StartRun(request)

        time.sleep(3)

        cancel_args = CancelExecutionRequest(run_id=run_id)
        cancel_request = api_pb2.CancelExecutionRequest(
            serialized_cancel_execution_request=serialize_value(cancel_args),
        )
        cancel_response = grpc_stub.CancelExecution(cancel_request)
        cancel_result = deserialize_value(
            cancel_response.serialized_cancel_execution_result, CancelExecutionResult
        )
        assert cancel_result.success, f"Cancel failed: {cancel_result.message}"

        time.sleep(5)

        from kubernetes import client, config
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()

        batch_api = client.BatchV1Api()
        job_name = f"dagster-run-{run_id}"

        import kubernetes.client.rest as k8s_rest
        with pytest.raises(k8s_rest.ApiException) as exc:
            batch_api.read_namespaced_job(name=job_name, namespace=CODEKIT_NAMESPACE)
        assert exc.value.status == 404, "Job should be deleted after cancel"
