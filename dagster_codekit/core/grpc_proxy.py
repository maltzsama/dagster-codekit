import math
import os
import subprocess
import sys
import threading
import time
from typing import Iterator, Optional, Any

import grpc
import structlog
from concurrent import futures

from dagster._grpc.__generated__ import (
    dagster_api_pb2 as api_pb2,
    dagster_api_pb2_grpc as api_pb2_grpc,
)
from dagster._serdes import deserialize_value, serialize_value
from dagster._utils.error import SerializableErrorInfo
from dagster._core.snap.execution_plan_snapshot import ExecutionPlanSnapshot, ExecutionStepSnap
from dagster._core.remote_representation.external_data import (
    RepositorySnap,
    StaticPartitionsSnap,
    TimeWindowPartitionsSnap,
    MultiPartitionsSnap,
    PartitionNamesSnap,
    PartitionExecutionErrorSnap,
)
from dagster._grpc.types import (
    ExecutionPlanSnapshotArgs,
    ExecuteExternalJobArgs,
    ExecuteRunArgs,
    ExternalJobArgs,
    StartRunResult,
    ListRepositoriesResponse,
    LoadableRepositorySymbol,
    CancelExecutionRequest,
    CancelExecutionResult,
    CanCancelExecutionRequest,
    CanCancelExecutionResult,
    GetCurrentRunsResult,
    ShutdownServerResult,
    PartitionNamesArgs,
)

from dagster_codekit.db.models import CodeLocation, Snapshot, db_session

logger = structlog.get_logger(__name__)

STREAMING_CHUNK_SIZE = 4000000


def _chunked_stream(serialized_data: str, event_cls, attr_name: str) -> Iterator[Any]:
    num_chunks = math.ceil(float(len(serialized_data)) / STREAMING_CHUNK_SIZE)
    for i in range(num_chunks):
        start_index = i * STREAMING_CHUNK_SIZE
        end_index = min((i + 1) * STREAMING_CHUNK_SIZE, len(serialized_data))
        yield event_cls(
            sequence_number=i,
            **{attr_name: serialized_data[start_index:end_index]},
        )


class CodekitProxyServicer(api_pb2_grpc.DagsterApiServicer):
    """gRPC proxy that impersonates a Dagster User Code Server.

    Serves static metadata from the database and delegates execution
    to ephemeral Kubernetes jobs.
    """

    def __init__(self, launcher_mode: str = "k8s",
                 k8s_namespace: str = "dagster",
                 k8s_service_account: str = "dagster",
                 k8s_image_pull_policy: str = "Always",
                 k8s_ttl_seconds: int = 300,
                 forward_env_vars: Optional[list] = None,
                 docker_network: str = "host",
                 docker_auto_remove: bool = True,
                 docker_forward_env_vars: Optional[list] = None):
        self.launcher_mode = launcher_mode
        self.k8s_namespace = k8s_namespace
        self.k8s_service_account = k8s_service_account
        self.k8s_image_pull_policy = k8s_image_pull_policy
        self.k8s_ttl_seconds = k8s_ttl_seconds
        self.forward_env_vars = forward_env_vars or []
        self.docker_network = docker_network
        self.docker_auto_remove = docker_auto_remove
        self.docker_forward_env_vars = docker_forward_env_vars or []
        logger.info("codekit_grpc_proxy_initialized", mode=launcher_mode)

    # =========================================================================
    # HEALTH CHECKS & BOILERPLATE
    # =========================================================================

    def Ping(self, request, context):
        return api_pb2.PingReply(echo=request.echo)

    def Heartbeat(self, request, context):
        return api_pb2.PingReply(echo=request.echo)

    def StreamingPing(self, request, context) -> Iterator[api_pb2.StreamingPingEvent]:
        sequence_number = request.sequence_number if request.sequence_number else 0
        while True:
            yield api_pb2.StreamingPingEvent(
                sequence_number=sequence_number, message="Codekit Proxy Alive"
            )
            sequence_number += 1
            time.sleep(1)

    def GetServerId(self, request, context):
        return api_pb2.GetServerIdReply(server_id="codekit-serverless-proxy")

    def GetCurrentImage(self, request, context):
        return api_pb2.GetCurrentImageReply(current_image=None)

    # =========================================================================
    # REPOSITORY DISCOVERY
    # =========================================================================

    def ListRepositories(self, request, context):
        try:
            with db_session():
                locations = list(CodeLocation.select())

            symbols = [
                LoadableRepositorySymbol(repository_name=loc.name, attribute=loc.name)
                for loc in locations
            ]

            response = ListRepositoriesResponse(
                repository_symbols=symbols,
                executable_path=sys.executable,
                repository_code_pointer_dict={},
                entry_point=[],
                container_image=None,
                container_context=None,
                dagster_library_versions={},
                defs_state_info=None,
            )

            return api_pb2.ListRepositoriesReply(
                serialized_list_repositories_response_or_error=serialize_value(response)
            )
        except Exception as e:
            logger.error("list_repositories_error", error=str(e))
            error_info = SerializableErrorInfo(
                message=str(e), stack=[], cls_name="CodekitError"
            )
            return api_pb2.ListRepositoriesReply(
                serialized_list_repositories_response_or_error=serialize_value(error_info)
            )

    # =========================================================================
    # METADATA SERVING
    # =========================================================================

    def ExternalRepository(self, request, context):
        try:
            location_name = self._extract_location_name(
                request.serialized_repository_python_origin
            )
            logger.info("external_repository_request", location=location_name)

            snapshot = self._get_latest_snapshot(location_name)
            if not snapshot:
                return self._error_reply(
                    context,
                    f"Location '{location_name}' not found. Please deploy via CI/CD.",
                    api_pb2.ExternalRepositoryReply,
                )

            return api_pb2.ExternalRepositoryReply(
                serialized_external_repository_data=snapshot.content_json
            )
        except Exception as e:
            logger.error("external_repository_error", error=str(e))
            return self._error_reply(context, str(e), api_pb2.ExternalRepositoryReply)

    def StreamingExternalRepository(
        self, request, context
    ) -> Iterator[api_pb2.StreamingExternalRepositoryEvent]:
        try:
            location_name = self._extract_location_name(
                request.serialized_repository_python_origin
            )
            logger.info("streaming_external_repository_request", location=location_name)

            snapshot = self._get_latest_snapshot(location_name)
            if not snapshot:
                context.set_details(f"Location '{location_name}' not found.")
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return

            yield from _chunked_stream(
                snapshot.content_json,
                api_pb2.StreamingExternalRepositoryEvent,
                "serialized_external_repository_chunk",
            )
        except Exception:
            return

    def ExternalJob(self, request, context):
        try:
            job_name = request.job_name
            location_name = self._extract_location_name(
                request.serialized_repository_origin
            )
            logger.info("external_job_request", location=location_name, job=job_name)

            repo_snap = self._load_repository_snap(location_name)
            if not repo_snap:
                raise Exception(f"Snapshot not found for {location_name}")

            job_data = None
            for jd in repo_snap.job_datas:
                if jd.name == job_name:
                    job_data = jd
                    break

            if not job_data:
                raise Exception(f"Job '{job_name}' not found in location '{location_name}'")

            return api_pb2.ExternalJobReply(serialized_job_data=serialize_value(job_data))
        except Exception as e:
            logger.error("external_job_error", error=str(e))
            error_info = SerializableErrorInfo(
                message=str(e), stack=[], cls_name="CodekitError"
            )
            return api_pb2.ExternalJobReply(serialized_error=serialize_value(error_info))

    def ExternalNotebookData(self, request, context):
        error_info = SerializableErrorInfo(
            message="Notebook execution is not supported in serverless mode.",
            stack=[],
            cls_name="CodekitError",
        )
        return api_pb2.ExternalNotebookDataReply(
            content=serialize_value(error_info)
        )

    # =========================================================================
    # PARTITIONS
    # =========================================================================

    def ExternalPartitionNames(self, request, context):
        try:
            args = deserialize_value(
                request.serialized_partition_names_args, PartitionNamesArgs
            )
            location_name = self._extract_location_name_from_origin(args.repository_origin)
            logger.info(
                "external_partition_names_request",
                location=location_name,
                partition_set=args.partition_set_name,
            )

            repo_snap = self._load_repository_snap(location_name)
            if not repo_snap:
                raise Exception(f"Snapshot not found for {location_name}")

            partition_set = self._find_partition_set(
                repo_snap, args.partition_set_name, args.job_name
            )
            if not partition_set:
                raise Exception(f"Partition set not found: {args.partition_set_name}")

            names = self._extract_partition_names(partition_set.partitions)
            result = PartitionNamesSnap(partition_names=names)

            return api_pb2.ExternalPartitionNamesReply(
                serialized_external_partition_names_or_external_partition_execution_error=serialize_value(result)
            )
        except Exception as e:
            logger.error("external_partition_names_error", error=str(e))
            error_snap = PartitionExecutionErrorSnap(
                error=SerializableErrorInfo(
                    message=str(e), stack=[], cls_name="CodekitError"
                )
            )
            return api_pb2.ExternalPartitionNamesReply(
                serialized_external_partition_names_or_external_partition_execution_error=serialize_value(error_snap)
            )

    def ExternalPartitionConfig(self, request, context):
        error_snap = PartitionExecutionErrorSnap(
            error=SerializableErrorInfo(
                message="Partition config resolution requires user code (not available in serverless mode).",
                stack=[],
                cls_name="CodekitError",
            )
        )
        return api_pb2.ExternalPartitionConfigReply(
            serialized_external_partition_config_or_external_partition_execution_error=serialize_value(error_snap)
        )

    def ExternalPartitionTags(self, request, context):
        error_snap = PartitionExecutionErrorSnap(
            error=SerializableErrorInfo(
                message="Partition tag resolution requires user code (not available in serverless mode).",
                stack=[],
                cls_name="CodekitError",
            )
        )
        return api_pb2.ExternalPartitionTagsReply(
            serialized_external_partition_tags_or_external_partition_execution_error=serialize_value(error_snap)
        )

    def ExternalPartitionSetExecutionParams(self, request, context):
        error_snap = PartitionExecutionErrorSnap(
            error=SerializableErrorInfo(
                message="Partition set execution params require user code (not available in serverless mode).",
                stack=[],
                cls_name="CodekitError",
            )
        )
        serialized = serialize_value(error_snap)
        yield from _chunked_stream(
            serialized, api_pb2.StreamingChunkEvent, "serialized_chunk"
        )

    def ExternalPipelineSubsetSnapshot(self, request, context):
        try:
            from dagster._grpc.types import JobSubsetSnapshotArgs
            from dagster._core.remote_representation.external_data import RemoteJobSubsetResult

            args = deserialize_value(
                request.serialized_pipeline_subset_snapshot_args, JobSubsetSnapshotArgs
            )
            job_name = args.job_origin.job_name
            location_name = self._extract_location_name_from_origin(
                args.job_origin.repository_origin
            )
            logger.info(
                "external_pipeline_subset_request",
                location=location_name,
                job=job_name,
            )

            repo_snap = self._load_repository_snap(location_name)
            if not repo_snap:
                raise Exception(f"Snapshot not found for {location_name}")

            job_data = None
            for jd in repo_snap.job_datas:
                if jd.name == job_name:
                    job_data = jd
                    break

            if not job_data:
                raise Exception(f"Job '{job_name}' not found")

            result = RemoteJobSubsetResult(
                success=True,
                serialized_job_data=serialize_value(job_data),
            )
            return api_pb2.ExternalPipelineSubsetSnapshotReply(
                serialized_external_pipeline_subset_result=serialize_value(result)
            )
        except Exception as e:
            logger.error("external_pipeline_subset_error", error=str(e))
            from dagster._core.remote_representation.external_data import RemoteJobSubsetResult
            result = RemoteJobSubsetResult(
                success=False,
                error=SerializableErrorInfo(
                    message=str(e), stack=[], cls_name="CodekitError"
                ),
            )
            return api_pb2.ExternalPipelineSubsetSnapshotReply(
                serialized_external_pipeline_subset_result=serialize_value(result)
            )

    # =========================================================================
    # SCHEDULE & SENSOR EXECUTION
    # =========================================================================

    def ExternalScheduleExecution(self, request, context):
        error_info = SerializableErrorInfo(
            message="Schedule execution is not supported in serverless mode. "
                    "Schedules should be evaluated by the Dagster instance directly.",
            stack=[],
            cls_name="CodekitError",
        )
        serialized = serialize_value(error_info)
        yield from _chunked_stream(
            serialized, api_pb2.StreamingChunkEvent, "serialized_chunk"
        )

    def SyncExternalScheduleExecution(self, request, context):
        error_info = SerializableErrorInfo(
            message="Schedule execution is not supported in serverless mode.",
            stack=[],
            cls_name="CodekitError",
        )
        return api_pb2.ExternalScheduleExecutionReply(
            serialized_schedule_result=serialize_value(error_info)
        )

    def ExternalSensorExecution(self, request, context):
        error_info = SerializableErrorInfo(
            message="Sensor execution is not supported in serverless mode. "
                    "Sensors should be evaluated by the Dagster instance directly.",
            stack=[],
            cls_name="CodekitError",
        )
        serialized = serialize_value(error_info)
        yield from _chunked_stream(
            serialized, api_pb2.StreamingChunkEvent, "serialized_chunk"
        )

    def SyncExternalSensorExecution(self, request, context):
        error_info = SerializableErrorInfo(
            message="Sensor execution is not supported in serverless mode.",
            stack=[],
            cls_name="CodekitError",
        )
        return api_pb2.ExternalSensorExecutionReply(
            serialized_sensor_result=serialize_value(error_info)
        )

    # =========================================================================
    # EXECUTION PLANNING
    # =========================================================================

    def ExecutionPlanSnapshot(self, request, context):
        try:
            args = deserialize_value(
                request.serialized_execution_plan_snapshot_args, ExecutionPlanSnapshotArgs
            )
            location_name = (
                args.job_origin.repository_origin.code_location_origin.location_name
            )
            job_name = args.job_origin.job_name

            repo_snap = self._load_repository_snap(location_name)
            if not repo_snap:
                raise Exception(f"Snapshot not found for {location_name}")

            job_data = None
            for jd in repo_snap.job_datas:
                if jd.name == job_name:
                    job_data = jd
                    break

            if not job_data:
                raise Exception(f"Job {job_name} not found in snapshot")

            plan = self._build_plan_from_snap(job_data, args)

            return api_pb2.ExecutionPlanSnapshotReply(
                serialized_execution_plan_snapshot=serialize_value(plan)
            )
        except Exception as e:
            logger.error("execution_plan_error", error=str(e))
            error_info = SerializableErrorInfo(
                message=str(e), stack=[], cls_name="CodekitPlanningError"
            )
            return api_pb2.ExecutionPlanSnapshotReply(
                serialized_execution_plan_snapshot=serialize_value(error_info)
            )

    # =========================================================================
    # RUN MANAGEMENT
    # =========================================================================

    def StartRun(self, request, context):
        try:
            args = deserialize_value(
                request.serialized_execute_run_args, ExecuteExternalJobArgs
            )
            location_name = (
                args.job_origin.repository_origin.code_location_origin.location_name
            )
            run_id = args.run_id

            snapshot = self._get_latest_snapshot(location_name)
            if not snapshot:
                raise Exception(f"Snapshot missing for launch: {location_name}")

            logger.info("launching_run", run_id=run_id, image=snapshot.image_tag, mode=self.launcher_mode)

            if self.launcher_mode == "docker":
                self._launch_docker_run(
                    run_id=run_id,
                    image=snapshot.image_tag,
                    execute_run_args=args,
                )
            else:
                self._launch_k8s_job(
                    run_id=run_id,
                    image=snapshot.image_tag,
                    execute_run_args=args,
                )

            return api_pb2.StartRunReply(
                serialized_start_run_result=serialize_value(
                    StartRunResult(
                        success=True,
                        message=f"Launched run {run_id}",
                    )
                )
            )
        except Exception as e:
            logger.error("launch_error", error=str(e))
            error_info = SerializableErrorInfo(
                message=str(e), stack=[], cls_name="CodekitLaunchError"
            )
            return api_pb2.StartRunReply(
                serialized_start_run_result=serialize_value(
                    StartRunResult(success=False, serializable_error_info=error_info)
                )
            )

    def CancelExecution(self, request, context):
        cancel_request = deserialize_value(
            request.serialized_cancel_execution_request, CancelExecutionRequest
        )
        logger.info("cancel_execution_request", run_id=cancel_request.run_id)
        return api_pb2.CancelExecutionReply(
            serialized_cancel_execution_result=serialize_value(
                CancelExecutionResult(
                    success=False,
                    message="Cancel not supported in serverless mode. "
                            "Delete the Kubernetes Job directly.",
                    serializable_error_info=None,
                )
            )
        )

    def CanCancelExecution(self, request, context):
        return api_pb2.CanCancelExecutionReply(
            serialized_can_cancel_execution_result=serialize_value(
                CanCancelExecutionResult(can_cancel=False)
            )
        )

    def GetCurrentRuns(self, request, context):
        return api_pb2.GetCurrentRunsReply(
            serialized_current_runs=serialize_value(
                GetCurrentRunsResult(current_runs=[], serializable_error_info=None)
            )
        )

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def ShutdownServer(self, request, context):
        logger.info("shutdown_server_requested")
        return api_pb2.ShutdownServerReply(
            serialized_shutdown_server_result=serialize_value(
                ShutdownServerResult(success=True, serializable_error_info=None)
            )
        )

    def ReloadCode(self, request, context):
        logger.info("reload_code_requested_noop")
        return api_pb2.ReloadCodeReply(serialized_error="")

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_latest_snapshot(self, location_name: str) -> Optional[Snapshot]:
        with db_session():
            location = CodeLocation.get_or_none(name=location_name)
            if not location:
                return None
            return (
                Snapshot.select()
                .where(Snapshot.location == location)
                .order_by(Snapshot.created_at.desc())
                .first()
            )

    def _load_repository_snap(self, location_name: str) -> Optional[RepositorySnap]:
        snapshot = self._get_latest_snapshot(location_name)
        if not snapshot:
            return None
        return deserialize_value(snapshot.content_json, RepositorySnap)

    def _extract_location_name(self, serialized_origin: str) -> str:
        origin = deserialize_value(serialized_origin)
        return origin.code_location_origin.location_name

    def _extract_location_name_from_origin(self, origin: Any) -> str:
        return origin.code_location_origin.location_name

    def _error_reply(self, context, message: str, reply_cls):
        context.set_details(message)
        context.set_code(grpc.StatusCode.INTERNAL)
        return reply_cls()

    def _build_plan_from_snap(
        self, job_data: Any, args: ExecutionPlanSnapshotArgs
    ) -> ExecutionPlanSnapshot:
        steps = []
        job_snap = job_data.job
        node_defs = job_snap.node_defs_snapshot

        for op_snap in node_defs.op_def_snaps:
            node_name = op_snap.name

            if args.op_selection and node_name not in args.op_selection:
                continue

            step_inputs = [
                {"name": inp.name, "dagster_type_key": "Any", "source": None}
                for inp in op_snap.input_def_snaps
            ]

            step_outputs = [
                {"name": out.name, "dagster_type_key": "Any"}
                for out in op_snap.output_def_snaps
            ]

            steps.append(
                ExecutionStepSnap(
                    key=node_name,
                    inputs=step_inputs,
                    outputs=step_outputs,
                    solid_handle_id=node_name,
                    kind="COMPUTE",
                    metadata_items=[],
                    tags={},
                )
            )

        return ExecutionPlanSnapshot(
            steps=steps,
            artifacts_persisted=True,
            pipeline_snapshot_id=job_snap.name,
        )

    def _find_partition_set(self, repo_snap: RepositorySnap, partition_set_name: str, job_name: str):
        for ps in repo_snap.partition_sets:
            if ps.name == partition_set_name:
                return ps
            if ps.job_name == job_name:
                return ps
        return None

    def _extract_partition_names(self, partitions: Any) -> list[str]:
        if isinstance(partitions, StaticPartitionsSnap):
            return partitions.partition_keys
        if isinstance(partitions, TimeWindowPartitionsSnap):
            return []
        if isinstance(partitions, MultiPartitionsSnap):
            return []
        return getattr(partitions, "partition_keys", [])

    def _launch_k8s_job(self, run_id: str, image: str, execute_run_args: ExecuteExternalJobArgs):
        try:
            from kubernetes import client, config as k8s_config
        except ImportError:
            raise Exception(
                "kubernetes package is not installed. Install with: "
                "pip install dagster-codekit[kubernetes]"
            )

        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()

        batch_api = client.BatchV1Api()

        location_name = execute_run_args.job_origin.repository_origin.code_location_origin.location_name

        with db_session():
            location = CodeLocation.get_or_none(CodeLocation.name == location_name)
            overrides = location.get_k8s_overrides() if location else {}

        service_account = overrides.get("service_account", self.k8s_service_account)
        image_pull_policy = overrides.get("image_pull_policy", self.k8s_image_pull_policy)
        ttl_seconds = overrides.get("ttl_seconds_after_finished", self.k8s_ttl_seconds)
        k8s_namespace = overrides.get("namespace", self.k8s_namespace)

        resources = overrides.get("resources", {})
        resource_requests = resources.get("requests", {"cpu": "250m", "memory": "512Mi"})
        resource_limits = resources.get("limits", {"cpu": "1000m", "memory": "2Gi"})

        extra_labels = overrides.get("labels", {})
        extra_annotations = overrides.get("annotations", {})
        extra_env = overrides.get("env", {})
        image_pull_secrets = overrides.get("image_pull_secrets", [])
        node_selector = overrides.get("node_selector", {})

        run_args = ExecuteRunArgs(
            job_origin=execute_run_args.job_origin,
            run_id=run_id,
            instance_ref=execute_run_args.instance_ref,
            set_exit_code_on_failure=False,
        )
        serialized_run_args = serialize_value(run_args)

        env = [
            client.V1EnvVar(name="DAGSTER_EXECUTE_RUN_ARGS", value=serialized_run_args),
            client.V1EnvVar(
                name="DAGSTER_HOME",
                value=os.getenv("DAGSTER_HOME", "/opt/dagster/dagster_home"),
            ),
        ]

        for var_name in self.forward_env_vars:
            val = os.getenv(var_name)
            if val:
                env.append(client.V1EnvVar(name=var_name, value=val))

        for key, val in extra_env.items():
            env.append(client.V1EnvVar(name=key, value=str(val)))

        job_name = f"dagster-run-{run_id}"

        labels = {
            "dagster/run-id": run_id,
            "app.kubernetes.io/name": "dagster-codekit-worker",
            "app.kubernetes.io/component": "run-worker",
            **extra_labels,
        }

        annotations = {
            "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
            **extra_annotations,
        }

        pull_secrets = None
        if image_pull_secrets:
            pull_secrets = [
                client.V1LocalObjectReference(name=s["name"])
                for s in image_pull_secrets
            ]

        container = client.V1Container(
            name="dagster-run-worker",
            image=image,
            image_pull_policy=image_pull_policy,
            command=["dagster", "api", "execute_run"],
            env=env,
            resources=client.V1ResourceRequirements(
                requests=resource_requests,
                limits=resource_limits,
            ),
        )

        pod_spec = client.V1PodSpec(
            service_account_name=service_account,
            restart_policy="Never",
            containers=[container],
            image_pull_secrets=pull_secrets,
        )

        if node_selector:
            pod_spec.node_selector = node_selector

        job_manifest = client.V1Job(
            metadata=client.V1ObjectMeta(name=job_name, labels=labels),
            spec=client.V1JobSpec(
                ttl_seconds_after_finished=ttl_seconds,
                backoff_limit=0,
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={"dagster/run-id": run_id},
                        annotations=annotations,
                    ),
                    spec=pod_spec,
                ),
            ),
        )

        try:
            batch_api.create_namespaced_job(namespace=k8s_namespace, body=job_manifest)
            logger.info("k8s_job_created", job=job_name, location=location_name)
        except client.ApiException as e:
            logger.error("k8s_api_error", status=e.status, reason=e.reason)
            raise Exception(f"Failed to create K8s Job: {e.reason}")

    def _launch_docker_run(self, run_id: str, image: str, execute_run_args: ExecuteExternalJobArgs):
        run_args = ExecuteRunArgs(
            job_origin=execute_run_args.job_origin,
            run_id=run_id,
            instance_ref=execute_run_args.instance_ref,
            set_exit_code_on_failure=False,
        )
        serialized_run_args = serialize_value(run_args)

        cmd = [
            "docker", "run",
            "--name", f"dagster-run-{run_id}",
            "--network", self.docker_network,
            "-e", f"DAGSTER_EXECUTE_RUN_ARGS={serialized_run_args}",
            "-e", f"DAGSTER_HOME={os.getenv('DAGSTER_HOME', '/opt/dagster/dagster_home')}",
        ]

        if self.docker_auto_remove:
            cmd.insert(2, "--rm")

        for var_name in self.docker_forward_env_vars:
            val = os.getenv(var_name)
            if val:
                cmd.extend(["-e", f"{var_name}={val}"])

        cmd.append(image)
        cmd.extend(["dagster", "api", "execute_run"])

        logger.info("docker_run_launching", run_id=run_id, image=image)

        def _run():
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=None,
                )
                logger.info(
                    "docker_run_finished",
                    run_id=run_id,
                    returncode=result.returncode,
                    stderr=result.stderr[-500:] if result.stderr else None,
                )
            except Exception as e:
                logger.error("docker_run_error", run_id=run_id, error=str(e))

        thread = threading.Thread(target=_run, daemon=True, name=f"docker-run-{run_id}")
        thread.start()


def run_grpc_server(host: str, port: int, db_conn: Any, max_workers: int = 10,
                   launcher_mode: str = "k8s",
                   k8s_namespace: str = "dagster",
                   k8s_service_account: str = "dagster",
                   k8s_image_pull_policy: str = "Always",
                   k8s_ttl_seconds: int = 300,
                   forward_env_vars: Optional[list] = None,
                   docker_network: str = "host",
                   docker_auto_remove: bool = True,
                   docker_forward_env_vars: Optional[list] = None):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    servicer = CodekitProxyServicer(
        launcher_mode=launcher_mode,
        k8s_namespace=k8s_namespace,
        k8s_service_account=k8s_service_account,
        k8s_image_pull_policy=k8s_image_pull_policy,
        k8s_ttl_seconds=k8s_ttl_seconds,
        forward_env_vars=forward_env_vars,
        docker_network=docker_network,
        docker_auto_remove=docker_auto_remove,
        docker_forward_env_vars=docker_forward_env_vars,
    )
    api_pb2_grpc.add_DagsterApiServicer_to_server(servicer, server)

    server.add_insecure_port(f"{host}:{port}")
    server.start()
    logger.info("grpc_proxy_running", host=host, port=port)
    return server
