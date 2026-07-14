import os
import json
import time
import structlog
from concurrent import futures
from typing import Iterator, Optional, Any

import grpc
from kubernetes import client, config as k8s_config

# --- DAGSTER INTERNALS (1.12.x+) ---
from dagster._grpc.__generated__ import (
    dagster_api_pb2 as api_pb2, 
    dagster_api_pb2_grpc as api_pb2_grpc
)
from dagster._serdes import deserialize_value, serialize_value
from dagster._utils.error import SerializableErrorInfo
from dagster._core.snap.execution_plan_snapshot import ExecutionPlanSnapshot, ExecutionStepSnap
from dagster._grpc.types import (
    ExecutionPlanSnapshotArgs,
    ExecuteExternalJobArgs,
    StartRunResult,
)

# --- CODEKIT MODULES ---
from dagster_codekit.db.models import CodeLocation, Snapshot

logger = structlog.get_logger(__name__)

# Configurações do Kubernetes Launcher
K8S_NAMESPACE = os.getenv("POD_NAMESPACE", "dagster")
# Lista de variaveis de ambiente criticas que devem ser passadas do Proxy para o Worker
# para garantir que ele conecte no mesmo banco e storage.
FORWARD_ENV_VARS = [
    "DAGSTER_POSTGRES_USER",
    "DAGSTER_POSTGRES_PASSWORD",
    "DAGSTER_POSTGRES_DB",
    "DAGSTER_POSTGRES_HOSTNAME",
    "DAGSTER_K8S_PG_PASSWORD_SECRET",  # Caso use secrets
    "DAGSTER_CURRENT_IMAGE",  # Caso queira usar a mesma imagem base
    # Adicione aqui variáveis de S3/Azure/GCS se necessário
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "CEPH_ENDPOINT_URL",
    "CEPH_BUCKET_NAME",
]


class CodekitProxyServicer(api_pb2_grpc.DagsterApiServicer):
    """
    O 'Impostor' gRPC.
    Finge ser um User Code Server, mas serve metadados estáticos do DB
    e delega execução para K8s Jobs efêmeros.
    """

    def __init__(self):
        logger.info("codekit grpc proxy initialized")

    # =========================================================================
    # 💓 HEALTH CHECKS & BOILERPLATE
    # =========================================================================

    def Ping(self, request, context):
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
        # Retorna erro pois este proxy gerencia múltiplas imagens (uma por location)
        # O Dagster lida bem com isso ignorando.
        return api_pb2.GetCurrentImageReply(current_image=None)

    # =========================================================================
    # 📚 METADATA SERVING (Repository Structure)
    # =========================================================================

    def ExternalRepository(self, request, context):
        """
        Retorna o grafo completo (Jobs, Assets, Schedules).
        Lê o JSON blob do banco e devolve cru.
        """
        try:
            location_name = self._extract_location_name(request.serialized_repository_python_origin)

            logger.info("metadata request", location=location_name)

            snapshot = self._get_latest_snapshot(location_name)
            if not snapshot:
                return self._error_reply(
                    context,
                    f"Location '{location_name}' not found. Please deploy via CI/CD.",
                    api_pb2.ExternalRepositoryReply,
                )

            # O JSON no banco já é o 'ExternalRepositoryData' serializado.
            # Basta repassar.
            return api_pb2.ExternalRepositoryReply(
                serialized_external_repository_data=snapshot.content_json
            )

        except Exception as e:
            logger.error("metadata error", error=str(e))
            return self._error_reply(context, str(e), api_pb2.ExternalRepositoryReply)

    # =========================================================================
    # 🗺️ EXECUTION PLANNING (No-Import Logic)
    # =========================================================================

    def ExecutionPlanSnapshot(self, request, context):
        """
        Gera o plano de execução para a UI.
        Reconstrói os passos lendo a topologia do JSON, sem importar código do usuário.
        """
        try:
            args = deserialize_value(
                request.serialized_execution_plan_snapshot_args, ExecutionPlanSnapshotArgs
            )
            location_name = args.job_origin.repository_origin.code_location_origin.location_name
            job_name = args.job_origin.job_name

            snapshot = self._get_latest_snapshot(location_name)
            if not snapshot:
                raise Exception(f"Snapshot not found for {location_name}")

            # 1. Carregar o JSON do Repositório
            # O Dagster serdes usa JSON, mas precisamos converter pra dict pra navegar
            repo_data = json.loads(snapshot.content_json)

            # 2. Encontrar o Job específico (pipeline_snapshot)
            # Na estrutura serializada: external_pipeline_datas -> [JobDataSnap]
            # Cada JobDataSnap tem um campo 'pipeline_snapshot' (que é o grafo)
            job_data_snap = next(
                (j for j in repo_data.get("external_pipeline_datas", []) if j["name"] == job_name),
                None,
            )

            if not job_data_snap:
                raise Exception(f"Job {job_name} not found in snapshot")

            pipeline_snap = job_data_snap["pipeline_snapshot"]

            # 3. Construir o Plano (Mapear Nodes -> Steps)
            plan = self._build_plan_from_json_graph(pipeline_snap, args)

            return api_pb2.ExecutionPlanSnapshotReply(
                serialized_execution_plan_snapshot=serialize_value(plan)
            )

        except Exception as e:
            logger.error("planning error", error=str(e))
            # Retorna um erro serializável para aparecer bonito na UI em vez de crashar o gRPC
            error_info = SerializableErrorInfo(
                message=str(e), stack=[], cls_name="CodekitPlanningError"
            )
            return api_pb2.ExecutionPlanSnapshotReply(
                serialized_execution_plan_snapshot=serialize_value(error_info)
            )

    # =========================================================================
    # 🚀 LAUNCHER (Kubernetes Execution)
    # =========================================================================

    def StartRun(self, request, context):
        """
        Recebe ordem de execução e dispara um K8s Job.
        """
        try:
            args = deserialize_value(request.serialized_execute_run_args, ExecuteExternalJobArgs)
            location_name = args.job_origin.repository_origin.code_location_origin.location_name
            run_id = args.run_id

            snapshot = self._get_latest_snapshot(location_name)
            if not snapshot:
                raise Exception(f"Snapshot missing for launch: {location_name}")

            logger.info("launching run", run_id=run_id, image=snapshot.image_tag)

            # Lança o Job
            self._launch_k8s_job(
                run_id=run_id, image=snapshot.image_tag, instance_ref=args.instance_ref
            )

            return api_pb2.StartRunReply(
                serialized_start_run_result=serialize_value(
                    StartRunResult(success=True, message=f"Launched K8s Job for {run_id}")
                )
            )

        except Exception as e:
            logger.error("launch error", error=str(e))
            error_info = SerializableErrorInfo(
                message=str(e), stack=[], cls_name="CodekitLaunchError"
            )
            return api_pb2.StartRunReply(
                serialized_start_run_result=serialize_value(
                    StartRunResult(success=False, serializable_error_info=error_info)
                )
            )

    # =========================================================================
    # 🛠️ HELPER METHODS
    # =========================================================================

    def _get_latest_snapshot(self, location_name: str) -> Optional[Snapshot]:
        """Helper para buscar no DB via Peewee."""
        location = CodeLocation.get_or_none(name=location_name)
        if not location:
            return None
        return (
            Snapshot.select()
            .where(Snapshot.location == location)
            .order_by(Snapshot.created_at.desc())
            .first()
        )

    def _extract_location_name(self, serialized_origin: str) -> str:
        """Deserializa a origem para pegar o nome da Location."""
        origin = deserialize_value(serialized_origin)
        # Caminho: RepositoryPythonOrigin -> CodeLocationOrigin -> location_name
        return origin.code_location_origin.location_name

    def _error_reply(self, context, message: str, reply_cls):
        """Helper para erros gRPC genéricos."""
        context.set_details(message)
        context.set_code(grpc.StatusCode.INTERNAL)
        return reply_cls()

    def _build_plan_from_json_graph(
        self, pipeline_snap: dict, args: ExecutionPlanSnapshotArgs
    ) -> ExecutionPlanSnapshot:
        """
        Reconstrói o plano de execução iterando sobre os nós do JSON.
        """
        steps = []

        # 'nodes' contém a lista de Ops/Assets
        for node in pipeline_snap.get("nodes", []):
            node_name = node["name"]

            # Filtro de seleção (Re-execute from failure / subset)
            if args.op_selection and node_name not in args.op_selection:
                continue

            # Mapeamento simplificado de Inputs/Outputs para o plano visual
            # O K8s fará a resolução real, aqui é só pra UI não ficar branca
            step_inputs = [
                {"name": inp["name"], "dagster_type_key": "Any", "source": None}
                for inp in node.get("inputs", [])
            ]

            step_outputs = [
                {"name": out["name"], "dagster_type_key": "Any"} for out in node.get("outputs", [])
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
            pipeline_snapshot_id=pipeline_snap["name"],
        )

    def _launch_k8s_job(self, run_id: str, image: str, instance_ref: Any):
        """
        Cria o Job no Kubernetes usando a API oficial.
        """
        # Carrega configuração (In-Cluster ou Kubeconfig local)
        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()

        batch_api = client.BatchV1Api()

        # Monta as variáveis de ambiente para o Worker
        # Copia as credenciais do ambiente do Proxy para o Job
        env = [
            client.V1EnvVar(name="DAGSTER_RUN_ID", value=run_id),
            # Garante que o worker saiba que é um worker
            client.V1EnvVar(name="DAGSTER_IS_K8S_WORKER", value="1"),
        ]

        for var_name in FORWARD_ENV_VARS:
            val = os.getenv(var_name)
            if val:
                env.append(client.V1EnvVar(name=var_name, value=val))

        # Nome determinístico para o Job
        job_name = f"dagster-run-{run_id}"

        # Definição do Job
        job_manifest = client.V1Job(
            metadata=client.V1ObjectMeta(
                name=job_name,
                labels={
                    "dagster/run-id": run_id,
                    "app.kubernetes.io/name": "dagster-codekit-worker",
                    "app.kubernetes.io/component": "run-worker",
                },
            ),
            spec=client.V1JobSpec(
                ttl_seconds_after_finished=300,  # Limpeza automática após 5 min
                backoff_limit=0,  # Não reinicia se o código do usuário falhar (Dagster trata isso)
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={"dagster/run-id": run_id},
                        # Annotations podem ser úteis para logs/metrics
                        annotations={"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"},
                    ),
                    spec=client.V1PodSpec(
                        service_account_name="dagster",  # Essencial para permissões (S3/DB)
                        restart_policy="Never",
                        containers=[
                            client.V1Container(
                                name="dagster-run-worker",
                                image=image,
                                image_pull_policy="Always",
                                # COMANDO MÁGICO: Este comando do Dagster CLI sabe:
                                # 1. Conectar no DB (via env vars)
                                # 2. Baixar o Run ID
                                # 3. Executar o plano
                                command=["dagster", "api", "execute_run"],
                                env=env,
                                resources=client.V1ResourceRequirements(
                                    requests={"cpu": "250m", "memory": "512Mi"},
                                    limits={"cpu": "1000m", "memory": "2Gi"},
                                ),
                            )
                        ],
                        # image_pull_secrets se necessário
                    ),
                ),
            ),
        )

        # Criação
        try:
            batch_api.create_namespaced_job(namespace=K8S_NAMESPACE, body=job_manifest)
            logger.info("k8s job created", job=job_name)
        except client.ApiException as e:
            logger.error("k8s api error", status=e.status, reason=e.reason)
            raise Exception(f"Failed to create K8s Job: {e.reason}")


def run_grpc_server(host: str, port: int, db_conn: Any, max_workers: int = 10):
    """
    Entrypoint para iniciar o servidor gRPC.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    api_pb2_grpc.add_DagsterApiServicer_to_server(CodekitProxyServicer(), server)

    server.add_insecure_port(f"{host}:{port}")
    server.start()
    logger.info(f"📡 gRPC Proxy running on {host}:{port}")
    return server
