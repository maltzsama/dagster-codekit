"""
Dagster internal API facade.

All dagster._core, dagster._grpc, dagster._serdes, and dagster._utils imports
are centralized here. When upgrading Dagster, this is the only module that
should need changes.
"""

from dagster import Definitions

from dagster._core.remote_representation.external_data import (
    JobDataSnap,
    MultiPartitionsSnap,
    PartitionExecutionErrorSnap,
    PartitionNamesSnap,
    PartitionSetSnap,
    RemoteJobSubsetResult,
    RepositorySnap,
    StaticPartitionsSnap,
    TimeWindowPartitionsSnap,
)

from dagster._core.snap.execution_plan_snapshot import (
    ExecutionPlanSnapshot,
    ExecutionStepSnap,
)

from dagster._grpc.__generated__ import (
    dagster_api_pb2 as api_pb2,
    dagster_api_pb2_grpc as api_pb2_grpc,
)

from dagster._grpc.types import (
    CanCancelExecutionRequest,
    CanCancelExecutionResult,
    CancelExecutionRequest,
    CancelExecutionResult,
    ExecuteExternalJobArgs,
    ExecuteRunArgs,
    ExecutionPlanSnapshotArgs,
    ExternalJobArgs,
    GetCurrentRunsResult,
    JobSubsetSnapshotArgs,
    ListRepositoriesResponse,
    LoadableRepositorySymbol,
    PartitionNamesArgs,
    RemoteRepositoryOrigin,
    ShutdownServerResult,
    StartRunResult,
)

from dagster._serdes import deserialize_value, serialize_value

from dagster._utils.error import SerializableErrorInfo

__all__ = [
    "Definitions",
    "RemoteRepositoryOrigin",
    "JobDataSnap",
    "MultiPartitionsSnap",
    "PartitionExecutionErrorSnap",
    "PartitionNamesSnap",
    "PartitionSetSnap",
    "RemoteJobSubsetResult",
    "RepositorySnap",
    "StaticPartitionsSnap",
    "TimeWindowPartitionsSnap",
    "ExecutionPlanSnapshot",
    "ExecutionStepSnap",
    "api_pb2",
    "api_pb2_grpc",
    "CanCancelExecutionRequest",
    "CanCancelExecutionResult",
    "CancelExecutionRequest",
    "CancelExecutionResult",
    "ExecuteExternalJobArgs",
    "ExecuteRunArgs",
    "ExecutionPlanSnapshotArgs",
    "ExternalJobArgs",
    "GetCurrentRunsResult",
    "JobSubsetSnapshotArgs",
    "ListRepositoriesResponse",
    "LoadableRepositorySymbol",
    "PartitionNamesArgs",
    "ShutdownServerResult",
    "StartRunResult",
    "deserialize_value",
    "serialize_value",
    "SerializableErrorInfo",
]
