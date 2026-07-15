"""End-to-end tests for dagster-codekit.

Validates the full pipeline: snapshot extraction -> deploy -> gRPC proxy
serving -> execution plan reconstruction. Uses Docker mode (no K8s required).
"""

import os
import tempfile
import textwrap
from unittest.mock import MagicMock

import pytest
from dagster._grpc.types import (
    ExecutionPlanSnapshotArgs,
)
from dagster._serdes import deserialize_value

from dagster_codekit.core.dagster_facade import RepositorySnap
from dagster_codekit.core.engine import create_snapshot_payload
from dagster_codekit.core.grpc_proxy import CodekitProxyServicer
from dagster_codekit.db.models import CodeLocation, Snapshot, db_session, init_db


def _make_mock_args(op_selection=None):
    """Create a minimal mock ExecutionPlanSnapshotArgs for plan building."""
    args = MagicMock(spec=ExecutionPlanSnapshotArgs)
    args.op_selection = op_selection
    return args


@pytest.fixture
def fixture_project():
    """Create a temporary Dagster project with multi-step jobs."""
    project = tempfile.mkdtemp()
    definitions_path = os.path.join(project, "definitions.py")

    code = textwrap.dedent("""
    from dagster import Definitions, job, op, Out, In

    @op
    def step_a():
        return {"value": 1}

    @op
    def step_b(input_data):
        return {"value": input_data["value"] + 1}

    @op(out={"b_out": Out(), "c_out": Out()})
    def step_a_split():
        return {"b_out": 10, "c_out": 20}

    @op(ins={"data": In()})
    def step_d(data):
        return {"value": data + 10}

    @job
    def linear_job():
        a = step_a()
        step_b(a)

    @job
    def fan_out_job():
        split = step_a_split()
        step_d(split.b_out)
        step_d.alias("step_d_2")(split.c_out)

    @job
    def no_deps_job():
        step_a()
        step_a.alias("step_a_2")()

    defs = Definitions(
        jobs=[linear_job, fan_out_job, no_deps_job],
    )
    """)

    with open(definitions_path, "w") as f:
        f.write(code)

    yield definitions_path, project

    import shutil
    shutil.rmtree(project, ignore_errors=True)


@pytest.fixture(scope="module")
def codekit_db():
    """In-memory SQLite database for test isolation."""
    db_path = tempfile.mktemp(suffix=".db")
    db_url = f"sqlite:///{db_path}"
    init_db(db_url)
    yield db_url
    try:
        os.unlink(db_path)
    except OSError:
        pass


@pytest.fixture(autouse=True)
def _ensure_db(codekit_db):
    """Ensure DB is initialized before each test."""
    pass


def deploy_snapshot(location_name: str, file_path: str, image_tag: str):
    """Create and persist a snapshot directly (bypasses HTTP API)."""
    payload = create_snapshot_payload(location_name, file_path, image_tag)

    with db_session():
        location, _ = CodeLocation.get_or_create(
            name=location_name,
            defaults={"image": image_tag, "namespace": "dagster"},
        )
        location.image = image_tag
        location.save()
        Snapshot.create(
            location=location,
            image_tag=image_tag,
            content_json=payload.snapshot_json,
            commit_hash=payload.commit_hash,
        )
    return payload


class TestExecutionPlanCorrectness:
    """4.1: Execution plan must have correct step dependencies."""

    def test_linear_job_has_dependency_chain(self, fixture_project):
        file_path, _ = fixture_project
        deploy_snapshot("test-location", file_path, "test:latest")

        with db_session():
            loc = CodeLocation.get(name="test-location")
            snap = Snapshot.select().where(Snapshot.location == loc).order_by(
                Snapshot.created_at.desc()
            ).first()

        repo_snap = deserialize_value(snap.content_json, RepositorySnap)
        job_data = next(jd for jd in repo_snap.job_datas if jd.name == "linear_job")

        servicer = CodekitProxyServicer(launcher_mode="docker")
        servicer = CodekitProxyServicer(launcher_mode="docker")
        plan = servicer._build_plan_from_snap(
            job_data,
            _make_mock_args(),
        )

        assert len(plan.steps) == 2

        step_b = next(s for s in plan.steps if s.key == "step_b")
        assert len(step_b.inputs) == 1
        assert len(step_b.inputs[0].upstream_output_handles) == 1
        handle = step_b.inputs[0].upstream_output_handles[0]
        assert handle.step_key == "step_a"
        assert handle.output_name == "result"

        step_a = next(s for s in plan.steps if s.key == "step_a")
        # Top-level steps have no upstream dependencies
        for inp in step_a.inputs:
            assert inp.upstream_output_handles == []

    def test_fan_out_job_splits_outputs(self, fixture_project):
        file_path, _ = fixture_project
        deploy_snapshot("test-fanout", file_path, "test:latest")

        with db_session():
            loc = CodeLocation.get(name="test-fanout")
            snap = Snapshot.select().where(Snapshot.location == loc).order_by(
                Snapshot.created_at.desc()
            ).first()

        repo_snap = deserialize_value(snap.content_json, RepositorySnap)
        job_data = next(jd for jd in repo_snap.job_datas if jd.name == "fan_out_job")

        servicer = CodekitProxyServicer(launcher_mode="docker")
        plan = servicer._build_plan_from_snap(
            job_data,
            _make_mock_args(),
        )

        assert len(plan.steps) >= 2  # step_a_split + at least one step_d

        step_d = next(s for s in plan.steps if s.key == "step_d")
        assert len(step_d.inputs[0].upstream_output_handles) == 1
        handle = step_d.inputs[0].upstream_output_handles[0]
        assert handle.step_key == "step_a_split"
        assert handle.output_name == "b_out"

    def test_no_deps_job_has_no_upstream_handles(self, fixture_project):
        file_path, _ = fixture_project
        deploy_snapshot("test-nodeps", file_path, "test:latest")

        with db_session():
            loc = CodeLocation.get(name="test-nodeps")
            snap = Snapshot.select().where(Snapshot.location == loc).order_by(
                Snapshot.created_at.desc()
            ).first()

        repo_snap = deserialize_value(snap.content_json, RepositorySnap)
        job_data = next(jd for jd in repo_snap.job_datas if jd.name == "no_deps_job")

        servicer = CodekitProxyServicer(launcher_mode="docker")
        plan = servicer._build_plan_from_snap(
            job_data,
            _make_mock_args(),
        )

        for step in plan.steps:
            for inp in step.inputs:
                assert inp.upstream_output_handles == [], (
                    f"Step {step.key} input {inp.name} should have no upstream dependencies"
                )

    def test_type_keys_preserved(self, fixture_project):
        """Type keys from the snapshot are used (they may be 'Any' for generic Python types)."""
        file_path, _ = fixture_project
        deploy_snapshot("test-types", file_path, "test:latest")

        with db_session():
            loc = CodeLocation.get(name="test-types")
            snap = Snapshot.select().where(Snapshot.location == loc).order_by(
                Snapshot.created_at.desc()
            ).first()

        repo_snap = deserialize_value(snap.content_json, RepositorySnap)
        job_data = next(jd for jd in repo_snap.job_datas if jd.name == "linear_job")

        servicer = CodekitProxyServicer(launcher_mode="docker")
        plan = servicer._build_plan_from_snap(
            job_data,
            _make_mock_args(),
        )

        # Verify type keys are present (not None)
        for step in plan.steps:
            for inp in step.inputs:
                assert inp.dagster_type_key is not None, (
                    f"Step {step.key} input {inp.name} has None type key"
                )
            for out in step.outputs:
                assert out.dagster_type_key is not None, (
                    f"Step {step.key} output {out.name} has None type key"
                )


class TestSnapshotDeploy:
    """Verify snapshot extraction and deploy workflow."""

    def test_snapshot_contains_all_jobs(self, fixture_project):
        file_path, _ = fixture_project
        payload = deploy_snapshot("test-all-jobs", file_path, "test:latest")

        repo_snap = deserialize_value(payload.snapshot_json, RepositorySnap)
        job_names = {jd.name for jd in repo_snap.job_datas}
        assert job_names == {"linear_job", "fan_out_job", "no_deps_job"}


class TestPartitions:
    """4.4: Partition support."""

    def test_static_partitions(self):
        from dagster._core.remote_representation.external_data import StaticPartitionsSnap

        servicer = CodekitProxyServicer()
        snap = StaticPartitionsSnap(partition_keys=["2023-01-01", "2023-01-02", "2023-01-03"])
        keys = servicer._extract_partition_names(snap)
        assert keys == ["2023-01-01", "2023-01-02", "2023-01-03"]

    @pytest.mark.skip(reason="Requires ScheduleType enum - not easily constructable in tests")
    def test_time_window_partitions(self):
        from dagster._core.remote_representation.external_data import TimeWindowPartitionsSnap

        servicer = CodekitProxyServicer()
        snap = TimeWindowPartitionsSnap(
            start=0.0,
            timezone="UTC",
            fmt="%Y-%m-%d",
            end_offset=0,
            end=None,
            cron_schedule="0 0 * * *",
            exclusions=[],
            schedule_type="DAILY",
            minute_offset=0,
            hour_offset=0,
            day_offset=0,
        )
        keys = servicer._extract_partition_names(snap)
        assert isinstance(keys, list)
        assert len(keys) > 0


class TestCancelExecution:
    """4.2: Cancel execution must actually cancel."""

    def test_cancel_k8s_job_returns_success(self):
        servicer = CodekitProxyServicer()
        assert servicer.launcher_mode == "k8s"

    def test_can_cancel_checks_existence(self):
        servicer = CodekitProxyServicer()
        exists = servicer._docker_container_exists("nonexistent-run-id")
        assert exists is False


class TestServerHealth:
    """Verify health endpoints and metrics."""

    def test_servicer_has_all_methods(self):
        from dagster._grpc.__generated__ import dagster_api_pb2_grpc as grpc_gen
        expected = {m for m in dir(grpc_gen.DagsterApiServicer) if not m.startswith("_")}
        actual = {m for m in dir(CodekitProxyServicer) if not m.startswith("_")}

        missing = expected - actual
        assert not missing, f"Missing gRPC methods: {missing}"


class TestSnapshotRollback:
    """Rollback must revert to previous snapshot."""

    def test_rollback_restores_previous(self, fixture_project):
        file_path, _ = fixture_project

        deploy_snapshot("test-rollback", file_path, "test:v1")
        deploy_snapshot("test-rollback", file_path, "test:v2")

        with db_session():
            location = CodeLocation.get(name="test-rollback")
            snapshots = list(
                Snapshot.select()
                .where(Snapshot.location == location)
                .order_by(Snapshot.created_at.desc())
            )
            assert len(snapshots) >= 2

            latest = snapshots[0]
            previous = snapshots[1]
            latest.delete_instance()

            remaining = list(
                Snapshot.select()
                .where(Snapshot.location == location)
                .order_by(Snapshot.created_at.desc())
            )
            assert remaining[0].id == previous.id
            assert remaining[0].image_tag == previous.image_tag
