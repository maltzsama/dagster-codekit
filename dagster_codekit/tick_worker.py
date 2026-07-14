"""Ephemeral tick evaluator for schedules and sensors.

Minimal, self-contained script injected into worker containers.
Requires only dagster (no dagster-codekit).
"""

import importlib.util
import os
import sys
import traceback

from dagster import Definitions
from dagster._core.host_representation.external_data import (
    ExternalScheduleExecutionData,
    ExternalSensorExecutionData,
)
from dagster._grpc.types import ExternalScheduleExecutionArgs, SensorExecutionArgs
from dagster._serdes import deserialize_value, serialize_value


def _load_defs(file_path: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    spec = importlib.util.spec_from_file_location("user_code", file_path)
    if not spec or not spec.loader:
        raise ImportError(f"Cannot load module: {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    defs = next((v for v in vars(module).values() if isinstance(v, Definitions)), None)
    if not defs:
        raise ValueError(f"No Definitions found in {file_path}")
    return defs


def main():
    args_json = os.environ.get("DAGSTER_TICK_ARGS", "")
    tick_type = os.environ.get("DAGSTER_TICK_TYPE", "")
    file_path = os.environ.get("DAGSTER_TICK_FILE", "")

    if not all([args_json, tick_type, file_path]):
        print("TICK_ERROR: missing required env vars", flush=True)
        sys.exit(1)

    try:
        defs = _load_defs(file_path)
        repo = defs.get_repository_def()

        if tick_type == "schedule":
            args = deserialize_value(args_json, ExternalScheduleExecutionArgs)
            sched = next((s for s in repo.schedule_defs if s.name == args.schedule_name), None)
            if not sched:
                raise ValueError(f"Schedule '{args.schedule_name}' not found")
            result = sched.evaluate_tick(
                scheduled_execution_time=args.scheduled_execution_timestamp,
                scheduled_execution_timezone=args.scheduled_execution_timezone,
            )
            print(serialize_value(ExternalScheduleExecutionData.from_schedule_data(result)), flush=True)

        elif tick_type == "sensor":
            args = deserialize_value(args_json, SensorExecutionArgs)
            sensor = next((s for s in repo.sensor_defs if s.name == args.sensor_name), None)
            if not sensor:
                raise ValueError(f"Sensor '{args.sensor_name}' not found")
            result = sensor.evaluate_tick(
                cursor=args.cursor,
                last_completion_time=args.last_completion_time,
                last_run_key=args.last_run_key,
                last_tick_completion_time=args.last_tick_completion_time,
                last_sensor_start_time=args.last_sensor_start_time,
            )
            print(serialize_value(ExternalSensorExecutionData.from_sensor_data(result)), flush=True)

        else:
            raise ValueError(f"Unknown tick type: {tick_type}")

    except Exception:
        error_json = serialize_value({
            "__class__": "SerializableErrorInfo",
            "message": traceback.format_exc(),
            "stack": [],
            "cls_name": "TickEvaluationError",
        })
        print(error_json, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
