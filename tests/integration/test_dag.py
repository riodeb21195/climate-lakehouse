"""The pipeline DAG imports in the real image and wires the jobs in the right order."""

import itertools
import json

import pytest

from tests.integration.conftest import compose

pytestmark = pytest.mark.integration

# Runs with Airflow's interpreter in the image; prints the DAG structure as JSON.
INSPECT = """
import json, runpy
dag = runpy.run_path("/opt/airflow/dags/climate_pipeline.py")["dag"]
print(json.dumps({
    "schedule": str(dag.schedule),
    "max_active_runs": dag.max_active_runs,
    "params": dag.params.dump(),
    "tasks": {t.task_id: {"downstream": sorted(t.downstream_task_ids), "command": t.bash_command,
                          "retries": t.retries} for t in dag.tasks},
}))
"""


@pytest.fixture(scope="module")
def dag() -> dict[str, object]:
    build = compose("--profile", "jobs", "build", "jobs")
    assert build.returncode == 0, build.stderr
    result = compose("run", "--rm", "--no-deps", "--entrypoint", "python", "jobs", "-c", INSPECT)
    assert result.returncode == 0, result.stderr[-3000:]
    parsed: dict[str, object] = json.loads(result.stdout.strip().splitlines()[-1])
    return parsed


def test_jobs_run_in_order(dag: dict[str, object]) -> None:
    tasks = dag["tasks"]
    assert isinstance(tasks, dict)
    order = ["download", "bronze", "silver", "gold", "reconcile", "maintain"]
    assert set(tasks) == set(order)
    for upstream, downstream in itertools.pairwise(order):
        assert tasks[upstream]["downstream"] == [downstream]
    assert tasks["maintain"]["downstream"] == []


def test_commands_target_existing_job_modules(dag: dict[str, object]) -> None:
    tasks = dag["tasks"]
    assert isinstance(tasks, dict)
    modules = {t: c["command"].split()[2] for t, c in tasks.items()}
    check = "; ".join(f"import {m}" for m in modules.values())
    venv_python = "/opt/climate/.venv/bin/python"
    result = compose("run", "--rm", "--no-deps", "--entrypoint", venv_python, "jobs", "-c", check)
    assert result.returncode == 0, result.stderr[-2000:]
    # only silver and gold take the backfill flag; only the download retries
    assert [t for t, c in tasks.items() if "full_refresh" in c["command"]] == ["silver", "gold"]
    assert [t for t, c in tasks.items() if c["retries"]] == ["download"]


def test_one_run_at_a_time(dag: dict[str, object]) -> None:
    assert dag["max_active_runs"] == 1
    assert dag["params"] == {"full_refresh": False}
