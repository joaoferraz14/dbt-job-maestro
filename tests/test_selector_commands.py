"""Tests for per-selector command flexibility and Airflow continue-on-failure.

Covers:
  * the shared dbt_commands helper (mapping, resolution, validation);
  * dbt Cloud job generation honouring default_command / selector_commands;
  * Airflow DAG generation honouring commands + continue_on_failure wiring.
"""

import pytest

from dbt_job_maestro.config import AirflowConfig, Config, JobConfig
from dbt_job_maestro.dbt_commands import (
    build_steps,
    resolve_command,
    validate_commands,
)
from dbt_job_maestro.airflow_dag_generator import AirflowDAGGenerator
from dbt_job_maestro.job_generator import JobGenerator

# ---------------------------------------------------------------------------
# dbt_commands helper
# ---------------------------------------------------------------------------


class TestBuildSteps:
    def test_build_single_step(self):
        assert build_steps("build", "maestro_x") == ["dbt build --selector maestro_x"]

    def test_run_single_step(self):
        assert build_steps("run", "maestro_x") == ["dbt run --selector maestro_x"]

    def test_test_single_step(self):
        assert build_steps("test", "maestro_x") == ["dbt test --selector maestro_x"]

    def test_run_test_two_steps(self):
        assert build_steps("run_test", "maestro_x") == [
            "dbt run --selector maestro_x",
            "dbt test --selector maestro_x",
        ]

    def test_extra_flags_appended(self):
        steps = build_steps("run_test", "x", extra_flags="--target prod --threads 8")
        assert steps == [
            "dbt run --selector x --target prod --threads 8",
            "dbt test --selector x --target prod --threads 8",
        ]

    def test_full_refresh_only_on_build_run(self):
        # full_refresh applies to build/run, never to the test step
        assert build_steps("build", "x", full_refresh=True) == [
            "dbt build --full-refresh --selector x"
        ]
        assert build_steps("run_test", "x", full_refresh=True) == [
            "dbt run --full-refresh --selector x",
            "dbt test --selector x",
        ]


class TestResolveCommand:
    def test_default_when_no_override(self):
        assert resolve_command("critical_revenue", "build", {}) == "build"

    def test_manual_override_wins(self):
        cmds = {"critical_revenue": "run_test"}
        assert resolve_command("critical_revenue", "build", cmds) == "run_test"

    def test_auto_selector_ignores_override(self):
        # Auto-generated selectors always use default_command, even if named.
        cmds = {"maestro_staging": "test"}
        assert resolve_command("maestro_staging", "build", cmds, "maestro") == "build"

    def test_manual_override_applies_with_prefix(self):
        cmds = {"critical_revenue": "test"}
        assert resolve_command("critical_revenue", "build", cmds, "maestro") == "test"


class TestValidateCommands:
    def test_accepts_valid(self):
        validate_commands("build", {"a": "run", "b": "run_test", "c": "test"})

    def test_rejects_bad_default(self):
        with pytest.raises(ValueError):
            validate_commands("compile", {})

    def test_rejects_bad_override(self):
        with pytest.raises(ValueError):
            validate_commands("build", {"a": "compile"})


# ---------------------------------------------------------------------------
# Config parsing / validation
# ---------------------------------------------------------------------------


class TestConfigCommands:
    def test_job_and_airflow_parse_commands(self, tmp_path):
        cfg_file = tmp_path / "maestro-config.yml"
        cfg_file.write_text(
            "\n".join(
                [
                    "job:",
                    "  default_command: run",
                    "  selector_commands:",
                    "    critical_revenue: run_test",
                    "airflow:",
                    "  default_command: build",
                    "  continue_on_failure: true",
                    "  selector_commands:",
                    "    critical_revenue: test",
                ]
            )
        )
        cfg = Config.from_yaml(str(cfg_file))
        assert cfg.job.default_command == "run"
        assert cfg.job.selector_commands == {"critical_revenue": "run_test"}
        assert cfg.airflow.default_command == "build"
        assert cfg.airflow.continue_on_failure is True
        assert cfg.airflow.selector_commands == {"critical_revenue": "test"}

    def test_invalid_job_command_raises(self, tmp_path):
        cfg_file = tmp_path / "bad.yml"
        cfg_file.write_text("job:\n  default_command: compile\n")
        with pytest.raises(ValueError):
            Config.from_yaml(str(cfg_file))

    def test_defaults(self):
        assert JobConfig().default_command == "build"
        assert AirflowConfig().default_command == "build"
        assert AirflowConfig().continue_on_failure is False


# ---------------------------------------------------------------------------
# dbt Cloud job generation
# ---------------------------------------------------------------------------


def _job_cfg(**kwargs):
    return JobConfig(
        account_id=1,
        project_id=2,
        environment_id=3,
        job_name_prefix="maestro",
        selector_prefix="maestro",
        orchestration_mode="simple",
        **kwargs,
    )


AUTO_SELECTORS = [
    {"name": "maestro_staging", "definition": {"union": [{"method": "fqn", "value": "a"}]}},
]
MANUAL_SELECTOR = {
    "name": "critical_revenue",
    "definition": {"union": [{"method": "fqn", "value": "fct_rev"}]},
}


class TestJobCommands:
    def test_default_command_applied_to_auto(self):
        gen = JobGenerator(_job_cfg(default_command="run"))
        jobs = gen.generate_jobs(AUTO_SELECTORS)["jobs"]
        assert jobs["maestro_maestro_staging"]["execute_steps"] == [
            "dbt run --selector maestro_staging"
        ]

    def test_run_test_produces_two_steps(self):
        gen = JobGenerator(_job_cfg(default_command="run_test"))
        jobs = gen.generate_jobs(AUTO_SELECTORS)["jobs"]
        assert jobs["maestro_maestro_staging"]["execute_steps"] == [
            "dbt run --selector maestro_staging",
            "dbt test --selector maestro_staging",
        ]

    def test_manual_override_only(self):
        # default build for auto; per-selector run_test for the manual selector
        gen = JobGenerator(
            _job_cfg(selector_commands={"critical_revenue": "run_test", "maestro_staging": "test"})
        )
        jobs = gen.generate_jobs(AUTO_SELECTORS + [MANUAL_SELECTOR])["jobs"]
        # auto ignores its override
        assert jobs["maestro_maestro_staging"]["execute_steps"] == [
            "dbt build --selector maestro_staging"
        ]
        # manual honours it
        assert jobs["maestro_critical_revenue"]["execute_steps"] == [
            "dbt run --selector critical_revenue",
            "dbt test --selector critical_revenue",
        ]

    def test_combined_job_concatenates_steps(self):
        small = [
            {"name": "maestro_a", "definition": {"union": [{"method": "fqn", "value": "a"}]}},
            {"name": "maestro_b", "definition": {"union": [{"method": "fqn", "value": "b"}]}},
        ]
        gen = JobGenerator(_job_cfg(min_models_per_job=5, default_command="run_test"))
        jobs = gen.generate_jobs(small)["jobs"]
        combined = jobs["maestro_combined_small_selectors"]["execute_steps"]
        assert combined == [
            "dbt run --selector maestro_a",
            "dbt test --selector maestro_a",
            "dbt run --selector maestro_b",
            "dbt test --selector maestro_b",
        ]


# ---------------------------------------------------------------------------
# Airflow DAG generation
# ---------------------------------------------------------------------------


def _airflow_cfg(**kwargs):
    return AirflowConfig(selector_prefix="maestro", dbt_target="", dbt_threads=0, **kwargs)


class TestAirflowCommands:
    def test_run_test_chains_run_then_test(self):
        gen = AirflowDAGGenerator(_airflow_cfg(default_command="run_test"))
        src = gen.generate_dags(AUTO_SELECTORS)["dbt_maestro_maestro_staging.py"]
        assert 'task_id="run_maestro_staging"' in src
        assert 'task_id="test_maestro_staging"' in src
        assert "run_maestro_staging >> test_maestro_staging" in src

    def test_manual_override_only_in_airflow(self):
        gen = AirflowDAGGenerator(
            _airflow_cfg(selector_commands={"maestro_staging": "run", "critical_revenue": "test"})
        )
        dags = gen.generate_dags(AUTO_SELECTORS + [MANUAL_SELECTOR])
        # auto ignores override -> default build
        assert "dbt build --selector maestro_staging" in dags["dbt_maestro_maestro_staging.py"]
        # manual honours override
        assert "dbt test --selector critical_revenue" in dags["dbt_maestro_critical_revenue.py"]

    def test_combined_chained_when_failfast(self):
        small = [
            {"name": "maestro_a", "definition": {"union": [{"method": "fqn", "value": "a"}]}},
            {"name": "maestro_b", "definition": {"union": [{"method": "fqn", "value": "b"}]}},
        ]
        gen = AirflowDAGGenerator(_airflow_cfg(min_models_per_dag=5, continue_on_failure=False))
        src = gen.generate_dags(small)["dbt_maestro_combined_small_selectors.py"]
        assert "run_maestro_a >> run_maestro_b" in src

    def test_combined_independent_when_continue_on_failure(self):
        small = [
            {"name": "maestro_a", "definition": {"union": [{"method": "fqn", "value": "a"}]}},
            {"name": "maestro_b", "definition": {"union": [{"method": "fqn", "value": "b"}]}},
        ]
        gen = AirflowDAGGenerator(_airflow_cfg(min_models_per_dag=5, continue_on_failure=True))
        src = gen.generate_dags(small)["dbt_maestro_combined_small_selectors.py"]
        # no cross-selector dependency edge
        assert ">>" not in src

    def test_continue_on_failure_keeps_run_test_chain(self):
        # Within a selector, run >> test must still hold even when siblings are independent.
        small = [
            {"name": "maestro_a", "definition": {"union": [{"method": "fqn", "value": "a"}]}},
            {"name": "maestro_b", "definition": {"union": [{"method": "fqn", "value": "b"}]}},
        ]
        gen = AirflowDAGGenerator(
            _airflow_cfg(min_models_per_dag=5, continue_on_failure=True, default_command="run_test")
        )
        src = gen.generate_dags(small)["dbt_maestro_combined_small_selectors.py"]
        assert "run_maestro_a >> test_maestro_a" in src
        assert "run_maestro_b >> test_maestro_b" in src
        # but no cross-selector edge (test_maestro_a >> run_maestro_b)
        assert "test_maestro_a >> run_maestro_b" not in src
