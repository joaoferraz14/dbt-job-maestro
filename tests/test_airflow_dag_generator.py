"""Tests for the Airflow DAG generator."""

import pytest

from dbt_job_maestro.config import AirflowConfig
from dbt_job_maestro.airflow_dag_generator import AirflowDAGGenerator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _airflow_available() -> bool:
    """Return True only if Airflow can actually be imported and used.

    A simple find_spec("airflow") check is insufficient: the package may be
    installed but not importable (e.g. broken install, or a sandbox that blocks
    Airflow's `secrets/` submodule). We attempt the real import used by the
    rendering tests so they skip cleanly when Airflow is unusable.
    """
    try:
        from airflow.models import DagBag  # noqa: F401

        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def default_cfg():
    return AirflowConfig(
        dag_id="dbt_maestro_dag",
        schedule_interval="0 6 * * *",
        start_date="2024-01-01",
        owner="airflow",
        retries=1,
        retry_delay_minutes=5,
        dbt_target="prod",
        dbt_threads=4,
        selector_prefix="maestro",
        orchestration_mode="dependency",
        tags=["dbt", "maestro"],
    )


@pytest.fixture
def gen(default_cfg):
    return AirflowDAGGenerator(default_cfg)


@pytest.fixture
def sample_selectors():
    return [
        {
            "name": "maestro_staging",
            "definition": {
                "union": [
                    {"method": "fqn", "value": "stg_orders"},
                    {"method": "fqn", "value": "stg_customers"},
                ]
            },
        },
        {
            "name": "maestro_marts",
            "definition": {
                "union": [
                    {"method": "fqn", "value": "fct_orders"},
                    {"method": "fqn", "value": "dim_customers"},
                ]
            },
        },
    ]


@pytest.fixture
def selectors_with_seeds(sample_selectors):
    return [
        {
            "name": "maestro_seeds",
            "definition": {"union": [{"method": "fqn", "value": "my_seed"}]},
        },
    ] + sample_selectors


@pytest.fixture
def selectors_with_snapshots(sample_selectors):
    return [
        {
            "name": "maestro_snapshots",
            "definition": {"union": [{"method": "fqn", "value": "orders_snapshot"}]},
        },
    ] + sample_selectors


# ---------------------------------------------------------------------------
# Selector type detection
# ---------------------------------------------------------------------------


class TestSelectorTypeDetection:
    def test_seeds_by_exact_name(self, gen):
        assert gen._get_selector_type("maestro_seeds") == "seeds"

    def test_seeds_by_suffix(self, gen):
        assert gen._get_selector_type("custom_seeds") == "seeds"

    def test_snapshots_by_exact_name(self, gen):
        assert gen._get_selector_type("maestro_snapshots") == "snapshots"

    def test_snapshots_by_suffix(self, gen):
        assert gen._get_selector_type("custom_snapshots") == "snapshots"

    def test_full_refresh_suffix(self, gen):
        assert gen._get_selector_type("maestro_full_refresh_incremental") == "full_refresh"

    def test_models_default(self, gen):
        assert gen._get_selector_type("maestro_staging") == "models"
        assert gen._get_selector_type("maestro_marts") == "models"
        assert gen._get_selector_type("manual_custom") == "models"


# ---------------------------------------------------------------------------
# dbt command generation
# ---------------------------------------------------------------------------


class TestDbtCommandGeneration:
    def test_models_uses_build(self, gen):
        cmd = gen._get_dbt_command({"name": "maestro_staging"})
        assert cmd.startswith("dbt build --selector maestro_staging")

    def test_seeds_uses_seed_verb(self, gen):
        cmd = gen._get_dbt_command({"name": "maestro_seeds"})
        assert cmd.startswith("dbt seed --selector maestro_seeds")

    def test_snapshots_uses_snapshot_verb(self, gen):
        cmd = gen._get_dbt_command({"name": "maestro_snapshots"})
        assert cmd.startswith("dbt snapshot --selector maestro_snapshots")

    def test_full_refresh_includes_flag(self, gen):
        cmd = gen._get_dbt_command({"name": "maestro_full_refresh_incremental"})
        assert "dbt build --full-refresh --selector maestro_full_refresh_incremental" in cmd

    def test_includes_target(self):
        cfg = AirflowConfig(dbt_target="production")
        g = AirflowDAGGenerator(cfg)
        assert "--target production" in g._get_dbt_command({"name": "maestro_staging"})

    def test_includes_project_dir(self):
        cfg = AirflowConfig(dbt_project_dir="/app/dbt")
        g = AirflowDAGGenerator(cfg)
        assert "--project-dir /app/dbt" in g._get_dbt_command({"name": "maestro_staging"})

    def test_includes_profiles_dir(self):
        cfg = AirflowConfig(dbt_profiles_dir="/home/ubuntu/.dbt")
        g = AirflowDAGGenerator(cfg)
        assert "--profiles-dir /home/ubuntu/.dbt" in g._get_dbt_command({"name": "maestro_staging"})

    def test_includes_threads(self):
        cfg = AirflowConfig(dbt_threads=16)
        g = AirflowDAGGenerator(cfg)
        assert "--threads 16" in g._get_dbt_command({"name": "maestro_staging"})

    def test_no_project_dir_when_empty(self, gen):
        cmd = gen._get_dbt_command({"name": "maestro_staging"})
        assert "--project-dir" not in cmd

    def test_no_profiles_dir_when_empty(self, gen):
        cmd = gen._get_dbt_command({"name": "maestro_staging"})
        assert "--profiles-dir" not in cmd


# ---------------------------------------------------------------------------
# FQN model extraction
# ---------------------------------------------------------------------------


class TestExtractFqnModels:
    def test_extracts_union_entries(self, gen, sample_selectors):
        models = gen._extract_fqn_models(sample_selectors[0])
        assert models == {"stg_orders", "stg_customers"}

    def test_empty_definition(self, gen):
        models = gen._extract_fqn_models({"name": "x", "definition": {}})
        assert models == set()

    def test_nested_definition(self, gen):
        sel = {
            "name": "complex",
            "definition": {
                "union": [
                    {"method": "fqn", "value": "model_a"},
                    {
                        "exclude": [{"method": "fqn", "value": "model_b"}],
                        "intersection": [{"method": "fqn", "value": "model_c"}],
                    },
                ]
            },
        }
        models = gen._extract_fqn_models(sel)
        assert "model_a" in models
        assert "model_b" in models
        assert "model_c" in models

    def test_non_fqn_method_ignored(self, gen):
        sel = {
            "name": "path_based",
            "definition": {"union": [{"method": "path", "value": "models/staging"}]},
        }
        assert gen._extract_fqn_models(sel) == set()


# ---------------------------------------------------------------------------
# Dependency resolution - parallel
# ---------------------------------------------------------------------------


class TestParallelMode:
    def test_no_deps_set(self, default_cfg, sample_selectors):
        default_cfg.orchestration_mode = "parallel"
        gen = AirflowDAGGenerator(default_cfg)
        deps = gen._resolve_dependencies(sample_selectors)
        for upstreams in deps.values():
            assert len(upstreams) == 0


# ---------------------------------------------------------------------------
# Dependency resolution - sequential
# ---------------------------------------------------------------------------


class TestSequentialMode:
    def test_first_has_no_deps(self, default_cfg, sample_selectors):
        default_cfg.orchestration_mode = "sequential"
        gen = AirflowDAGGenerator(default_cfg)
        deps = gen._resolve_dependencies(sample_selectors)
        assert len(deps["maestro_staging"]) == 0

    def test_second_depends_on_first(self, default_cfg, sample_selectors):
        default_cfg.orchestration_mode = "sequential"
        gen = AirflowDAGGenerator(default_cfg)
        deps = gen._resolve_dependencies(sample_selectors)
        assert "maestro_staging" in deps["maestro_marts"]

    def test_chain_of_three(self, default_cfg):
        default_cfg.orchestration_mode = "sequential"
        gen = AirflowDAGGenerator(default_cfg)
        sels = [
            {"name": "s1", "definition": {"union": []}},
            {"name": "s2", "definition": {"union": []}},
            {"name": "s3", "definition": {"union": []}},
        ]
        deps = gen._resolve_dependencies(sels)
        assert deps["s1"] == set()
        assert deps["s2"] == {"s1"}
        assert deps["s3"] == {"s2"}

    def test_single_selector_no_deps(self, default_cfg):
        default_cfg.orchestration_mode = "sequential"
        gen = AirflowDAGGenerator(default_cfg)
        sels = [{"name": "only", "definition": {"union": []}}]
        deps = gen._resolve_dependencies(sels)
        assert deps["only"] == set()


# ---------------------------------------------------------------------------
# Dependency resolution - dependency mode (type-based)
# ---------------------------------------------------------------------------


class TestDependencyMode:
    def test_seeds_before_models(self, default_cfg, selectors_with_seeds):
        default_cfg.orchestration_mode = "dependency"
        gen = AirflowDAGGenerator(default_cfg)
        deps = gen._resolve_dependencies(selectors_with_seeds)
        # Both model selectors must depend on seeds
        assert "maestro_seeds" in deps["maestro_staging"]
        assert "maestro_seeds" in deps["maestro_marts"]

    def test_seeds_have_no_upstream(self, default_cfg, selectors_with_seeds):
        default_cfg.orchestration_mode = "dependency"
        gen = AirflowDAGGenerator(default_cfg)
        deps = gen._resolve_dependencies(selectors_with_seeds)
        assert len(deps["maestro_seeds"]) == 0

    def test_snapshots_before_models(self, default_cfg, selectors_with_snapshots):
        default_cfg.orchestration_mode = "dependency"
        gen = AirflowDAGGenerator(default_cfg)
        deps = gen._resolve_dependencies(selectors_with_snapshots)
        assert "maestro_snapshots" in deps["maestro_staging"]
        assert "maestro_snapshots" in deps["maestro_marts"]

    def test_pure_model_selectors_no_deps_between_them(self, default_cfg, sample_selectors):
        default_cfg.orchestration_mode = "dependency"
        gen = AirflowDAGGenerator(default_cfg)
        deps = gen._resolve_dependencies(sample_selectors)
        # staging and marts are both "models" type → same rank → no type-based dep
        assert "maestro_marts" not in deps["maestro_staging"]
        assert "maestro_staging" not in deps["maestro_marts"]

    def test_manifest_cross_selector_dep(self, default_cfg, sample_selectors):
        """If fct_orders depends on stg_orders (different selectors), dep is detected."""
        manifest_data = {
            "nodes": {
                "model.proj.stg_orders": {
                    "name": "stg_orders",
                    "resource_type": "model",
                    "depends_on": {"nodes": []},
                },
                "model.proj.stg_customers": {
                    "name": "stg_customers",
                    "resource_type": "model",
                    "depends_on": {"nodes": []},
                },
                "model.proj.fct_orders": {
                    "name": "fct_orders",
                    "resource_type": "model",
                    "depends_on": {"nodes": ["model.proj.stg_orders"]},
                },
                "model.proj.dim_customers": {
                    "name": "dim_customers",
                    "resource_type": "model",
                    "depends_on": {"nodes": ["model.proj.stg_customers"]},
                },
            }
        }
        default_cfg.orchestration_mode = "dependency"
        gen = AirflowDAGGenerator(default_cfg, manifest_data=manifest_data)
        deps = gen._resolve_dependencies(sample_selectors)
        # maestro_marts depends on maestro_staging because fct_orders → stg_orders
        assert "maestro_staging" in deps["maestro_marts"]


# ---------------------------------------------------------------------------
# Selector filtering
# ---------------------------------------------------------------------------


class TestSelectorFiltering:
    def test_freshness_selectors_removed(self, gen):
        sels = [
            {"name": "maestro_staging", "definition": {"union": []}},
            {"name": "freshness_staging", "definition": {"union": []}},
            {"name": "automatically_generated_freshness_x", "definition": {"union": []}},
        ]
        filtered = gen._filter_selectors(sels)
        assert len(filtered) == 1
        assert filtered[0]["name"] == "maestro_staging"

    def test_all_pass_when_no_freshness(self, gen, sample_selectors):
        filtered = gen._filter_selectors(sample_selectors)
        assert len(filtered) == len(sample_selectors)


# ---------------------------------------------------------------------------
# DAG code generation
# ---------------------------------------------------------------------------


class TestDAGGeneration:
    def test_generates_non_empty_string(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        assert isinstance(code, str) and len(code) > 0

    def test_contains_airflow_imports(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        assert "from airflow import DAG" in code
        assert "from airflow.operators.bash import BashOperator" in code
        assert "from datetime import datetime, timedelta" in code

    def test_contains_dag_id(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        assert 'dag_id="dbt_maestro_dag"' in code

    def test_contains_schedule(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        assert 'schedule_interval="0 6 * * *"' in code

    def test_contains_start_date(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        assert "datetime(2024, 1, 1)" in code

    def test_contains_tags(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        assert "dbt" in code
        assert "maestro" in code

    def test_task_per_selector(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        assert "run_maestro_staging" in code
        assert "run_maestro_marts" in code

    def test_correct_dbt_build_command_in_code(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        assert "dbt build --selector maestro_staging" in code
        assert "dbt build --selector maestro_marts" in code

    def test_freshness_selectors_excluded(self, gen):
        sels = [
            {"name": "maestro_staging", "definition": {"union": []}},
            {"name": "freshness_staging", "definition": {"union": []}},
        ]
        code = gen.generate_dag(sels)
        assert "run_maestro_staging" in code
        assert "freshness_staging" not in code

    def test_valid_python_syntax(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        try:
            compile(code, "<generated>", "exec")
        except SyntaxError as exc:
            pytest.fail(f"Generated DAG has invalid Python syntax: {exc}")

    def test_parallel_mode_no_dep_lines(self, default_cfg, sample_selectors):
        default_cfg.orchestration_mode = "parallel"
        gen = AirflowDAGGenerator(default_cfg)
        code = gen.generate_dag(sample_selectors)
        assert ">>" not in code

    def test_sequential_mode_has_dep_line(self, default_cfg, sample_selectors):
        default_cfg.orchestration_mode = "sequential"
        gen = AirflowDAGGenerator(default_cfg)
        code = gen.generate_dag(sample_selectors)
        assert ">>" in code
        assert "run_maestro_staging >> run_maestro_marts" in code

    def test_seeds_task_uses_dbt_seed(self, gen, selectors_with_seeds):
        code = gen.generate_dag(selectors_with_seeds)
        assert "dbt seed --selector maestro_seeds" in code

    def test_snapshots_task_uses_dbt_snapshot(self, gen, selectors_with_snapshots):
        code = gen.generate_dag(selectors_with_snapshots)
        assert "dbt snapshot --selector maestro_snapshots" in code

    def test_retries_in_default_args(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        assert '"retries": 1' in code

    def test_retry_delay_in_default_args(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        assert "timedelta(minutes=5)" in code

    def test_catchup_false(self, gen, sample_selectors):
        code = gen.generate_dag(sample_selectors)
        assert "catchup=False" in code

    def test_custom_dag_id(self, default_cfg, sample_selectors):
        default_cfg.dag_id = "my_custom_dag"
        gen = AirflowDAGGenerator(default_cfg)
        code = gen.generate_dag(sample_selectors)
        assert 'dag_id="my_custom_dag"' in code

    def test_multiple_upstreams_rendered_as_list(self, default_cfg):
        """When a downstream has 2+ upstreams, render [u1, u2] >> downstream."""
        default_cfg.orchestration_mode = "parallel"
        gen = AirflowDAGGenerator(default_cfg)
        sels = [
            {"name": "maestro_seeds", "definition": {"union": []}},
            {"name": "maestro_snapshots", "definition": {"union": []}},
            {"name": "maestro_marts", "definition": {"union": []}},
        ]
        # Manually inject deps to test rendering (not resolve_dependencies logic)
        deps = {
            "maestro_seeds": set(),
            "maestro_snapshots": set(),
            "maestro_marts": {"maestro_seeds", "maestro_snapshots"},
        }
        task_vars = {
            "maestro_seeds": "run_maestro_seeds",
            "maestro_snapshots": "run_maestro_snapshots",
            "maestro_marts": "run_maestro_marts",
        }
        dep_lines = gen._render_dependency_lines(sels, deps, task_vars)
        assert len(dep_lines) == 1
        assert "[" in dep_lines[0]
        assert "run_maestro_marts" in dep_lines[0]


# ---------------------------------------------------------------------------
# File writing
# ---------------------------------------------------------------------------


class TestFileWriting:
    def test_write_creates_file(self, gen, sample_selectors, tmp_path):
        code = gen.generate_dag(sample_selectors)
        output = tmp_path / "dbt_dag.py"
        gen.write_dag(code, str(output))
        assert output.exists()

    def test_written_content_matches(self, gen, sample_selectors, tmp_path):
        code = gen.generate_dag(sample_selectors)
        output = tmp_path / "dbt_dag.py"
        gen.write_dag(code, str(output))
        assert output.read_text() == code

    def test_creates_parent_directory(self, gen, sample_selectors, tmp_path):
        code = gen.generate_dag(sample_selectors)
        output = tmp_path / "dags" / "subdir" / "dbt_dag.py"
        gen.write_dag(code, str(output))
        assert output.exists()

    def test_generated_file_is_valid_python(self, gen, sample_selectors, tmp_path):
        code = gen.generate_dag(sample_selectors)
        output = tmp_path / "dbt_dag.py"
        gen.write_dag(code, str(output))
        compile(output.read_text(), str(output), "exec")


# ---------------------------------------------------------------------------
# Config parsing (AirflowConfig from YAML)
# ---------------------------------------------------------------------------


class TestAirflowConfigFromYaml:
    def test_defaults_when_no_airflow_section(self, tmp_path):
        from dbt_job_maestro.config import Config

        cfg_file = tmp_path / "maestro.yml"
        cfg_file.write_text("selector:\n  selector_prefix: maestro\n")
        cfg = Config.from_yaml(str(cfg_file))
        assert cfg.airflow.dag_id == "dbt_maestro_dag"
        assert cfg.airflow.orchestration_mode == "dependency"
        assert cfg.airflow.dbt_target == "prod"

    def test_custom_airflow_values(self, tmp_path):
        from dbt_job_maestro.config import Config

        cfg_file = tmp_path / "maestro.yml"
        cfg_file.write_text(
            "airflow:\n"
            "  dag_id: my_dag\n"
            "  schedule_interval: '0 4 * * *'\n"
            "  orchestration_mode: parallel\n"
            "  dbt_target: staging\n"
            "  dbt_threads: 16\n"
        )
        cfg = Config.from_yaml(str(cfg_file))
        assert cfg.airflow.dag_id == "my_dag"
        assert cfg.airflow.schedule_interval == "0 4 * * *"
        assert cfg.airflow.orchestration_mode == "parallel"
        assert cfg.airflow.dbt_target == "staging"
        assert cfg.airflow.dbt_threads == 16

    def test_to_yaml_includes_airflow_section(self, tmp_path):
        from dbt_job_maestro.config import Config

        cfg = Config()
        output = tmp_path / "maestro.yml"
        cfg.to_yaml(str(output))
        content = output.read_text()
        assert "airflow:" in content
        assert "dag_id:" in content
        assert "orchestration_mode:" in content


# ---------------------------------------------------------------------------
# Airflow DAG rendering (requires apache-airflow installed)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _airflow_available(), reason="apache-airflow not installed")
class TestAirflowDAGRendering:
    def test_dag_loads_without_import_errors(self, gen, sample_selectors, tmp_path):
        from airflow.models import DagBag

        code = gen.generate_dag(sample_selectors)
        dag_file = tmp_path / "dbt_maestro_dag.py"
        gen.write_dag(code, str(dag_file))

        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        assert (
            dag_file.stem not in dagbag.import_errors
        ), f"DAG import errors: {dagbag.import_errors}"

    def test_dag_registered_in_dagbag(self, gen, sample_selectors, tmp_path):
        from airflow.models import DagBag

        gen.config.dag_id = "test_dbt_dag"
        code = gen.generate_dag(sample_selectors)
        dag_file = tmp_path / "dbt_maestro_dag.py"
        gen.write_dag(code, str(dag_file))

        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        assert "test_dbt_dag" in dagbag.dags

    def test_dag_has_correct_task_count(self, gen, sample_selectors, tmp_path):
        from airflow.models import DagBag

        gen.config.dag_id = "test_task_count"
        code = gen.generate_dag(sample_selectors)
        dag_file = tmp_path / "dbt_maestro_dag.py"
        gen.write_dag(code, str(dag_file))

        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        dag = dagbag.dags["test_task_count"]
        assert len(dag.tasks) == len(sample_selectors)

    def test_task_ids_match_selectors(self, gen, sample_selectors, tmp_path):
        from airflow.models import DagBag

        gen.config.dag_id = "test_task_ids"
        code = gen.generate_dag(sample_selectors)
        dag_file = tmp_path / "dbt_maestro_dag.py"
        gen.write_dag(code, str(dag_file))

        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        dag = dagbag.dags["test_task_ids"]
        task_ids = {t.task_id for t in dag.tasks}
        expected = {f"run_{s['name']}" for s in sample_selectors}
        assert task_ids == expected

    def test_sequential_mode_dependency_wired(self, default_cfg, sample_selectors, tmp_path):
        from airflow.models import DagBag

        default_cfg.orchestration_mode = "sequential"
        default_cfg.dag_id = "test_seq_dag"
        gen = AirflowDAGGenerator(default_cfg)
        code = gen.generate_dag(sample_selectors)
        dag_file = tmp_path / "dbt_maestro_dag.py"
        gen.write_dag(code, str(dag_file))

        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        dag = dagbag.dags["test_seq_dag"]
        # run_maestro_marts should have run_maestro_staging as upstream
        marts_task = dag.get_task("run_maestro_marts")
        upstream_ids = {t.task_id for t in marts_task.upstream_list}
        assert "run_maestro_staging" in upstream_ids

    def test_parallel_mode_no_dependencies(self, default_cfg, sample_selectors, tmp_path):
        from airflow.models import DagBag

        default_cfg.orchestration_mode = "parallel"
        default_cfg.dag_id = "test_par_dag"
        gen = AirflowDAGGenerator(default_cfg)
        code = gen.generate_dag(sample_selectors)
        dag_file = tmp_path / "dbt_maestro_dag.py"
        gen.write_dag(code, str(dag_file))

        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        dag = dagbag.dags["test_par_dag"]
        for task in dag.tasks:
            assert len(task.upstream_list) == 0
