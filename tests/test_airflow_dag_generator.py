"""Tests for the Airflow DAG generator (one DAG file per selector)."""

import pytest

from dbt_job_maestro.config import AirflowConfig, CustomFullRefreshSchedule
from dbt_job_maestro.airflow_dag_generator import AirflowDAGGenerator, GENERATED_MARKER

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("operator", ["bash", "dbt"])
@pytest.mark.parametrize("mode", ["none", "simple"])
def test_all_dag_schedules_are_opt_in(default_cfg, sample_selectors, operator, mode):
    default_cfg.operator = operator
    default_cfg.dbt_project_dir = "/tmp/dbt-project"
    default_cfg.dbt_profiles_dir = "/tmp/dbt-profiles"
    default_cfg.dbt_profile = "test"
    default_cfg.orchestration_mode = mode
    default_cfg.manual_schedule_interval = "0 1 * * *"
    default_cfg.full_refresh.enabled = True
    default_cfg.seeds_full_refresh.enabled = True
    default_cfg.full_refresh.custom_schedules = [
        CustomFullRefreshSchedule(name="custom", models=["model_a"])
    ]
    selectors = sample_selectors + [
        {"name": "manual", "definition": {"union": [{"method": "fqn", "value": "m"}]}}
    ]
    dags = AirflowDAGGenerator(default_cfg).generate_dags(selectors)
    assert len(dags) >= 6
    for source in dags.values():
        assert ("schedule_interval=None" in source) is (mode == "none")


def test_default_dags_have_no_schedule(sample_selectors):
    config = AirflowConfig()
    assert config.orchestration_mode == "none"
    dags = AirflowDAGGenerator(config).generate_dags(sample_selectors)
    assert dags
    assert all("schedule_interval=None" in source for source in dags.values())


def _airflow_available() -> bool:
    """Return True only if Airflow can actually be imported and used."""
    try:
        from airflow.models import DagBag  # noqa: F401

        return True
    except Exception:
        return False


def _installed_airflow_major() -> int:
    """Return the major version of the installed Airflow, defaulting to 2.

    The rendering tests below load generated DAGs into a real DagBag, so they
    must generate for whichever Airflow is actually installed - Airflow 3
    rejects ``schedule_interval`` outright.
    """
    try:
        import airflow

        return int(airflow.__version__.split(".")[0])
    except Exception:
        return 2


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def default_cfg():
    return AirflowConfig(
        dag_id_prefix="dbt_maestro",
        schedule_interval="0 6 * * *",
        start_date="2024-01-01",
        owner="airflow",
        retries=1,
        retry_delay_minutes=5,
        dbt_target="prod",
        dbt_threads=4,
        selector_prefix="maestro",
        orchestration_mode="simple",
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
        g = AirflowDAGGenerator(AirflowConfig(dbt_target="production"))
        assert "--target production" in g._get_dbt_command({"name": "maestro_staging"})

    def test_includes_project_dir(self):
        g = AirflowDAGGenerator(AirflowConfig(dbt_project_dir="/app/dbt"))
        assert "--project-dir /app/dbt" in g._get_dbt_command({"name": "maestro_staging"})

    def test_includes_profiles_dir(self):
        g = AirflowDAGGenerator(AirflowConfig(dbt_profiles_dir="/home/ubuntu/.dbt"))
        assert "--profiles-dir /home/ubuntu/.dbt" in g._get_dbt_command({"name": "maestro_staging"})

    def test_includes_threads(self):
        g = AirflowDAGGenerator(AirflowConfig(dbt_threads=16))
        assert "--threads 16" in g._get_dbt_command({"name": "maestro_staging"})

    def test_no_project_dir_when_empty(self, gen):
        assert "--project-dir" not in gen._get_dbt_command({"name": "maestro_staging"})


# ---------------------------------------------------------------------------
# One DAG file per selector
# ---------------------------------------------------------------------------


class TestOneDagPerSelector:
    def test_one_file_per_selector(self, gen, sample_selectors):
        dags = gen.generate_dags(sample_selectors)
        assert set(dags) == {
            "dbt_maestro_maestro_staging.py",
            "dbt_maestro_maestro_marts.py",
        }

    def test_each_dag_has_unique_dag_id(self, gen, sample_selectors):
        dags = gen.generate_dags(sample_selectors)
        assert 'dag_id="dbt_maestro_maestro_staging"' in dags["dbt_maestro_maestro_staging.py"]
        assert 'dag_id="dbt_maestro_maestro_marts"' in dags["dbt_maestro_maestro_marts.py"]

    def test_files_carry_generated_marker(self, gen, sample_selectors):
        dags = gen.generate_dags(sample_selectors)
        for src in dags.values():
            assert GENERATED_MARKER in src

    def test_all_files_are_valid_python(self, gen, sample_selectors):
        for name, src in gen.generate_dags(sample_selectors).items():
            compile(src, name, "exec")

    def test_freshness_and_full_refresh_selector_excluded(self, gen):
        sels = [
            {"name": "maestro_staging", "definition": {"union": []}},
            {"name": "freshness_staging", "definition": {"union": []}},
            {"name": "automatically_generated_freshness_x", "definition": {"union": []}},
            {"name": "maestro_full_refresh_incremental", "definition": {"union": []}},
        ]
        dags = gen.generate_dags(sels)
        assert set(dags) == {"dbt_maestro_maestro_staging.py"}

    def test_dag_contains_dbt_command(self, gen, sample_selectors):
        dags = gen.generate_dags(sample_selectors)
        assert "dbt build --selector maestro_staging" in dags["dbt_maestro_maestro_staging.py"]

    def test_seeds_dag_uses_seed_verb(self, gen, selectors_with_seeds):
        dags = gen.generate_dags(selectors_with_seeds)
        assert "dbt seed --selector maestro_seeds" in dags["dbt_maestro_maestro_seeds.py"]

    def test_hyphenated_selector_names_compile(self, gen):
        """Selector names with hyphens/dots must still yield valid Python."""
        sels = [
            {
                "name": "ae-dbt-prod-unscheduled-fct_all_finance_data",
                "definition": {"union": [{"method": "fqn", "value": "fct_all_finance_data"}]},
            }
        ]
        dags = gen.generate_dags(sels)
        fname = "dbt_maestro_ae-dbt-prod-unscheduled-fct_all_finance_data.py"
        assert fname in dags
        src = dags[fname]
        compile(src, fname, "exec")  # would raise SyntaxError before the fix
        # task_id preserves the original name; variable is sanitised
        assert 'task_id="run_ae-dbt-prod-unscheduled-fct_all_finance_data"' in src
        assert "run_ae_dbt_prod_unscheduled_fct_all_finance_data = BashOperator(" in src

    def test_combined_special_char_names_chain_compiles(self, default_cfg):
        default_cfg.min_models_per_dag = 5
        gen = AirflowDAGGenerator(default_cfg)
        sels = [
            {"name": "maestro_a-one", "definition": {"union": [{"method": "fqn", "value": "x"}]}},
            {"name": "maestro_b.two", "definition": {"union": [{"method": "fqn", "value": "y"}]}},
        ]
        dags = gen.generate_dags(sels)
        combined = dags["dbt_maestro_combined_small_selectors.py"]
        compile(combined, "combined", "exec")
        assert "run_maestro_a_one >> run_maestro_b_two" in combined

    def test_imports_and_catchup(self, gen, sample_selectors):
        src = gen.generate_dags(sample_selectors)["dbt_maestro_maestro_staging.py"]
        assert "from airflow import DAG" in src
        assert "from airflow.operators.bash import BashOperator" in src
        assert "catchup=False" in src
        assert "datetime(2024, 1, 1)" in src

    def test_explicit_bash_operator_matches_legacy_default(self, default_cfg, sample_selectors):
        default_source = AirflowDAGGenerator(default_cfg).generate_dags(sample_selectors)
        default_cfg.operator = "bash"
        explicit_source = AirflowDAGGenerator(default_cfg).generate_dags(sample_selectors)
        assert explicit_source == default_source


class TestDbtOperator:
    def test_generates_dbt_build_and_deps_operators(self, default_cfg, sample_selectors):
        default_cfg.operator = "dbt"
        default_cfg.dbt_profile = "analytics"
        default_cfg.dbt_project_dir = "/app/dbt"
        default_cfg.dbt_profiles_dir = "/app/profiles"
        default_cfg.dbt_target = "production"
        default_cfg.dbt_threads = 12
        default_cfg.dbt_deps_pool = "dbt_deps"

        source = AirflowDAGGenerator(default_cfg).generate_dags(sample_selectors)[
            "dbt_maestro_maestro_staging.py"
        ]

        assert "from cosmos import (" in source
        for operator in (
            "DbtBuildLocalOperator",
            "DbtRunLocalOperator",
            "DbtSeedLocalOperator",
            "DbtSnapshotLocalOperator",
            "DbtTestLocalOperator",
            "ProfileConfig",
        ):
            assert f"    {operator}," in source
        assert "profile_name='analytics'" in source
        assert "target_name='production'" in source
        assert "profiles_yml_filepath='/app/profiles/profiles.yml'" in source
        assert "run_maestro_staging = DbtBuildLocalOperator(" in source
        assert "dbt_cmd_flags=['--selector', 'maestro_staging', '--threads', '12']" in source
        assert "project_dir='/app/dbt'" in source
        assert "dbt_deps = BashOperator(" in source
        assert (
            'bash_command="dbt deps --project-dir /app/dbt --profiles-dir /app/profiles"' in source
        )
        assert 'pool="dbt_deps"' in source
        assert "dbt_deps >> run_maestro_staging" in source
        assert source.count("BashOperator(") == 1
        compile(source, "dbt_operator_dag", "exec")

    def test_maps_run_test_to_chained_dbt_operators(self, default_cfg):
        default_cfg.operator = "dbt"
        default_cfg.dbt_profile = "analytics"
        default_cfg.default_command = "run_test"
        source = AirflowDAGGenerator(default_cfg).generate_dags(
            [{"name": "maestro_staging", "definition": {"union": []}}]
        )["dbt_maestro_maestro_staging.py"]

        assert "run_maestro_staging = DbtRunLocalOperator(" in source
        assert "test_maestro_staging = DbtTestLocalOperator(" in source
        assert "run_maestro_staging >> test_maestro_staging" in source

    def test_infers_profile_from_dbt_project(self, default_cfg, tmp_path):
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: maestro_test\nversion: '1.0.0'\nprofile: analytics\n"
        )
        default_cfg.operator = "dbt"
        default_cfg.dbt_project_dir = str(project_dir)
        source = AirflowDAGGenerator(default_cfg).generate_dags(
            [{"name": "maestro_staging", "definition": {"union": []}}]
        )["dbt_maestro_maestro_staging.py"]

        assert "profile_name='analytics'" in source

    def test_dbt_operator_preserves_paths_with_spaces(self, default_cfg):
        default_cfg.operator = "dbt"
        default_cfg.dbt_profile = "analytics"
        default_cfg.dbt_project_dir = "/opt/dbt projects/analytics"
        default_cfg.dbt_profiles_dir = "/opt/airflow profiles"
        source = AirflowDAGGenerator(default_cfg).generate_dags(
            [{"name": "maestro_staging", "definition": {"union": []}}]
        )["dbt_maestro_maestro_staging.py"]

        assert "project_dir='/opt/dbt projects/analytics'" in source
        assert "profiles_yml_filepath='/opt/airflow profiles/profiles.yml'" in source
        assert (
            "bash_command=\"dbt deps --project-dir '/opt/dbt projects/analytics' "
            "--profiles-dir '/opt/airflow profiles'\""
        ) in source
        compile(source, "dbt_operator_paths", "exec")

    def test_maps_seed_snapshot_and_full_refresh_commands(self, default_cfg):
        default_cfg.operator = "dbt"
        default_cfg.dbt_profile = "analytics"
        default_cfg.run_dbt_deps = False
        default_cfg.full_refresh.enabled = True
        selectors = [
            {"name": "maestro_seeds", "definition": {"union": []}},
            {"name": "maestro_snapshots", "definition": {"union": []}},
            {"name": "maestro_full_refresh_incremental", "definition": {"union": []}},
        ]
        source_by_name = AirflowDAGGenerator(default_cfg).generate_dags(selectors)

        assert "DbtSeedLocalOperator(" in source_by_name["dbt_maestro_maestro_seeds.py"]
        assert "DbtSnapshotLocalOperator(" in source_by_name["dbt_maestro_maestro_snapshots.py"]
        full_refresh = source_by_name["dbt_maestro_full_refresh_incremental.py"]
        assert "DbtBuildLocalOperator(" in full_refresh
        assert "'--full-refresh'" in full_refresh

    def test_maps_custom_full_refresh_selection(self, default_cfg):
        default_cfg.operator = "dbt"
        default_cfg.dbt_profile = "analytics"
        default_cfg.run_dbt_deps = False
        default_cfg.full_refresh.custom_schedules = [
            CustomFullRefreshSchedule(
                name="tagged", cron_schedule="0 3 * * 1", tags=["weekly", "critical"]
            )
        ]
        source = AirflowDAGGenerator(default_cfg).generate_dags([])[
            "dbt_maestro_full_refresh_tagged.py"
        ]
        assert "DbtBuildLocalOperator(" in source
        assert "'--full-refresh', '--select', 'tag:weekly', 'tag:critical'" in source

    def test_missing_profile_is_reported_for_dbt_operator(self, default_cfg):
        default_cfg.operator = "dbt"
        generator = AirflowDAGGenerator(default_cfg)
        with pytest.raises(ValueError, match=r"airflow\.dbt_profile.*dbt_project\.yml"):
            generator.generate_dags([{"name": "maestro_staging", "definition": {"union": []}}])


# ---------------------------------------------------------------------------
# "dbt deps" task
# ---------------------------------------------------------------------------


class TestDbtDepsTask:
    def test_deps_task_present_by_default(self, gen, sample_selectors):
        src = gen.generate_dags(sample_selectors)["dbt_maestro_maestro_staging.py"]
        assert 'task_id="dbt_deps"' in src
        assert 'bash_command="dbt deps"' in src
        assert "dbt_deps >> run_maestro_staging" in src

    def test_deps_task_uses_project_and_profiles_dir_only(self):
        g = AirflowDAGGenerator(
            AirflowConfig(
                dbt_project_dir="/app/dbt",
                dbt_profiles_dir="/home/ubuntu/.dbt",
                dbt_target="production",
                dbt_threads=16,
            )
        )
        _, cmd = g._dbt_deps_task()
        assert cmd == "dbt deps --project-dir /app/dbt --profiles-dir /home/ubuntu/.dbt"
        assert "--target" not in cmd
        assert "--threads" not in cmd

    def test_deps_task_can_be_disabled(self, default_cfg):
        default_cfg.run_dbt_deps = False
        gen = AirflowDAGGenerator(default_cfg)
        src = gen.generate_dags([{"name": "maestro_staging", "definition": {"union": []}}])[
            "dbt_maestro_maestro_staging.py"
        ]
        assert "dbt_deps" not in src
        assert ">>" not in src

    def test_deps_task_no_pool_by_default(self, gen, sample_selectors):
        src = gen.generate_dags(sample_selectors)["dbt_maestro_maestro_staging.py"]
        assert "pool=" not in src

    def test_deps_task_uses_configured_pool(self, default_cfg):
        default_cfg.dbt_deps_pool = "dbt_deps"
        gen = AirflowDAGGenerator(default_cfg)
        src = gen.generate_dags([{"name": "maestro_staging", "definition": {"union": []}}])[
            "dbt_maestro_maestro_staging.py"
        ]
        assert 'pool="dbt_deps"' in src
        compile(src, "dag", "exec")

    def test_deps_task_gates_run_test_chain(self, default_cfg):
        default_cfg.default_command = "run_test"
        gen = AirflowDAGGenerator(default_cfg)
        src = gen.generate_dags([{"name": "maestro_staging", "definition": {"union": []}}])[
            "dbt_maestro_maestro_staging.py"
        ]
        assert "dbt_deps >> run_maestro_staging" in src
        assert "run_maestro_staging >> test_maestro_staging" in src

    def test_deps_task_gates_every_group_in_combined_dag(self, default_cfg):
        default_cfg.min_models_per_dag = 5
        gen = AirflowDAGGenerator(default_cfg)
        sels = [
            {"name": "maestro_a", "definition": {"union": [{"method": "fqn", "value": "a"}]}},
            {"name": "maestro_b", "definition": {"union": [{"method": "fqn", "value": "b"}]}},
        ]
        src = gen.generate_dags(sels)["dbt_maestro_combined_small_selectors.py"]
        assert "dbt_deps >> run_maestro_a" in src
        assert "dbt_deps >> run_maestro_b" in src

    def test_all_files_are_valid_python(self, gen, sample_selectors):
        for name, src in gen.generate_dags(sample_selectors).items():
            compile(src, name, "exec")


# ---------------------------------------------------------------------------
# Selector inclusion
# ---------------------------------------------------------------------------


class TestSelectorInclusion:
    def test_exclude_maestro(self, default_cfg):
        default_cfg.include_maestro_selectors_in_dags = False
        gen = AirflowDAGGenerator(default_cfg)
        sels = [
            {"name": "maestro_staging", "definition": {"union": []}},
            {"name": "critical_revenue", "definition": {"union": []}},
        ]
        dags = gen.generate_dags(sels)
        assert set(dags) == {"dbt_maestro_critical_revenue.py"}

    def test_exclude_manual(self, default_cfg):
        default_cfg.include_manual_selectors_in_dags = False
        gen = AirflowDAGGenerator(default_cfg)
        sels = [
            {"name": "maestro_staging", "definition": {"union": []}},
            {"name": "critical_revenue", "definition": {"union": []}},
        ]
        dags = gen.generate_dags(sels)
        assert set(dags) == {"dbt_maestro_maestro_staging.py"}


# ---------------------------------------------------------------------------
# Schedules & SLA
# ---------------------------------------------------------------------------


class TestSchedulesAndSla:
    def test_simple_mode_uses_schedule_interval(self, gen, sample_selectors):
        src = gen.generate_dags(sample_selectors)["dbt_maestro_maestro_staging.py"]
        assert 'schedule_interval="0 6 * * *"' in src

    def test_none_mode_disables_schedule(self, default_cfg, sample_selectors):
        default_cfg.orchestration_mode = "none"
        gen = AirflowDAGGenerator(default_cfg)
        src = gen.generate_dags(sample_selectors)["dbt_maestro_maestro_staging.py"]
        assert "schedule_interval=None" in src

    def test_staggered_mode_offsets_crons(self, default_cfg, sample_selectors):
        default_cfg.orchestration_mode = "staggered"
        default_cfg.start_hour = 6
        default_cfg.start_minute = 0
        default_cfg.cron_increment_minutes = 30
        gen = AirflowDAGGenerator(default_cfg)
        dags = gen.generate_dags(sample_selectors)
        crons = sorted(
            next(line for line in src.splitlines() if "schedule_interval=" in line).strip()
            for src in dags.values()
        )
        # first DAG at 06:00, second at 06:30
        assert 'schedule_interval="0 6 * * *",' in crons
        assert 'schedule_interval="30 6 * * *",' in crons

    def test_manual_schedule_override(self, default_cfg):
        default_cfg.manual_schedule_interval = "0 */2 * * *"
        gen = AirflowDAGGenerator(default_cfg)
        sels = [
            {"name": "maestro_staging", "definition": {"union": []}},
            {"name": "critical_revenue", "definition": {"union": []}},
        ]
        dags = gen.generate_dags(sels)
        assert 'schedule_interval="0 */2 * * *"' in dags["dbt_maestro_critical_revenue.py"]
        assert 'schedule_interval="0 6 * * *"' in dags["dbt_maestro_maestro_staging.py"]

    def test_sla_emitted_when_set(self, default_cfg, sample_selectors):
        default_cfg.sla_minutes = 90
        gen = AirflowDAGGenerator(default_cfg)
        src = gen.generate_dags(sample_selectors)["dbt_maestro_maestro_staging.py"]
        assert '"sla": timedelta(minutes=90)' in src

    def test_no_sla_when_zero(self, gen, sample_selectors):
        src = gen.generate_dags(sample_selectors)["dbt_maestro_maestro_staging.py"]
        assert '"sla"' not in src

    def test_manual_sla_override(self, default_cfg):
        default_cfg.sla_minutes = 120
        default_cfg.manual_sla_minutes = 30
        gen = AirflowDAGGenerator(default_cfg)
        sels = [
            {"name": "maestro_staging", "definition": {"union": []}},
            {"name": "critical_revenue", "definition": {"union": []}},
        ]
        dags = gen.generate_dags(sels)
        assert "timedelta(minutes=30)" in dags["dbt_maestro_critical_revenue.py"]
        assert '"sla": timedelta(minutes=120)' in dags["dbt_maestro_maestro_staging.py"]

    def test_manual_sla_inherits_when_negative(self, default_cfg):
        default_cfg.sla_minutes = 120
        default_cfg.manual_sla_minutes = -1
        gen = AirflowDAGGenerator(default_cfg)
        dags = gen.generate_dags([{"name": "critical_revenue", "definition": {"union": []}}])
        assert '"sla": timedelta(minutes=120)' in dags["dbt_maestro_critical_revenue.py"]


# ---------------------------------------------------------------------------
# Combining small selectors (min_models_per_dag)
# ---------------------------------------------------------------------------


class TestMinModelsPerDag:
    @pytest.fixture
    def mixed(self):
        return [
            {
                "name": "maestro_big",
                "definition": {
                    "union": [
                        {"method": "fqn", "value": "a"},
                        {"method": "fqn", "value": "b"},
                        {"method": "fqn", "value": "c"},
                    ]
                },
            },
            {"name": "maestro_small1", "definition": {"union": [{"method": "fqn", "value": "x"}]}},
            {"name": "maestro_small2", "definition": {"union": [{"method": "fqn", "value": "y"}]}},
        ]

    def test_disabled_by_default(self, gen, mixed):
        dags = gen.generate_dags(mixed)
        assert "dbt_maestro_combined_small_selectors.py" not in dags
        assert len(dags) == 3

    def test_combines_small_selectors(self, default_cfg, mixed):
        default_cfg.min_models_per_dag = 2
        gen = AirflowDAGGenerator(default_cfg)
        dags = gen.generate_dags(mixed)
        assert "dbt_maestro_maestro_big.py" in dags
        assert "dbt_maestro_combined_small_selectors.py" in dags
        combined = dags["dbt_maestro_combined_small_selectors.py"]
        assert "dbt build --selector maestro_small1" in combined
        assert "dbt build --selector maestro_small2" in combined

    def test_combined_dag_chains_tasks(self, default_cfg, mixed):
        default_cfg.min_models_per_dag = 2
        gen = AirflowDAGGenerator(default_cfg)
        combined = gen.generate_dags(mixed)["dbt_maestro_combined_small_selectors.py"]
        assert "run_maestro_small1 >> run_maestro_small2" in combined

    def test_manual_selectors_never_combined(self, default_cfg):
        default_cfg.min_models_per_dag = 5
        gen = AirflowDAGGenerator(default_cfg)
        sels = [{"name": "tiny_manual", "definition": {"union": [{"method": "fqn", "value": "z"}]}}]
        dags = gen.generate_dags(sels)
        assert "dbt_maestro_tiny_manual.py" in dags
        assert "dbt_maestro_combined_small_selectors.py" not in dags


# ---------------------------------------------------------------------------
# Manual vs auto DAG-kind tags
# ---------------------------------------------------------------------------


class TestDagKindTags:
    def test_auto_selector_tagged_auto(self, gen, sample_selectors):
        src = gen.generate_dags(sample_selectors)["dbt_maestro_maestro_staging.py"]
        assert "tags=['dbt', 'maestro', 'auto']" in src

    def test_manual_selector_tagged_manual(self, gen):
        sels = [{"name": "critical_revenue", "definition": {"union": []}}]
        src = gen.generate_dags(sels)["dbt_maestro_critical_revenue.py"]
        assert "'manual'" in src
        assert "'auto'" not in src

    def test_combined_dag_tagged_auto_combined(self, default_cfg, sample_selectors):
        default_cfg.min_models_per_dag = 5
        gen = AirflowDAGGenerator(default_cfg)
        # both sample selectors have 2 fqn models -> both combined
        src = gen.generate_dags(sample_selectors)["dbt_maestro_combined_small_selectors.py"]
        assert "'auto'" in src and "'combined'" in src

    def test_full_refresh_tagged_full_refresh(self, default_cfg, sample_selectors):
        default_cfg.full_refresh.enabled = True
        gen = AirflowDAGGenerator(default_cfg)
        src = gen.generate_dags(sample_selectors)["dbt_maestro_full_refresh_incremental.py"]
        assert "'auto'" in src and "'full-refresh'" in src

    def test_kind_tag_not_duplicated_in_base_tags(self, default_cfg, sample_selectors):
        default_cfg.tags = ["dbt", "maestro", "auto"]
        gen = AirflowDAGGenerator(default_cfg)
        src = gen.generate_dags(sample_selectors)["dbt_maestro_maestro_staging.py"]
        # 'auto' already present in base tags -> appears exactly once
        assert src.count("'auto'") == 1


# ---------------------------------------------------------------------------
# Full refresh DAGs
# ---------------------------------------------------------------------------


class TestFullRefreshDags:
    def test_auto_full_refresh_dag(self, default_cfg, sample_selectors):
        default_cfg.full_refresh.enabled = True
        default_cfg.full_refresh.cron_schedule = "0 0 * * 0"
        gen = AirflowDAGGenerator(default_cfg)
        dags = gen.generate_dags(sample_selectors)
        assert "dbt_maestro_full_refresh_incremental.py" in dags
        src = dags["dbt_maestro_full_refresh_incremental.py"]
        assert "dbt build --full-refresh --selector maestro_full_refresh_incremental" in src
        assert 'schedule_interval="0 0 * * 0"' in src

    def test_custom_full_refresh_dag(self, default_cfg, sample_selectors):
        default_cfg.full_refresh.custom_schedules = [
            CustomFullRefreshSchedule(
                name="weekly_customers", cron_schedule="0 3 * * 1", selector="maestro_customers"
            )
        ]
        gen = AirflowDAGGenerator(default_cfg)
        dags = gen.generate_dags(sample_selectors)
        assert "dbt_maestro_full_refresh_weekly_customers.py" in dags
        src = dags["dbt_maestro_full_refresh_weekly_customers.py"]
        assert "dbt build --full-refresh --selector maestro_customers" in src
        assert 'schedule_interval="0 3 * * 1"' in src

    def test_seeds_full_refresh_dag(self, default_cfg, sample_selectors):
        default_cfg.seeds_full_refresh.enabled = True
        default_cfg.seeds_full_refresh.cron_schedule = "0 1 * * 0"
        gen = AirflowDAGGenerator(default_cfg)
        dags = gen.generate_dags(sample_selectors)
        assert "dbt_maestro_seeds_full_refresh.py" in dags
        assert "dbt seed --full-refresh --selector maestro_seeds" in (
            dags["dbt_maestro_seeds_full_refresh.py"]
        )


# ---------------------------------------------------------------------------
# Idempotent writing & cleanup
# ---------------------------------------------------------------------------


class TestWriteDags:
    def test_writes_all_files(self, gen, sample_selectors, tmp_path):
        dags = gen.generate_dags(sample_selectors)
        written, removed = gen.write_dags(dags, str(tmp_path))
        assert set(written) == set(dags)
        assert removed == []
        for name in dags:
            assert (tmp_path / name).exists()

    def test_creates_directory(self, gen, sample_selectors, tmp_path):
        target = tmp_path / "dags" / "nested"
        dags = gen.generate_dags(sample_selectors)
        gen.write_dags(dags, str(target))
        assert target.exists()

    def test_removes_stale_generated_files(self, gen, sample_selectors, tmp_path):
        stale = tmp_path / "dbt_maestro_old.py"
        stale.write_text(f'"""old"""\n{GENERATED_MARKER}\n')
        dags = gen.generate_dags(sample_selectors)
        _, removed = gen.write_dags(dags, str(tmp_path))
        assert "dbt_maestro_old.py" in removed
        assert not stale.exists()

    def test_removes_legacy_single_dag_file(self, gen, sample_selectors, tmp_path):
        """A pre-rewrite dbt_maestro_dag.py (docstring marker only) is cleaned up."""
        legacy = tmp_path / "dbt_maestro_dag.py"
        legacy.write_text(
            '"""\nAuto-generated by dbt-job-maestro.\n'
            'DO NOT EDIT - regenerate with: maestro generate-dags\n"""\n'
        )
        _, removed = gen.write_dags(gen.generate_dags(sample_selectors), str(tmp_path))
        assert "dbt_maestro_dag.py" in removed
        assert not legacy.exists()

    def test_preserves_handwritten_files(self, gen, sample_selectors, tmp_path):
        handwritten = tmp_path / "my_dag.py"
        handwritten.write_text("# hand written\nprint('hi')\n")
        dags = gen.generate_dags(sample_selectors)
        _, removed = gen.write_dags(dags, str(tmp_path))
        assert "my_dag.py" not in removed
        assert handwritten.exists()

    def test_regeneration_is_idempotent(self, gen, sample_selectors, tmp_path):
        dags = gen.generate_dags(sample_selectors)
        gen.write_dags(dags, str(tmp_path))
        first = {p.name: p.read_text() for p in tmp_path.glob("*.py")}
        gen.write_dags(gen.generate_dags(sample_selectors), str(tmp_path))
        second = {p.name: p.read_text() for p in tmp_path.glob("*.py")}
        assert first == second


# ---------------------------------------------------------------------------
# Config parsing (AirflowConfig from YAML)
# ---------------------------------------------------------------------------


class TestAirflowConfigFromYaml:
    def test_defaults_when_no_airflow_section(self, tmp_path):
        from dbt_job_maestro.config import Config

        cfg_file = tmp_path / "maestro.yml"
        cfg_file.write_text("selector:\n  selector_prefix: maestro\n")
        cfg = Config.from_yaml(str(cfg_file))
        assert cfg.airflow.dag_id_prefix == "dbt_maestro"
        assert cfg.airflow.orchestration_mode == "none"
        assert cfg.airflow.dbt_target == "prod"
        assert cfg.airflow.min_models_per_dag == 1

    def test_custom_airflow_values(self, tmp_path):
        from dbt_job_maestro.config import Config

        cfg_file = tmp_path / "maestro.yml"
        cfg_file.write_text(
            "airflow:\n"
            "  dag_id_prefix: my_dags\n"
            "  schedule_interval: '0 4 * * *'\n"
            "  manual_schedule_interval: '0 */2 * * *'\n"
            "  orchestration_mode: staggered\n"
            "  sla_minutes: 60\n"
            "  min_models_per_dag: 3\n"
            "  dbt_threads: 16\n"
        )
        cfg = Config.from_yaml(str(cfg_file))
        assert cfg.airflow.dag_id_prefix == "my_dags"
        assert cfg.airflow.schedule_interval == "0 4 * * *"
        assert cfg.airflow.manual_schedule_interval == "0 */2 * * *"
        assert cfg.airflow.orchestration_mode == "staggered"
        assert cfg.airflow.sla_minutes == 60
        assert cfg.airflow.min_models_per_dag == 3
        assert cfg.airflow.dbt_threads == 16

    def test_invalid_orchestration_mode_raises(self, tmp_path):
        from dbt_job_maestro.config import Config

        cfg_file = tmp_path / "maestro.yml"
        cfg_file.write_text("airflow:\n  orchestration_mode: bogus\n")
        with pytest.raises(ValueError):
            Config.from_yaml(str(cfg_file))

    def test_legacy_orchestration_mode_normalized(self, tmp_path):
        """Pre-rewrite modes (parallel/sequential/dependency) collapse to 'simple'."""
        from dbt_job_maestro.config import Config

        for legacy in ("parallel", "sequential", "dependency"):
            cfg_file = tmp_path / f"maestro_{legacy}.yml"
            cfg_file.write_text(f"airflow:\n  orchestration_mode: {legacy}\n")
            cfg = Config.from_yaml(str(cfg_file))
            assert cfg.airflow.orchestration_mode == "simple"

    def test_to_yaml_includes_airflow_section(self, tmp_path):
        from dbt_job_maestro.config import Config

        cfg = Config()
        output = tmp_path / "maestro.yml"
        cfg.to_yaml(str(output))
        content = output.read_text()
        assert "airflow:" in content
        assert "dag_id_prefix:" in content
        assert "operator: bash" in content
        assert "orchestration_mode:" in content
        assert "min_models_per_dag:" in content

    def test_to_yaml_round_trips(self, tmp_path):
        from dbt_job_maestro.config import Config

        output = tmp_path / "maestro.yml"
        Config().to_yaml(str(output))
        # Generated template must parse back without error
        cfg = Config.from_yaml(str(output))
        assert cfg.airflow.orchestration_mode == "none"
        assert cfg.job.orchestration_mode == "none"
        assert cfg.airflow.operator == "bash"

    def test_to_yaml_round_trips_dbt_operator_settings(self, tmp_path):
        from dbt_job_maestro.config import Config

        cfg = Config()
        cfg.airflow.operator = "dbt"
        cfg.airflow.dbt_profile = "analytics"
        output = tmp_path / "maestro.yml"
        cfg.to_yaml(str(output))

        loaded = Config.from_yaml(str(output))
        assert loaded.airflow.operator == "dbt"
        assert loaded.airflow.dbt_profile == "analytics"


# ---------------------------------------------------------------------------
# Airflow DAG rendering (requires apache-airflow installed)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _airflow_available(), reason="apache-airflow not installed")
class TestAirflowDAGRendering:
    """Load generated DAGs into a real DagBag.

    Each test targets the installed Airflow major version, so the suite passes
    on both 2.x and 3.x rather than only on whichever one happens to be pinned.
    """

    @pytest.fixture
    def live_cfg(self, default_cfg):
        default_cfg.airflow_version = _installed_airflow_major()
        return default_cfg

    @pytest.fixture
    def live_gen(self, live_cfg):
        return AirflowDAGGenerator(live_cfg)

    def test_dags_load_without_import_errors(self, live_gen, sample_selectors, tmp_path):
        from airflow.models import DagBag

        live_gen.write_dags(live_gen.generate_dags(sample_selectors), str(tmp_path))
        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"

    def test_each_selector_registered_as_dag(self, live_gen, sample_selectors, tmp_path):
        from airflow.models import DagBag

        live_gen.write_dags(live_gen.generate_dags(sample_selectors), str(tmp_path))
        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        assert "dbt_maestro_maestro_staging" in dagbag.dags
        assert "dbt_maestro_maestro_marts" in dagbag.dags

    @pytest.mark.parametrize("operator", ["bash", "dbt"])
    def test_external_trigger_dags_have_no_timetable(
        self, live_cfg, sample_selectors, tmp_path, operator
    ):
        from airflow.models import DagBag

        live_cfg.operator = operator
        live_cfg.dbt_project_dir = str(tmp_path)
        live_cfg.dbt_profiles_dir = str(tmp_path)
        live_cfg.dbt_profile = "test"
        live_cfg.orchestration_mode = "none"
        live_cfg.manual_schedule_interval = "0 1 * * *"
        live_cfg.full_refresh.enabled = True
        live_cfg.seeds_full_refresh.enabled = True
        live_cfg.full_refresh.custom_schedules = [
            CustomFullRefreshSchedule(name="custom", models=["model_a"])
        ]
        generator = AirflowDAGGenerator(live_cfg)
        selectors = sample_selectors + [
            {"name": "manual", "definition": {"union": [{"method": "fqn", "value": "m"}]}}
        ]
        generator.write_dags(generator.generate_dags(selectors), str(tmp_path / "dags"))
        dagbag = DagBag(dag_folder=str(tmp_path / "dags"), include_examples=False)
        assert not dagbag.import_errors, dagbag.import_errors
        assert len(dagbag.dags) >= 6
        for dag in dagbag.dags.values():
            assert dag.timetable.__class__.__name__ == "NullTimetable"

    def test_combined_dag_wires_chain(self, live_cfg, tmp_path):
        from airflow.models import DagBag

        live_cfg.min_models_per_dag = 2
        gen = AirflowDAGGenerator(live_cfg)
        sels = [
            {"name": "maestro_s1", "definition": {"union": [{"method": "fqn", "value": "a"}]}},
            {"name": "maestro_s2", "definition": {"union": [{"method": "fqn", "value": "b"}]}},
        ]
        gen.write_dags(gen.generate_dags(sels), str(tmp_path))
        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        dag = dagbag.dags["dbt_maestro_combined_small_selectors"]
        downstream = dag.get_task("run_maestro_s2")
        assert "run_maestro_s1" in {t.task_id for t in downstream.upstream_list}

    def test_cosmos_dag_imports_and_registers_dbt_operators(self, live_cfg, tmp_path):
        from airflow.models import DagBag
        from airflow import __version__ as airflow_version
        from cosmos.operators.local import (
            DbtRunLocalOperator,
            DbtTestLocalOperator,
        )

        if int(airflow_version.split(".", 1)[0]) >= 3:
            from airflow.providers.standard.operators.bash import BashOperator
        else:
            from airflow.operators.bash import BashOperator

        project_dir = tmp_path / "dbt_project"
        project_dir.mkdir()
        (project_dir / "dbt_project.yml").write_text(
            "name: maestro_test\nversion: '1.0.0'\nprofile: local_profile\n"
        )
        profiles_dir = tmp_path / "profiles"
        profiles_dir.mkdir()
        (profiles_dir / "profiles.yml").write_text(
            "local_profile:\n  target: local\n  outputs:\n    local:\n"
            "      type: duckdb\n      path: ':memory:'\n      threads: 1\n"
        )

        live_cfg.operator = "dbt"
        live_cfg.dbt_project_dir = str(project_dir)
        live_cfg.dbt_profiles_dir = str(profiles_dir)
        live_cfg.dbt_target = "local"
        live_cfg.dbt_threads = 1
        live_cfg.run_dbt_deps = True
        live_cfg.default_command = "run_test"
        live_cfg.min_models_per_dag = 2
        generator = AirflowDAGGenerator(live_cfg)
        selectors = [
            {
                "name": "maestro_staging",
                "definition": {"union": [{"method": "fqn", "value": "staging_model"}]},
            },
            {
                "name": "maestro_marts",
                "definition": {"union": [{"method": "fqn", "value": "mart_model"}]},
            },
        ]
        generator.write_dags(generator.generate_dags(selectors), str(tmp_path / "dags"))

        dagbag = DagBag(dag_folder=str(tmp_path / "dags"), include_examples=False)
        assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"
        dag = dagbag.dags["dbt_maestro_combined_small_selectors"]
        assert isinstance(dag.get_task("dbt_deps"), BashOperator)
        assert isinstance(dag.get_task("run_maestro_staging"), DbtRunLocalOperator)
        assert isinstance(dag.get_task("test_maestro_staging"), DbtTestLocalOperator)
        assert isinstance(dag.get_task("run_maestro_marts"), DbtRunLocalOperator)
        assert "dbt_deps" in {
            task.task_id for task in dag.get_task("run_maestro_staging").upstream_list
        }
        assert "test_maestro_staging" in {
            task.task_id for task in dag.get_task("run_maestro_marts").upstream_list
        }


# ---------------------------------------------------------------------------
# Airflow major-version targeting (airflow_version)
# ---------------------------------------------------------------------------


class TestAirflowVersionTargeting:
    """Airflow 3 removed schedule_interval and task SLAs, and moved BashOperator.

    Version 2 output must stay byte-identical to pre-option behaviour so existing
    generated files don't churn.
    """

    def _render(self, default_cfg, version, **overrides):
        default_cfg.airflow_version = version
        for key, value in overrides.items():
            setattr(default_cfg, key, value)
        gen = AirflowDAGGenerator(default_cfg)
        dags = gen.generate_dags(
            [{"name": "maestro_s1", "definition": {"union": [{"method": "fqn", "value": "a"}]}}]
        )
        return next(iter(dags.values()))

    def test_default_is_version_2(self):
        assert AirflowConfig().airflow_version == 2

    def test_v2_uses_schedule_interval_kwarg(self, default_cfg):
        source = self._render(default_cfg, 2)
        assert "schedule_interval=" in source
        assert "\n    schedule=" not in source

    def test_v3_uses_schedule_kwarg(self, default_cfg):
        source = self._render(default_cfg, 3)
        assert "    schedule=" in source
        assert "schedule_interval=" not in source

    def test_v2_uses_legacy_bash_import(self, default_cfg):
        source = self._render(default_cfg, 2)
        assert "from airflow.operators.bash import BashOperator" in source

    def test_v3_uses_provider_bash_import(self, default_cfg):
        source = self._render(default_cfg, 3)
        assert "from airflow.providers.standard.operators.bash import BashOperator" in source
        assert "from airflow.operators.bash import BashOperator" not in source

    def test_v2_emits_sla(self, default_cfg):
        source = self._render(default_cfg, 2, sla_minutes=120)
        assert '"sla": timedelta(minutes=120)' in source

    def test_v3_omits_sla(self, default_cfg):
        source = self._render(default_cfg, 3, sla_minutes=120)
        assert '"sla"' not in source

    def test_v3_none_schedule_still_renders(self, default_cfg):
        source = self._render(default_cfg, 3, orchestration_mode="none")
        assert "    schedule=None," in source

    @pytest.mark.parametrize("version", [1, 4, 0])
    def test_invalid_version_rejected(self, default_cfg, version):
        default_cfg.airflow_version = version
        with pytest.raises(ValueError, match="airflow_version"):
            default_cfg.validate()

    def test_valid_versions_accepted(self, default_cfg):
        for version in (2, 3):
            default_cfg.airflow_version = version
            default_cfg.validate()

    def test_from_yaml_parses_version(self, tmp_path):
        from dbt_job_maestro.config import Config

        config_file = tmp_path / "cfg.yml"
        config_file.write_text("airflow:\n  airflow_version: 3\n")
        assert Config.from_yaml(str(config_file)).airflow.airflow_version == 3

    def test_from_yaml_defaults_to_2(self, tmp_path):
        from dbt_job_maestro.config import Config

        config_file = tmp_path / "cfg.yml"
        config_file.write_text("airflow:\n  dag_id_prefix: dbt_maestro\n")
        assert Config.from_yaml(str(config_file)).airflow.airflow_version == 2

    def test_from_yaml_rejects_invalid_version(self, tmp_path):
        from dbt_job_maestro.config import Config

        config_file = tmp_path / "cfg.yml"
        config_file.write_text("airflow:\n  airflow_version: 4\n")
        with pytest.raises(ValueError, match="airflow_version"):
            Config.from_yaml(str(config_file))

    def test_to_yaml_documents_version(self, tmp_path):
        from dbt_job_maestro.config import Config

        output = tmp_path / "maestro.yml"
        Config().to_yaml(str(output))
        assert "airflow_version:" in output.read_text()
