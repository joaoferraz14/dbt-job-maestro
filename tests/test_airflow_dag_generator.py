"""Tests for the Airflow DAG generator (one DAG file per selector)."""

import pytest

from dbt_job_maestro.config import AirflowConfig, CustomFullRefreshSchedule
from dbt_job_maestro.airflow_dag_generator import AirflowDAGGenerator, GENERATED_MARKER

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _airflow_available() -> bool:
    """Return True only if Airflow can actually be imported and used."""
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
        written, removed = gen.write_dags(dags, str(tmp_path))
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
        assert cfg.airflow.orchestration_mode == "simple"
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
        assert "orchestration_mode:" in content
        assert "min_models_per_dag:" in content

    def test_to_yaml_round_trips(self, tmp_path):
        from dbt_job_maestro.config import Config

        output = tmp_path / "maestro.yml"
        Config().to_yaml(str(output))
        # Generated template must parse back without error
        cfg = Config.from_yaml(str(output))
        assert cfg.airflow.orchestration_mode == "simple"


# ---------------------------------------------------------------------------
# Airflow DAG rendering (requires apache-airflow installed)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _airflow_available(), reason="apache-airflow not installed")
class TestAirflowDAGRendering:
    def test_dags_load_without_import_errors(self, gen, sample_selectors, tmp_path):
        from airflow.models import DagBag

        gen.write_dags(gen.generate_dags(sample_selectors), str(tmp_path))
        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"

    def test_each_selector_registered_as_dag(self, gen, sample_selectors, tmp_path):
        from airflow.models import DagBag

        gen.write_dags(gen.generate_dags(sample_selectors), str(tmp_path))
        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        assert "dbt_maestro_maestro_staging" in dagbag.dags
        assert "dbt_maestro_maestro_marts" in dagbag.dags

    def test_combined_dag_wires_chain(self, default_cfg, tmp_path):
        from airflow.models import DagBag

        default_cfg.min_models_per_dag = 2
        gen = AirflowDAGGenerator(default_cfg)
        sels = [
            {"name": "maestro_s1", "definition": {"union": [{"method": "fqn", "value": "a"}]}},
            {"name": "maestro_s2", "definition": {"union": [{"method": "fqn", "value": "b"}]}},
        ]
        gen.write_dags(gen.generate_dags(sels), str(tmp_path))
        dagbag = DagBag(dag_folder=str(tmp_path), include_examples=False)
        dag = dagbag.dags["dbt_maestro_combined_small_selectors"]
        downstream = dag.get_task("run_maestro_s2")
        assert "run_maestro_s1" in {t.task_id for t in downstream.upstream_list}
