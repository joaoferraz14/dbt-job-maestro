"""Tests for job generator."""

import pytest
from dbt_job_maestro.config import JobConfig
from dbt_job_maestro.job_generator import JobGenerator


@pytest.fixture
def base_config():
    """Base job config with required fields."""
    return JobConfig(
        account_id=111,
        project_id=222,
        environment_id=333,
        job_name_prefix="maestro",
        selector_prefix="maestro",
        cron_schedule="0 6 * * *",
        orchestration_mode="simple",
        target_name="prod",
        threads=4,
        timeout_seconds=3600,
    )


@pytest.fixture
def sample_selectors():
    """Sample selectors with model counts."""
    return [
        {
            "name": "maestro_staging",
            "definition": {
                "union": [
                    {"method": "fqn", "value": "stg_orders"},
                    {"method": "fqn", "value": "stg_customers"},
                    {"method": "fqn", "value": "stg_products"},
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


class TestSimpleMode:
    """Test simple orchestration mode."""

    def test_generates_jobs_for_each_selector(self, base_config, sample_selectors):
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        jobs = result["jobs"]
        assert len(jobs) == 2

    def test_job_uses_cron_schedule(self, base_config, sample_selectors):
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        for job in result["jobs"].values():
            assert job["triggers"]["schedule"] is True
            assert job["schedule"]["cron"] == "0 6 * * *"

    def test_job_has_correct_execute_step(self, base_config, sample_selectors):
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        job = result["jobs"]["maestro_maestro_staging"]
        assert job["execute_steps"] == ["dbt build --selector maestro_staging"]

    def test_job_has_correct_metadata(self, base_config, sample_selectors):
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        job = result["jobs"]["maestro_maestro_staging"]
        assert job["account_id"] == 111
        assert job["project_id"] == 222
        assert job["environment_id"] == 333
        assert job["settings"]["target_name"] == "prod"
        assert job["settings"]["threads"] == 4
        assert job["execution"]["timeout_seconds"] == 3600


class TestStaggeredMode:
    """Test staggered orchestration mode."""

    def test_jobs_get_staggered_crons(self, base_config, sample_selectors):
        base_config.orchestration_mode = "staggered"
        base_config.start_hour = 6
        base_config.start_minute = 0
        base_config.cron_increment_minutes = 30
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        jobs = list(result["jobs"].values())
        assert jobs[0]["schedule"]["cron"] == "0 6 * * *"
        assert jobs[1]["schedule"]["cron"] == "30 6 * * *"

    def test_stagger_wraps_past_60_minutes(self, base_config):
        base_config.orchestration_mode = "staggered"
        base_config.start_hour = 6
        base_config.start_minute = 0
        base_config.cron_increment_minutes = 30
        selectors = [
            {
                "name": f"maestro_job{i}",
                "definition": {"union": [{"method": "fqn", "value": f"m{i}"}]},
            }
            for i in range(3)
        ]
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(selectors)
        jobs = list(result["jobs"].values())
        assert jobs[0]["schedule"]["cron"] == "0 6 * * *"
        assert jobs[1]["schedule"]["cron"] == "30 6 * * *"
        assert jobs[2]["schedule"]["cron"] == "0 7 * * *"

    def test_stagger_with_days_of_week(self, base_config, sample_selectors):
        base_config.orchestration_mode = "staggered"
        base_config.start_hour = 6
        base_config.start_minute = 0
        base_config.cron_increment_minutes = 30
        base_config.cron_days_of_week = ["MON", "WED", "FRI"]
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        job = list(result["jobs"].values())[0]
        assert job["schedule"]["cron"] == "0 6 * * 1,3,5"

    def test_all_staggered_jobs_have_schedule_true(self, base_config, sample_selectors):
        base_config.orchestration_mode = "staggered"
        base_config.start_hour = 6
        base_config.start_minute = 0
        base_config.cron_increment_minutes = 30
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        for job in result["jobs"].values():
            assert job["triggers"]["schedule"] is True

    def test_backward_compat_cron_incremental(self, base_config, sample_selectors):
        """cron_incremental should be normalized to staggered."""
        from dbt_job_maestro.config import Config
        import tempfile
        import os

        yaml_content = """
job:
  account_id: 111
  project_id: 222
  environment_id: 333
  orchestration_mode: cron_incremental
  start_hour: 6
  start_minute: 0
  cron_increment_minutes: 30
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(yaml_content)
            path = f.name
        try:
            config = Config.from_yaml(path)
            assert config.job.orchestration_mode == "staggered"
        finally:
            os.unlink(path)


class TestNoneMode:
    """Test none orchestration mode (manual trigger only)."""

    def test_schedule_is_false(self, base_config, sample_selectors):
        base_config.orchestration_mode = "none"
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        for job in result["jobs"].values():
            assert job["triggers"]["schedule"] is False

    def test_no_schedule_key(self, base_config, sample_selectors):
        base_config.orchestration_mode = "none"
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        for job in result["jobs"].values():
            assert "schedule" not in job

    def test_still_creates_jobs(self, base_config, sample_selectors):
        base_config.orchestration_mode = "none"
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        assert len(result["jobs"]) == 2


class TestMinModelsPerJob:
    """Test combining small selectors."""

    def test_small_selectors_combined(self, base_config):
        base_config.min_models_per_job = 5
        selectors = [
            {
                "name": "maestro_big",
                "definition": {"union": [{"method": "fqn", "value": f"m{i}"} for i in range(6)]},
            },
            {
                "name": "maestro_small1",
                "definition": {"union": [{"method": "fqn", "value": "s1"}]},
            },
            {
                "name": "maestro_small2",
                "definition": {
                    "union": [{"method": "fqn", "value": "s2"}, {"method": "fqn", "value": "s3"}]
                },
            },
        ]
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(selectors)
        jobs = result["jobs"]
        assert "maestro_maestro_big" in jobs
        assert "maestro_combined_small_selectors" in jobs
        combined = jobs["maestro_combined_small_selectors"]
        assert len(combined["execute_steps"]) == 2
        assert combined["execute_steps"][0] == "dbt build --selector maestro_small1"
        assert combined["execute_steps"][1] == "dbt build --selector maestro_small2"

    def test_min_models_1_no_combining(self, base_config, sample_selectors):
        base_config.min_models_per_job = 1
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        assert "maestro_combined_small_selectors" not in result["jobs"]

    def test_stale_combined_job_removed_when_threshold_lowered(self, base_config):
        """Changing min_models_per_job should remove the old combined job."""
        selectors = [
            {
                "name": "maestro_big",
                "definition": {"union": [{"method": "fqn", "value": f"m{i}"} for i in range(6)]},
            },
            {
                "name": "maestro_small1",
                "definition": {"union": [{"method": "fqn", "value": "s1"}]},
            },
        ]

        # First run: small1 is combined because threshold is 5
        base_config.min_models_per_job = 5
        gen = JobGenerator(base_config)
        first_result = gen.generate_jobs(selectors)
        assert "maestro_combined_small_selectors" in first_result["jobs"]

        # Second run: threshold lowered so small1 is now a large selector
        base_config.min_models_per_job = 1
        gen2 = JobGenerator(base_config)
        second_result = gen2.generate_jobs(selectors, existing_jobs=first_result)

        # Stale combined job must be gone; individual job must exist
        assert "maestro_combined_small_selectors" not in second_result["jobs"]
        assert "maestro_maestro_small1" in second_result["jobs"]

    def test_stale_individual_jobs_removed_when_threshold_raised(self, base_config):
        """Raising min_models_per_job should replace individual small jobs with combined job."""
        selectors = [
            {
                "name": "maestro_big",
                "definition": {"union": [{"method": "fqn", "value": f"m{i}"} for i in range(6)]},
            },
            {
                "name": "maestro_small1",
                "definition": {"union": [{"method": "fqn", "value": "s1"}]},
            },
        ]

        # First run: threshold is 1, individual jobs created
        base_config.min_models_per_job = 1
        gen = JobGenerator(base_config)
        first_result = gen.generate_jobs(selectors)
        assert "maestro_maestro_small1" in first_result["jobs"]

        # Second run: threshold raised so small1 should be combined
        base_config.min_models_per_job = 5
        gen2 = JobGenerator(base_config)
        second_result = gen2.generate_jobs(selectors, existing_jobs=first_result)

        assert "maestro_combined_small_selectors" in second_result["jobs"]
        assert "maestro_maestro_small1" not in second_result["jobs"]


class TestExecutionOrder:
    """Test execution_order sorting."""

    def test_seeds_first(self, base_config):
        base_config.orchestration_mode = "staggered"
        base_config.start_hour = 6
        base_config.start_minute = 0
        base_config.cron_increment_minutes = 30
        base_config.execution_order = ["seeds", "models"]
        selectors = [
            {
                "name": "maestro_staging",
                "definition": {"union": [{"method": "fqn", "value": "m1"}]},
            },
            {"name": "maestro_seeds", "definition": {"union": [{"method": "fqn", "value": "s1"}]}},
        ]
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(selectors)
        # Seeds job should come first (earlier cron)
        seed_job = result["jobs"]["maestro_maestro_seeds"]
        model_job = result["jobs"]["maestro_maestro_staging"]
        assert seed_job["schedule"]["cron"] == "0 6 * * *"
        assert model_job["schedule"]["cron"] == "30 6 * * *"


class TestSelectorFiltering:
    """Test selector filtering logic."""

    def test_freshness_selectors_excluded(self, base_config):
        selectors = [
            {
                "name": "maestro_staging",
                "definition": {"union": [{"method": "fqn", "value": "m1"}]},
            },
            {"name": "freshness_staging", "definition": {"union": []}},
            {"name": "automatically_generated_freshness_x", "definition": {"union": []}},
        ]
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(selectors)
        assert len(result["jobs"]) == 1

    def test_full_refresh_selector_excluded(self, base_config):
        selectors = [
            {
                "name": "maestro_staging",
                "definition": {"union": [{"method": "fqn", "value": "m1"}]},
            },
            {"name": "maestro_full_refresh_incremental", "definition": {"union": []}},
        ]
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(selectors)
        assert len(result["jobs"]) == 1

    def test_exclude_manual_selectors(self, base_config):
        base_config.include_manual_selectors_in_jobs = False
        selectors = [
            {
                "name": "maestro_staging",
                "definition": {"union": [{"method": "fqn", "value": "m1"}]},
            },
            {
                "name": "my_custom_selector",
                "definition": {"union": [{"method": "fqn", "value": "m2"}]},
            },
        ]
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(selectors)
        assert len(result["jobs"]) == 1
        assert "maestro_maestro_staging" in result["jobs"]

    def test_exclude_maestro_selectors(self, base_config):
        base_config.include_maestro_selectors_in_jobs = False
        selectors = [
            {
                "name": "maestro_staging",
                "definition": {"union": [{"method": "fqn", "value": "m1"}]},
            },
            {
                "name": "my_custom_selector",
                "definition": {"union": [{"method": "fqn", "value": "m2"}]},
            },
        ]
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(selectors)
        assert len(result["jobs"]) == 1
        assert "maestro_my_custom_selector" in result["jobs"]


class TestFullRefreshJobs:
    """Test full refresh job generation."""

    def test_auto_full_refresh_when_enabled(self, base_config, sample_selectors):
        base_config.full_refresh.enabled = True
        base_config.full_refresh.cron_schedule = "0 2 * * 0"
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        assert "maestro_full_refresh_incremental" in result["jobs"]
        fr_job = result["jobs"]["maestro_full_refresh_incremental"]
        assert "--full-refresh" in fr_job["execute_steps"][0]
        assert fr_job["schedule"]["cron"] == "0 2 * * 0"

    def test_seeds_full_refresh_when_enabled(self, base_config, sample_selectors):
        base_config.seeds_full_refresh.enabled = True
        base_config.seeds_full_refresh.cron_schedule = "0 3 * * 0"
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(sample_selectors)
        assert "maestro_seeds_full_refresh" in result["jobs"]
        seed_job = result["jobs"]["maestro_seeds_full_refresh"]
        assert "dbt seed --full-refresh" in seed_job["execute_steps"][0]


class TestJobNameGeneration:
    """Test job name generation from selector names."""

    def test_strips_auto_prefix(self, base_config):
        gen = JobGenerator(base_config)
        assert (
            gen._generate_job_name("automatically_generated_selector_staging") == "maestro_staging"
        )

    def test_strips_tag_prefix(self, base_config):
        gen = JobGenerator(base_config)
        assert gen._generate_job_name("tag_nightly") == "maestro_nightly"

    def test_strips_path_prefix(self, base_config):
        gen = JobGenerator(base_config)
        assert gen._generate_job_name("path_marts") == "maestro_marts"

    def test_no_prefix_passthrough(self, base_config):
        gen = JobGenerator(base_config)
        assert gen._generate_job_name("my_custom") == "maestro_my_custom"


class TestManuallyCreatedJobs:
    """Test that manually created jobs are preserved."""

    def test_manual_job_not_overwritten(self, base_config):
        selectors = [
            {
                "name": "maestro_staging",
                "definition": {"union": [{"method": "fqn", "value": "m1"}]},
            },
        ]
        existing = {
            "jobs": {
                "maestro_staging": {
                    "description": "manually_created: do not overwrite",
                    "execute_steps": ["dbt run"],
                }
            }
        }
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(selectors, existing_jobs=existing)
        assert result["jobs"]["maestro_staging"]["execute_steps"] == ["dbt run"]


class TestNoneModeWithCombined:
    """Test none mode with combined small selectors."""

    def test_combined_job_no_schedule(self, base_config):
        base_config.orchestration_mode = "none"
        base_config.min_models_per_job = 5
        selectors = [
            {"name": "maestro_s1", "definition": {"union": [{"method": "fqn", "value": "m1"}]}},
            {"name": "maestro_s2", "definition": {"union": [{"method": "fqn", "value": "m2"}]}},
        ]
        gen = JobGenerator(base_config)
        result = gen.generate_jobs(selectors)
        combined = result["jobs"]["maestro_combined_small_selectors"]
        assert combined["triggers"]["schedule"] is False
        assert "schedule" not in combined
