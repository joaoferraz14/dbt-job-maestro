"""Tests for CLI commands."""

import pytest
import json
import tempfile
import os
from pathlib import Path
from click.testing import CliRunner

from dbt_job_maestro.cli import main


@pytest.fixture
def runner():
    """Create a CLI runner."""
    return CliRunner()


@pytest.fixture
def sample_manifest():
    """Create a sample manifest.json for testing."""
    return {
        "nodes": {
            "model.test_project.model_a": {
                "name": "model_a",
                "fqn": ["test_project", "staging", "model_a"],
                "path": "staging/model_a.sql",
                "original_file_path": "models/staging/model_a.sql",
                "tags": ["staging", "daily"],
                "resource_type": "model",
                "depends_on": {"nodes": []},
            },
            "model.test_project.model_b": {
                "name": "model_b",
                "fqn": ["test_project", "marts", "model_b"],
                "path": "marts/model_b.sql",
                "original_file_path": "models/marts/model_b.sql",
                "tags": ["marts"],
                "resource_type": "model",
                "depends_on": {"nodes": ["model.test_project.model_a"]},
            },
            "model.test_project.model_c": {
                "name": "model_c",
                "fqn": ["test_project", "marts", "model_c"],
                "path": "marts/model_c.sql",
                "original_file_path": "models/marts/model_c.sql",
                "tags": ["marts", "weekly"],
                "resource_type": "model",
                "depends_on": {"nodes": ["model.test_project.model_b"]},
            },
        }
    }


@pytest.fixture
def temp_manifest(sample_manifest):
    """Create a temporary manifest file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(sample_manifest, f)
        return f.name


class TestMainCommand:
    """Test the main CLI group."""

    def test_main_help(self, runner):
        """Test main --help."""
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "dbt-job-maestro" in result.output

    def test_main_version(self, runner):
        """Test main --version."""
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "0.3.0" in result.output


class TestGenerateCommand:
    """Test the generate command."""

    def test_generate_help(self, runner):
        """Test generate --help."""
        result = runner.invoke(main, ["generate", "--help"])
        assert result.exit_code == 0
        assert "--manifest" in result.output
        assert "--exclude-tag" in result.output
        assert "--exclude-path" in result.output
        assert "--exclude-model" in result.output

    def test_generate_with_manifest(self, runner, temp_manifest):
        """Test generate with a manifest file."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                main, ["generate", "--manifest", temp_manifest, "--output", "selectors.yml"]
            )
            assert result.exit_code == 0
            assert "Selectors generated successfully" in result.output
            assert Path("selectors.yml").exists()

    def test_generate_fqn_method(self, runner, temp_manifest):
        """Test generate produces FQN-based selectors."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                main,
                [
                    "generate",
                    "--manifest",
                    temp_manifest,
                    "--output",
                    "selectors.yml",
                ],
            )
            assert result.exit_code == 0
            assert Path("selectors.yml").exists()

    def test_generate_with_exclude_tag(self, runner, temp_manifest):
        """Test generate with exclude-tag option."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                main,
                [
                    "generate",
                    "--manifest",
                    temp_manifest,
                    "--exclude-tag",
                    "weekly",
                    "--output",
                    "selectors.yml",
                ],
            )
            assert result.exit_code == 0
            assert "Excluding tags: weekly" in result.output

    def test_generate_with_multiple_exclude_tags(self, runner, temp_manifest):
        """Test generate with multiple exclude-tag options."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                main,
                [
                    "generate",
                    "--manifest",
                    temp_manifest,
                    "--exclude-tag",
                    "weekly",
                    "--exclude-tag",
                    "daily",
                    "--output",
                    "selectors.yml",
                ],
            )
            assert result.exit_code == 0

    def test_generate_with_exclude_path(self, runner, temp_manifest):
        """Test generate with exclude-path option."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                main,
                [
                    "generate",
                    "--manifest",
                    temp_manifest,
                    "--exclude-path",
                    "staging",
                    "--output",
                    "selectors.yml",
                ],
            )
            assert result.exit_code == 0
            assert "Excluding paths: staging" in result.output

    def test_generate_with_exclude_model(self, runner, temp_manifest):
        """Test generate with exclude-model option."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                main,
                [
                    "generate",
                    "--manifest",
                    temp_manifest,
                    "--exclude-model",
                    "model_a",
                    "--output",
                    "selectors.yml",
                ],
            )
            assert result.exit_code == 0
            assert "Excluding models: model_a" in result.output

    def test_generate_with_no_freshness(self, runner, temp_manifest):
        """Test generate with --no-include-freshness flag."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                main,
                [
                    "generate",
                    "--manifest",
                    temp_manifest,
                    "--no-include-freshness",
                    "--output",
                    "selectors.yml",
                ],
            )
            assert result.exit_code == 0

    def test_generate_with_include_freshness(self, runner, temp_manifest):
        """Test generate with --include-freshness flag."""
        with runner.isolated_filesystem():
            result = runner.invoke(
                main,
                [
                    "generate",
                    "--manifest",
                    temp_manifest,
                    "--include-freshness",
                    "--output",
                    "selectors.yml",
                ],
            )
            assert result.exit_code == 0

    def test_generate_missing_manifest(self, runner):
        """Test generate with non-existent manifest file."""
        result = runner.invoke(main, ["generate", "--manifest", "nonexistent.json"])
        assert result.exit_code != 0

    def test_generate_with_config_file(self, runner, temp_manifest):
        """Test generate with config file."""
        with runner.isolated_filesystem():
            # Create config file
            config_content = f"""
manifest_path: {temp_manifest}
selectors_output_file: selectors.yml
selector:
  exclude_tags:
    - deprecated
"""
            with open("maestro-config.yml", "w") as f:
                f.write(config_content)

            result = runner.invoke(main, ["generate", "--config", "maestro-config.yml"])
            assert result.exit_code == 0


class TestGenerateJobsCommand:
    """Test the generate-jobs command."""

    def test_generate_jobs_help(self, runner):
        """Test generate-jobs --help."""
        result = runner.invoke(main, ["generate-jobs", "--help"])
        assert result.exit_code == 0
        assert "--selectors" in result.output
        assert "--output" in result.output

    def test_generate_jobs_with_selectors(self, runner, temp_manifest):
        """Test generate-jobs with selectors file."""
        with runner.isolated_filesystem():
            # First generate selectors
            runner.invoke(
                main, ["generate", "--manifest", temp_manifest, "--output", "selectors.yml"]
            )

            # Then generate jobs
            result = runner.invoke(
                main, ["generate-jobs", "--selectors", "selectors.yml", "--output", "jobs.yml"]
            )
            assert result.exit_code == 0
            assert Path("jobs.yml").exists()

    def test_generate_jobs_missing_selectors(self, runner):
        """Test generate-jobs with non-existent selectors file."""
        result = runner.invoke(main, ["generate-jobs", "--selectors", "nonexistent.yml"])
        assert result.exit_code != 0


class TestInfoCommand:
    """Test the info command."""

    def test_info_help(self, runner):
        """Test info --help."""
        result = runner.invoke(main, ["info", "--help"])
        assert result.exit_code == 0
        assert "--manifest" in result.output

    def test_info_with_manifest(self, runner, temp_manifest):
        """Test info with manifest file."""
        result = runner.invoke(main, ["info", "--manifest", temp_manifest])
        assert result.exit_code == 0
        assert "Total Models: 3" in result.output
        assert "Tags" in result.output
        assert "Path Prefixes" in result.output
        assert "Dependency Analysis" in result.output

    def test_info_missing_manifest(self, runner):
        """Test info with non-existent manifest."""
        result = runner.invoke(main, ["info", "--manifest", "nonexistent.json"])
        assert result.exit_code != 0


class TestInitCommand:
    """Test the init command."""

    def test_init_help(self, runner):
        """Test init --help."""
        result = runner.invoke(main, ["init", "--help"])
        assert result.exit_code == 0
        assert "--output" in result.output

    def test_init_creates_config(self, runner):
        """Test init creates config file."""
        with runner.isolated_filesystem():
            result = runner.invoke(main, ["init", "--output", "maestro-config.yml"])
            assert result.exit_code == 0
            assert Path("maestro-config.yml").exists()

    def test_init_default_output(self, runner):
        """Test init with default output file."""
        with runner.isolated_filesystem():
            result = runner.invoke(main, ["init"])
            assert result.exit_code == 0
            assert Path("maestro-config.yml").exists()

    def test_init_overwrites_with_confirmation(self, runner):
        """Test init asks for confirmation when file exists."""
        with runner.isolated_filesystem():
            # Create existing file
            Path("maestro-config.yml").write_text("existing content")

            # Run init with yes input
            result = runner.invoke(main, ["init"], input="y\n")
            assert result.exit_code == 0

    def test_init_cancels_without_confirmation(self, runner):
        """Test init cancels when user says no."""
        with runner.isolated_filesystem():
            # Create existing file
            Path("maestro-config.yml").write_text("existing content")

            # Run init with no input
            result = runner.invoke(main, ["init"], input="n\n")
            assert "Cancelled" in result.output


class TestCLIIntegration:
    """Integration tests for CLI workflow."""

    def test_full_workflow(self, runner, temp_manifest):
        """Test complete workflow: generate -> generate-jobs."""
        with runner.isolated_filesystem():
            # Step 1: Generate selectors
            result1 = runner.invoke(
                main,
                [
                    "generate",
                    "--manifest",
                    temp_manifest,
                    "--output",
                    "selectors.yml",
                ],
            )
            assert result1.exit_code == 0
            assert Path("selectors.yml").exists()

            # Step 2: Generate jobs
            result2 = runner.invoke(
                main, ["generate-jobs", "--selectors", "selectors.yml", "--output", "jobs.yml"]
            )
            assert result2.exit_code == 0
            assert Path("jobs.yml").exists()

    def test_workflow_with_config(self, runner, temp_manifest):
        """Test workflow using config file."""
        with runner.isolated_filesystem():
            # Create config
            config_content = f"""
manifest_path: {temp_manifest}
selectors_output_file: selectors.yml
jobs_output_file: jobs.yml
selector:
  exclude_tags:
    - deprecated
job:
  account_id: 12345
  project_id: 67890
  environment_id: 11111
"""
            with open("maestro-config.yml", "w") as f:
                f.write(config_content)

            # Generate selectors
            result1 = runner.invoke(main, ["generate", "--config", "maestro-config.yml"])
            assert result1.exit_code == 0

            # Generate jobs
            result2 = runner.invoke(main, ["generate-jobs", "--config", "maestro-config.yml"])
            assert result2.exit_code == 0


# Cleanup temp files
@pytest.fixture(autouse=True)
def cleanup(temp_manifest):
    yield
    try:
        os.unlink(temp_manifest)
    except (OSError, TypeError):
        pass
