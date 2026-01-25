"""Tests for configuration handling."""

import pytest
import tempfile
import os
from pathlib import Path

from dbt_job_maestro.config import Config, SelectorConfig, JobConfig, DeploymentConfig


class TestSelectorConfig:
    """Test SelectorConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = SelectorConfig()

        assert config.method == "fqn"
        assert config.selector_prefix == "maestro"
        assert config.exclude_tags == []
        assert config.exclude_paths == []
        assert config.exclude_models == []
        assert config.min_models_per_selector == 1
        assert config.group_by_dependencies is True
        assert config.include_freshness_selectors is False
        assert config.preserve_manual_selectors is True

    def test_custom_values(self):
        """Test configuration with custom values."""
        config = SelectorConfig(
            method="mixed",
            selector_prefix="custom",
            exclude_tags=["deprecated", "test"],
            exclude_paths=["staging/legacy"],
            exclude_models=["temp_model"],
            min_models_per_selector=3,
            group_by_dependencies=False,
            include_freshness_selectors=True,
        )

        assert config.method == "mixed"
        assert config.selector_prefix == "custom"
        assert config.exclude_tags == ["deprecated", "test"]
        assert config.exclude_paths == ["staging/legacy"]
        assert config.exclude_models == ["temp_model"]
        assert config.min_models_per_selector == 3
        assert config.group_by_dependencies is False
        assert config.include_freshness_selectors is True

    def test_all_methods(self):
        """Test all valid selector methods."""
        for method in ["fqn", "path", "tag", "mixed"]:
            config = SelectorConfig(method=method)
            assert config.method == method


class TestJobConfig:
    """Test JobConfig dataclass."""

    def test_default_values(self):
        """Test default job configuration values."""
        config = JobConfig()

        assert config.account_id is None
        assert config.project_id is None
        assert config.environment_id is None
        assert config.include_maestro_selectors_in_jobs is True
        assert config.include_manual_selectors_in_jobs is True

    def test_custom_values(self):
        """Test job configuration with custom values."""
        config = JobConfig(
            account_id=12345,
            project_id=67890,
            environment_id=11111,
            include_maestro_selectors_in_jobs=True,
            include_manual_selectors_in_jobs=True,
        )

        assert config.account_id == 12345
        assert config.project_id == 67890
        assert config.environment_id == 11111
        assert config.include_maestro_selectors_in_jobs is True
        assert config.include_manual_selectors_in_jobs is True


class TestDeploymentConfig:
    """Test DeploymentConfig dataclass."""

    def test_default_values(self):
        """Test default deployment configuration values."""
        config = DeploymentConfig()

        assert config.deploy_branch == "main"
        assert config.require_dbt_jobs_as_code is True
        assert config.dbt_project_path == "."

    def test_custom_values(self):
        """Test deployment configuration with custom values."""
        config = DeploymentConfig(
            deploy_branch="production",
            require_dbt_jobs_as_code=False,
            dbt_project_path="./dbt_project",
        )

        assert config.deploy_branch == "production"
        assert config.require_dbt_jobs_as_code is False
        assert config.dbt_project_path == "./dbt_project"


class TestConfig:
    """Test main Config class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = Config()

        assert config.manifest_path == "target/manifest.json"
        assert config.selectors_output_file == "selectors.yml"
        assert config.jobs_output_file == "jobs.yml"
        assert config.output_dir == "."
        assert isinstance(config.selector, SelectorConfig)
        assert isinstance(config.job, JobConfig)
        assert isinstance(config.deployment, DeploymentConfig)

    def test_from_yaml(self):
        """Test loading configuration from YAML file."""
        yaml_content = """
manifest_path: custom/manifest.json
selectors_output_file: custom_selectors.yml
jobs_output_file: custom_jobs.yml
output_dir: output

selector:
  method: mixed
  exclude_tags:
    - deprecated
    - test
  exclude_paths:
    - staging/legacy
  exclude_models:
    - temp_model
  min_models_per_selector: 2

job:
  account_id: 12345
  project_id: 67890
  environment_id: 11111

deployment:
  deploy_branch: production
  require_dbt_jobs_as_code: false
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            config = Config.from_yaml(config_path)

            assert config.manifest_path == "custom/manifest.json"
            assert config.selectors_output_file == "custom_selectors.yml"
            assert config.jobs_output_file == "custom_jobs.yml"
            assert config.output_dir == "output"

            assert config.selector.method == "mixed"
            assert config.selector.exclude_tags == ["deprecated", "test"]
            assert config.selector.exclude_paths == ["staging/legacy"]
            assert config.selector.exclude_models == ["temp_model"]
            assert config.selector.min_models_per_selector == 2

            assert config.job.account_id == 12345
            assert config.job.project_id == 67890
            assert config.job.environment_id == 11111

            assert config.deployment.deploy_branch == "production"
            assert config.deployment.require_dbt_jobs_as_code is False

        finally:
            os.unlink(config_path)

    def test_from_yaml_partial(self):
        """Test loading partial configuration from YAML."""
        yaml_content = """
manifest_path: custom/manifest.json
selector:
  method: tag
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(yaml_content)
            config_path = f.name

        try:
            config = Config.from_yaml(config_path)

            # Custom values
            assert config.manifest_path == "custom/manifest.json"
            assert config.selector.method == "tag"

            # Defaults for unspecified values
            assert config.selectors_output_file == "selectors.yml"
            assert config.selector.exclude_tags == []

        finally:
            os.unlink(config_path)

    def test_to_yaml(self):
        """Test saving configuration to YAML file."""
        config = Config()
        config.manifest_path = "test/manifest.json"
        config.selector.method = "mixed"
        config.selector.exclude_tags = ["deprecated"]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            config_path = f.name

        try:
            config.to_yaml(config_path)

            # Verify file was created and contains expected content
            assert Path(config_path).exists()

            # Load it back and verify
            loaded = Config.from_yaml(config_path)
            assert loaded.manifest_path == "test/manifest.json"
            assert loaded.selector.method == "mixed"
            assert "deprecated" in loaded.selector.exclude_tags

        finally:
            os.unlink(config_path)

    def test_from_yaml_empty_file(self):
        """Test loading from empty YAML file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("")
            config_path = f.name

        try:
            config = Config.from_yaml(config_path)
            # Should use all defaults
            assert config.manifest_path == "target/manifest.json"
            assert config.selector.method == "fqn"

        finally:
            os.unlink(config_path)

    def test_from_yaml_missing_file(self):
        """Test loading from non-existent YAML file."""
        with pytest.raises(FileNotFoundError):
            Config.from_yaml("nonexistent_config.yml")


class TestConfigExclusionLists:
    """Test exclusion list handling in configuration."""

    def test_exclude_tags_list(self):
        """Test exclude_tags as list."""
        config = SelectorConfig(exclude_tags=["tag1", "tag2", "tag3"])
        assert len(config.exclude_tags) == 3
        assert "tag1" in config.exclude_tags
        assert "tag2" in config.exclude_tags
        assert "tag3" in config.exclude_tags

    def test_exclude_paths_list(self):
        """Test exclude_paths as list."""
        config = SelectorConfig(exclude_paths=["staging/legacy", "models/temp", "marts/deprecated"])
        assert len(config.exclude_paths) == 3
        assert "staging/legacy" in config.exclude_paths

    def test_exclude_models_list(self):
        """Test exclude_models as list."""
        config = SelectorConfig(exclude_models=["temp_model", "debug_model", "test_model"])
        assert len(config.exclude_models) == 3
        assert "temp_model" in config.exclude_models

    def test_include_path_groups(self):
        """Test include_path_groups configuration."""
        config = SelectorConfig(include_path_groups=["staging/critical", "marts/revenue"])
        assert len(config.include_path_groups) == 2

    def test_freshness_selector_names(self):
        """Test freshness_selector_names configuration."""
        config = SelectorConfig(freshness_selector_names=["selector_staging", "selector_marts"])
        assert len(config.freshness_selector_names) == 2


class TestConfigIntegration:
    """Integration tests for configuration handling."""

    def test_config_roundtrip(self):
        """Test config save and load roundtrip."""
        original = Config()
        original.manifest_path = "test/manifest.json"
        original.selector.method = "mixed"
        original.selector.exclude_tags = ["deprecated", "test"]
        original.selector.exclude_paths = ["staging/legacy"]
        original.selector.exclude_models = ["temp_model"]
        original.job.account_id = 12345
        original.deployment.deploy_branch = "production"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            config_path = f.name

        try:
            original.to_yaml(config_path)
            loaded = Config.from_yaml(config_path)

            assert loaded.manifest_path == original.manifest_path
            assert loaded.selector.method == original.selector.method
            assert loaded.selector.exclude_tags == original.selector.exclude_tags
            assert loaded.selector.exclude_paths == original.selector.exclude_paths
            assert loaded.selector.exclude_models == original.selector.exclude_models
            assert loaded.job.account_id == original.job.account_id
            assert loaded.deployment.deploy_branch == original.deployment.deploy_branch

        finally:
            os.unlink(config_path)
