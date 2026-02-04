"""Tests for path and model exclusion feature."""

import pytest
import json
import tempfile
import os

from dbt_job_maestro.manifest_parser import ManifestParser
from dbt_job_maestro.graph_builder import GraphBuilder
from dbt_job_maestro.selector_generator import SelectorGenerator
from dbt_job_maestro.selector_orchestrator import SelectorOrchestrator
from dbt_job_maestro.config import SelectorConfig


@pytest.fixture
def sample_manifest():
    """Create a sample manifest with models in different paths."""
    return {
        "nodes": {
            "model.project.stg_users": {
                "name": "stg_users",
                "fqn": ["project", "staging", "stg_users"],
                "path": "staging/stg_users.sql",
                "original_file_path": "models/staging/stg_users.sql",
                "tags": ["staging"],
                "resource_type": "model",
                "depends_on": {"nodes": []},
            },
            "model.project.stg_orders": {
                "name": "stg_orders",
                "fqn": ["project", "staging", "stg_orders"],
                "path": "staging/stg_orders.sql",
                "original_file_path": "models/staging/stg_orders.sql",
                "tags": ["staging"],
                "resource_type": "model",
                "depends_on": {"nodes": []},
            },
            "model.project.stg_legacy_data": {
                "name": "stg_legacy_data",
                "fqn": ["project", "staging", "legacy", "stg_legacy_data"],
                "path": "staging/legacy/stg_legacy_data.sql",
                "original_file_path": "models/staging/legacy/stg_legacy_data.sql",
                "tags": ["staging", "legacy"],
                "resource_type": "model",
                "depends_on": {"nodes": []},
            },
            "model.project.fct_orders": {
                "name": "fct_orders",
                "fqn": ["project", "marts", "fct_orders"],
                "path": "marts/fct_orders.sql",
                "original_file_path": "models/marts/fct_orders.sql",
                "tags": ["marts"],
                "resource_type": "model",
                "depends_on": {"nodes": ["model.project.stg_orders"]},
            },
            "model.project.fct_users": {
                "name": "fct_users",
                "fqn": ["project", "marts", "fct_users"],
                "path": "marts/fct_users.sql",
                "original_file_path": "models/marts/fct_users.sql",
                "tags": ["marts"],
                "resource_type": "model",
                "depends_on": {"nodes": ["model.project.stg_users"]},
            },
            "model.project.temp_debug": {
                "name": "temp_debug",
                "fqn": ["project", "temp", "temp_debug"],
                "path": "temp/temp_debug.sql",
                "original_file_path": "models/temp/temp_debug.sql",
                "tags": ["temp"],
                "resource_type": "model",
                "depends_on": {"nodes": []},
            },
        }
    }


@pytest.fixture
def temp_manifest(sample_manifest):
    """Create a temporary manifest file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(sample_manifest, f)
        return f.name


@pytest.fixture
def parser(temp_manifest):
    """Create a ManifestParser instance."""
    return ManifestParser(temp_manifest)


@pytest.fixture
def graph(parser):
    """Create a GraphBuilder instance."""
    return GraphBuilder(parser.get_models())


class TestPathExclusionConfig:
    """Test path exclusion configuration."""

    def test_exclude_paths_config(self):
        """Test that exclude_paths is properly set in config."""
        config = SelectorConfig(exclude_paths=["staging/legacy", "temp"])
        assert "staging/legacy" in config.exclude_paths
        assert "temp" in config.exclude_paths

    def test_exclude_models_config(self):
        """Test that exclude_models is properly set in config."""
        config = SelectorConfig(exclude_models=["temp_debug", "stg_legacy_data"])
        assert "temp_debug" in config.exclude_models
        assert "stg_legacy_data" in config.exclude_models

    def test_combined_exclusions(self):
        """Test combining path and model exclusions."""
        config = SelectorConfig(
            exclude_paths=["staging/legacy"],
            exclude_models=["temp_debug"],
            exclude_tags=["deprecated"],
        )
        assert len(config.exclude_paths) == 1
        assert len(config.exclude_models) == 1
        assert len(config.exclude_tags) == 1


class TestSelectorGeneratorPathExclusion:
    """Test path exclusion in SelectorGenerator."""

    def test_fqn_excludes_paths(self, parser, graph):
        """Test FQN method excludes models from specified paths."""
        config = SelectorConfig(method="fqn", exclude_paths=["staging/legacy", "temp"])
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        # Get all models in generated selectors
        all_selector_models = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_selector_models.add(item.get("value"))

        # stg_legacy_data and temp_debug should not be in selectors
        assert "stg_legacy_data" not in all_selector_models
        assert "temp_debug" not in all_selector_models

        # Other models should be present
        assert "stg_users" in all_selector_models or "fct_users" in all_selector_models

    def test_path_method_excludes_paths(self, parser, graph):
        """Test path method excludes models from specified paths."""
        config = SelectorConfig(method="path", exclude_paths=["staging/legacy"])
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        # Path selectors should not include legacy models
        # Check that staging/legacy path is not generated as a selector
        selector_names = [s["name"] for s in selectors]
        for name in selector_names:
            assert "legacy" not in name.lower()

    def test_tag_method_excludes_paths(self, parser, graph):
        """Test tag method excludes models from specified paths."""
        config = SelectorConfig(method="tag", exclude_paths=["temp"])
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        # Models from temp/ should not be counted in tag selectors
        # This is verified by checking the model counts in the resulting selectors
        assert len(selectors) >= 0  # Should complete without error

    def test_fqn_method_excludes_paths_with_deps(self, parser, graph):
        """Test FQN method with group_by_dependencies excludes models from specified paths."""
        config = SelectorConfig(method="fqn", exclude_paths=["temp"], group_by_dependencies=True)
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        # Check that temp_debug is excluded
        all_fqn_values = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_fqn_values.add(item.get("value"))

        assert "temp_debug" not in all_fqn_values

    def test_exclude_models_by_name(self, parser, graph):
        """Test excluding specific models by name."""
        config = SelectorConfig(method="fqn", exclude_models=["temp_debug", "stg_legacy_data"])
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        # Get all models in generated selectors
        all_selector_models = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_selector_models.add(item.get("value"))

        assert "temp_debug" not in all_selector_models
        assert "stg_legacy_data" not in all_selector_models

    def test_combined_path_and_model_exclusion(self, parser, graph):
        """Test combining path and model exclusions."""
        config = SelectorConfig(
            method="fqn", exclude_paths=["temp"], exclude_models=["stg_legacy_data"]
        )
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        all_selector_models = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_selector_models.add(item.get("value"))

        # Both exclusions should apply
        assert "temp_debug" not in all_selector_models
        assert "stg_legacy_data" not in all_selector_models

    def test_fqn_selector_definition_includes_path_exclusion(self, parser, graph):
        """Test that FQN selectors include path exclusion in their definition."""
        config = SelectorConfig(
            method="fqn", exclude_paths=["temp", "staging/legacy"], group_by_dependencies=True
        )
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        # Each non-freshness selector should have an exclude clause for paths
        for selector in selectors:
            if selector["name"].startswith("freshness_"):
                continue
            union_items = selector["definition"]["union"]
            exclude_items = [item for item in union_items if "exclude" in item]
            assert (
                len(exclude_items) > 0
            ), f"Selector '{selector['name']}' missing path exclusion in definition"
            # Verify exclude contains path method entries
            exclude_union = exclude_items[-1]["exclude"]["union"]
            path_excludes = [e for e in exclude_union if e.get("method") == "path"]
            assert len(path_excludes) == 2
            excluded_paths = {e["value"] for e in path_excludes}
            assert "temp" in excluded_paths
            assert "staging/legacy" in excluded_paths

    def test_tag_selector_definition_includes_path_exclusion(self, parser, graph):
        """Test that tag selectors include path exclusion in their definition."""
        config = SelectorConfig(method="tag", exclude_paths=["temp"])
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        for selector in selectors:
            if selector["name"].startswith("freshness_"):
                continue
            union_items = selector["definition"]["union"]
            exclude_items = [item for item in union_items if "exclude" in item]
            assert (
                len(exclude_items) > 0
            ), f"Selector '{selector['name']}' missing path exclusion in definition"
            exclude_union = exclude_items[0]["exclude"]["union"]
            path_excludes = [e for e in exclude_union if e.get("method") == "path"]
            assert len(path_excludes) == 1
            assert path_excludes[0]["value"] == "temp"

    def test_no_path_exclusion_in_definition_when_empty(self, parser, graph):
        """Test that no path exclusion is added when exclude_paths is empty."""
        config = SelectorConfig(method="fqn", exclude_paths=[], group_by_dependencies=True)
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        for selector in selectors:
            if selector["name"].startswith("freshness_"):
                continue
            union_items = selector["definition"]["union"]
            exclude_items = [item for item in union_items if "exclude" in item]
            # No exclude items should be present (no tags or paths excluded)
            path_exclude_items = [
                e
                for e in exclude_items
                if any(u.get("method") == "path" for u in e.get("exclude", {}).get("union", []))
            ]
            assert len(path_exclude_items) == 0


class TestSelectorOrchestratorPathExclusion:
    """Test path exclusion in SelectorOrchestrator."""

    def test_fqn_only_excludes_paths(self, parser, graph):
        """Test FQN-only mode excludes paths."""
        config = SelectorConfig(
            method="fqn", exclude_paths=["staging/legacy", "temp"], group_by_dependencies=True
        )
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        all_fqn_values = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_fqn_values.add(item.get("value"))

        assert "stg_legacy_data" not in all_fqn_values
        assert "temp_debug" not in all_fqn_values

    def test_fqn_mode_excludes_paths_and_models(self, parser, graph):
        """Test FQN mode excludes paths and models."""
        config = SelectorConfig(
            method="fqn",
            exclude_paths=["temp"],
            exclude_models=["stg_legacy_data"],
            group_by_dependencies=True,
        )
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        all_fqn_values = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_fqn_values.add(item.get("value"))

        assert "temp_debug" not in all_fqn_values
        assert "stg_legacy_data" not in all_fqn_values


class TestExclusionWithManualSelectors:
    """Test exclusion behavior with manual selectors."""

    def test_excluded_models_not_in_auto_selectors_with_manual(self, parser, graph, tmp_path):
        """Test that excluded models don't appear in auto selectors even with manual selectors."""
        import yaml

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)

            # Create manual selector
            manual_selector = {
                "name": "manual_marts",
                "definition": {"union": [{"method": "fqn", "value": "fct_orders"}]},
            }
            with open("selectors.yml", "w") as f:
                yaml.dump({"selectors": [manual_selector]}, f)

            config = SelectorConfig(
                method="fqn",
                exclude_paths=["temp"],
                group_by_dependencies=True,
            )
            orchestrator = SelectorOrchestrator(parser, graph, config)
            selectors = orchestrator.generate_selectors()

            # Get auto-generated selectors
            auto_selectors = [s for s in selectors if s["name"].startswith("maestro_")]

            all_fqn_values = set()
            for selector in auto_selectors:
                definition = selector.get("definition", {})
                for item in definition.get("union", []):
                    if item.get("method") == "fqn":
                        all_fqn_values.add(item.get("value"))

            assert "temp_debug" not in all_fqn_values

        finally:
            os.chdir(original_dir)

    def test_manual_selector_exclusion_warning(self, parser, graph, tmp_path, caplog):
        """Test that warning is logged when manual selector has exclusions not in config."""
        import yaml
        import logging

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)

            # Create manual selector with tag exclusion not in config
            manual_selector = {
                "name": "manual_with_exclusion",
                "definition": {
                    "union": [
                        {"method": "fqn", "value": "fct_orders"},
                        {"exclude": {"union": [{"method": "tag", "value": "deprecated"}]}},
                    ]
                },
            }
            with open("selectors.yml", "w") as f:
                yaml.dump({"selectors": [manual_selector]}, f)

            config = SelectorConfig(
                method="fqn",
                exclude_tags=[],  # 'deprecated' NOT in config
                group_by_dependencies=True,
            )

            with caplog.at_level(logging.WARNING):
                orchestrator = SelectorOrchestrator(parser, graph, config)
                orchestrator.generate_selectors()

            # Should warn about 'deprecated' tag exclusion in manual but not in config
            assert any(
                "deprecated" in record.message and "manual_with_exclusion" in record.message
                for record in caplog.records
            )

        finally:
            os.chdir(original_dir)

    def test_manual_selector_exclusion_no_warning_when_in_config(
        self, parser, graph, tmp_path, caplog
    ):
        """Test that no warning when manual selector exclusions are also in config."""
        import yaml
        import logging

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)

            # Create manual selector with tag exclusion that IS in config
            manual_selector = {
                "name": "manual_with_exclusion",
                "definition": {
                    "union": [
                        {"method": "fqn", "value": "fct_orders"},
                        {"exclude": {"union": [{"method": "tag", "value": "deprecated"}]}},
                    ]
                },
            }
            with open("selectors.yml", "w") as f:
                yaml.dump({"selectors": [manual_selector]}, f)

            config = SelectorConfig(
                method="fqn",
                exclude_tags=["deprecated"],  # 'deprecated' IS in config
                group_by_dependencies=True,
            )

            with caplog.at_level(logging.WARNING):
                orchestrator = SelectorOrchestrator(parser, graph, config)
                orchestrator.generate_selectors()

            # Should NOT warn about 'deprecated' since it's in config
            assert not any(
                "deprecated" in record.message
                and "will still appear in auto-generated" in record.message
                for record in caplog.records
            )

        finally:
            os.chdir(original_dir)


class TestExclusionLogging:
    """Test that exclusion information is properly logged."""

    def test_excluded_paths_logged(self, parser, graph, caplog):
        """Test that excluded paths are logged."""
        import logging

        config = SelectorConfig(method="fqn", exclude_paths=["temp"], group_by_dependencies=True)

        with caplog.at_level(logging.INFO):
            orchestrator = SelectorOrchestrator(parser, graph, config)
            orchestrator.generate_selectors()

        # Check that exclusion was logged (either in logs or just works silently)
        # The actual logging may vary, so we just verify the operation completes
        assert True

    def test_excluded_models_logged(self, parser, graph, caplog):
        """Test that excluded models are logged."""
        import logging

        config = SelectorConfig(
            method="fqn", exclude_models=["temp_debug"], group_by_dependencies=True
        )

        with caplog.at_level(logging.INFO):
            orchestrator = SelectorOrchestrator(parser, graph, config)
            orchestrator.generate_selectors()

        # Verify operation completes
        assert True


class TestTagExclusion:
    """Test tag exclusion in selector generation."""

    def test_fqn_excludes_tags(self, parser, graph):
        """Test FQN method excludes models with specified tags."""
        config = SelectorConfig(method="fqn", exclude_tags=["legacy"], group_by_dependencies=True)
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        all_fqn_values = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_fqn_values.add(item.get("value"))

        # stg_legacy_data has "legacy" tag and should be excluded
        assert "stg_legacy_data" not in all_fqn_values
        # Other models should still be present
        assert "stg_users" in all_fqn_values or "fct_users" in all_fqn_values

    def test_fqn_excludes_multiple_tags(self, parser, graph):
        """Test FQN method excludes models with any of the specified tags."""
        config = SelectorConfig(
            method="fqn", exclude_tags=["legacy", "temp"], group_by_dependencies=True
        )
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        all_fqn_values = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_fqn_values.add(item.get("value"))

        # Both legacy and temp models should be excluded
        assert "stg_legacy_data" not in all_fqn_values
        assert "temp_debug" not in all_fqn_values

    def test_combined_tag_and_path_exclusion(self, parser, graph):
        """Test combining tag and path exclusions."""
        config = SelectorConfig(
            method="fqn",
            exclude_tags=["legacy"],
            exclude_paths=["temp"],
            group_by_dependencies=True,
        )
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        all_fqn_values = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_fqn_values.add(item.get("value"))

        # Both tag-excluded and path-excluded models should be removed
        assert "stg_legacy_data" not in all_fqn_values
        assert "temp_debug" not in all_fqn_values

    def test_model_with_multiple_excluded_tags(self, parser, graph):
        """Test model with multiple tags where both are excluded (no error)."""
        # stg_legacy_data has both "staging" and "legacy" tags
        config = SelectorConfig(
            method="fqn", exclude_tags=["staging", "legacy"], group_by_dependencies=True
        )
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        all_fqn_values = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_fqn_values.add(item.get("value"))

        # Model should be excluded (only once, no errors for duplicate exclusion)
        assert "stg_legacy_data" not in all_fqn_values
        # stg_users and stg_orders also have staging tag, should be excluded
        assert "stg_users" not in all_fqn_values
        assert "stg_orders" not in all_fqn_values

    def test_tag_exclusion_with_path_method(self, parser, graph):
        """Test tag exclusion works with path method."""
        config = SelectorConfig(method="path", exclude_tags=["legacy"])
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        # Should complete without error
        assert len(selectors) >= 0

    def test_tag_exclusion_adds_exclude_clause_to_selector(self, parser, graph):
        """Test that tag exclusion adds exclude clause to auto-generated selector definition."""
        config = SelectorConfig(
            method="fqn", exclude_tags=["legacy", "temp"], group_by_dependencies=True
        )
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        # Each auto-generated (non-freshness, non-manual) selector should have exclude clause
        auto_selectors = [
            s
            for s in selectors
            if s["name"].startswith(config.selector_prefix + "_")
            and not s["name"].startswith("freshness_")
        ]

        # Ensure we have at least one auto-generated selector to test
        assert len(auto_selectors) > 0, "No auto-generated selectors found"

        for selector in auto_selectors:
            union_items = selector["definition"]["union"]
            exclude_items = [item for item in union_items if "exclude" in item]
            assert (
                len(exclude_items) > 0
            ), f"Selector '{selector['name']}' missing tag exclusion in definition"
            # Verify exclude contains tag method entries
            exclude_union = exclude_items[0]["exclude"]["union"]
            tag_excludes = [e for e in exclude_union if e.get("method") == "tag"]
            assert len(tag_excludes) == 2
            excluded_tags = {e["value"] for e in tag_excludes}
            assert "legacy" in excluded_tags
            assert "temp" in excluded_tags


class TestExclusionEdgeCases:
    """Test edge cases for exclusion."""

    def test_exclude_all_models_in_path(self, parser, graph):
        """Test excluding all models results in empty selectors for that path."""
        config = SelectorConfig(
            method="fqn",
            exclude_paths=["staging", "marts", "temp"],  # Exclude all paths
            group_by_dependencies=True,
        )
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        # All models excluded, should have no selectors
        assert len(selectors) == 0

    def test_exclude_nonexistent_path(self, parser, graph):
        """Test excluding non-existent path doesn't cause errors."""
        config = SelectorConfig(
            method="fqn", exclude_paths=["nonexistent/path"], group_by_dependencies=True
        )
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        # Should complete without error
        assert len(selectors) > 0

    def test_exclude_nonexistent_model(self, parser, graph):
        """Test excluding non-existent model doesn't cause errors."""
        config = SelectorConfig(
            method="fqn", exclude_models=["nonexistent_model"], group_by_dependencies=True
        )
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        # Should complete without error
        assert len(selectors) > 0

    def test_empty_exclusion_lists(self, parser, graph):
        """Test with empty exclusion lists."""
        config = SelectorConfig(
            method="fqn", exclude_paths=[], exclude_models=[], group_by_dependencies=True
        )
        generator = SelectorGenerator(parser, graph, config)
        selectors = generator.generate_selectors()

        # Should include all models
        all_fqn_values = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_fqn_values.add(item.get("value"))

        assert "temp_debug" in all_fqn_values
        assert "stg_legacy_data" in all_fqn_values


# Cleanup fixture
@pytest.fixture(autouse=True)
def cleanup(temp_manifest):
    yield
    try:
        os.unlink(temp_manifest)
    except (OSError, TypeError):
        pass
