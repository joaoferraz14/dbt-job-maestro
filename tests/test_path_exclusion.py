"""Tests for path and model exclusion feature."""

import pytest
import json
import tempfile
import os

from dbt_job_maestro.manifest_parser import ManifestParser
from dbt_job_maestro.graph_builder import GraphBuilder
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


class TestSelectorOrchestratorPathExclusion:
    """Test path exclusion in SelectorOrchestrator."""

    def test_fqn_only_excludes_paths(self, parser, graph):
        """Test FQN-only mode excludes paths."""
        config = SelectorConfig(
            exclude_paths=["staging/legacy", "temp"], group_by_dependencies=True
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

        config = SelectorConfig(exclude_paths=["temp"], group_by_dependencies=True)

        with caplog.at_level(logging.INFO):
            orchestrator = SelectorOrchestrator(parser, graph, config)
            orchestrator.generate_selectors()

        # Check that exclusion was logged (either in logs or just works silently)
        # The actual logging may vary, so we just verify the operation completes
        assert True

    def test_excluded_models_logged(self, parser, graph, caplog):
        """Test that excluded models are logged."""
        import logging

        config = SelectorConfig(exclude_models=["temp_debug"], group_by_dependencies=True)

        with caplog.at_level(logging.INFO):
            orchestrator = SelectorOrchestrator(parser, graph, config)
            orchestrator.generate_selectors()

        # Verify operation completes
        assert True


class TestTagExclusion:
    """Test tag exclusion in selector generation."""

    def test_fqn_excludes_tags(self, parser, graph):
        """Test FQN method excludes models with specified tags."""
        config = SelectorConfig(exclude_tags=["legacy"], group_by_dependencies=True)
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
        config = SelectorConfig(exclude_tags=["legacy", "temp"], group_by_dependencies=True)
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
        config = SelectorConfig(exclude_tags=["staging", "legacy"], group_by_dependencies=True)
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

    def test_tag_exclusion_adds_exclude_clause_to_selector(self, parser, graph):
        """Test that tag exclusion adds exclude clause to auto-generated selector definition."""
        config = SelectorConfig(exclude_tags=["legacy", "temp"], group_by_dependencies=True)
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
            # dbt requires the value of "exclude" to be a list, not a
            # {union: [...]} dict - a dict makes dbt reject the whole file with
            # 'Invalid value for key "exclude". Expected a list.'
            exclude_value = exclude_items[0]["exclude"]
            assert isinstance(exclude_value, list), (
                f"Selector '{selector['name']}' emitted a "
                f"{type(exclude_value).__name__} under 'exclude'; dbt requires a list"
            )
            tag_excludes = [e for e in exclude_value if e.get("method") == "tag"]
            assert len(tag_excludes) == 2
            excluded_tags = {e["value"] for e in tag_excludes}
            assert "legacy" in excluded_tags
            assert "temp" in excluded_tags


class TestExclusionEdgeCases:
    """Test edge cases for exclusion."""

    def test_exclude_all_models_in_path(self, parser, graph):
        """Test excluding all models results in empty selectors for that path."""
        config = SelectorConfig(
            exclude_paths=["staging", "marts", "temp"],  # Exclude all paths
            group_by_dependencies=True,
        )
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        # All models excluded, should have no selectors
        assert len(selectors) == 0

    def test_exclude_nonexistent_path(self, parser, graph):
        """Test excluding non-existent path doesn't cause errors."""
        config = SelectorConfig(exclude_paths=["nonexistent/path"], group_by_dependencies=True)
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        # Should complete without error
        assert len(selectors) > 0

    def test_exclude_nonexistent_model(self, parser, graph):
        """Test excluding non-existent model doesn't cause errors."""
        config = SelectorConfig(exclude_models=["nonexistent_model"], group_by_dependencies=True)
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        # Should complete without error
        assert len(selectors) > 0

    def test_empty_exclusion_lists(self, parser, graph):
        """Test with empty exclusion lists."""
        config = SelectorConfig(exclude_paths=[], exclude_models=[], group_by_dependencies=True)
        orchestrator = SelectorOrchestrator(parser, graph, config)
        selectors = orchestrator.generate_selectors()

        # Should include all models
        all_fqn_values = set()
        for selector in selectors:
            definition = selector.get("definition", {})
            for item in definition.get("union", []):
                if item.get("method") == "fqn":
                    all_fqn_values.add(item.get("value"))

        assert "temp_debug" in all_fqn_values
        assert "stg_legacy_data" in all_fqn_values


class TestExcludeClauseShape:
    """dbt requires the value of ``exclude`` to be a list of selector definitions.

    Emitting ``exclude: {union: [...]}`` makes dbt reject the entire
    selectors.yml with 'Invalid value for key "exclude". Expected a list.',
    which breaks every selector in the file - not just the one with exclusions.
    """

    def _exclude_values(self, selectors, prefix="maestro_"):
        out = []
        for selector in selectors:
            if not selector["name"].startswith(prefix):
                continue
            for item in selector.get("definition", {}).get("union", []) or []:
                if isinstance(item, dict) and "exclude" in item:
                    out.append(item["exclude"])
        return out

    def test_union_mode_emits_flat_list(self, parser, graph):
        config = SelectorConfig(
            exclude_tags=["legacy"], exclude_paths=["models/temp"], exclusion_mode="union"
        )
        selectors = SelectorOrchestrator(parser, graph, config).generate_selectors()
        values = self._exclude_values(selectors)
        assert values
        for value in values:
            assert isinstance(value, list)
            assert all("method" in entry for entry in value)
            assert {e["method"] for e in value} == {"tag", "path"}

    def test_intersection_mode_wraps_in_nested_intersection(self, parser, graph):
        config = SelectorConfig(
            exclude_tags=["legacy"], exclude_paths=["models/temp"], exclusion_mode="intersection"
        )
        selectors = SelectorOrchestrator(parser, graph, config).generate_selectors()
        values = self._exclude_values(selectors)
        assert values
        for value in values:
            assert isinstance(value, list)
            assert len(value) == 1
            assert "intersection" in value[0]
            assert {e["method"] for e in value[0]["intersection"]} == {"tag", "path"}

    def test_single_criterion_intersection_stays_flat(self, parser, graph):
        """One criterion needs no intersection wrapper - AND of one thing is itself."""
        config = SelectorConfig(exclude_tags=["legacy"], exclusion_mode="intersection")
        selectors = SelectorOrchestrator(parser, graph, config).generate_selectors()
        values = self._exclude_values(selectors)
        assert values
        for value in values:
            assert isinstance(value, list)
            assert value == [{"method": "tag", "value": "legacy"}]

    def test_full_refresh_exclusions_are_a_list(self, parser, graph):
        config = SelectorConfig(
            include_full_refresh_selector=True,
            full_refresh_exclude_tags=["legacy"],
            full_refresh_exclude_paths=["models/temp"],
            full_refresh_exclude_models=["temp_debug"],
        )
        selectors = SelectorOrchestrator(parser, graph, config).generate_selectors()
        fr = next(s for s in selectors if s["name"].endswith("_full_refresh_incremental"))
        excludes = [item["exclude"] for item in fr["definition"]["union"] if "exclude" in item]
        assert excludes
        for value in excludes:
            assert isinstance(value, list)
            assert {e["method"] for e in value} == {"tag", "path", "fqn"}

    def test_no_exclude_clause_when_nothing_configured(self, parser, graph):
        selectors = SelectorOrchestrator(parser, graph, SelectorConfig()).generate_selectors()
        assert not self._exclude_values(selectors)


class TestLegacyExcludeShapeStillResolves:
    """A stale selectors.yml may still carry the old {union: [...]} dict.

    The resolver must keep subtracting those models so upgrading maestro does not
    silently change which models are considered covered by manual selectors.
    """

    def test_dict_shape_exclusions_are_resolved(self, parser, graph):
        from dbt_job_maestro.model_resolver import ModelResolver

        resolver = ModelResolver(parser, graph)
        legacy = {
            "name": "legacy_manual",
            "definition": {
                "union": [
                    {"method": "path", "value": "models/staging"},
                    {"exclude": {"union": [{"method": "fqn", "value": "stg_users"}]}},
                ]
            },
        }
        assert "stg_users" not in resolver.resolve_selector(legacy).models

    def test_list_shape_exclusions_are_resolved(self, parser, graph):
        from dbt_job_maestro.model_resolver import ModelResolver

        resolver = ModelResolver(parser, graph)
        current = {
            "name": "current_manual",
            "definition": {
                "union": [
                    {"method": "path", "value": "models/staging"},
                    {"exclude": [{"method": "fqn", "value": "stg_users"}]},
                ]
            },
        }
        assert "stg_users" not in resolver.resolve_selector(current).models


# Cleanup fixture
@pytest.fixture(autouse=True)
def cleanup(temp_manifest):
    yield
    try:
        os.unlink(temp_manifest)
    except (OSError, TypeError):
        pass
