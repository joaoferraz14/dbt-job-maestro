"""Tests for GraphBuilder."""

import pytest
from dbt_job_maestro.graph_builder import GraphBuilder


@pytest.fixture
def sample_models():
    """Sample model data for testing."""
    return {
        "model_a": {
            "name": "model_a",
            "path": "staging/model_a.sql",
            "original_file_path": "models/staging/model_a.sql",
            "tags": ["staging", "daily"],
            "dependencies": [],
            "sources": ["source.raw.users"],
        },
        "model_b": {
            "name": "model_b",
            "path": "staging/model_b.sql",
            "original_file_path": "models/staging/model_b.sql",
            "tags": ["staging"],
            "dependencies": ["model_a"],
            "sources": [],
        },
        "model_c": {
            "name": "model_c",
            "path": "marts/model_c.sql",
            "original_file_path": "models/marts/model_c.sql",
            "tags": ["marts", "daily"],
            "dependencies": ["model_b"],
            "sources": [],
        },
        "model_d": {
            "name": "model_d",
            "path": "marts/model_d.sql",
            "original_file_path": "models/marts/model_d.sql",
            "tags": ["marts", "weekly"],
            "dependencies": [],
            "sources": [],
        },
        "model_e": {
            "name": "model_e",
            "path": "staging/legacy/model_e.sql",
            "original_file_path": "models/staging/legacy/model_e.sql",
            "tags": ["staging", "legacy"],
            "dependencies": [],
            "sources": [],
        },
    }


@pytest.fixture
def graph(sample_models):
    """Create a GraphBuilder instance."""
    return GraphBuilder(sample_models)


class TestGraphBuilderBasic:
    """Test basic GraphBuilder functionality."""

    def test_initialization(self, graph, sample_models):
        """Test GraphBuilder initialization."""
        assert graph.models == sample_models
        assert graph.graph is not None

    def test_graph_structure(self, graph):
        """Test that graph is built correctly."""
        # model_a should be connected to model_b
        assert "model_b" in graph.graph.get("model_a", set())
        # model_b should be connected to model_a and model_c
        assert "model_a" in graph.graph.get("model_b", set())
        assert "model_c" in graph.graph.get("model_b", set())


class TestConnectedComponents:
    """Test connected component finding."""

    def test_find_connected_components(self, graph):
        """Test finding connected components."""
        components = graph.find_connected_components()

        # Should have at least 2 components:
        # 1. model_a -> model_b -> model_c (connected)
        # 2. model_d (independent)
        # 3. model_e (independent)
        assert len(components) >= 2

        # Find the main component (a, b, c)
        main_component = None
        for comp in components:
            if "model_a" in comp:
                main_component = comp
                break

        assert main_component is not None
        assert "model_a" in main_component
        assert "model_b" in main_component
        assert "model_c" in main_component

    def test_find_connected_components_with_exclusions(self, graph):
        """Test finding components with excluded models."""
        excluded = {"model_b"}
        components = graph.find_connected_components(exclude_models=excluded)

        # model_b should not appear in any component
        for comp in components:
            assert "model_b" not in comp

    def test_find_connected_components_all_excluded(self, graph):
        """Test finding components when all models are excluded."""
        excluded = {"model_a", "model_b", "model_c", "model_d", "model_e"}
        components = graph.find_connected_components(exclude_models=excluded)

        assert len(components) == 0


class TestIndependentModels:
    """Test independent model finding."""

    def test_find_independent_models(self, graph):
        """Test finding independent models."""
        independent = graph.find_independent_models()

        # model_d and model_e have no dependencies and nothing depends on them
        assert "model_d" in independent
        assert "model_e" in independent

        # model_a, model_b, model_c are not independent
        assert "model_a" not in independent
        assert "model_b" not in independent
        assert "model_c" not in independent


class TestGroupByTag:
    """Test grouping by tag."""

    def test_group_by_tag_staging(self, graph):
        """Test grouping models by staging tag."""
        models = graph.group_by_tag("staging")

        assert "model_a" in models
        assert "model_b" in models
        assert "model_e" in models
        assert "model_c" not in models
        assert "model_d" not in models

    def test_group_by_tag_daily(self, graph):
        """Test grouping models by daily tag."""
        models = graph.group_by_tag("daily")

        assert "model_a" in models
        assert "model_c" in models
        assert "model_b" not in models

    def test_group_by_tag_nonexistent(self, graph):
        """Test grouping by non-existent tag."""
        models = graph.group_by_tag("nonexistent")
        assert len(models) == 0


class TestGroupByPath:
    """Test grouping by path."""

    def test_group_by_path_staging(self, graph):
        """Test grouping models by staging path."""
        models = graph.group_by_path("staging")

        assert "model_a" in models
        assert "model_b" in models
        assert "model_e" in models
        assert "model_c" not in models
        assert "model_d" not in models

    def test_group_by_path_marts(self, graph):
        """Test grouping models by marts path."""
        models = graph.group_by_path("marts")

        assert "model_c" in models
        assert "model_d" in models
        assert "model_a" not in models

    def test_group_by_path_with_models_prefix(self, graph):
        """Test grouping by path with models/ prefix."""
        models = graph.group_by_path("models/staging")

        # Should normalize the prefix
        assert "model_a" in models
        assert "model_b" in models

    def test_group_by_path_nested(self, graph):
        """Test grouping by nested path."""
        models = graph.group_by_path("staging/legacy")

        assert "model_e" in models
        assert "model_a" not in models

    def test_group_by_path_nonexistent(self, graph):
        """Test grouping by non-existent path."""
        models = graph.group_by_path("nonexistent/path")
        assert len(models) == 0


class TestGetModelsInPaths:
    """Test get_models_in_paths method (for path exclusion)."""

    def test_get_models_in_single_path(self, graph):
        """Test getting models in a single path."""
        models = graph.get_models_in_paths(["staging"])

        assert "model_a" in models
        assert "model_b" in models
        assert "model_e" in models
        assert "model_c" not in models

    def test_get_models_in_multiple_paths(self, graph):
        """Test getting models in multiple paths."""
        models = graph.get_models_in_paths(["staging", "marts"])

        # Should include all models from both paths
        assert "model_a" in models
        assert "model_b" in models
        assert "model_c" in models
        assert "model_d" in models
        assert "model_e" in models

    def test_get_models_in_nested_path(self, graph):
        """Test getting models in nested path."""
        models = graph.get_models_in_paths(["staging/legacy"])

        assert "model_e" in models
        assert "model_a" not in models

    def test_get_models_in_empty_paths(self, graph):
        """Test getting models with empty path list."""
        models = graph.get_models_in_paths([])
        assert len(models) == 0

    def test_get_models_in_nonexistent_paths(self, graph):
        """Test getting models in non-existent paths."""
        models = graph.get_models_in_paths(["nonexistent"])
        assert len(models) == 0


class TestGetModelsWithTags:
    """Test get_models_with_tags method (for tag exclusion)."""

    def test_get_models_with_single_tag(self, graph):
        """Test getting models with a single tag."""
        models = graph.get_models_with_tags(["staging"])

        assert "model_a" in models
        assert "model_b" in models
        assert "model_e" in models
        assert "model_c" not in models
        assert "model_d" not in models

    def test_get_models_with_multiple_tags(self, graph):
        """Test getting models with multiple tags."""
        models = graph.get_models_with_tags(["staging", "marts"])

        # Should include models from both tags
        assert "model_a" in models
        assert "model_b" in models
        assert "model_c" in models
        assert "model_d" in models
        assert "model_e" in models

    def test_get_models_with_overlapping_tags(self, graph):
        """Test getting models where some have both tags (no duplicates)."""
        models = graph.get_models_with_tags(["staging", "daily"])

        # model_a has both staging and daily, should only appear once
        assert "model_a" in models
        assert "model_c" in models  # has daily
        assert "model_b" in models  # has staging
        # Set ensures no duplicates
        assert len(models) == len(set(models))

    def test_get_models_with_nonexistent_tag(self, graph):
        """Test getting models with non-existent tag."""
        models = graph.get_models_with_tags(["nonexistent"])
        assert len(models) == 0

    def test_get_models_with_empty_tags(self, graph):
        """Test getting models with empty tag list."""
        models = graph.get_models_with_tags([])
        assert len(models) == 0

    def test_get_models_with_mixed_tags(self, graph):
        """Test getting models with mix of existing and non-existing tags."""
        models = graph.get_models_with_tags(["staging", "nonexistent", "legacy"])

        # Should include staging and legacy models, ignore nonexistent
        assert "model_a" in models
        assert "model_b" in models
        assert "model_e" in models  # has both staging and legacy


class TestGetModelsByNames:
    """Test get_models_by_names method (for model exclusion)."""

    def test_get_models_by_single_name(self, graph):
        """Test getting models by single name."""
        models = graph.get_models_by_names(["model_a"])

        assert "model_a" in models
        assert len(models) == 1

    def test_get_models_by_multiple_names(self, graph):
        """Test getting models by multiple names."""
        models = graph.get_models_by_names(["model_a", "model_b", "model_c"])

        assert "model_a" in models
        assert "model_b" in models
        assert "model_c" in models
        assert len(models) == 3

    def test_get_models_by_nonexistent_name(self, graph):
        """Test getting models by non-existent name."""
        models = graph.get_models_by_names(["nonexistent_model"])
        assert len(models) == 0

    def test_get_models_by_mixed_names(self, graph):
        """Test getting models with mix of existing and non-existing names."""
        models = graph.get_models_by_names(["model_a", "nonexistent", "model_b"])

        assert "model_a" in models
        assert "model_b" in models
        assert "nonexistent" not in models
        assert len(models) == 2

    def test_get_models_by_empty_names(self, graph):
        """Test getting models with empty name list."""
        models = graph.get_models_by_names([])
        assert len(models) == 0


class TestModelsWithSources:
    """Test getting models with sources."""

    def test_get_models_with_sources(self, graph):
        """Test getting models that have source dependencies."""
        models = graph.get_models_with_sources()

        assert "model_a" in models
        assert "model_b" not in models
        assert "model_c" not in models

    def test_get_models_with_sources_empty(self):
        """Test when no models have sources."""
        models_without_sources = {
            "model_a": {
                "name": "model_a",
                "path": "staging/model_a.sql",
                "tags": [],
                "dependencies": [],
                "sources": [],
            }
        }
        graph = GraphBuilder(models_without_sources)
        models = graph.get_models_with_sources()

        assert len(models) == 0


class TestComponentSize:
    """Test component size calculation."""

    def test_get_component_size(self, graph):
        """Test getting component size."""
        components = graph.find_connected_components()

        for comp in components:
            size = graph.get_component_size(comp)
            assert size == len(comp)


class TestEdgeCases:
    """Test edge cases."""

    def test_empty_models(self):
        """Test with empty models dict."""
        graph = GraphBuilder({})

        components = graph.find_connected_components()
        assert len(components) == 0

        independent = graph.find_independent_models()
        assert len(independent) == 0

    def test_single_model(self):
        """Test with single model."""
        models = {
            "model_a": {
                "name": "model_a",
                "path": "model_a.sql",
                "tags": ["test"],
                "dependencies": [],
                "sources": [],
            }
        }
        graph = GraphBuilder(models)

        components = graph.find_connected_components()
        assert len(components) == 1
        assert "model_a" in components[0]

        independent = graph.find_independent_models()
        assert "model_a" in independent

    def test_circular_dependency(self):
        """Test with circular dependencies."""
        models = {
            "model_a": {
                "name": "model_a",
                "path": "model_a.sql",
                "tags": [],
                "dependencies": ["model_b"],
                "sources": [],
            },
            "model_b": {
                "name": "model_b",
                "path": "model_b.sql",
                "tags": [],
                "dependencies": ["model_a"],
                "sources": [],
            },
        }
        graph = GraphBuilder(models)

        # Should handle circular deps gracefully
        components = graph.find_connected_components()
        assert len(components) == 1
        assert "model_a" in components[0]
        assert "model_b" in components[0]

    def test_self_dependency(self):
        """Test model with self dependency."""
        models = {
            "model_a": {
                "name": "model_a",
                "path": "model_a.sql",
                "tags": [],
                "dependencies": ["model_a"],  # Self reference
                "sources": [],
            }
        }
        graph = GraphBuilder(models)

        # Should handle self-reference gracefully
        components = graph.find_connected_components()
        assert len(components) == 1

    def test_dependency_on_nonexistent_model(self):
        """Test model depending on non-existent model."""
        models = {
            "model_a": {
                "name": "model_a",
                "path": "model_a.sql",
                "tags": [],
                "dependencies": ["nonexistent_model"],
                "sources": [],
            }
        }
        graph = GraphBuilder(models)

        # Should handle missing dependency gracefully
        components = graph.find_connected_components()
        assert len(components) == 1
        assert "model_a" in components[0]
